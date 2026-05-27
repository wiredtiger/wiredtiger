/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 */
#include "test_util.h"
#include "wt_internal.h"

/*
 * JIRA ticket reference: WT-17660
 *
 * Unit test for the read-side illegal-page-type guard added by WT-17660 in
 * __wti_page_inmem (src/btree/bt_page.c). The test synthesizes a disk image
 * whose WT_PAGE_HEADER.type byte is out of the valid range and calls
 * __wti_page_inmem directly, then asserts that:
 *
 *   - the call returns a non-zero error (illegal value);
 *   - the diagnostic message captured via the WT_EVENT_HANDLER carries the
 *     fields the guard is meant to emit: "illegal page type", numeric type,
 *     dhandle name, and the "block_cookie=?" sentinel for the NULL-WT_REF
 *     code path.
 *
 * Scenarios:
 *   1. type = WT_PAGE_INVALID (0)
 *   2. type = WT_PAGE_TYPE_COUNT (8, the first beyond-range value)
 *
 * Both should be rejected by the early __wt_page_type_valid() check that
 * mirrors WT-14750's write-side __rec_write guard.
 */

#define CAPTURE_LEN (16 * 1024)
static char captured[CAPTURE_LEN];

/*
 * append_captured --
 *     Append a message to the captured buffer, truncating on overflow.
 */
static void
append_captured(const char *msg)
{
    size_t cur, left, mlen;

    cur = strlen(captured);
    if (cur >= sizeof(captured) - 1)
        return;
    left = sizeof(captured) - 1 - cur;
    mlen = strlen(msg);
    if (mlen > left)
        mlen = left;
    memcpy(captured + cur, msg, mlen);
    captured[cur + mlen] = '\0';
}

/*
 * event_handle_error --
 *     Capture error messages into our buffer (and forward to stderr for
 *     debugging when the test fails).
 */
static int
event_handle_error(
  WT_EVENT_HANDLER *handler, WT_SESSION *session, int error, const char *message)
{
    (void)handler;
    (void)session;
    (void)error;
    append_captured(message);
    append_captured("\n");
    return (0);
}

/*
 * event_handle_message --
 *     Capture verbose/log messages (e.g. __wt_log_data_dump emits via the
 *     message handler).
 */
static int
event_handle_message(WT_EVENT_HANDLER *handler, WT_SESSION *session, const char *message)
{
    (void)handler;
    (void)session;
    append_captured(message);
    append_captured("\n");
    return (0);
}

/*
 * fabricate_and_call --
 *     Build a synthetic disk image with the given page type, invoke __wti_page_inmem under the
 *     provided dhandle, and return the result.
 */
static int
fabricate_and_call(WT_SESSION *session, WT_DATA_HANDLE *dhandle, uint8_t bad_type, WT_PAGE **pagep)
{
    WT_PAGE_HEADER *dsk;
    WT_SESSION_IMPL *session_impl;
    int ret;
    uint8_t buf[4096];

    session_impl = (WT_SESSION_IMPL *)session;

    /*
     * The buffer must be zeroed: __wti_page_inmem reads several WT_PAGE_HEADER fields up front.
     * Only set what we need; everything else stays at zero.
     */
    memset(buf, 0, sizeof(buf));
    dsk = (WT_PAGE_HEADER *)buf;
    dsk->mem_size = sizeof(buf);
    dsk->write_gen = 0xDEADBEEFULL;
    dsk->u.entries = 0;
    dsk->type = bad_type;

    *pagep = NULL;
    /*
     * NULL WT_REF: exercises the "no addr cookie available" branch of the diagnostic, which should
     * print "block_cookie=?". A real read path always has a non-NULL ref, but the helper must cope
     * with NULL defensively because it's called from a panic-adjacent path.
     */
    WT_WITH_DHANDLE(
      session_impl, dhandle, ret = __wti_page_inmem(session_impl, NULL, buf, 0, pagep, NULL));
    return (ret);
}

/*
 * run_scenario --
 *     Drive one (type, label) scenario.
 */
static void
run_scenario(WT_SESSION *session, const char *label, uint8_t bad_type)
{
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle;
    WT_PAGE *page;
    int ret;
    char type_needle[64];

    captured[0] = '\0';

    /*
     * Clear any prior panic state up front so the cursor open below is not rejected by
     * WT_SESSION_CHECK_PANIC. Also set the data-corruption flag so __wt_panic_func will return
     * WT_PANIC instead of calling __wt_abort -- the conn was opened with
     * debug_mode=(corruption_abort=false), which clears WT_CONN_DEBUG_CORRUPTION_ABORT.
     */
    F_SET_ATOMIC_32(S2C((WT_SESSION_IMPL *)session), WT_CONN_DATA_CORRUPTION);
    F_CLR_ATOMIC_32(S2C((WT_SESSION_IMPL *)session), WT_CONN_PANIC);

    /*
     * Open a cursor briefly just to acquire a real dhandle, then close it so we don't leave a
     * cursor open across the injected panic (a cursor close after WT_CONN_PANIC fails).
     */
    testutil_check(session->open_cursor(session, "table:t", NULL, NULL, &cursor));
    dhandle = ((WT_CURSOR_BTREE *)cursor)->dhandle;
    testutil_check(cursor->close(cursor));

    ret = fabricate_and_call(session, dhandle, bad_type, &page);

    /* The new guard must reject the image. */
    testutil_assertfmt(ret != 0, "[%s] __wti_page_inmem returned 0 for type %u; expected an error",
      label, (unsigned)bad_type);
    testutil_assertfmt(
      page == NULL, "[%s] __wti_page_inmem produced a page despite returning an error", label);

    /* The diagnostic helper must emit the expected fields. */
    testutil_assertfmt(strstr(captured, "illegal page type") != NULL,
      "[%s] captured output missing 'illegal page type'.\nCaptured:\n%s", label, captured);

    testutil_check(__wt_snprintf(type_needle, sizeof(type_needle), "header type=%u",
      (unsigned)bad_type));
    testutil_assertfmt(strstr(captured, type_needle) != NULL,
      "[%s] captured output missing '%s'.\nCaptured:\n%s", label, type_needle, captured);

    /* NULL WT_REF path must produce the '?' cookie sentinel. */
    testutil_assertfmt(strstr(captured, "block_cookie=?") != NULL,
      "[%s] captured output missing 'block_cookie=?' (NULL ref path).\nCaptured:\n%s", label,
      captured);

    /* The dhandle name must appear so an SRE can identify the file. */
    testutil_assertfmt(strstr(captured, "dhandle=file:t.wt") != NULL ||
        strstr(captured, "dhandle=table:t") != NULL,
      "[%s] captured output missing dhandle name.\nCaptured:\n%s", label, captured);

    /* mem_size and write_gen should appear too (forensic fields). */
    testutil_assertfmt(strstr(captured, "write_gen=3735928559") != NULL,
      "[%s] captured output missing 'write_gen=3735928559' (0xDEADBEEF).\nCaptured:\n%s", label,
      captured);

    printf("PASS [%s]: type=%u captured the expected diagnostic\n", label, (unsigned)bad_type);
    if (getenv("VERBOSE") != NULL)
        printf("--- captured ---\n%s--- end captured ---\n", captured);
}

/*
 * has_hex_cookie --
 *     Verify the captured output contains "block_cookie=<hex...>" with at least min_hex hex
 *     characters following the '=' (i.e. NOT the '?' sentinel emitted on the NULL-WT_REF path).
 *     This is the field a field-triage engineer cares about: it identifies the on-disk block so
 *     the bytes can be re-read independently of the running process.
 */
static bool
has_hex_cookie(const char *buf, size_t min_hex)
{
    const char *p, *q;
    size_t n;

    p = strstr(buf, "block_cookie=");
    if (p == NULL)
        return (false);
    q = p + strlen("block_cookie=");
    /* Reject the '?' sentinel explicitly. */
    if (*q == '?')
        return (false);
    n = 0;
    while (*q != '\0' && ((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f'))) {
        ++q;
        ++n;
    }
    return (n >= min_hex);
}

/*
 * populate_table --
 *     Insert enough rows to grow the btree beyond a single root leaf, so subsequent reads exercise
 *     real on-disk leaf pages with valid block address cookies.
 */
static void
populate_table(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    char value[256];
    int i;

    testutil_check(session->open_cursor(session, "table:t", NULL, NULL, &cursor));
    /* ~2000 rows * 256B values comfortably exceeds the default leaf page size. */
    memset(value, 'x', sizeof(value) - 1);
    value[sizeof(value) - 1] = '\0';
    for (i = 0; i < 2000; i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, value);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_check(session->checkpoint(session, NULL));
}

/*
 * run_realref_scenario --
 *     Field-realistic scenario: position a cursor on a real on-disk leaf page so we have a valid
 *     WT_REF with a populated ref->addr, copy that page's actual disk image, flip the type byte in
 *     the copy, and call __wti_page_inmem with (real_ref, corrupted_copy). This exercises the
 *     production diagnostic path that an engineer triaging a real panic would see: a real hex
 *     block cookie, the real dhandle name, and a hex dump of mostly-real bytes with the bad type
 *     byte highlighted.
 */
static void
run_realref_scenario(WT_CONNECTION *conn)
{
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_PAGE *page;
    WT_PAGE_HEADER *bad_dsk;
    WT_REF *real_ref;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;
    const WT_PAGE_HEADER *real_dsk;
    uint8_t *buf;
    uint32_t image_size;
    int ret;

    captured[0] = '\0';

    /*
     * Clear panic state and arm the corruption flag so __wti_page_inmem's call to
     * __wt_illegal_value -> __wt_panic_func returns WT_PANIC instead of aborting.
     */
    F_SET_ATOMIC_32((WT_CONNECTION_IMPL *)conn, WT_CONN_DATA_CORRUPTION);
    F_CLR_ATOMIC_32((WT_CONNECTION_IMPL *)conn, WT_CONN_PANIC);

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    session_impl = (WT_SESSION_IMPL *)session;

    /*
     * Position the cursor on the first record. This loads a real leaf page; cbt->ref is the
     * WT_REF for that page, and cbt->ref->addr holds the block address cookie WT used to fetch
     * it from disk.
     */
    testutil_check(session->open_cursor(session, "table:t", NULL, NULL, &cursor));
    testutil_check(cursor->next(cursor));
    cbt = (WT_CURSOR_BTREE *)cursor;
    real_ref = cbt->ref;
    testutil_assert(real_ref != NULL);
    testutil_assert(real_ref->page != NULL);
    testutil_assert(real_ref->page->dsk != NULL);

    /*
     * Copy the real disk image so we can corrupt it without disturbing WT's in-memory page. The
     * size is whatever the page header recorded when it was written.
     */
    real_dsk = real_ref->page->dsk;
    image_size = real_dsk->mem_size;
    testutil_assert(image_size > WT_PAGE_HEADER_SIZE);
    buf = dmalloc(image_size);
    memcpy(buf, real_dsk, image_size);
    bad_dsk = (WT_PAGE_HEADER *)buf;
    /*
     * Sanity: the real leaf should be a row-store leaf before we corrupt it. If this assertion
     * ever fires, the test setup has changed and our 'flip exactly one byte' premise no longer
     * matches reality.
     */
    testutil_assertfmt(bad_dsk->type == WT_PAGE_ROW_LEAF,
      "expected ROW_LEAF before corruption, got type %u", (unsigned)bad_dsk->type);
    bad_dsk->type = WT_PAGE_INVALID;

    /*
     * Invoke __wti_page_inmem with (real_ref, corrupted copy). The new early-out check should
     * reject the image, and the diagnostic helper should:
     *   - call __wt_ref_addr_copy(ref) successfully and emit a hex block_cookie= value;
     *   - log the real dhandle name 'file:t.wt';
     *   - emit a hex dump of the corrupted copy.
     */
    /*
     * __wt_ref_addr_copy (called from the new diagnostic helper) asserts that the caller holds a
     * valid WT_GEN_SPLIT generation: in production this is satisfied because the read path is
     * already inside a btree walk. We enter it explicitly here so the assertion passes.
     */
    __wt_session_gen_enter(session_impl, WT_GEN_SPLIT);
    page = NULL;
    WT_WITH_DHANDLE(session_impl, cbt->dhandle,
      ret = __wti_page_inmem(session_impl, real_ref, buf, 0, &page, NULL));
    __wt_session_gen_leave(session_impl, WT_GEN_SPLIT);

    testutil_assertfmt(ret != 0, "%s", "[real_ref] __wti_page_inmem returned 0; expected an error");
    testutil_assertfmt(
      page == NULL, "%s", "[real_ref] __wti_page_inmem produced a page on error");

    /* Diagnostic: same fields as the NULL-ref scenarios, *plus* a real hex cookie. */
    testutil_assertfmt(strstr(captured, "illegal page type 0 (WT_PAGE_INVALID)") != NULL,
      "[real_ref] missing 'illegal page type 0 (WT_PAGE_INVALID)'.\nCaptured:\n%s", captured);
    testutil_assertfmt(strstr(captured, "dhandle=file:t.wt") != NULL,
      "[real_ref] missing 'dhandle=file:t.wt'.\nCaptured:\n%s", captured);
    testutil_assertfmt(strstr(captured, "block_cookie=?") == NULL,
      "[real_ref] block_cookie should be hex, not '?': real WT_REF has a populated "
      "ref->addr.\nCaptured:\n%s",
      captured);
    /* Minimum sane cookie: 8 hex chars (~4 bytes). Real cookies are usually larger. */
    testutil_assertfmt(has_hex_cookie(captured, 8),
      "[real_ref] missing hex 'block_cookie=<hex>...' (>=8 hex chars).\nCaptured:\n%s", captured);
    /* mem_size must match what we copied from the real on-disk page (not 0xDEADBEEF). */
    {
        char needle[64];
        testutil_check(
          __wt_snprintf(needle, sizeof(needle), "mem_size=%" PRIu32, image_size));
        testutil_assertfmt(strstr(captured, needle) != NULL,
          "[real_ref] missing '%s' from real page header.\nCaptured:\n%s", needle, captured);
    }
    /* Hex dump must include the first chunk of the corrupted bytes. */
    testutil_assertfmt(strstr(captured, "chunk 1 of") != NULL,
      "[real_ref] missing hex dump 'chunk 1 of N'.\nCaptured:\n%s", captured);

    free(buf);
    /*
     * The conn is now panicked. Cursor close after WT_CONN_PANIC fails, so just leak the cursor
     * deliberately -- the process is about to exit. Future scenarios in run_scenario() reset
     * WT_CONN_PANIC before doing anything cursor-related, but we keep this scenario last.
     */
    (void)cursor;

    printf("PASS [real_ref]: real WT_REF produced a hex block_cookie and full dhandle context\n");
    if (getenv("VERBOSE") != NULL)
        printf("--- captured ---\n%s--- end captured ---\n", captured);
}

int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CONNECTION *conn;
    WT_EVENT_HANDLER eh;
    WT_SESSION *session;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    memset(&eh, 0, sizeof(eh));
    eh.handle_error = event_handle_error;
    eh.handle_message = event_handle_message;

    /*
     * debug_mode=(corruption_abort=false) clears WT_CONN_DEBUG_CORRUPTION_ABORT, so __wt_panic_func
     * will return WT_PANIC rather than calling __wt_abort -- provided WT_CONN_DATA_CORRUPTION is
     * set on the connection. We set that per-scenario.
     */
    testutil_check(
      wiredtiger_open(opts->home, &eh, "create,debug_mode=(corruption_abort=false)", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* A trivial table gives us a real WT_DATA_HANDLE / WT_BTREE to point at. */
    testutil_check(session->create(session, "table:t", "key_format=i,value_format=S"));
    /* Populate so the real-ref scenario can position a cursor on a real on-disk leaf page. */
    populate_table(session);

    run_scenario(session, "WT_PAGE_INVALID", WT_PAGE_INVALID);
    run_scenario(session, "WT_PAGE_TYPE_COUNT", WT_PAGE_TYPE_COUNT);

    /*
     * The real-ref scenario needs to read a page off disk so page->dsk is populated. Close and
     * reopen the connection to drop all pages from cache; the next cursor walk will then page in
     * from disk. The conn is panicked at this point from earlier scenarios -- close ignores
     * errors, the on-disk btree was checkpointed by populate_table before any panic state.
     */
    (void)conn->close(conn, NULL);
    testutil_check(
      wiredtiger_open(opts->home, &eh, "debug_mode=(corruption_abort=false)", &conn));

    /*
     * Run the real-ref (field-realistic) scenario last: it leaves the conn panicked with a cursor
     * pinned, so subsequent API calls would fail.
     */
    run_realref_scenario(conn);

    /*
     * The conn is in a panicked state after our injected illegal-value calls; ignore close errors.
     */
    (void)conn->close(conn, NULL);
    testutil_cleanup(opts);

    printf("PASS: wt17660_page_type_diag\n");
    return (EXIT_SUCCESS);
}
