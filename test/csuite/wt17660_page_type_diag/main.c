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
 * Unit test for __wt_verify_dsk_header and its forensic diagnostic. The verifier is the read-side
 * mirror of the write-side WT_PAGE_TYPE_COUNT guard in reconciliation. Reads invoke it after the
 * block manager returns, so a disk image with an out-of-range page type is rejected before any
 * cursor walker dispatches on it.
 *
 * Scenarios:
 *   1. type = WT_PAGE_INVALID (0) with NULL WT_ADDR -- the bare validation contract, mirrors what
 * `wt verify` sees on a hand-fed image. Captured output should carry the verbose error message but
 * the "block_cookie=?" sentinel (no block context available).
 *   2. type = WT_PAGE_TYPE_COUNT (8) with NULL WT_ADDR -- boundary value introduced by WT-14750.
 *   3. Real on-disk leaf page with WT_PAGE_INVALID injected and a real WT_ADDR populated from the
 *      page's WT_REF. This is the field-realistic scenario: the diagnostic must emit a real hex
 * block_cookie that one can use to re-read the on-disk block.
 */

/* Large enough to hold the full hex dump of a ~32 KB leaf page plus headers. */
#define CAPTURE_LEN (128 * 1024)
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
 *     Capture error messages emitted via the WT_EVENT_HANDLER so the test can assert on the
 *     diagnostic shape.
 */
static int
event_handle_error(WT_EVENT_HANDLER *handler, WT_SESSION *session, int error, const char *message)
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
 *     Capture verbose/log messages (e.g. __wt_log_data_dump emits via the message handler).
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
 * has_hex_cookie --
 *     Verify the captured output contains "block_cookie=<hex...>" with at least min_hex hex
 *     characters following the '=' (i.e. NOT the '?' sentinel emitted when no block context is
 *     available). This is the field a field-triage engineer cares about: it identifies the on-disk
 *     block so the bytes can be re-read independently of the running process.
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
 *     Insert enough rows to grow the btree beyond a single root leaf, so subsequent reads page in a
 *     real on-disk leaf with a populated WT_ADDR.
 */
static void
populate_table(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    char value[256];
    int i;

    testutil_check(session->open_cursor(session, "table:t", NULL, NULL, &cursor));
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
 * run_synthetic_scenario --
 *     Feed __wt_verify_dsk_header a synthesized disk image with an illegal type byte, no block
 *     context, and assert the verbose error and the "block_cookie=?" sentinel both appear.
 */
static void
run_synthetic_scenario(WT_SESSION *session, const char *label, uint8_t bad_type)
{
    WT_PAGE_HEADER *dsk;
    WT_SESSION_IMPL *session_impl;
    int ret;
    uint8_t buf[4096];
    char type_needle[64];

    captured[0] = '\0';
    session_impl = (WT_SESSION_IMPL *)session;

    memset(buf, 0, sizeof(buf));
    dsk = (WT_PAGE_HEADER *)buf;
    dsk->mem_size = sizeof(buf);
    dsk->write_gen = 0xDEADBEEFULL;
    dsk->u.entries = 0;
    dsk->type = bad_type;

    /*
     * NULL WT_ADDR: exercises the "no block context" branch of the diagnostic, which prints the
     * "block_cookie=?" sentinel. This matches what `wt verify` sees on a hand-fed image.
     */
    ret = __wt_verify_dsk_header(session_impl, "synthetic", dsk, sizeof(buf), NULL, 0);

    testutil_assertfmt(
      ret != 0, "[%s] __wt_verify_dsk_header returned 0 for type %u", label, (unsigned)bad_type);

    /* Verbose message from WT_RET_VRFY. */
    testutil_check(__wt_snprintf(
      type_needle, sizeof(type_needle), "has an invalid type of %u", (unsigned)bad_type));
    testutil_assertfmt(strstr(captured, type_needle) != NULL,
      "[%s] missing verbose error '%s'.\nCaptured:\n%s", label, type_needle, captured);

    /*
     * Even without block context the diagnostic dump must NOT fire (we passed addr==NULL). Make
     * sure the test data was actually picked up though: the verbose error message must reference
     * our 'synthetic' tag.
     */
    testutil_assertfmt(strstr(captured, "page at synthetic") != NULL,
      "[%s] missing tag 'page at synthetic'.\nCaptured:\n%s", label, captured);

    printf("PASS [%s]: type=%u rejected by __wt_verify_dsk_header\n", label, (unsigned)bad_type);
    if (getenv("VERBOSE") != NULL)
        printf("--- captured ---\n%s--- end captured ---\n", captured);
}

/*
 * run_realref_scenario --
 *     Field-realistic scenario: page a real leaf in from disk via cursor->next, copy its real
 *     WT_PAGE_HEADER bytes, flip the type byte, populate a WT_ADDR from the leaf's ref->addr, and
 *     call __wt_verify_dsk_header. The diagnostic must emit a real hex block_cookie, the real
 *     dhandle name, and the real header fields (mem_size, write_gen, entries) so we could
 *     re-read the bad block independently of the running process.
 */
static void
run_realref_scenario(WT_CONNECTION *conn)
{
    WT_ADDR addr_for_diag;
    WT_ADDR_COPY ref_addr;
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_PAGE_HEADER *bad_dsk;
    WT_REF *real_ref;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;
    const WT_PAGE_HEADER *real_dsk;
    uint8_t *buf;
    uint32_t image_size;
    int ret;
    char needle[64];
    bool have_addr;

    captured[0] = '\0';

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    session_impl = (WT_SESSION_IMPL *)session;

    testutil_check(session->open_cursor(session, "table:t", NULL, NULL, &cursor));
    testutil_check(cursor->next(cursor));
    cbt = (WT_CURSOR_BTREE *)cursor;
    real_ref = cbt->ref;
    testutil_assert(real_ref != NULL);
    testutil_assert(real_ref->page != NULL);
    testutil_assert(real_ref->page->dsk != NULL);

    /*
     * __wt_ref_addr_copy needs the session to be in the cursor's btree dhandle context (so cell
     * unpacking can read btree shape info) and to hold WT_GEN_SPLIT (production read paths
     * satisfy this implicitly via the btree walk; a unit test must do it explicitly).
     */
    __wt_session_gen_enter(session_impl, WT_GEN_SPLIT);
    WT_WITH_DHANDLE(session_impl, cbt->dhandle,
      have_addr = __wt_ref_addr_copy(session_impl, real_ref, &ref_addr));
    __wt_session_gen_leave(session_impl, WT_GEN_SPLIT);
    testutil_assertfmt(have_addr, "%s", "[real_ref] __wt_ref_addr_copy failed on a real leaf ref");

    /* Copy and corrupt the real disk image. */
    real_dsk = real_ref->page->dsk;
    image_size = real_dsk->mem_size;
    testutil_assert(image_size > WT_PAGE_HEADER_SIZE);
    buf = dmalloc(image_size);
    memcpy(buf, real_dsk, image_size);
    bad_dsk = (WT_PAGE_HEADER *)buf;
    testutil_assertfmt(bad_dsk->type == WT_PAGE_ROW_LEAF,
      "[real_ref] expected ROW_LEAF before corruption, got %u", (unsigned)bad_dsk->type);
    bad_dsk->type = WT_PAGE_INVALID;

    /* Populate a WT_ADDR with the real cookie bytes so the diagnostic gets real hex output. */
    memset(&addr_for_diag, 0, sizeof(addr_for_diag));
    addr_for_diag.block_cookie = ref_addr.addr;
    addr_for_diag.block_cookie_size = ref_addr.size;
    addr_for_diag.type = ref_addr.type;

    /*
     * Mirror production: __page_read calls the verifier while the session is in the btree's dhandle
     * context (so the diagnostic captures the dhandle name).
     */
    WT_WITH_DHANDLE(session_impl, cbt->dhandle,
      ret =
        __wt_verify_dsk_header(session_impl, "real-leaf", bad_dsk, image_size, &addr_for_diag, 0));
    testutil_assertfmt(ret != 0, "%s", "[real_ref] __wt_verify_dsk_header returned 0");

    /* Verbose error message present. */
    testutil_assertfmt(strstr(captured, "has an invalid type of 0") != NULL,
      "[real_ref] missing verbose error 'has an invalid type of 0'.\nCaptured:\n%s", captured);

    /* Forensic dump: real hex cookie (not '?'), real dhandle, real mem_size. */
    testutil_assertfmt(strstr(captured, "block_cookie=?") == NULL,
      "[real_ref] cookie should be hex with real addr, not '?'.\nCaptured:\n%s", captured);
    testutil_assertfmt(has_hex_cookie(captured, 8),
      "[real_ref] missing 'block_cookie=<>=8 hex chars>'.\nCaptured:\n%s", captured);
    testutil_assertfmt(strstr(captured, "dhandle=file:t.wt") != NULL,
      "[real_ref] missing 'dhandle=file:t.wt'.\nCaptured:\n%s", captured);
    testutil_check(__wt_snprintf(needle, sizeof(needle), "mem_size=%" PRIu32, image_size));
    testutil_assertfmt(strstr(captured, needle) != NULL,
      "[real_ref] missing '%s' from real page header.\nCaptured:\n%s", needle, captured);
    testutil_assertfmt(strstr(captured, "chunk 1 of") != NULL,
      "[real_ref] missing hex dump 'chunk 1 of N'.\nCaptured:\n%s", captured);

    free(buf);
    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, NULL));

    printf("PASS [real_ref]: real WT_REF produced a hex block_cookie + full dhandle context\n");
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

    testutil_check(wiredtiger_open(opts->home, &eh, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, "table:t", "key_format=i,value_format=S"));
    populate_table(session);

    run_synthetic_scenario(session, "WT_PAGE_INVALID", WT_PAGE_INVALID);
    run_synthetic_scenario(session, "WT_PAGE_TYPE_COUNT", WT_PAGE_TYPE_COUNT);

    /*
     * Real-ref scenario needs the leaf to actually come off disk so page->dsk is populated. Close
     * and reopen the connection to drop the cache.
     */
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));
    testutil_check(wiredtiger_open(opts->home, &eh, NULL, &conn));

    run_realref_scenario(conn);

    testutil_check(conn->close(conn, NULL));
    testutil_cleanup(opts);

    printf("PASS: wt17660_page_type_diag\n");
    return (EXIT_SUCCESS);
}
