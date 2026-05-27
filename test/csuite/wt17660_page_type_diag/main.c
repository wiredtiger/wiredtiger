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

    run_scenario(session, "WT_PAGE_INVALID", WT_PAGE_INVALID);
    run_scenario(session, "WT_PAGE_TYPE_COUNT", WT_PAGE_TYPE_COUNT);

    /*
     * The conn is in a panicked state after our injected illegal-value calls; ignore close errors.
     */
    (void)conn->close(conn, NULL);
    testutil_cleanup(opts);

    printf("PASS: wt17660_page_type_diag\n");
    return (EXIT_SUCCESS);
}
