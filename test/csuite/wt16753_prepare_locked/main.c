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
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * Regression test: scan with ignore_prepare blocks on WT_PREPARE_LOCKED update.
 *
 * __wt_txn_upd_visible_type() spins unconditionally on WT_PREPARE_LOCKED state. The timing stress
 * flag prepare_locked_delay widens this transient window to 5 seconds so a concurrent reader can
 * reliably observe it.
 *
 * Thread 1 (committer):
 *   Prepares a transaction, then commits it. The timing stress makes the commit sleep 5 seconds
 *   while each update is in WT_PREPARE_LOCKED state.
 *
 * Thread 2 (reader):
 *   Scans with ignore_prepare=true. With the fix, the scan skips the LOCKED update immediately
 *   and completes in well under 5 seconds. Without the fix, the scan spins for the full 5 seconds
 *   (or forever under ASAN).
 */

#include "test_util.h"

#define URI "table:wt16753"
#define COMMITTED_VALUE "committed"
#define PREPARED_VALUE "prepared"
#define SCAN_TIMEOUT_SEC 3

static WT_CONNECTION *conn;

/* Shared flag: set by the committer after reconfigure and before commit_transaction. */
static volatile bool commit_starting;

/*
 * committer_thread --
 *     Prepare and commit a transaction. The timing stress flag is enabled just before commit so
 *     only this commit_transaction call hits the 5-second sleep in the LOCKED window.
 */
static WT_THREAD_RET
committer_thread(void *arg)
{
    WT_CURSOR *cursor;
    WT_SESSION *session;

    (void)arg;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->begin_transaction(session, NULL));
    testutil_check(session->open_cursor(session, URI, NULL, NULL, &cursor));
    cursor->set_key(cursor, 1);
    cursor->set_value(cursor, PREPARED_VALUE);
    testutil_check(cursor->insert(cursor));
    testutil_check(cursor->close(cursor));
    testutil_check(session->prepare_transaction(session, "prepare_timestamp=20"));

    /* Enable stress flag just before commit so checkpoint/other ops are not affected. */
    testutil_check(conn->reconfigure(conn, "timing_stress_for_test=[prepare_locked_delay]"));

    /* Signal the reader that we are about to enter the LOCKED window. */
    commit_starting = true;

    testutil_check(
      session->commit_transaction(session, "commit_timestamp=25,durable_timestamp=25"));

    /* Disable stress flag after commit. */
    testutil_check(conn->reconfigure(conn, "timing_stress_for_test=[]"));

    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*
 * reader_thread --
 *     Scan with ignore_prepare=true and read_timestamp=15. The LOCKED prepared update should be
 *     skipped immediately (fix present) or block for 5+ seconds (fix missing).
 */
static WT_THREAD_RET
reader_thread(void *arg)
{
    struct timespec start, end;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    double elapsed;
    int ret;
    const char *val;
    bool *passed = (bool *)arg;

    /* Wait until the committer is about to enter commit_transaction. */
    while (!commit_starting)
        __wt_yield();
    /* Brief additional delay so the commit enters the LOCKED sleep. */
    __wt_sleep(0, 100000); /* 100ms */

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->begin_transaction(session, "ignore_prepare=true,read_timestamp=15"));
    testutil_check(session->open_cursor(session, URI, NULL, NULL, &cursor));

    __wt_epoch(NULL, &start);
    cursor->set_key(cursor, 1);
    ret = cursor->search(cursor);
    __wt_epoch(NULL, &end);

    elapsed = WT_TIMEDIFF_SEC(end, start);

    if (ret == 0) {
        testutil_check(cursor->get_value(cursor, &val));
        printf("reader: search returned \"%s\" in %.2f seconds\n", val, elapsed);

        if (strcmp(val, COMMITTED_VALUE) != 0) {
            fprintf(stderr, "FAIL: expected \"%s\" but got \"%s\"\n", COMMITTED_VALUE, val);
            *passed = false;
        } else if (elapsed > SCAN_TIMEOUT_SEC) {
            fprintf(stderr,
              "FAIL: search took %.1fs (> %ds)  likely blocked on WT_PREPARE_LOCKED\n", elapsed,
              SCAN_TIMEOUT_SEC);
            *passed = false;
        } else {
            printf("PASS: search completed in %.2fs with correct value\n", elapsed);
            *passed = true;
        }
    } else if (ret == WT_NOTFOUND) {
        printf("reader: search returned WT_NOTFOUND in %.2f seconds\n", elapsed);
        fprintf(stderr, "FAIL: WT_NOTFOUND  baseline committed value not visible\n");
        *passed = false;
    } else {
        printf("reader: search returned %d in %.2f seconds\n", ret, elapsed);
        fprintf(stderr, "FAIL: unexpected return code %d\n", ret);
        *passed = false;
    }

    testutil_check(cursor->close(cursor));
    testutil_check(session->rollback_transaction(session, NULL));
    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*
 * main --
 *     Test that scan with ignore_prepare skips WT_PREPARE_LOCKED updates without blocking.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    wt_thread_t commit_tid, reader_tid;
    bool passed;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    /* Open connection without stress flag  it will be enabled per-commit. */
    testutil_check(wiredtiger_open(opts->home, NULL, "create,statistics=(all)", &conn));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, URI, "key_format=i,value_format=S"));
    testutil_check(conn->set_timestamp(conn, "oldest_timestamp=1,stable_timestamp=1"));

    /* Insert and commit baseline value at timestamp 10. */
    testutil_check(session->begin_transaction(session, NULL));
    testutil_check(session->open_cursor(session, URI, NULL, NULL, &cursor));
    cursor->set_key(cursor, 1);
    cursor->set_value(cursor, COMMITTED_VALUE);
    testutil_check(cursor->insert(cursor));
    testutil_check(cursor->close(cursor));
    testutil_check(session->commit_transaction(session, "commit_timestamp=10"));

    /* Checkpoint so the baseline is on disk. */
    testutil_check(conn->set_timestamp(conn, "stable_timestamp=10"));
    testutil_check(session->checkpoint(session, NULL));
    testutil_check(session->close(session, NULL));

    /* Run the test. */
    commit_starting = false;
    passed = false;

    testutil_check(__wt_thread_create(NULL, &commit_tid, committer_thread, NULL));
    testutil_check(__wt_thread_create(NULL, &reader_tid, reader_thread, &passed));

    testutil_check(__wt_thread_join(NULL, &reader_tid));
    testutil_check(__wt_thread_join(NULL, &commit_tid));

    testutil_check(conn->close(conn, NULL));
    testutil_assert(passed);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
