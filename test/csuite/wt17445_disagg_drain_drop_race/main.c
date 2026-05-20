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
#include "test_util.h"

#include <pthread.h>
#include <sys/resource.h>
#include <sys/wait.h>

/*
 * Regression test for a drain worker racing with a concurrent drop of the
 * same layered table during follower -> leader step-up.
 *
 * During step-up, __wti_layered_drain_ingest_tables enqueues open ingest dhandles and begins
 * copying each to stable. A concurrent drop tries to acquire an exclusive dhandle lock. The drain
 * worker holds a read lock on the dhandle for the duration of the copy, which blocks the drop
 * until the copy finishes. If the dhandle is already dead when the drain worker acquires the read
 * lock (drop completed first), the worker skips the table cleanly.
 *
 * Two timing stress flags exercise the two orderings:
 *
 *   drain_ingest_table_slow: 300 ms sleep inside __layered_copy_ingest_table while the read lock
 *     is held. The drop blocks on the exclusive lock and returns EBUSY; the retry after step-up
 *     completes succeeds. Drain wins the lock.
 *
 *   drain_ingest_table_pre_lock_slow: 300 ms sleep in __layered_drain_worker_run before acquiring
 *     the read lock. The drop wins the exclusive lock, sets WT_DHANDLE_DEAD, and returns 0.
 *     Drain then acquires the read lock, sees DEAD, and skips cleanly. Drop wins the lock.
 *
 * The race is triggered by:
 *   1. Opening as a follower with the timing stress enabled.
 *   2. Reconfiguring to leader (step-up) on a background thread.
 *   3. Spinning on layered_drain_data.running (accessible via wt_internal.h, included through
 *      test_util.h) to detect when the drain worker has started.
 *   4. Calling session->drop(force=true, checkpoint_wait=false) to race against the drain worker.
 *
 * A forked child runs each scenario so that a WT_PANIC / SIGABRT in the buggy code path is caught
 * as a child signal and reported as a test failure without killing the test runner.
 */

/*
 * Command-line flags for testutil_parse_single_opt.
 *   b: build directory override
 *   h: home directory for the WiredTiger database
 *   p: preserve the home directory after the test completes
 */
#define GETOPTS "b:h:p"

#define TABLE_NAME "test17445"
#define TABLE_URI "layered:" TABLE_NAME
#define NUM_ROWS 100

/*
 * File-scope so heap strings in opts stay reachable across the child's _exit and don't trip LSAN.
 */
static TEST_OPTS _opts;

/*
 * Thread argument for the concurrent step-up.
 */
typedef struct {
    WT_CONNECTION *conn;
    int result;
} STEPUP_ARG;

/*
 * stepup_thread --
 *     Reconfigure from follower to leader in a background thread.
 */
static void *
stepup_thread(void *arg)
{
    STEPUP_ARG *a = (STEPUP_ARG *)arg;
    a->result = a->conn->reconfigure(a->conn, "disaggregated=(role=leader)");
    return (NULL);
}

/*
 * wait_for_drain --
 *     Spin until the drain worker sets layered_drain_data.running. The 300 ms stress sleep then
 *     widens the window enough for the drop to race with the drain worker.
 */
static void
wait_for_drain(WT_CONNECTION *conn)
{
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    /*
     * Poll at 1 ms intervals. running=true is set before thread group creation and is cleared when
     * the work queue becomes empty, so the window may be only a few ms. 10 ms polling missed the
     * window on loaded CI machines; 1 ms polling provides 5-20 catches over a typical 5-20 ms
     * window.
     */
    for (int i = 0; i < 5000; ++i) {
        if (__wt_atomic_load_bool_relaxed(&conn_impl->layered_drain_data.running))
            return;
        __wt_sleep(0, WT_THOUSAND);
    }
}

/*
 * sync_leader_checkpoint --
 *     Fetch the leader's last checkpoint metadata from the page log and reconfigure the follower
 *     connection with it, so database_size is correctly initialized before step-up. In production,
 *     a follower tracks the leader's checkpoints continuously; this replicates that before the race
 *     is triggered.
 *
 * Returns true on success or when no checkpoint is available yet, false on error.
 */
static bool
sync_leader_checkpoint(WT_CONNECTION *conn, WT_SESSION *session, const char *follower_config)
{
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    const char *cfg[2] = {follower_config, NULL};
    WT_ITEM checkpoint_meta = {0};
    uint64_t checkpoint_lsn = 0;
    wt_timestamp_t checkpoint_ts = 0;

    int ret = __wti_layered_get_disagg_checkpoint(
      session_impl, cfg, &checkpoint_lsn, &checkpoint_ts, &checkpoint_meta);
    if (ret != 0) {
        __wt_buf_free(session_impl, &checkpoint_meta);
        return (false);
    }
    if (checkpoint_meta.size == 0) {
        __wt_buf_free(session_impl, &checkpoint_meta);
        return (true);
    }

    char reconfig[8192];
    testutil_snprintf(reconfig, sizeof(reconfig), "disaggregated=(checkpoint_meta=\"%.*s\")",
      (int)checkpoint_meta.size, (const char *)checkpoint_meta.data);
    __wt_buf_free(session_impl, &checkpoint_meta);

    return (conn->reconfigure(conn, reconfig) == 0);
}

/*
 * subtest_run --
 *     Run the racy scenario in the current process. Called only from a forked child so a panic
 *     (SIGABRT) doesn't kill the test runner. Calls _exit: never returns.
 *
 * expect_ebusy: true if drain wins the lock (drain_ingest_table_slow), meaning the first drop
 *     should return EBUSY and succeed on retry. False if drop wins
 *     (drain_ingest_table_pre_lock_slow), meaning the first drop should succeed immediately.
 */
static void WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn))
  subtest_run(TEST_OPTS *opts, const char *stress_flag, bool expect_ebusy)
{
    /* No core files; a panic may trigger diagnostic assertions during cleanup. */
    struct rlimit rlim = {0};
    testutil_check(setrlimit(RLIMIT_CORE, &rlim));

    char follower_config[2048];
    testutil_snprintf(follower_config, sizeof(follower_config),
      "statistics=(all),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"],"
      "disaggregated=(role=follower,page_log=palite,drain_threads=2),"
      "timing_stress_for_test=[%s]",
      opts->build_dir, stress_flag);

    WT_CONNECTION *conn;
    if (wiredtiger_open(opts->home, NULL, follower_config, &conn) != 0)
        _exit(EXIT_FAILURE);

    WT_SESSION *session;
    if (conn->open_session(conn, NULL, NULL, &session) != 0) {
        conn->close(conn, NULL);
        _exit(EXIT_FAILURE);
    }

    if (!sync_leader_checkpoint(conn, session, follower_config)) {
        conn->close(conn, NULL);
        _exit(EXIT_FAILURE);
    }

    /*
     * Open a cursor to trigger lazy-open of the ingest dhandle. The drain worker only queues
     * dhandles that are already marked WT_DHANDLE_OPEN, so we must touch the table before step-up.
     */
    WT_CURSOR *cursor;
    if (session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor) != 0) {
        conn->close(conn, NULL);
        _exit(EXIT_FAILURE);
    }
    testutil_check(cursor->close(cursor));

    STEPUP_ARG stepup_arg = {.conn = conn, .result = 0};
    pthread_t tid;
    testutil_check(pthread_create(&tid, NULL, stepup_thread, &stepup_arg));

    wait_for_drain(conn);

    /*
     * Give drain workers time to acquire the read lock before the drop races in. running=true is
     * set before background threads are created and the queue is populated, so lock acquisition
     * can lag the flag by up to ~50 ms even on a loaded machine. Sleeping 100 ms puts the drop
     * safely inside the 300 ms stress window (lock held for 300 ms after acquisition), with 200 ms
     * of margin on both sides.
     */
    if (expect_ebusy)
        __wt_sleep(0, 100 * WT_THOUSAND);

    /*
     * Use checkpoint_wait=false so the drop does not block on the checkpoint lock held by the
     * step-up thread throughout __disagg_step_up.
     *
     * drain wins (expect_ebusy=true): drain holds the read lock; the drop tries __wt_try_writelock,
     *   fails immediately, and returns EBUSY. Retry after step-up completes.
     * drop wins (expect_ebusy=false): drain has not yet acquired the read lock; the drop wins the
     *   exclusive lock, sets WT_DHANDLE_DEAD, and returns 0. No retry needed.
     */
    int drop_ret = session->drop(session, TABLE_URI, "force=true,checkpoint_wait=false");
    fprintf(stderr, "drop (checkpoint_wait=false): %s\n", wiredtiger_strerror(drop_ret));

    testutil_check(pthread_join(tid, NULL));

    if (expect_ebusy) {
        testutil_assert(drop_ret == EBUSY);
        drop_ret = session->drop(session, TABLE_URI, "force=true");
        fprintf(stderr, "drop retry: %s\n", wiredtiger_strerror(drop_ret));
        testutil_assert(drop_ret == 0);
    } else {
        testutil_assert(drop_ret == 0);
    }

    (void)conn->close(conn, NULL);

    _exit(stepup_arg.result != 0 ? EXIT_FAILURE : EXIT_SUCCESS);
}

/*
 * setup_db --
 *     Create a fresh leader database, create a layered table, and write some rows. The connection
 *     close triggers a checkpoint to the page log so the follower can sync it before step-up.
 */
static void
setup_db(TEST_OPTS *opts)
{
    testutil_recreate_dir(opts->home);

    char leader_config[2048];
    testutil_snprintf(leader_config, sizeof(leader_config),
      "create,"
      "statistics=(all),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"],"
      "disaggregated=(role=leader,page_log=palite,drain_threads=1)",
      opts->build_dir);

    WT_CONNECTION *conn;
    testutil_check(wiredtiger_open(opts->home, NULL, leader_config, &conn));
    WT_SESSION *session;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(
      session, TABLE_URI, "key_format=i,value_format=S,block_manager=disagg,type=layered"));

    WT_CURSOR *cursor;
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    for (int i = 0; i < NUM_ROWS; ++i) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, "value");
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_check(conn->close(conn, NULL));
}

/*
 * run_scenario --
 *     Set up a fresh database and fork a child to run the race scenario. Returns true on success.
 */
static bool
run_scenario(TEST_OPTS *opts, const char *label, const char *stress_flag, bool expect_ebusy)
{
    printf("Scenario: %s\n", label);

    setup_db(opts);

    pid_t pid = fork();
    testutil_assert(pid >= 0);

    if (pid == 0)
        subtest_run(opts, stress_flag, expect_ebusy);

    int status;
    testutil_assert(waitpid(pid, &status, 0) == pid);

    if (WIFSIGNALED(status)) {
        fprintf(stderr, "  FAIL: child killed by signal %d -- likely WT_PANIC\n", WTERMSIG(status));
        return (false);
    }

    testutil_assert(WIFEXITED(status));
    if (WEXITSTATUS(status) == EXIT_SUCCESS) {
        printf("  PASS\n");
        return (true);
    }

    fprintf(stderr, "  FAIL: child exited with failure\n");
    return (false);
}

/*
 * main --
 *     Test that a concurrent drop during step-up drain does not panic, in both race orderings.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts = &_opts;
    opts->table_type = TABLE_ROW;

    testutil_parse_begin_opt(argc, argv, GETOPTS, opts);
    for (int ch; (ch = __wt_getopt(opts->progname, argc, argv, GETOPTS)) != EOF;)
        if (testutil_parse_single_opt(opts, ch) != 0)
            testutil_die(EINVAL, "unexpected option");
    testutil_parse_end_opt(opts);

    /* Drain wins: drop blocks on the exclusive lock and must retry. */
    bool ok = run_scenario(opts, "drain wins", "drain_ingest_table_slow", true);

    /* Drop wins: drop acquires the exclusive lock before drain and succeeds immediately. */
    ok = run_scenario(opts, "drop wins", "drain_ingest_table_pre_lock_slow", false) && ok;

    if (!opts->preserve)
        testutil_remove(opts->home);
    testutil_cleanup(opts);
    return (ok ? EXIT_SUCCESS : EXIT_FAILURE);
}
