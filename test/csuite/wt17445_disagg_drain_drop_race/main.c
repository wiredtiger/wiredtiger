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
 * WT_TIMING_STRESS_DRAIN_INGEST_TABLE_SLOW injects a 300 ms sleep at the start of
 * __layered_copy_ingest_table, while the read lock is held, to widen the race window.
 *
 * The race is triggered by:
 *   1. Opening as a follower with the timing stress enabled.
 *   2. Reconfiguring to leader (step-up) on a background thread.
 *   3. Spinning on layered_drain_data.running (accessible via wt_internal.h, included through
 *      test_util.h) to detect when the drain worker holds the read lock inside the stress sleep.
 *   4. Calling session->drop(force=true, checkpoint_wait=false) from the main thread to race
 *      against the drain worker.
 *
 * A forked child runs the race so that a WT_PANIC / SIGABRT in the buggy code path is caught
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
    STEPUP_ARG *a;

    a = (STEPUP_ARG *)arg;
    a->result = a->conn->reconfigure(a->conn, "disaggregated=(role=leader)");
    return (NULL);
}

/*
 * wait_for_drain --
 *     Spin until the drain worker sets layered_drain_data.running. Keying off this flag ensures
 *     the drop races with the drain worker while it holds the read lock inside the stress sleep.
 */
static void
wait_for_drain(WT_CONNECTION *conn)
{
    WT_CONNECTION_IMPL *conn_impl;
    int i;

    conn_impl = (WT_CONNECTION_IMPL *)conn;
    for (i = 0; i < 500; ++i) {
        if (__wt_atomic_load_bool_relaxed(&conn_impl->layered_drain_data.running))
            return;
        __wt_sleep(0, 10 * WT_THOUSAND); /* 10 ms */
    }
}

/*
 * sync_leader_checkpoint --
 *     Fetch the leader's last checkpoint metadata from the page log and reconfigure the follower
 *     connection with it, so database_size is correctly initialized before step-up. In production,
 *     a follower tracks the leader's checkpoints continuously; this replicates that before the
 *     race is triggered.
 *
 *     Returns true on success or when no checkpoint is available yet, false on error.
 */
static bool
sync_leader_checkpoint(WT_CONNECTION *conn, WT_SESSION *session, const char *follower_config)
{
    WT_ITEM checkpoint_meta;
    WT_SESSION_IMPL *session_impl;
    char reconfig[8192];
    const char *cfg[2];
    uint64_t checkpoint_lsn;
    wt_timestamp_t checkpoint_ts;
    int ret;

    session_impl = (WT_SESSION_IMPL *)session;
    cfg[0] = follower_config;
    cfg[1] = NULL;

    WT_CLEAR(checkpoint_meta);
    checkpoint_lsn = 0;
    checkpoint_ts = 0;

    ret = __wti_layered_get_disagg_checkpoint(
      session_impl, cfg, &checkpoint_lsn, &checkpoint_ts, &checkpoint_meta);
    if (ret != 0) {
        __wt_buf_free(session_impl, &checkpoint_meta);
        return (false);
    }
    if (checkpoint_meta.size == 0) {
        __wt_buf_free(session_impl, &checkpoint_meta);
        return (true);
    }

    testutil_snprintf(reconfig, sizeof(reconfig), "disaggregated=(checkpoint_meta=\"%.*s\")",
      (int)checkpoint_meta.size, (const char *)checkpoint_meta.data);
    __wt_buf_free(session_impl, &checkpoint_meta);

    return (conn->reconfigure(conn, reconfig) == 0);
}

/*
 * subtest_run --
 *     Run the racy scenario in the current process. Called only from a forked child so a panic
 *     (SIGABRT) doesn't kill the test runner. Calls _exit: never returns.
 */
static void WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn)) subtest_run(TEST_OPTS *opts)
{
    struct rlimit rlim;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    STEPUP_ARG stepup_arg;
    pthread_t tid;
    char follower_config[2048];

    /* No core files; a panic may trigger diagnostic assertions during cleanup. */
    memset(&rlim, 0, sizeof(rlim));
    testutil_check(setrlimit(RLIMIT_CORE, &rlim));

    testutil_snprintf(follower_config, sizeof(follower_config),
      "statistics=(all),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"],"
      "disaggregated=(role=follower,page_log=palite,drain_threads=2),"
      "timing_stress_for_test=[drain_ingest_table_slow]",
      opts->build_dir);

    if (wiredtiger_open(opts->home, NULL, follower_config, &conn) != 0)
        _exit(EXIT_FAILURE);

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
    if (session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor) != 0) {
        conn->close(conn, NULL);
        _exit(EXIT_FAILURE);
    }
    testutil_check(cursor->close(cursor));

    stepup_arg.conn = conn;
    stepup_arg.result = 0;
    testutil_check(pthread_create(&tid, NULL, stepup_thread, &stepup_arg));

    wait_for_drain(conn);

    /*
     * Use checkpoint_wait=false so the drop does not block on the checkpoint lock held by the
     * step-up thread throughout __disagg_step_up.
     */
    (void)session->drop(session, TABLE_URI, "force=true,checkpoint_wait=false");

    testutil_check(pthread_join(tid, NULL));
    (void)conn->close(conn, NULL);

    /* Non-zero means the drain worker returned an error instead of skipping the dropped table. */
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
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    char leader_config[2048];
    int i;

    testutil_recreate_dir(opts->home);

    testutil_snprintf(leader_config, sizeof(leader_config),
      "create,"
      "statistics=(all),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"],"
      "disaggregated=(role=leader,page_log=palite,drain_threads=1)",
      opts->build_dir);

    testutil_check(wiredtiger_open(opts->home, NULL, leader_config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, TABLE_URI,
      "key_format=i,value_format=S,block_manager=disagg,type=layered"));

    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    for (i = 0; i < NUM_ROWS; ++i) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, "value");
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_check(conn->close(conn, NULL));
}

/*
 * main --
 *     Test that a concurrent drop during step-up drain does not panic.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts;
    pid_t pid;
    int ch, status;

    opts = &_opts;
    opts->table_type = TABLE_ROW;

    testutil_parse_begin_opt(argc, argv, GETOPTS, opts);
    while ((ch = __wt_getopt(opts->progname, argc, argv, GETOPTS)) != EOF)
        if (testutil_parse_single_opt(opts, ch) != 0)
            testutil_die(EINVAL, "unexpected option");
    testutil_parse_end_opt(opts);

    setup_db(opts);

    pid = fork();
    testutil_assert(pid >= 0);

    if (pid == 0)
        subtest_run(opts);

    testutil_assert(waitpid(pid, &status, 0) == pid);

    if (WIFSIGNALED(status)) {
        fprintf(stderr, "Child killed by signal %d -- likely WT_PANIC\n", WTERMSIG(status));
        return (EXIT_FAILURE);
    }

    testutil_assert(WIFEXITED(status));
    if (WEXITSTATUS(status) == EXIT_SUCCESS) {
        printf("Child exited successfully: drain/drop race did not panic\n");
    } else if (WEXITSTATUS(status) == EXIT_FAILURE) {
        fprintf(stderr, "Child exited with failure: step-up returned non-zero\n");
        return (EXIT_FAILURE);
    } else {
        fprintf(stderr, "Child exited with unexpected status %d\n", WEXITSTATUS(status));
        return (EXIT_FAILURE);
    }

    if (!opts->preserve)
        testutil_remove(opts->home);
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
