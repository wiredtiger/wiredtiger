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

#include "format.h"
#include <poll.h>
#include <sys/mman.h>

/*
 * disagg_redirect_output --
 *     Redirect output to a file in the run directory, unless running quietly.
 */
static void
disagg_redirect_output(const char *output_file)
{
    char path[256];

    testutil_snprintf(path, sizeof(path), "%s/%s", g.home, output_file);

    printf("===> Output will be written to %s\n", path);
    printf("     (If you want to watch live, run: tail -f %s)\n\n", path);
    fflush(stdout);

    if (freopen(path, "w", stdout) == NULL)
        testutil_die(errno, "freopen stdout %s", path);
    if (dup2(fileno(stdout), fileno(stderr)) == -1)
        testutil_die(errno, "dup2 stderr->stdout");

    __wt_stream_set_no_buffer(stdout);
    __wt_stream_set_no_buffer(stderr);
}

/*
 * disagg_teardown_multi_node --
 *     Wait for and clean up any follower processes if we're in multi-node disagg mode.
 */
void
disagg_teardown_multi_node(void)
{
    if (!disagg_is_multi_node())
        return;

    if (g.follower_pid > 0) { /* Parent: leader */
        /* Wait for the follower process to exit. */
        track("Waiting for follower to finish execution.", 0ULL);
        testutil_timeout_wait(720, g.follower_pid);
        g.follower_pid = 0;
    }
    close(g.disagg_multi_sync_socket);
    testutil_check(munmap(g.disagg_multi_db_hash, sizeof(DISAGG_MULTI_DB_HASH)));
    g.disagg_multi_db_hash = NULL;
}

/*
 * disagg_setup_multi_node --
 *     Set up the environment for multi-node disagg, forking follower processes as needed.
 */
void
disagg_setup_multi_node(void)
{
    pid_t pid;
    int sv[2];
    char follower_home[256];

    if (!disagg_is_multi_node())
        return;

    testutil_snprintf(follower_home, sizeof(follower_home), "%s/follower", g.home);
    memset(&g.checkpoint_metadata, 0, sizeof(g.checkpoint_metadata));

    /*
     * Create required dir before forking to avoid parent/child races. Skip on reopen, since the run
     * directories already exist.
     */
    if (!g.reopen) {
        testutil_recreate_dir(g.home);
        testutil_mkdir(follower_home);
    }

    /* Initialize a shared page log directory path for all nodes. */
    testutil_snprintf(g.home_page_log, sizeof(g.home_page_log), "%s", g.home);
    /*
     * Allocate a shared memory region to hold hash values shared between leader and follower
     * processes, used by disagg multi node tests to validate data consistency.
     */
    g.disagg_multi_db_hash = mmap(NULL, sizeof(DISAGG_MULTI_DB_HASH), PROT_READ | PROT_WRITE,
      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    testutil_assert_errno(g.disagg_multi_db_hash != MAP_FAILED);

    /* Create a socket pair for leader-follower synchronization.*/
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == -1)
        testutil_die(errno, "Failed to create socket pair for leader-follower sync");

    fflush(NULL);
    pid = fork();
    testutil_assert_errno(pid >= 0);
    if (pid == 0) { /* Child: follower */
        progname = "t[follower]";
        config_single(NULL, "disagg.mode=follower", true);
        path_setup(follower_home);
        disagg_redirect_output("follower.out");
        close(sv[0]);
        g.disagg_multi_sync_socket = sv[1];
    } else { /* Parent: leader */
        progname = "t[leader]";
        config_single(NULL, "disagg.mode=leader", true);
        disagg_redirect_output("leader.out");
        close(sv[1]);
        g.disagg_multi_sync_socket = sv[0];
    }

    g.follower_pid = pid;
}

/*
 * disagg_multi_sync_point --
 *     Synchronization point in disagg multi-node setup for leader-follower. The wait is bounded: if
 *     the other process never arrives (its workers are stalled), waiting forever would surface only
 *     as a silent CI idle-timeout with no diagnostics, so dump state and abort instead.
 */
static void
disagg_multi_sync_point(WT_SESSION *session)
{
    struct pollfd pfd;
    char send = 'S'; /* S for sync */
    int ret;
    char recv;

    /* Signal from leader or follower to synchronize. */
    if (write(g.disagg_multi_sync_socket, &send, 1) != 1)
        testutil_die(errno, "disagg_multi_sync_point: write");

    track("Reached sync point. Waiting for other process...", 0ULL);

    /*
     * Wait for synchronization signal from the other process, with a 30-minute bound. The bound is
     * deliberately far above the lag we expect: followers have been seen trailing the leader by
     * more than ten minutes even in release builds (FIXME-WT-18605), and this guard exists to turn
     * a permanent stall into a failure with diagnostics, not to police lag.
     */
    pfd.fd = g.disagg_multi_sync_socket;
    pfd.events = POLLIN;
    do {
        ret = poll(&pfd, 1, 30 * 60 * WT_THOUSAND);
    } while (ret == -1 && errno == EINTR);

    if (ret == 1) {
        if (read(g.disagg_multi_sync_socket, &recv, 1) != 1)
            testutil_die(errno, "disagg_multi_sync_point: wrong read content");
        return;
    }
    if (ret == -1)
        testutil_die(errno, "disagg_multi_sync_point: poll failure");

    abort_with_state_dump(
      session->connection, "multi-node sync point not reached within 30 minutes");
}

/*
 * disagg_sync_multi_node --
 *     Synchronization point in disagg multi-node setup for leader-follower data validation.
 */
void
disagg_sync_multi_node(WT_SESSION *session)
{
    uint64_t hash = 0;
    if (!disagg_is_multi_node())
        return;

    if (GV(DISAGG_MULTI_VALIDATION)) {
        hash = checksum_database(session);
        if (g.disagg_leader)
            g.disagg_multi_db_hash->leader_hash = hash;
        else
            g.disagg_multi_db_hash->follower_hash = hash;
    }

    /* Initial synchronization between leader and follower processes. */
    disagg_multi_sync_point(session);

    if (GV(DISAGG_MULTI_VALIDATION)) {
        /*
         * If there's a mismatch, then we're going to assert. Before we do, preserve the state of
         * ingest and stable tables.
         */

        bool hash_match =
          g.disagg_multi_db_hash->leader_hash == g.disagg_multi_db_hash->follower_hash;
        if (!hash_match && GV(DISAGG_PRESERVE))
            testutil_disagg_preserve(session->connection, "preserve", g.stable_timestamp);

        /* Exit synchronization between leader and follower processes. */
        disagg_multi_sync_point(session);

        /* Assert after sync point to ensure both nodes have preserved the data. */
        testutil_assert(hash_match);
    }
}

/*
 * disagg_is_multi_node --
 *     Return true if disagg is configured for multi-node.
 */
bool
disagg_is_multi_node(void)
{
    const char *page_log;
    bool disagg_enabled;

    page_log = GVS(DISAGG_PAGE_LOG);
    disagg_enabled = (strcmp(page_log, "off") != 0 && strcmp(page_log, "none") != 0);

    return (disagg_enabled && GV(DISAGG_MULTI));
}

/*
 * disagg_is_mode_switch --
 *     Check if disagg is configured to use "switch" mode.
 */
bool
disagg_is_mode_switch(void)
{
    return (g.disagg_storage_config && strcmp(GVS(DISAGG_MODE), "switch") == 0);
}

/*
 * stepdown_writers_paused --
 *     Return true once every worker has acknowledged the write pause. An acknowledgment is only
 *     published with no transaction in flight, so once all workers have acknowledged, no write is
 *     in progress and none can start until the pause is lifted.
 */
static bool
stepdown_writers_paused(void)
{
    TINFO **tlp;
    bool ack;

    if (tinfo_list == NULL)
        return (true);
    for (tlp = tinfo_list; *tlp != NULL; ++tlp) {
        ack = __wt_atomic_load_bool_v_acquire(&(*tlp)->pause_ack);
        if (!ack)
            return (false);
    }
    return (true);
}

/*
 * stepdown_pause_worker_writes --
 *     Pause worker writes. Clear any stale acknowledgment first so the wait below only sees
 *     acknowledgments published after the pause was raised; workers only publish while the pause is
 *     set, so the clear cannot race a concurrent acknowledgment.
 */
static void
stepdown_pause_worker_writes(void)
{
    TINFO **tlp;

    if (tinfo_list != NULL)
        for (tlp = tinfo_list; *tlp != NULL; ++tlp)
            __wt_atomic_store_bool_v_release(&(*tlp)->pause_ack, false);
    __wt_atomic_store_bool_v_release(&g.stepdown_pause_writes, true);
}

/*
 * stepdown_stat --
 *     Return a connection statistic.
 */
static int64_t
stepdown_stat(WT_SESSION *session, int key)
{
    WT_CURSOR *cursor;
    int64_t value;
    const char *desc, *pvalue;

    testutil_check(session->open_cursor(session, "statistics:", NULL, NULL, &cursor));
    cursor->set_key(cursor, key);
    testutil_check(cursor->search(cursor));
    testutil_check(cursor->get_value(cursor, &desc, &pvalue, &value));
    testutil_check(cursor->close(cursor));
    return (value);
}

/*
 * stepdown_stable_at_committed --
 *     With the workers paused, advance stable to cover every committed timestamp.
 */
static void
stepdown_stable_at_committed(WT_SESSION *session)
{
    char config[64];

    timestamp_sync_threads_commit_ts();
    g.stable_timestamp = timestamp_minimum_committed();
    testutil_snprintf(config, sizeof(config), "stable_timestamp=%" PRIx64, g.stable_timestamp);
    lock_writelock(session, &g.prepare_commit_lock);
    testutil_check(g.wts_conn->set_timestamp(g.wts_conn, config));
    lock_writeunlock(session, &g.prepare_commit_lock);
}

/*
 * stepdown_demote_refused --
 *     Attempt the demotion and require it to be refused and counted by the given statistic.
 */
static void
stepdown_demote_refused(WT_SESSION *session, int refusal_stat, const char *why)
{
    int64_t before;

    before = stepdown_stat(session, refusal_stat);
    testutil_assert(g.wts_conn->reconfigure(g.wts_conn, "disaggregated=(role=follower)") == EINVAL);
    testutil_assertfmt(stepdown_stat(session, refusal_stat) == before + 1,
      "step-down refusal for %s was not counted", why);
    testutil_assert(stepdown_stat(session, WT_STAT_CONN_DISAGG_ROLE_LEADER) == 1);
    track_msg("[stepdown] demotion refused as expected: %s", why);
}

/*
 * stepdown_prepared_begin --
 *     Begin a write transaction before the arm on a table of its own, so it stays unarmed.
 */
static void
stepdown_prepared_begin(WT_SESSION *session, WT_CURSOR **cursorp)
{
    static const char *uri = "layered:stepdown_prepared";
    WT_CURSOR *cursor;
    char key[32];

    testutil_check(session->create(session, uri, "key_format=S,value_format=S"));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    testutil_check(session->begin_transaction(session, NULL));
    testutil_snprintf(key, sizeof(key), "%" PRIu64, __wt_atomic_load_uint64_acquire(&g.timestamp));
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, "unarmed");
    testutil_check(cursor->insert(cursor));
    *cursorp = cursor;
}

/* !!!
 * disagg_async_stepdown --
 *     Perform a planned step-down while worker threads are still live:
 *     1. Stop the checkpoint and timestamp threads so they cannot interfere.
 *     2. Begin a write transaction that is still open at the arm.
 *     3. Arm the step-down. Transactions in flight commit stable only; later ones mirror their
 *        writes to ingest.
 *     4. Let the workers keep writing for a window, exercising armed leader writes.
 *     5. Pause worker writes and wait for every worker to acknowledge, guaranteeing no writer is
 *        still active, then advance stable over every commit and checkpoint.
 *     6. Prepare the unarmed transaction: the demotion must be refused while it is unresolved,
 *        and again after it commits until a checkpoint covers the commit.
 *     7. Demote, retrying after a fresh checkpoint while refused, re-enable worker writes (now
 *        follower writes) and read the latest checkpoint.
 */
void
disagg_async_stepdown(wt_thread_t *checkpoint_tid, wt_thread_t *timestamp_tid)
{
    SAP sap;
    WT_CURSOR *prepared_cursor;
    WT_DECL_RET;
    WT_SESSION *prepared_session, *session;
    uint64_t drain_polls, prepared_id, ts;
    u_int retries;

    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(g.wts_conn, &sap, NULL, NULL, &session);

    track_msg("[stepdown] stopping checkpoint and timestamp threads");

    /* Stop the checkpoint thread: the step-down takes its own checkpoints with writers paused. */
    if (g.checkpoint_config == CHECKPOINT_ON) {
        __wt_atomic_store_bool_v_relaxed(&g.checkpoint_quit, true);
        testutil_check(__wt_thread_join(NULL, checkpoint_tid));
    }

    /* Stop the timestamp thread: the step-down moves stable itself. */
    if (g.transaction_timestamps_config) {
        __wt_atomic_store_bool_v_relaxed(&g.timestamp_quit, true);
        testutil_check(__wt_thread_join(NULL, timestamp_tid));
    }

    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(g.wts_conn, &sap, NULL, NULL, &prepared_session);
    stepdown_prepared_begin(prepared_session, &prepared_cursor);

    track_msg("[stepdown] arming");
    testutil_check(g.wts_conn->reconfigure(g.wts_conn, "disaggregated=(step_down_arm=true)"));
    testutil_assert(stepdown_stat(session, WT_STAT_CONN_DISAGG_STEP_DOWN_ARMED) == 1);

    track_msg("[stepdown] armed write window");
    __wt_sleep(DISAGG_STEPDOWN_INGEST_WINDOW_SEC, 0);

    /*
     * Pause worker writes and wait until every worker acknowledges with no transaction in flight,
     * guaranteeing no writer is still active (e.g. stuck in eviction) when the checkpoint starts.
     */
    track_msg("[stepdown] pausing worker writes");
    stepdown_pause_worker_writes();
    for (drain_polls = 60 * WT_THOUSAND / 250; drain_polls > 0; --drain_polls) {
        if (stepdown_writers_paused())
            break;
        __wt_sleep(0, 250 * WT_THOUSAND);
    }
    testutil_assert(drain_polls > 0);
    track_msg(
      "[stepdown] writes paused after %" PRIu64 "ms", (60 * WT_THOUSAND / 250 - drain_polls) * 250);

    stepdown_stable_at_committed(session);

    /* The prepare timestamp lands above stable, as the workers' prepares do. */
    prepared_id = __wt_atomic_add_uint64_v(&g.prepared_id, 1);
    ts = next_timestamp(prepared_session);
    testutil_check(
      prepared_session->timestamp_transaction_uint(prepared_session, WT_TS_TXN_TYPE_PREPARE, ts));
    testutil_check(prepared_session->prepared_id_transaction_uint(prepared_session, prepared_id));
    testutil_check(prepared_session->prepare_transaction(prepared_session, NULL));

    testutil_check(session->checkpoint(session, NULL));
    stepdown_demote_refused(
      session, WT_STAT_CONN_DISAGG_STEP_DOWN_REFUSED_PREPARED, "unarmed prepared transaction");

    /* The commit lands above the checkpoint, stable only. */
    ts = next_timestamp(prepared_session);
    lock_readlock(prepared_session, &g.prepare_commit_lock);
    testutil_check(
      prepared_session->timestamp_transaction_uint(prepared_session, WT_TS_TXN_TYPE_COMMIT, ts));
    testutil_check(
      prepared_session->timestamp_transaction_uint(prepared_session, WT_TS_TXN_TYPE_DURABLE, ts));
    testutil_check(prepared_session->commit_transaction(prepared_session, NULL));
    lock_readunlock(prepared_session, &g.prepare_commit_lock);
    testutil_check(prepared_cursor->close(prepared_cursor));
    wt_wrap_close_session(prepared_session);
    testutil_assert((uint64_t)stepdown_stat(session, WT_STAT_CONN_DISAGG_PLAIN_HIGH) >= ts);
    stepdown_demote_refused(session, WT_STAT_CONN_DISAGG_STEP_DOWN_REFUSED_PLAIN_HIGH,
      "checkpoint below the resolved prepared commit");

    /*
     * Reset the leader-side KEK push history. This races with disagg_key_rotation() appending to or
     * reading the same history on its own thread; both sides serialize on key_push_lock.
     */
    disagg_key_history_clear();

    /*
     * Complete the role transition while the workers are read-only. Every commit is at or below
     * stable, so the checkpoint covers plain_high and the first attempt is expected to succeed.
     */
    track_msg("[role change] leader -> follower (async)");
    __wt_atomic_store_bool_v_release(&g.disagg_leader, false);
    for (retries = 0;; ++retries) {
        stepdown_stable_at_committed(session);
        testutil_check(session->checkpoint(session, NULL));
        testutil_assertfmt(
          (uint64_t)stepdown_stat(session, WT_STAT_CONN_DISAGG_PLAIN_HIGH) <= g.stable_timestamp,
          "plain_high above stable %" PRIu64 " with writers paused", g.stable_timestamp);
        ret = g.wts_conn->reconfigure(g.wts_conn, "disaggregated=(role=follower)");
        if (ret == 0)
            break;
        testutil_assert(ret == EINVAL && retries < 10);
    }
    testutil_assert(stepdown_stat(session, WT_STAT_CONN_DISAGG_STEP_DOWN_ARMED) == 0);
    track_msg("[stepdown] demoted with plain_high %" PRId64 " and stable %" PRIu64,
      stepdown_stat(session, WT_STAT_CONN_DISAGG_PLAIN_HIGH), g.stable_timestamp);

    /*
     * Pick up the latest checkpoint while workers are still paused; it reconfigures the connection.
     */
    follower_read_latest_checkpoint();

    /* Re-enable worker writes; they now run as follower writes into ingest. */
    __wt_atomic_store_bool_v_release(&g.stepdown_pause_writes, false);

    /* Reset the quit flags now that the threads are joined. */
    __wt_atomic_store_bool_v_relaxed(&g.checkpoint_quit, false);
    __wt_atomic_store_bool_v_relaxed(&g.timestamp_quit, false);

    wt_wrap_close_session(session);
}

/*
 * disagg_stepdown_thread --
 *     Thread wrapper for disagg_async_stepdown(). Runs the step-down in the background so the
 *     operations() spin loop continues ticking (track_ops) while it proceeds. Sets args->done under
 *     a release barrier once the step-down and role transition are complete.
 */
WT_THREAD_RET
disagg_stepdown_thread(void *arg)
{
    STEPDOWN_ARGS *args;

    args = (STEPDOWN_ARGS *)arg;
    disagg_async_stepdown(args->checkpoint_tid, args->timestamp_tid);
    __wt_atomic_store_bool_v_release(&args->done, true);
    return (WT_THREAD_RET_VALUE);
}

/*
 * disagg_switch_roles --
 *     Toggle the current disagg role between "leader" and "follower". With async step-down the
 *     leader -> follower transition happens inside operations(), so this only performs step-up.
 */
void
disagg_switch_roles(void)
{
    SAP sap;
    WT_SESSION *session;

    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(g.wts_conn, &sap, NULL, NULL, &session);

    /* Perform step-up or step-down. */
    __wt_atomic_store_bool_v_release(&g.disagg_leader, !g.disagg_leader);

    if (!g.disagg_leader) {
        /* Stepping down: [leader -> follower]. */

        /*
         * The async path completes the step-down inside operations() (the background step-down
         * thread reconfigures to follower and flips g.disagg_leader), so only the synchronous path
         * steps down here.
         */
        testutil_assert(!GV(DISAGG_STEPDOWN_ASYNC));

        /*
         * Reset the leader-side KEK push history. The async path clears it itself inside
         * disagg_async_stepdown(); this is the synchronous path's counterpart.
         */
        disagg_key_history_clear();

        track_msg("[role change] leader -> follower (sync)");
        timestamp_sync_threads_commit_ts();
        timestamp_once(session, false, false);
        testutil_check(session->checkpoint(session, NULL));
        testutil_check(g.wts_conn->reconfigure(g.wts_conn, "disaggregated=(role=follower)"));
        follower_read_latest_checkpoint();
        wts_prepare_discover(g.wts_conn);
    } else {
        /* Stepping up: [follower -> leader] */
        track_msg("[role change] follower -> leader");

        /*
         * Push stable past the follower phase's commits before stepping up; otherwise eviction
         * couldn't reconcile pages holding updates newer than stable, and those pages would stay
         * pinned in cache during step-up.
         */
        timestamp_sync_threads_commit_ts();
        timestamp_once(session, false, false);

        testutil_check(g.wts_conn->reconfigure(g.wts_conn, "disaggregated=(role=leader)"));
        testutil_check(session->checkpoint(session, NULL));

        /* Verify that this step-up checkpoint persisted the correct KEK. */
        disagg_key_validate_after_checkpoint(session);
    }
    wt_wrap_close_session(session);
    /* After every switch, verify the contents of each table */
    wts_verify_mirrors(g.wts_conn, NULL, NULL);
}
