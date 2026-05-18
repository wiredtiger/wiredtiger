/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Regression test for BF-42866: drain worker racing with a concurrent drop of the same layered
 * table during follower -> leader step-up.
 *
 * During step-up, __wti_layered_drain_ingest_tables enqueues open ingest dhandles and begins
 * copying each to stable. A concurrent drop tries to acquire an exclusive dhandle lock. The drain
 * worker holds a read lock on the dhandle for the duration of the copy, which blocks the drop
 * until the copy finishes. If the dhandle is already dead when the drain worker acquires the read
 * lock (drop completed first), the worker skips the table cleanly.
 *
 * WT_TIMING_STRESS_DRAIN_INGEST_TABLE_SLOW injects a 300 ms sleep at the start of
 * __layered_copy_ingest_table, while the read lock is held, to widen the race window.
 */

#ifndef _WIN32

#include <chrono>
#include <signal.h>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../../utility/test_util.h"

static const std::string TABLE_NAME = "test_drain_drop_race";
static const std::string TABLE_URI = "layered:" + TABLE_NAME;
static const int NUM_ROWS = 100;

/*
 * leader_cfg --
 *     Connection config for the initial leader that creates and populates the table.
 */
static std::string
leader_cfg()
{
    return "create,"
           "statistics=(all),"
           "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
           "disaggregated=(role=leader,page_log=palite,drain_threads=1)";
}

/*
 * follower_cfg --
 *     Connection config for the follower that will be promoted. drain_ingest_table_slow injects a
 *     300 ms sleep at the start of __layered_copy_ingest_table (after the read lock is held),
 *     giving the concurrent drop thread time to attempt the exclusive lock and block.
 */
static std::string
follower_cfg()
{
    return "statistics=(all),"
           "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
           "disaggregated=(role=follower,page_log=palite,drain_threads=2),"
           "timing_stress_for_test=[drain_ingest_table_slow]";
}

/*
 * setup_db --
 *     Create a fresh leader database, create a layered table, write some rows, and close. The
 *     ingest table is left with data so that __layered_drain_ingest_tables has real work to do.
 */
static void
setup_db(const std::string &home)
{
    testutil_system("rm -rf %s && mkdir -p %s", home.c_str(), home.c_str());

    connection_wrapper conn(home, leader_cfg().c_str());
    conn.clear_do_cleanup();
    WT_SESSION *session = (WT_SESSION *)conn.create_session();

    REQUIRE(session->create(session, TABLE_URI.c_str(),
              "key_format=i,value_format=S,block_manager=disagg,type=layered") == 0);

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    for (int i = 0; i < NUM_ROWS; ++i) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, "value");
        REQUIRE(cursor->insert(cursor) == 0);
    }
    REQUIRE(cursor->close(cursor) == 0);
}

/*
 * sync_leader_checkpoint --
 *     Pick up the leader's last checkpoint so database_size is correctly initialized before
 *     step-up. In production a follower tracks the leader's checkpoints continuously; this
 *     replicates that before we trigger the race.
 */
static bool
sync_leader_checkpoint(WT_CONNECTION *conn, WT_SESSION *session)
{
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    std::string follower_config = follower_cfg();
    const char *cfg[] = {follower_config.c_str(), nullptr};
    WT_ITEM checkpoint_meta;
    WT_CLEAR(checkpoint_meta);
    uint64_t checkpoint_lsn = 0;
    wt_timestamp_t checkpoint_ts = 0;

    int ret = __wti_layered_get_disagg_checkpoint(
      session_impl, cfg, &checkpoint_lsn, &checkpoint_ts, &checkpoint_meta);
    if (ret != 0 || checkpoint_meta.size == 0) {
        __wt_buf_free(session_impl, &checkpoint_meta);
        return true;
    }

    std::string meta_str((const char *)checkpoint_meta.data, checkpoint_meta.size);
    __wt_buf_free(session_impl, &checkpoint_meta);
    std::string reconfig = std::string("disaggregated=(checkpoint_meta=\"") + meta_str + "\")";
    return conn->reconfigure(conn, reconfig.c_str()) == 0;
}

/*
 * wait_for_drain --
 *     Spin until the drain worker sets layered_drain_data.running. Keying off this flag ensures the
 *     drop races with the drain worker while it holds the read lock inside the stress sleep.
 */
static void
wait_for_drain(WT_CONNECTION *conn)
{
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    for (int i = 0; i < 500; ++i) {
        if (__wt_atomic_load_bool_relaxed(&conn_impl->layered_drain_data.running))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

/*
 * run_race --
 *     Runs the racy scenario in the current process. Called only from a forked child so a panic
 *     (SIGABRT) doesn't kill the test runner.
 *
 * Returns 0 on clean completion, 1 if an intermediate step fails unexpectedly without a panic.
 */
static int
run_race(const std::string &home)
{
    WT_CONNECTION *conn = nullptr;
    if (wiredtiger_open(home.c_str(), nullptr, follower_cfg().c_str(), &conn) != 0)
        return 1;

    WT_SESSION *session = nullptr;
    if (conn->open_session(conn, nullptr, nullptr, &session) != 0) {
        conn->close(conn, nullptr);
        return 1;
    }

    if (!sync_leader_checkpoint(conn, session)) {
        conn->close(conn, nullptr);
        return 1;
    }

    /*
     * Open a cursor to trigger lazy-open of the ingest dhandle. The drain worker only queues
     * dhandles that are already marked WT_DHANDLE_OPEN, so we must touch the table before step-up.
     */
    WT_CURSOR *cursor = nullptr;
    if (session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) != 0) {
        conn->close(conn, nullptr);
        return 1;
    }
    cursor->close(cursor);

    int reconfig_ret = 0;
    std::thread reconfig_thread(
      [&]() { reconfig_ret = conn->reconfigure(conn, "disaggregated=(role=leader)"); });

    wait_for_drain(conn);

    /*
     * Use checkpoint_wait=false so the drop does not block on the checkpoint lock held by the
     * reconfigure thread throughout __disagg_step_up.
     */
    (void)session->drop(session, TABLE_URI.c_str(), "force=true,checkpoint_wait=false");

    reconfig_thread.join();
    (void)conn->close(conn, nullptr);

    /* Non-zero means the drain worker returned an error instead of skipping the dropped table. */
    return reconfig_ret != 0 ? 1 : 0;
}

/*
 * race_crashes --
 *     Fork a child that runs the racy scenario. Returns true if the child was killed by a signal
 *     (i.e. WT_PANIC abort() -> SIGABRT), false if it exited cleanly.
 */
static bool
race_crashes(const std::string &home)
{
    pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
        signal(SIGABRT, SIG_DFL);
        _exit(run_race(home));
    }

    int status = 0;
    waitpid(pid, &status, 0);
    return WIFSIGNALED(status) || !WIFEXITED(status) || WEXITSTATUS(status) != 0;
}

TEST_CASE(
  "Drain/drop race: concurrent drop during step-up drain should not panic", "[drain_drop_race]")
{
    const std::string home = "WT_TEST.drain_drop_race";
    setup_db(home);

    CHECK(!race_crashes(home));

    utils::wiredtiger_cleanup(home);
}

#endif /* !_WIN32 */
