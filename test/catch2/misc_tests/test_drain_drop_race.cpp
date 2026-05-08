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
 * The race:
 *  1. A follower is promoted to leader via conn->reconfigure("disaggregated=(role=leader)").
 *  2. __wti_layered_drain_ingest_tables enqueues all open ingest dhandles and releases the
 *     handle-list lock, then begins copying each ingest table to stable.
 *  3. Concurrently, a thread calls session->drop() on the same layered table.
 *  4. The drop acquires an exclusive dhandle lock (not blocked by the drain worker's
 *     session_inuse pin), closes the btree, and removes the metadata entry.
 *  5. The drain worker wakes up, tries to open a cursor on the now-dead/missing ingest file,
 *     gets ENOENT, and (before the fix) panics with WT_PANIC.
 *
 * The timing stress flag WT_TIMING_STRESS_DRAIN_INGEST_TABLE_SLOW injects a 300 ms sleep at
 * the start of __layered_copy_ingest_table, before any cursor is opened, widening the race
 * window to make the test deterministic.
 *
 * This test currently FAILS (the child process panics with SIGABRT) because the bug is not
 * yet fixed. After the fix it should pass cleanly.
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
 *     Connection config for the follower that will be promoted. The drain_ingest_table_slow flag
 *     injects a 300 ms sleep at the start of __layered_copy_ingest_table before any cursors are
 *     opened. This reliably widens the race window so that a concurrent drop lands while the drain
 *     worker is mid-copy.
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

    /*
     * Open a cursor on the layered table to trigger lazy-open of the ingest dhandle. The drain
     * worker only queues dhandles that are already marked WT_DHANDLE_OPEN, so we must touch the
     * table before triggering step-up.
     */
    WT_CURSOR *cursor = nullptr;
    if (session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) != 0) {
        conn->close(conn, nullptr);
        return 1;
    }
    cursor->close(cursor);

    /*
     * Thread 1 (background): trigger follower -> leader step-up. This calls
     * __wti_layered_drain_ingest_tables, which enqueues the ingest dhandle and then calls
     * __layered_copy_ingest_table. With the stress flag set that function sleeps 300 ms before
     * opening any cursors, creating a wide window for the concurrent drop.
     */
    int reconfig_ret = 0;
    std::thread reconfig_thread(
      [&]() { reconfig_ret = conn->reconfigure(conn, "disaggregated=(role=leader)"); });

    /*
     * Main thread: spin until the drain has started (layered_drain_data.running becomes true),
     * then immediately drop the layered table. Using the running flag rather than a fixed sleep
     * ensures the drop races with the drain worker while it is inside the 300 ms stress sleep in
     * __layered_copy_ingest_table, not with the earlier checkpoint-restart phase of step-up.
     *
     * Before the fix: the drain worker wakes from its sleep, opens a cursor on the now-dead/missing
     * ingest file, gets ENOENT, and panics. The process is killed by SIGABRT.
     * After the fix: the drain worker detects the dead dhandle and skips it gracefully.
     */
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    for (int i = 0; i < 500; ++i) {
        if (__wt_atomic_load_bool_relaxed(&conn_impl->layered_drain_data.running))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    /*
     * Use checkpoint_wait=false so the drop does not block on the checkpoint lock held by the
     * reconfigure thread throughout __disagg_step_up.
     */
    (void)session->drop(session, TABLE_URI.c_str(), "force=true,checkpoint_wait=false");

    reconfig_thread.join();
    (void)conn->close(conn, nullptr);

    /*
     * A non-zero reconfig_ret means the drain worker encountered an error (e.g. ENOENT because the
     * concurrent drop removed the ingest file). Treat this as a test failure even when the process
     * did not panic the drain should complete cleanly with no error after any correct fix.
     */
    return reconfig_ret != 0 ? 1 : 0;
}

/*
 * race_crashes --
 *     Fork a child that runs the racy scenario. Returns true if the child was killed by a signal
 *     (i.e. WT_PANIC abort() SIGABRT), false if it exited cleanly.
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

    if (WIFSIGNALED(status))
        return true;
    return !WIFEXITED(status) || WEXITSTATUS(status) != 0;
}

TEST_CASE(
  "Drain/drop race: concurrent drop during step-up drain should not panic", "[drain_drop_race]")
{
    const std::string home = "WT_TEST.drain_drop_race";
    setup_db(home);

    /*
     * This CHECK currently FAILS (race_crashes returns true) because BF-42866 is not yet fixed.
     * After the fix the drain worker should handle the dropped ident gracefully and the child
     * should exit cleanly.
     */
    CHECK(!race_crashes(home));

    utils::wiredtiger_cleanup(home);
}

#endif /* !_WIN32 */
