/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * test_layered_incomplete_table.cpp
 *
 * Catch2 equivalent of test_layered90.py, covering all eight combinations of
 * {file:T.wt_ingest, file:T.wt_stable} presence  {leader, follower} role by
 * directly removing metadata entries from an otherwise-complete layered table.
 *
 * During wiredtiger_open, __metadata_clean_incomplete_table asserts:
 *   - file:T.wt_ingest MUST exist (on both leader and follower).
 *   - file:T.wt_stable MUST exist on a leader; optional on a follower.
 *
 * Expected outcomes:
 *
 *   role     | ingest | stable | result
 *   ---------|--------|--------|--------
 *   leader   |  yes   |  yes   | succeed
 *   leader   |  yes   |  no    | abort
 *   leader   |  no    |  yes   | abort
 *   leader   |  no    |  no    | abort
 *   follower |  yes   |  yes   | succeed
 *   follower |  yes   |  no    | succeed  (stable is optional on follower)
 *   follower |  no    |  yes   | abort
 *   follower |  no    |  no    | abort
 *
 * "should abort" cases run in a forked child so that WT_ASSERT_ALWAYS killing
 * the process does not take down the test runner.
 */

#ifndef _WIN32

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../../utility/test_util.h"

/* Supplied by CMake as the absolute path to the palite shared library. */
#ifndef PALITE_EXTENSION
#define PALITE_EXTENSION "./ext/page_log/palite/libwiredtiger_palite.so"
#endif

static const char *TABLE_NAME = "test_layered_inc";
static const char *TABLE_URI = "table:test_layered_inc";

/*
 * build_cfg --
 *     Construct a wiredtiger_open config string for the given role. Pass create=true for the first
 *     open; omit it on reopens.
 */
static std::string
build_cfg(const char *role, bool create)
{
    std::string cfg;
    if (create)
        cfg += "create,";
    cfg += "statistics=(all),";
    cfg += std::string("extensions=[") + PALITE_EXTENSION + "],";
    cfg += std::string("disaggregated=(role=") + role + ",page_log=palite)";
    return cfg;
}

/*
 * prepare_db --
 *     Create a fresh database with a complete layered table (both ingest and stable metadata
 *     present), then optionally remove one or both file metadata entries to manufacture the desired
 *     incomplete state.
 *
 * Metadata surgery (for testing purposes) is done in a separate, non-disaggregated connection so
 *     that the layered data handles are never open when we remove their entries. Removing metadata
 *     while a data handle is live triggers a panic on close.
 */
static void
prepare_db(const char *home, bool keep_ingest, bool keep_stable)
{
    /* Start from a clean slate. */
    testutil_system("rm -rf %s && mkdir -p %s", home, home);

    if (keep_ingest && !keep_stable) {
        /*
         * "ingest present, stable absent" state: open as a follower, which naturally creates only
         * file:T.wt_ingest and never creates file:T.wt_stable. This avoids the leader-startup path
         * that would recreate the stable entry before recovery runs the incomplete-table check
         * defeating any attempt to remove stable after a leader open.
         */
        WT_CONNECTION *conn = nullptr;
        REQUIRE(wiredtiger_open(home, nullptr, build_cfg("follower", true).c_str(), &conn) == 0);

        WT_SESSION *session = nullptr;
        REQUIRE(conn->open_session(conn, nullptr, nullptr, &session) == 0);

        REQUIRE(session->create(session, TABLE_URI,
                  "key_format=S,value_format=S,block_manager=disagg,type=layered") == 0);

        WT_CURSOR *cursor = nullptr;
        REQUIRE(session->open_cursor(session, TABLE_URI, nullptr, nullptr, &cursor) == 0);
        cursor->set_key(cursor, "k");
        cursor->set_value(cursor, "v");
        REQUIRE(cursor->insert(cursor) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        REQUIRE(conn->close(conn, nullptr) == 0);
        return;
    }

    /*
     * All other cases: open as leader to create a complete layered table (both file:T.wt_ingest and
     * file:T.wt_stable present after checkpoint).
     */

    /* Phase 1: Open as leader, create a complete layered table, and close. */
    {
        WT_CONNECTION *conn = nullptr;
        REQUIRE(wiredtiger_open(home, nullptr, build_cfg("leader", true).c_str(), &conn) == 0);

        WT_SESSION *session = nullptr;
        REQUIRE(conn->open_session(conn, nullptr, nullptr, &session) == 0);

        REQUIRE(session->create(session, TABLE_URI,
                  "key_format=S,value_format=S,block_manager=disagg,type=layered") == 0);

        WT_CURSOR *cursor = nullptr;
        REQUIRE(session->open_cursor(session, TABLE_URI, nullptr, nullptr, &cursor) == 0);
        cursor->set_key(cursor, "k");
        cursor->set_value(cursor, "v");
        REQUIRE(cursor->insert(cursor) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        /* Clean close: all file handles are released before we touch metadata. */
        REQUIRE(conn->close(conn, nullptr) == 0);
    }

    if (keep_ingest && keep_stable)
        return; /* Nothing to remove; the DB is already in the desired state. */

    /*
     * Phase 2: Reopen as follower so that the disaggregated role is
     * explicitly set and does not inherit the "leader" stored in the turtle
     * file from Phase 1.  With role=follower, recovery's
     * __metadata_clean_incomplete_table only checks for the ingest entry,
     * which is intact at this point, so recovery passes.  The layered-table
     * data handles are never accessed here (lazy open), so no active handle
     * exists for the entries we are about to remove, and the close succeeds
     * cleanly.
     */
    {
        WT_CONNECTION *conn = nullptr;
        REQUIRE(wiredtiger_open(home, nullptr, build_cfg("follower", false).c_str(), &conn) == 0);

        WT_SESSION *session = nullptr;
        REQUIRE(conn->open_session(conn, nullptr, nullptr, &session) == 0);

        auto *session_impl = reinterpret_cast<WT_SESSION_IMPL *>(session);
        char key_buf[256];

        if (!keep_ingest) {
            snprintf(key_buf, sizeof(key_buf), "file:%s.wt_ingest", TABLE_NAME);
            REQUIRE(__wt_metadata_remove(session_impl, key_buf) == 0);
        }

        /* Close commits the metadata deletions via checkpoint. */
        REQUIRE(conn->close(conn, nullptr) == 0);
    }
}

/*
 * try_reopen --
 *     Attempt to reopen an existing database with the given role. Returns the wiredtiger_open
 *     return value; closes the connection on success.
 */
static int
try_reopen(const char *home, const char *role)
{
    WT_CONNECTION *conn = nullptr;
    int ret = wiredtiger_open(home, nullptr, build_cfg(role, false).c_str(), &conn);
    if (ret == 0 && conn != nullptr)
        conn->close(conn, nullptr);
    return ret;
}

/*
 * reopen_aborts --
 *     Fork a child process that tries to reopen the database with the given role. Returns true if
 *     the child exited abnormally (killed by a signal such as SIGABRT, or with a non-zero exit
 *     status), indicating that WT_ASSERT_ALWAYS fired as expected.
 */
static bool
reopen_aborts(const char *home, const char *role)
{
    pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
        /*
         * Child process: reset SIGABRT to the default disposition so that Catch2's signal handler
         * (which throws a C++ exception) cannot interfere with the abort() call inside
         * WT_ASSERT_ALWAYS. Without this, the Catch2 handler catches the signal, throws an
         * exception that escapes the if-block, and the child falls through to execute the parent's
         * waitpid path corrupting the test result.
         */
        signal(SIGABRT, SIG_DFL);
        int ret = try_reopen(home, role);
        _exit(ret == 0 ? 0 : 1);
    }

    /* Parent: wait for the child and inspect its exit status. */
    int status = 0;
    waitpid(pid, &status, 0);

    if (WIFSIGNALED(status))
        return true; /* Killed by a signal (e.g. SIGABRT from WT_ASSERT_ALWAYS). */
    return !WIFEXITED(status) || WEXITSTATUS(status) != 0;
}

/* ---------------------------------------------------------------------------
 * Test cases
 * ------------------------------------------------------------------------- */

TEST_CASE("Layered table incomplete metadata: leader role", "[layered_incomplete]")
{
    SECTION("leader + ingest + stable: should succeed")
    {
        const char *home = "WT_TEST.layered_inc_leader_complete";
        prepare_db(home, /*keep_ingest=*/true, /*keep_stable=*/true);
        REQUIRE(try_reopen(home, "leader") == 0);
        utils::wiredtiger_cleanup(home);
    }

    SECTION("leader + ingest, no stable: should abort")
    {
        const char *home = "WT_TEST.layered_inc_leader_no_stable";
        prepare_db(home, /*keep_ingest=*/true, /*keep_stable=*/false);
        CHECK(reopen_aborts(home, "leader"));
        utils::wiredtiger_cleanup(home);
    }

    SECTION("leader + stable, no ingest: should abort")
    {
        const char *home = "WT_TEST.layered_inc_leader_no_ingest";
        prepare_db(home, /*keep_ingest=*/false, /*keep_stable=*/true);
        CHECK(reopen_aborts(home, "leader"));
        utils::wiredtiger_cleanup(home);
    }

    SECTION("leader + neither ingest nor stable: should abort")
    {
        const char *home = "WT_TEST.layered_inc_leader_neither";
        prepare_db(home, /*keep_ingest=*/false, /*keep_stable=*/false);
        CHECK(reopen_aborts(home, "leader"));
        utils::wiredtiger_cleanup(home);
    }
}

TEST_CASE("Layered table incomplete metadata: follower role", "[layered_incomplete]")
{
    SECTION("follower + ingest + stable: should succeed")
    {
        const char *home = "WT_TEST.layered_inc_follower_complete";
        prepare_db(home, /*keep_ingest=*/true, /*keep_stable=*/true);
        REQUIRE(try_reopen(home, "follower") == 0);
        utils::wiredtiger_cleanup(home);
    }

    SECTION("follower + ingest, no stable: should succeed (stable optional on follower)")
    {
        const char *home = "WT_TEST.layered_inc_follower_no_stable";
        prepare_db(home, /*keep_ingest=*/true, /*keep_stable=*/false);
        REQUIRE(try_reopen(home, "follower") == 0);
        utils::wiredtiger_cleanup(home);
    }

    SECTION("follower + stable, no ingest: should abort")
    {
        const char *home = "WT_TEST.layered_inc_follower_no_ingest";
        prepare_db(home, /*keep_ingest=*/false, /*keep_stable=*/true);
        CHECK(reopen_aborts(home, "follower"));
        utils::wiredtiger_cleanup(home);
    }

    SECTION("follower + neither ingest nor stable: should abort")
    {
        const char *home = "WT_TEST.layered_inc_follower_neither";
        prepare_db(home, /*keep_ingest=*/false, /*keep_stable=*/false);
        CHECK(reopen_aborts(home, "follower"));
        utils::wiredtiger_cleanup(home);
    }
}

#endif /* !_WIN32 */
