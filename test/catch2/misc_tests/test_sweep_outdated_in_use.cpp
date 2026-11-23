/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef _WIN32

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../../utility/test_util.h"
#include "layered_disagg_utils.h"

static const std::string HOME = "WT_TEST.sweep_outdated_in_use";
static const std::string TABLE_URI = "table:test_sweep_outdated";
static const std::string STABLE_URI = "file:test_sweep_outdated.wt_stable";

/*
 * sweep_close --
 *     Run the sweep close step on a dhandle the way the sweep server does, returning its result.
 */
static int
sweep_close(WT_SESSION_IMPL *sweep_session, WT_DATA_HANDLE *dhandle)
{
    int close_ret, lock_ret;

    close_ret = 0;
    WT_WITH_DHANDLE(sweep_session, dhandle,
      WT_WITH_DHANDLE_WRITE_LOCK_NOWAIT(
        sweep_session, lock_ret, close_ret = __ut_sweep_close_dhandle_locked(sweep_session)));
    REQUIRE(lock_ret == 0);
    return (close_ret);
}

TEST_CASE("Sweep keeps a dirty outdated disagg tree that an open transaction references",
  "[sweep][sweep_outdated_in_use]")
{
    utils::wiredtiger_cleanup(HOME);
    testutil_system("mkdir -p %s", HOME.c_str());

    connection_wrapper conn_wrap(HOME, layered_disagg_build_cfg("leader").c_str());
    WT_SESSION *session = (WT_SESSION *)conn_wrap.create_session();
    WT_SESSION_IMPL *sweep_session = conn_wrap.create_session();

    REQUIRE(session->create(session, TABLE_URI.c_str(),
              "key_format=S,value_format=S,block_manager=disagg,type=layered") == 0);

    /* Leave a transaction open with an update on the live stable tree, and close its cursor. */
    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, STABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    WT_DATA_HANDLE *dhandle = ((WT_CURSOR_BTREE *)cursor)->dhandle;
    WT_BTREE *btree = (WT_BTREE *)dhandle->handle;
    REQUIRE(F_ISSET(btree, WT_BTREE_DISAGGREGATED));

    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "key");
    cursor->set_value(cursor, "value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    REQUIRE(__wt_atomic_load_bool_relaxed(&btree->modified));
    REQUIRE(__wt_atomic_load_int32_relaxed(&dhandle->session_inuse) > 0);
    __wt_atomic_store_bool_relaxed(&dhandle->outdated, true);

    SECTION("the open transaction keeps the handle open")
    {
        REQUIRE(sweep_close(sweep_session, dhandle) == 0);
        CHECK(F_ISSET(dhandle, WT_DHANDLE_OPEN));
        CHECK_FALSE(F_ISSET(dhandle, WT_DHANDLE_DEAD));

        REQUIRE(session->rollback_transaction(session, nullptr) == 0);
    }

    SECTION("the handle is closed once the transaction ends")
    {
        REQUIRE(session->rollback_transaction(session, nullptr) == 0);
        REQUIRE(__wt_atomic_load_int32_relaxed(&dhandle->session_inuse) == 0);

        REQUIRE(sweep_close(sweep_session, dhandle) == 0);
        CHECK(F_ISSET(dhandle, WT_DHANDLE_DEAD));
    }
}

#endif /* !_WIN32 */
