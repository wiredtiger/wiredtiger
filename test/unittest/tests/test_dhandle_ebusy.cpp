/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "utils.h"
#include "wrappers/connection_wrapper.h"

TEST_CASE("session->dhandle is NULL after EBUSY from get_dhandle", "[dhandle][dhandle_ebusy]")
{
    const std::string home = "WT_TEST.dhandle_ebusy";
    utils::wiredtigerCleanup(home);

    {
        ConnectionWrapper conn(home);

        WT_SESSION_IMPL *session_impl = conn.createSession();
        WT_SESSION *session = &session_impl->iface;
        REQUIRE(session->create(session, "file:cursor_test.wt", "key_format=i,value_format=i") ==
          0);

        WT_CURSOR *bulk = nullptr;
        REQUIRE(session->open_cursor(
                  session, "file:cursor_test.wt", nullptr, "bulk", &bulk) == 0);

        WT_SESSION_IMPL *session_impl_b = conn.createSession();
        REQUIRE(session_impl_b->dhandle == nullptr);

        int ret =
          __wt_session_get_dhandle(session_impl_b, "file:cursor_test.wt", nullptr, nullptr, 0);
        REQUIRE(ret == EBUSY);
        CHECK(session_impl_b->dhandle == nullptr);

        REQUIRE(bulk->close(bulk) == 0);
    }

    utils::wiredtigerCleanup(home);
}

TEST_CASE("skip reopening a dhandle closed by sweep", "[dhandle][dhandle_skip_reopen]")
{
    const std::string home = "WT_TEST.dhandle_skip_reopen";
    utils::wiredtigerCleanup(home);

    {
        ConnectionWrapper conn(home);

        WT_SESSION_IMPL *session_impl = conn.createSession();
        WT_SESSION *session = &session_impl->iface;
        REQUIRE(session->create(session, "file:cursor_test.wt", "key_format=i,value_format=i") ==
          0);
        REQUIRE(
          __wt_session_get_dhandle(session_impl, "file:cursor_test.wt", nullptr, nullptr, 0) == 0);

        WT_DATA_HANDLE *dhandle = session_impl->dhandle;
        REQUIRE(__wt_conn_dhandle_close(session_impl, false, true, false) == 0);
        REQUIRE(__wt_session_release_dhandle(session_impl) == 0);
        CHECK(F_ISSET(dhandle, WT_DHANDLE_DEAD));

        WT_SESSION_IMPL *session_impl_b = conn.createSession();
        int ret = __wt_session_get_dhandle(
          session_impl_b, "file:cursor_test.wt", nullptr, nullptr, WT_DHANDLE_SKIP_OPEN);
        CHECK(ret == EBUSY);
        CHECK(session_impl_b->dhandle == nullptr);
        CHECK(F_ISSET(dhandle, WT_DHANDLE_DEAD));
    }

    utils::wiredtigerCleanup(home);
}
