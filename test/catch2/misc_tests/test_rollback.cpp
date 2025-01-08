/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

/*
 * [wt_rollback]: test_rollback.cpp
 * Tests the error handling for rollback workflows.
 */

void
check_error(WT_SESSION_IMPL *session, int error, int sub_level_error, std::string error_msg_content)
{
    CHECK(session->err_info.err == error);
    CHECK(session->err_info.sub_level_err == sub_level_error);
    CHECK(session->err_info.err_msg == error_msg_content);
}

TEST_CASE("Test functions for rollback workflows", "[rollback]")
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    session_impl = (WT_SESSION_IMPL *)session;

    SECTION("Test __wti_evict_app_assist_worker")
    {
        CHECK(__wti_evict_app_assist_worker(session_impl, true, true, 1.0) == WT_ROLLBACK);
        check_error(session_impl, WT_ROLLBACK, WT_CACHE_OVERFLOW, "Cache capacity has overflown");
    }

    SECTION("Test __wt_txn_is_blocking")
    {
        CHECK(__wt_txn_is_blocking(session_impl) == WT_ROLLBACK);
        check_error(session_impl, WT_ROLLBACK, WT_WRITE_CONFLICT,
          "Write conflict between concurrent operations");
    }

    SECTION("Test __txn_modify_block")
    {
        // CHECK(__txn_modify_block(session_impl,))
        check_error(session_impl, WT_ROLLBACK, WT_OLDEST_FOR_EVICTION,
          "Transaction has the oldest pinned transaction ID");
    }
}
