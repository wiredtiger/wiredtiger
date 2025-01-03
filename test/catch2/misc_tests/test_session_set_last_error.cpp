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
 * [session_set_last_error]: test_session_set_last_error.cpp
 * Tests the function for storing verbose information about the last error of the session.
 */

TEST_CASE("Session set last error - test storing verbose info about the last error in the session",
  "[session_set_last_error]")
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;
    const char *err_msg_content;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    session_impl = (WT_SESSION_IMPL *)session;

    SECTION("Test with initial values")
    {
        err_msg_content = "";
        CHECK(__wt_session_set_last_error(session_impl, 0, WT_NONE, err_msg_content) == 0);
        CHECK(session_impl->err_info.err == 0);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }

    SECTION("Test with EINVAL error")
    {
        err_msg_content = "Some EINVAL error";
        CHECK(__wt_session_set_last_error(session_impl, EINVAL, WT_NONE, err_msg_content) == 0);
        CHECK(session_impl->err_info.err == EINVAL);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }
}
