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
#include "../utils.h"

/*
 * [session_set_last_error]: test_session_set_last_error.cpp
 * Tests the function for storing verbose information about the last error of the session.
 */

TEST_CASE("Session set last error - test storing verbose info about the last error in the session",
  "[session_set_last_error]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_ERROR_INFO *err_info = &(session_impl->err_info);

    SECTION("Test with initial values")
    {
        const char *err_msg_content = WT_ERROR_INFO_EMPTY;
        REQUIRE(__wt_session_set_last_error(session_impl, 0, WT_NONE, err_msg_content) == 0);
        utils::check_error_info(err_info, 0, WT_NONE, err_msg_content);
    }

    SECTION("Test with EINVAL error")
    {
        const char *err_msg_content = "Some EINVAL error";
        REQUIRE(__wt_session_set_last_error(session_impl, EINVAL, WT_NONE, err_msg_content) == 0);
        utils::check_error_info(err_info, EINVAL, WT_NONE, err_msg_content);
    }

    SECTION("Test with multiple errors")
    {
        const char *err_msg_content_EINVAL = "Some EINVAL error";
        REQUIRE(__wt_session_set_last_error(session_impl, 0, WT_NONE, WT_ERROR_INFO_EMPTY) == 0);
        utils::check_error_info(err_info, 0, WT_NONE, WT_ERROR_INFO_EMPTY);
        REQUIRE(
          __wt_session_set_last_error(session_impl, EINVAL, WT_NONE, err_msg_content_EINVAL) == 0);
        utils::check_error_info(err_info, EINVAL, WT_NONE, err_msg_content_EINVAL);
        REQUIRE(
          __wt_session_set_last_error(session_impl, EINVAL, WT_NONE, err_msg_content_EINVAL) == 0);
        utils::check_error_info(err_info, EINVAL, WT_NONE, err_msg_content_EINVAL);
        REQUIRE(__wt_session_set_last_error(session_impl, 0, WT_NONE, WT_ERROR_INFO_SUCCESS) == 0);
        utils::check_error_info(err_info, 0, WT_NONE, WT_ERROR_INFO_SUCCESS);
    }

    SECTION("Test with large error message")
    {
        const char *err_msg_content =
          "WiredTiger is a production quality, high performance, scalable, NoSQL, Open Source "
          "extensible platform for data management. WiredTiger is developed and maintained by "
          "MongoDB, Inc., where it is the principal database storage engine. WiredTiger supports "
          "row-oriented storage (where all columns of a row are stored together), and "
          "column-oriented storage (where columns are stored in groups, allowing for more "
          "efficient access and storage of column subsets). WiredTiger includes ACID transactions "
          "with standard isolation levels and durability at both checkpoint and commit-level "
          "granularity. WiredTiger can be used as a simple key/value store, but also has a "
          "complete schema layer, including indices and projections.";
        REQUIRE(__wt_session_set_last_error(session_impl, EINVAL, WT_NONE, err_msg_content) == 0);
        utils::check_error_info(err_info, EINVAL, WT_NONE, err_msg_content);
    }
}
