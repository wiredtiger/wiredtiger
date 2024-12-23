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
 * [session_get_last_error]: test_session_get_last_error.cpp
 * Tests the API for getting verbose information about the last error of the session.
 */

static void
check_error(
  WT_SESSION *session, int expected_err, int expected_sub_level_err, const char *expected_err_msg)
{
    /* Prepare return arguments. */
    int err, sub_level_err;
    const char *err_msg;

    /* Call the error info API. */
    session->get_last_error(session, &err, &sub_level_err, &err_msg);

    /* Test that the API returns expected values. */
    CHECK(err == expected_err);
    CHECK(sub_level_err == expected_sub_level_err);
    CHECK(strcmp(err_msg, expected_err_msg) == 0);
}

TEST_CASE("Session get last error - test getting verbose info about the last error in the session",
  "[session_get_last_error]")
{
    WT_CONNECTION *conn;
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    SECTION("Test default values")
    {
        check_error(session, 0, WT_NONE, "");
    }

    SECTION("Test max sessions")
    {
        /* Attempt opening 33000 sessions (max amount configured for a user).*/
        for (int i = 0; i < 33; ++i) {
            conn->open_session(conn, NULL, NULL, &session);
        }

        check_error(
          session, WT_ERROR, WT_SESSION_MAX, "out of sessions (including internal sessions)");
    }

    SECTION("Test compaction already running")
    {
        check_error(session, EINVAL, WT_COMPACTION_ALREADY_RUNNING,
          "cannot reconfigure background compaction while it's already running");
    }
}
