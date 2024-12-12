/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/mock_session.h"

/*
 * [session_get_last_error]: test_session_get_last_error.cpp
 * Tests the API for getting verbose information about the last error of the session
 */

TEST_CASE("Session get last error - test getting verbose info about the last error in the session",
  "[session_get_last_error]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();

    SECTION("Test API placeholder")
    {
        WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();

        /* Prepare return arguments */
        int temp1;
        int temp2;
        char *temp3;
        int *err = &temp1;
        int *sub_level_err = &temp2;
        char **err_msg = &temp3;

        /* Call placeholder API */
        __ut_session_get_last_error(session, err, sub_level_err, (const char **)err_msg);

        /* Test the API placeholder returns expected placeholder values */
        REQUIRE(*err == 0);
        REQUIRE(*sub_level_err == 0);
        REQUIRE(strcmp(*err_msg, "Placeholder error message") == 0);

        free(*err_msg);
    }
}
