/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../../wrappers/connection_wrapper.h"
#include "../utils_sub_level_error.h"
#include <vector>

/*
 * [sub_level_error_compact]: test_sub_level_error_compact.cpp
 * Tests the error handling for compact workflows.
 */

using namespace utils;

void
check_compact(WT_CONNECTION_IMPL *connection, WT_SESSION_IMPL *session, bool error,
  const char *config, bool running, const char *background_compact_config)
{
    WT_ERROR_INFO *err_info = &(session->err_info);
    int expected_error = 0;
    int expected_sub_level_error = WT_NONE;
    const char *expected_error_msg = "";

    if (error) {
        expected_error = EINVAL;
        expected_sub_level_error = WT_BACKGROUND_COMPACT_ALREADY_RUNNING;
        expected_error_msg = "Cannot reconfigure background compaction while it's already running.";
    }

    connection->background_compact.running = running;
    connection->background_compact.config = background_compact_config;

    CHECK(__wt_background_compact_signal(session, config) == expected_error);
    check_error_info(err_info, expected_error, expected_sub_level_error, expected_error_msg);
}

TEST_CASE("Test functions for error handling in compaction workflows",
  "[sub_level_error_compact],[sub_level_error]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;

    SECTION("Test __wt_background_compact_signal")
    {
        // Tests all possible branches for the if statement which sets the sub_level error.
        std::vector<std::tuple<bool, const char *, bool, const char *>> branches = {
          {false, "background=false", false, NULL},
          {false, "background=true", false, NULL},
          {false, "background=true", true,
            "dryrun=false,exclude=,free_space_target=20MB,run_once=false,timeout=1200"},
          {true, "background=true", true, ""},
        };

        for (const auto &tuple : branches) {
            const auto &error = std::get<0>(tuple);
            const auto &config = std::get<1>(tuple);
            const auto &running = std::get<2>(tuple);
            const auto &background_compact_config = std::get<3>(tuple);

            check_compact(
              conn_impl, session_impl, error, config, running, background_compact_config);

            // Allow enough time for function to release background compact lock.
            __wt_sleep(0, 100);
        }

        // Reset back to the initial values.
        conn_impl->background_compact.running = false;
        conn_impl->background_compact.config = NULL;
    }
}
