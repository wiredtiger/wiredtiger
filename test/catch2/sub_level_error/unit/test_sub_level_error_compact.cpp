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

TEST_CASE("Test functions for error handling in compaction workflows",
  "[sub_level_error_compact],[sub_level_error]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    WT_ERROR_INFO *err_info = &(session_impl->err_info);

    SECTION("Test __wt_background_compact_signal - in-memory or readonly database")
    {
        // Set database as in-memory and readonly.
        F_SET(conn_impl, WT_CONN_IN_MEMORY | WT_CONN_READONLY);
        CHECK(__wt_background_compact_signal(session_impl, NULL) == ENOTSUP);
        check_error_info(err_info, 0, WT_NONE, "");
        F_CLR(conn_impl, WT_CONN_IN_MEMORY | WT_CONN_READONLY);
    }

    SECTION("Test __wt_background_compact_signal - spin lock")
    {
        CHECK(__wt_background_compact_signal(session_impl, "background=true") == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        CHECK(__wt_background_compact_signal(session_impl, NULL) == EBUSY);
        check_error_info(
          err_info, EBUSY, WT_NONE, "Background compact is busy processing a previous command");
    }

    SECTION("Test __wt_background_compact_signal - invalid config string")
    {
        // Config string doesn't contain background key.
        CHECK(__wt_background_compact_signal(session_impl, "") == WT_NOTFOUND);
        check_error_info(err_info, 0, WT_NONE, "");
    }

    SECTION("Test __wt_background_compact_signal - compact configuration")
    {
        // Set background compaction config string to false.
        CHECK(__wt_background_compact_signal(session_impl, "background=false") == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Set background compaction config string to true.
        CHECK(__wt_background_compact_signal(session_impl, "background=true") == 0);
        check_error_info(err_info, 0, WT_NONE, "");
        __wt_free(session_impl, conn_impl->background_compact.config);
        conn_impl->background_compact.config = "";
        // Wait for lock on background compaction to be released.
        __wt_sleep(0, 100);

        // Set background compaction running to true and background compaction config to match
        // the base config.
        conn_impl->background_compact.running = true;
        conn_impl->background_compact.config =
          "dryrun=false,exclude=,free_space_target=20MB,run_once=false,timeout=1200";

        CHECK(__wt_background_compact_signal(session_impl, "background=true") == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Set background compaction config to not match base config.
        conn_impl->background_compact.config = "";

        CHECK(__wt_background_compact_signal(session_impl, "background=true") == EINVAL);
        check_error_info(err_info, EINVAL, WT_BACKGROUND_COMPACT_ALREADY_RUNNING,
          "Cannot reconfigure background compaction while it's already running.");

        // Reset back to the initial values.
        conn_impl->background_compact.running = false;
        conn_impl->background_compact.config = NULL;
    }
}
