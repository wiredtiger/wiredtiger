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
#include <thread>
#include <chrono>

/*
 * [wt_compact]: test_compact.cpp
 * Tests the error handling for compact workflows.
 */

TEST_CASE("Test functions for compaction", "[compact]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;

    SECTION("Test __wt_background_compact_signal")
    {
        // Set background compaction running to true and make the configuration to an empty string.
        conn_impl->background_compact.running = true;
        conn_impl->background_compact.config = "";

        CHECK(__wt_background_compact_signal(session_impl, "background=true") == EINVAL);
        const char *err_msg_content =
          "Cannot reconfigure background compaction while it's already running.";
        CHECK(session_impl->err_info.err == EINVAL);
        CHECK(session_impl->err_info.sub_level_err == WT_BACKGROUND_COMPACT_ALREADY_RUNNING);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);

        // Reset back to the initial values.
        conn_impl->background_compact.running = false;
        conn_impl->background_compact.config = NULL;
    }
}
