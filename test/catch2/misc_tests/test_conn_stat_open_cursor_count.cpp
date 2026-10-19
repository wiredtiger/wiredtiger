/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"

#include "../utils.h"
#include "../wrappers/connection_wrapper.h"

/*
 * The connection-level open cursor statistic sums a per-session counter across the whole session
 * array. Internal sessions opened by background subsystems hold cursors that never pass through the
 * application session API, so the walk must count them too. Driving an internal session directly
 * exercises that path, which the Python statistics suite cannot reach.
 */
TEST_CASE("conn stats: open cursor count includes internal sessions", "[statistics][session]")
{
    const std::string home = "WT_TEST.conn_stat_open_cursor_count";

    /* The connection wrapper creates the home directory and cleans it up on destruction. */
    connection_wrapper conn(home, "create,statistics=(all)");
    WT_CONNECTION_IMPL *conn_impl = conn.get_wt_connection_impl();
    WT_SESSION_IMPL *session = conn.create_session();

    int64_t *stat = &conn_impl->stats[0]->cursor_open_count;

    __wt_conn_stat_init(session);
    int64_t before = *stat;

    WT_SESSION_IMPL *internal_session = nullptr;
    REQUIRE(
      __wt_open_internal_session(conn_impl, "stat_internal", false, 0, 0, &internal_session) == 0);

    /*
     * Open a cursor on the internal session. A metadata cursor avoids creating a user table, so the
     * connection wrapper can remove the home directory on teardown and the test leaves no files
     * behind. The exact increment depends on cursor internals, so assert direction only.
     */
    WT_CURSOR *cursor = nullptr;
    REQUIRE(internal_session->iface.open_cursor(
              &internal_session->iface, "metadata:", nullptr, nullptr, &cursor) == 0);

    __wt_conn_stat_init(session);
    CHECK(*stat > before);

    REQUIRE(cursor->close(cursor) == 0);

    __wt_conn_stat_init(session);
    CHECK(*stat == before);

    REQUIRE(__wt_session_close_internal(internal_session) == 0);
}
