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

TEST_CASE("data handle statistics: default bucket count", "[statistics][dhandle]")
{
    connection_wrapper conn("WT_TEST.dhandle_stat_default", "create,statistics=(all)");
    CHECK(conn.get_wt_connection_impl()->stat_dsrc_buckets == 23);
}

TEST_CASE("data handle statistics: configured bucket count", "[statistics][dhandle]")
{
    const uint32_t buckets = GENERATE(1, 3, 4, 23, 64, 1024);
    const std::string config =
      "create,statistics=(all),statistics_dhandle_buckets=" + std::to_string(buckets);
    connection_wrapper conn("WT_TEST.dhandle_stat_buckets", config.c_str());
    WT_CONNECTION_IMPL *conn_impl = conn.get_wt_connection_impl();
    REQUIRE(conn_impl->stat_dsrc_buckets == buckets);

    WT_SESSION_IMPL *session = conn.create_session();
    WT_DATA_HANDLE handle = {};
    REQUIRE(__wt_stat_dsrc_init(session, &handle) == 0);
    for (uint32_t i = 0; i < buckets; ++i) {
        REQUIRE(handle.stats[i] == &handle.stat_array[i]);
        handle.stats[i]->cursor_insert = 1;
    }

    CHECK(WT_STAT_DSRC_READ(session, handle.stats, cursor_insert) == buckets);
    WT_DSRC_STATS aggregate = {};
    __wt_stat_dsrc_aggregate(session, handle.stats, &aggregate);
    CHECK(aggregate.cursor_insert == buckets);

    WT_STATP_DSRC_SET(session, handle.stats, cursor_insert, 7);
    CHECK(handle.stats[0]->cursor_insert == 7);
    for (uint32_t i = 1; i < buckets; ++i)
        CHECK(handle.stats[i]->cursor_insert == 0);
    CHECK(WT_STAT_DSRC_READ(session, handle.stats, cursor_insert) == 7);

    for (uint32_t i = 0; i < buckets; ++i)
        handle.stats[i]->cursor_insert = 1;
    __wt_stat_dsrc_clear_all(session, handle.stats, 0);
    CHECK(WT_STAT_DSRC_READ(session, handle.stats, cursor_insert) == 0);

    for (uint32_t i = 0; i < 35; ++i) {
        WT_SESSION_IMPL *other = conn.create_session();
        CHECK(other->stat_dsrc_bucket == other->id % buckets);
        CHECK(other->stat_conn_bucket == other->id % WT_STAT_CONN_COUNTER_SLOTS);
        WT_STATP_DSRC_INCR(other, handle.stats, cursor_insert);
    }
    CHECK(WT_STAT_DSRC_READ(session, handle.stats, cursor_insert) == 35);

    __wt_stat_dsrc_discard(session, &handle);
    CHECK(handle.stat_array == nullptr);
    CHECK(handle.stats == nullptr);
}

TEST_CASE("data handle statistics: bucket count is connection local", "[statistics][dhandle]")
{
    const std::string home = "WT_TEST.dhandle_stat_reopen";
    {
        connection_wrapper conn(home, "create,statistics_dhandle_buckets=7");
        CHECK(conn.get_wt_connection_impl()->stat_dsrc_buckets == 7);
        conn.clear_do_cleanup();
    }
    {
        connection_wrapper conn(home, "statistics_dhandle_buckets=64");
        CHECK(conn.get_wt_connection_impl()->stat_dsrc_buckets == 64);
        conn.clear_do_cleanup();
    }
    {
        connection_wrapper conn(home);
        CHECK(conn.get_wt_connection_impl()->stat_dsrc_buckets == 23);
    }
}
