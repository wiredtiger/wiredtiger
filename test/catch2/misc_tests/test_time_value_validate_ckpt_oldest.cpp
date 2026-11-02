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
 * A value time window from a page rebuilt out of deltas is not held to its parent aggregate when
 * its stop is already obsolete at the checkpoint's oldest timestamp, because the checkpoint's
 * writer would have dropped the cell before building the aggregate. Each timestamp the aggregate
 * bounds is exercised on its own.
 */
namespace {

enum class bound { durable_start, start, durable_stop, stop };

/* A parent aggregate wide enough that each of the cases below exceeds exactly one bound. */
WT_TIME_AGGREGATE
make_parent()
{
    WT_TIME_AGGREGATE ta;
    WT_TIME_AGGREGATE_INIT(&ta);
    ta.oldest_start_ts = 10;
    ta.newest_start_durable_ts = 20;
    ta.newest_stop_durable_ts = 40;
    ta.newest_stop_ts = 30;
    ta.newest_txn = 100;
    ta.newest_stop_txn = 100;
    return ta;
}

/* A time window inside the parent on every timestamp, then pushed outside one of them. */
WT_TIME_WINDOW
make_window(bound exceeded)
{
    WT_TIME_WINDOW tw;
    WT_TIME_WINDOW_INIT(&tw);
    tw.start_ts = tw.durable_start_ts = 12;
    tw.start_txn = 1;
    tw.stop_ts = tw.durable_stop_ts = 25;
    tw.stop_txn = 2;

    switch (exceeded) {
    case bound::durable_start:
        tw.start_ts = tw.durable_start_ts = 21;
        break;
    case bound::start:
        tw.start_ts = tw.durable_start_ts = 5;
        break;
    case bound::durable_stop:
        tw.durable_stop_ts = 41;
        break;
    case bound::stop:
        tw.stop_ts = tw.durable_stop_ts = 31;
        break;
    }
    return tw;
}

} // namespace

TEST_CASE("time value validate: checkpoint-obsolete cells from deltas", "[time_window][verify]")
{
    const std::string home = "WT_TEST.time_value_validate_ckpt_oldest";
    connection_wrapper conn(home, "create");
    WT_CONNECTION_IMPL *conn_impl = conn.get_wt_connection_impl();
    WT_SESSION_IMPL *session = conn.create_session();

    WT_DATA_HANDLE dhandle;
    WT_CLEAR(dhandle);
    session->dhandle = &dhandle;

    WT_TIME_AGGREGATE parent = make_parent();

    /* Every window is valid against a parent wide enough to hold it. */
    SECTION("a window inside the parent passes")
    {
        WT_TIME_WINDOW tw = make_window(bound::stop);
        tw.stop_ts = tw.durable_stop_ts = 25;
        tw.start_ts = tw.durable_start_ts = 12;
        for (bool from_delta : {false, true})
            CHECK(__wt_time_value_validate(session, &tw, &parent, from_delta, true) == 0);
    }

    {
        const bound b =
          GENERATE(bound::durable_start, bound::start, bound::durable_stop, bound::stop);
        WT_TIME_WINDOW tw = make_window(b);

        SECTION("a full image still fails when the window exceeds the parent")
        {
            conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = 50;
            CHECK(__wt_time_value_validate(session, &tw, &parent, false, true) == EINVAL);
        }

        SECTION("a delta page tolerates it when the stop is obsolete at the checkpoint")
        {
            conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = 50;
            CHECK(__wt_time_value_validate(session, &tw, &parent, true, true) == 0);
        }

        SECTION("a delta page fails when the stop is newer than the checkpoint's oldest")
        {
            conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = 20;
            CHECK(__wt_time_value_validate(session, &tw, &parent, true, true) == EINVAL);
        }

        SECTION("a delta page fails when the checkpoint has no oldest timestamp")
        {
            conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = WT_TS_NONE;
            CHECK(__wt_time_value_validate(session, &tw, &parent, true, true) == EINVAL);
        }

        SECTION("a delta page fails when the stop is prepared")
        {
            /* A prepared stop is not an obsolete stop, so the start bounds are still enforced. */
            if (b == bound::durable_start || b == bound::start) {
                conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = 50;
                tw.stop_prepare_ts = 30;
                tw.stop_prepared_id = 1;
                tw.stop_ts = WT_TS_MAX;
                tw.durable_stop_ts = WT_TS_NONE;
                CHECK(__wt_time_value_validate(session, &tw, &parent, true, true) == EINVAL);
            }
        }
    }

    SECTION("a window with no stop gets no tolerance on a delta page")
    {
        conn_impl->disaggregated_storage.last_checkpoint_oldest_timestamp = 50;
        WT_TIME_WINDOW tw = make_window(bound::durable_start);
        tw.stop_ts = WT_TS_MAX;
        tw.durable_stop_ts = WT_TS_NONE;
        tw.stop_txn = WT_TXN_MAX;
        CHECK(__wt_time_value_validate(session, &tw, &parent, true, true) == EINVAL);
    }

    session->dhandle = nullptr;
}
