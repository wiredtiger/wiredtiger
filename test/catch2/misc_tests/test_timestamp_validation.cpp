/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <iostream>
#include <string>
#include "../wrappers/mock_session.h"

static constexpr wt_timestamp_t stable_timestamp = 5;
static constexpr wt_timestamp_t oldest_timestamp = 3;
static constexpr wt_timestamp_t checkpoint_timestamp = 7;

static void
set_connection_timestamps(WT_SESSION_IMPL *session)
{
    WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;

    __wt_atomic_store_uint64_relaxed(&txn_global->stable_timestamp, stable_timestamp);
    __wt_atomic_store_bool_release(&txn_global->has_stable_timestamp, true);
    __wt_atomic_store_uint64_relaxed(&txn_global->oldest_timestamp, oldest_timestamp);
    __wt_atomic_store_bool_release(&txn_global->has_oldest_timestamp, true);
}

static void
check_connection_timestamps(const std::string &message)
{
    char ts_string[WT_TS_INT_STRING_SIZE];
    const std::string stable_message = "connection stable timestamp " +
      std::string(__wt_timestamp_to_string(stable_timestamp, ts_string));
    const std::string oldest_message = "connection oldest timestamp " +
      std::string(__wt_timestamp_to_string(oldest_timestamp, ts_string));

    CHECK(message.find(stable_message) != std::string::npos);
    CHECK(message.find(oldest_message) != std::string::npos);
}

TEST_CASE("Display current aggregate WT_TIME_VALIDATE_RET failure", "[timestamp]")
{
    auto session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    WT_TIME_AGGREGATE ta = {};

    set_connection_timestamps(session);
    ta.oldest_start_ts = 20;
    ta.newest_stop_ts = 10;

    CHECK(__wt_time_aggregate_validate(session, &ta, NULL, false) == EINVAL);
    check_connection_timestamps(session_mock->get_last_message());
    std::cout << "Aggregate validation error: " << session_mock->get_last_message() << std::endl;
}

TEST_CASE("Display current value WT_TIME_VALIDATE_RET failure", "[timestamp]")
{
    auto session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    WT_TIME_WINDOW tw = {};

    set_connection_timestamps(session);
    tw.start_ts = 20;
    tw.stop_ts = 10;

    CHECK(__wt_time_value_validate(session, &tw, NULL, false, false) == EINVAL);
    check_connection_timestamps(session_mock->get_last_message());
    std::cout << "Value validation error: " << session_mock->get_last_message() << std::endl;
}

TEST_CASE("Display current aggregate WT_TIME_ERROR failure", "[timestamp]")
{
    auto session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    WT_TIME_AGGREGATE parent, ta;

    session_mock->setup_block_manager_file_operations();
    set_connection_timestamps(session);
    WT_TIME_AGGREGATE_INIT(&parent);
    WT_TIME_AGGREGATE_INIT(&ta);
    ta.newest_start_durable_ts = 10;
    ta.oldest_start_ts = 10;

    CHECK(__wt_time_aggregate_validate(session, &ta, &parent, false) == EINVAL);
    check_connection_timestamps(session_mock->get_last_message());
    std::cout << "Aggregate stable-point validation error: " << session_mock->get_last_message()
              << std::endl;
}

TEST_CASE("Display current value WT_TIME_ERROR failure", "[timestamp]")
{
    auto session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    WT_TIME_AGGREGATE parent;
    WT_TIME_WINDOW tw = {};

    session_mock->setup_block_manager_file_operations();
    set_connection_timestamps(session);
    WT_TIME_AGGREGATE_INIT(&parent);
    tw.durable_start_ts = 10;
    tw.start_ts = 10;
    tw.stop_ts = WT_TS_MAX;
    tw.stop_txn = WT_TXN_MAX;

    CHECK(__wt_time_value_validate(session, &tw, &parent, false, false) == EINVAL);
    check_connection_timestamps(session_mock->get_last_message());
    std::cout << "Value stable-point validation error: " << session_mock->get_last_message()
              << std::endl;
}

TEST_CASE("Display current value delta WT_TIME_ERROR failure", "[timestamp]")
{
    auto session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    WT_TIME_AGGREGATE parent;
    WT_TIME_WINDOW tw = {};
    char ts_string[WT_TS_INT_STRING_SIZE];
    const std::string checkpoint_message = "stable time " +
      std::string(__wt_timestamp_to_string(checkpoint_timestamp, ts_string));

    session_mock->setup_block_manager_file_operations();
    set_connection_timestamps(session);
    WT_TIME_AGGREGATE_INIT(&parent);
    S2BT(session)->checkpoint_timestamp = checkpoint_timestamp;
    tw.durable_start_ts = checkpoint_timestamp + 1;
    tw.start_ts = checkpoint_timestamp + 1;
    tw.stop_ts = WT_TS_MAX;
    tw.stop_txn = WT_TXN_MAX;

    CHECK(__wt_time_value_validate(session, &tw, &parent, true, false) == EINVAL);
    check_connection_timestamps(session_mock->get_last_message());
    CHECK(session_mock->get_last_message().find(checkpoint_message) != std::string::npos);
    std::cout << "Value delta stable-point validation error: " << session_mock->get_last_message()
              << std::endl;
}
