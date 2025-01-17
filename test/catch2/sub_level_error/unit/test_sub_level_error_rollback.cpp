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

/*
 * [sub_level_error_rollback]: test_sub_level_error_rollback.cpp
 * Tests the error handling for rollback workflows.
 */

using namespace utils;

TEST_CASE("Test functions for error handling in rollback workflows",
  "[sub_level_error_rollback],[sub_level_error]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_ERROR_INFO *err_info = &(session_impl->err_info);

    SECTION("Test WT_CACHE_OVERFLOW in __wti_evict_app_assist_worker")
    {
        WT_CURSOR *cursor;

        // Turn on eviction server and set eviction trigger, cache max wait and cache size to low
        // values.
        conn_impl->evict_server_running = true;
        conn_impl->evict->cache_max_wait_us = 1;
        conn_impl->evict->eviction_trigger = 1;
        conn_impl->cache_size = 1;

        // Create a table and insert key and value to create a page to evict.
        REQUIRE(session->create(session, "table:rollback", "key_format=S,value_format=S") == 0);
        session->open_cursor(session, "table:rollback", NULL, NULL, &cursor);
        session->begin_transaction(session, NULL);
        cursor->set_key(cursor, "key");
        cursor->set_value(cursor, "value");
        cursor->update(cursor);
        session->commit_transaction(session, NULL);
        cursor->close(cursor);

        CHECK(__wti_evict_app_assist_worker(session_impl, false, false, 100) == WT_ROLLBACK);
        check_error_info(err_info, WT_ROLLBACK, WT_CACHE_OVERFLOW, "Cache capacity has overflown");

        // Drop the table.
        session->drop(session, "table:rollback", NULL);
    }

    SECTION("Test WT_WRITE_CONFLICT in __txn_modify_block")
    {
        // Create a table and place a lock on it so session can have a set dhandle.
        REQUIRE(session->create(session, "table:rollback", "key_format=S,value_format=S") == 0);
        FLD_SET(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        REQUIRE(__wt_conn_dhandle_alloc(session_impl, "table:rollback", NULL) == 0);

        // Allocate update. The update type must not be WT_TXN_ABORTED (2), so we set it to
        // WT_UPDATE_TOMBSTONE (4).
        WT_UPDATE *upd;
        REQUIRE(__wt_upd_alloc(session_impl, NULL, WT_UPDATE_TOMBSTONE, &upd, NULL) == 0);

        // Transaction must be invisible, so we say that the session has a transaction snapshot and
        // that the transaction ID is greater than the max snap transaction ID.
        F_SET(session_impl->txn, WT_TXN_HAS_SNAPSHOT);
        session_impl->txn->snapshot_data.snap_max = 0;
        upd->txnid = 1;
        CHECK(__txn_modify_block(session_impl, NULL, upd, NULL));
        check_error_info(
          err_info, WT_ROLLBACK, WT_WRITE_CONFLICT, "Write conflict between concurrent operations");

        // Free update.
        __wt_free(session_impl, upd);

        // Clear lock so the table can be dropped.
        FLD_CLR(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        session->drop(session, "table:rollback", NULL);
    }

    SECTION("Test WT_OLDEST_FOR_EVICTION in __wt_txn_is_blocking")
    {
        // Set transaction as prepared.
        F_SET(session_impl->txn, WT_TXN_PREPARE);

        // Check is transaction is prepared
        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");
        // Clear flag.
        F_CLR(session_impl->txn, WT_TXN_PREPARE);

        // Check if there are no updates, the thread operation did not time
        // out and the operation is not running in a transaction.
        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Say that we have 1 modification.
        session_impl->txn->mod_count = 1;

        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Set operations timers to low value.
        session_impl->operation_start_us = session_impl->operation_timeout_us = 1;
        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");
        // Reset values.
        session_impl->operation_start_us = session_impl->operation_timeout_us = 0;

        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Set transaction running to true.
        F_SET(session_impl, WT_TXN_RUNNING);

        // Checking IDs
        CHECK(__wt_txn_is_blocking(session_impl) == 0);
        check_error_info(err_info, 0, WT_NONE, "");

        // Set transaction's ID to be equal to the oldest transaction ID.
        WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session_impl);
        txn_shared->id = S2C(session)->txn_global.oldest_id;

        CHECK(__wt_txn_is_blocking(session_impl) == WT_ROLLBACK);
        check_error_info(err_info, WT_ROLLBACK, WT_OLDEST_FOR_EVICTION,
          "Transaction has the oldest pinned transaction ID");

        // Set pinned ID to be equal to the oldest transaction ID.
        txn_shared->id = 0;
        txn_shared->pinned_id = S2C(session)->txn_global.oldest_id;

        CHECK(__wt_txn_is_blocking(session_impl) == WT_ROLLBACK);
        check_error_info(err_info, WT_ROLLBACK, WT_OLDEST_FOR_EVICTION,
          "Transaction has the oldest pinned transaction ID");

        // Reset back to the initial values.
        session_impl->txn->mod_count = 0;
        F_CLR(session_impl, WT_TXN_RUNNING);

        txn_shared->id = 0;
        txn_shared->pinned_id = 0;
    }
}
