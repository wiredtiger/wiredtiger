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
 * [wt_rollback]: test_rollback.cpp
 * Tests the error handling for rollback workflows.
 */

void
check_error(WT_SESSION_IMPL *session, int error, int sub_level_error, std::string error_msg_content)
{
    CHECK(session->err_info.err == error);
    CHECK(session->err_info.sub_level_err == sub_level_error);
    CHECK(session->err_info.err_msg == error_msg_content);
}

TEST_CASE("Test functions for rollback workflows", "[rollback]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;

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
        check_error(session_impl, WT_ROLLBACK, WT_CACHE_OVERFLOW, "Cache capacity has overflown");

        // Drop the table.
        session->drop(session, "table:rollback", NULL);
    }

    SECTION("Test WT_WRITE_CONFLICT in __txn_modify_block")
    {
        // Create a table and place a lock on it so session can have a set dhandle.
        REQUIRE(session->create(session, "table:rollback", "key_format=S,value_format=S") == 0);
        FLD_SET(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        REQUIRE(__wt_conn_dhandle_alloc(session_impl, "table:rollback", NULL) == 0);

        // Allocate update.
        WT_UPDATE *upd;
        REQUIRE(__wt_upd_alloc(session_impl, NULL, 2, &upd, NULL) == 0);

        /*
         * Transaction must be invisible, so we say that the session has a transaction snapshot and
         * that the transaction ID is greater than the max snap transaction ID. The update type must
         * not be WT_TXN_ABORTED (2), so we set it to 1.
         */
        F_SET(session_impl->txn, WT_TXN_HAS_SNAPSHOT);
        session_impl->txn->snapshot_data.snap_max = 0;
        upd->txnid = 1;
        upd->type = 1;
        CHECK(__txn_modify_block(session_impl, NULL, upd, NULL));
        check_error(session_impl, WT_ROLLBACK, WT_WRITE_CONFLICT,
          "Write conflict between concurrent operations");

        // Free update.
        __wt_free(session_impl, upd);

        // Clear lock so the table can be dropped.
        FLD_CLR(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        session->drop(session, "table:rollback", NULL);
    }

    SECTION("Test WT_OLDEST_FOR_EVICTION in __wt_txn_is_blocking")
    {
        // Say that we have 1 change to make and set transaction running to true.
        session_impl->txn->mod_count = 1;
        F_SET(session_impl, WT_TXN_RUNNING);

        // Set transaction's ID and pinned ID to be equal to the oldest transaction ID.
        WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session_impl);
        txn_shared->id = txn_shared->pinned_id = S2C(session)->txn_global.oldest_id;

        CHECK(__wt_txn_is_blocking(session_impl) == WT_ROLLBACK);
        check_error(session_impl, WT_ROLLBACK, WT_OLDEST_FOR_EVICTION,
          "Transaction has the oldest pinned transaction ID");

        // Reset back to the initial values.
        session_impl->txn->mod_count = 0;
        F_CLR(session_impl, WT_TXN_RUNNING);

        txn_shared->id = 0;
        txn_shared->pinned_id = 0;
    }
}
