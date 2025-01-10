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

    SECTION("Test __wti_evict_app_assist_worker")
    {
        WT_CURSOR *cursor;

        // Set eviction trigger, cache max wait and cache size to low values.
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

    SECTION("Test __txn_modify_block")
    {
        // Create a table so session can have a set dhandle.
        REQUIRE(session->create(session, "table:rollback", "key_format=S,value_format=S") == 0);
        FLD_SET(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        REQUIRE(__wt_conn_dhandle_alloc(session_impl, "table:rollback", NULL) == 0);

        // Allocate update.
        WT_UPDATE *upd;
        REQUIRE(__wt_upd_alloc(session_impl, NULL, 2, &upd, NULL) == 0);

        // Transaction must be invisible, so we say that the session have a transaction snapshot
        // and the transaction id is greater than the max snap transaction id.
        F_SET(session_impl->txn, WT_TXN_HAS_SNAPSHOT);
        session_impl->txn->snapshot_data.snap_max = 0;
        upd->txnid = 1;
        upd->type = 1;
        CHECK(__txn_modify_block(session_impl, NULL, upd, NULL));
        check_error(session_impl, WT_ROLLBACK, WT_WRITE_CONFLICT,
          "Write conflict between concurrent operations");

        // Clear lock so the table can be dropped.
        FLD_CLR(session_impl->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST);
        session->drop(session, "table:rollback", NULL);
    }

    SECTION("Test __wt_txn_is_blocking")
    {
        // Allow rollbacks.
        session_impl->txn->mod_count = 1;
        session_impl->operation_start_us = 1;
        session_impl->operation_timeout_us = 1;
        F_SET(session_impl, WT_TXN_RUNNING);

        // Set transaction id and oldest pinned id to be the same.
        WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session_impl);
        txn_shared->id = 2;
        txn_shared->pinned_id = 2;

        CHECK(__wt_txn_is_blocking(session_impl) == WT_ROLLBACK);
        check_error(session_impl, WT_ROLLBACK, WT_OLDEST_FOR_EVICTION,
          "Transaction has the oldest pinned transaction ID");

        // Reset back to the initial values.
        session_impl->txn->mod_count = 0;
        session_impl->operation_start_us = 0;
        session_impl->operation_timeout_us = 0;
        F_CLR(session_impl, WT_TXN_RUNNING);

        txn_shared->id = 0;
        txn_shared->pinned_id = 0;
    }
}
