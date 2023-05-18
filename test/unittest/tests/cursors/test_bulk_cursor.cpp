/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <thread>
#include <catch2/catch.hpp>
#include <iostream>
#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../wrappers/item_wrapper.h"

static int
insert_key_value(WT_CURSOR *cursor, const char *key, const char *value)
{
    item_wrapper item_key(key);
    item_wrapper item_value(value);
    __wt_cursor_set_raw_key(cursor, item_key.get_item());
    __wt_cursor_set_raw_value(cursor, item_value.get_item());
    return cursor->insert(cursor);
}

static void
insert_sample_values(WT_CURSOR *cursor)
{
    REQUIRE(insert_key_value(cursor, "key1", "value1") == 0);
    REQUIRE(insert_key_value(cursor, "key2", "value2") == 0);
    REQUIRE(insert_key_value(cursor, "key3", "value3") == 0);
    REQUIRE(insert_key_value(cursor, "key4", "value4") == 0);
    REQUIRE(insert_key_value(cursor, "key5", "value5") == 0);
}

static void
thread_function_checkpoint(WT_SESSION *session)
{
    session->checkpoint(session, NULL);
}

static void
thread_function_drop(WT_SESSION *session, std::string const &uri)
{
    session->drop(session, uri.c_str(), "force=true");
}


static void
print_dhandles(WT_SESSION_IMPL *session_impl)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    printf("Session dhandle: %p\n", session_impl->dhandle);
    conn = S2C(session_impl);

    TAILQ_FOREACH (dhandle, &conn->dhqh, q) {
        printf(".   dhandle 0c%p\n", dhandle);
    }
}

static bool
check_txn_updates(std::string const &label, WT_SESSION_IMPL *session_impl)
{
    bool ok = true;
    WT_TXN *txn = session_impl->txn;

    printf("check_txn_updates() - %s\n", label.c_str());
    print_dhandles(session_impl);
    printf("  txn = 0x%p, txn->id = 0x%llx, txn->mod = 0x%p, txn->mod_count = %u\n", txn, txn->id, txn->mod, txn->mod_count);

    WT_TXN_OP *op = txn->mod;
    for (int i = 0; i < txn->mod_count; i++, op++) {
        switch (op->type) {
        case WT_TXN_OP_NONE:
        case WT_TXN_OP_REF_DELETE:
        case WT_TXN_OP_TRUNCATE_COL:
        case WT_TXN_OP_TRUNCATE_ROW:
            break;
        case WT_TXN_OP_BASIC_COL:
        case WT_TXN_OP_BASIC_ROW:
        case WT_TXN_OP_INMEM_COL:
        case WT_TXN_OP_INMEM_ROW:
            WT_UPDATE *upd = op->u.op_upd;
            printf("    mod %i, op->type = %i, upd->txnid = 0x%llx\n", i, op->type, upd->txnid);
            break;
        }
    }

    return ok;
}

static void
cursor_test(std::string const &config, bool close, int expected_commit_result)
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();
    WT_SESSION *session = &session_impl->iface;
    std::string uri = "table:cursor_test";

    REQUIRE(session->create(session, uri.c_str(), "key_format=S,value_format=S") == 0);
    REQUIRE(session->begin_transaction(session, "") == 0);

    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri.c_str(), nullptr, config.c_str(), &cursor) == 0);

    insert_sample_values(cursor);

    std::string close_as_string = close ? "true" : "false";

    SECTION("Checkpoint during transaction then commit: config = " + config +
      ", close = " + close_as_string)
    {
        int result = session->checkpoint(session, NULL);
        REQUIRE(result == EINVAL);

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Checkpoint in 2nd thread during transaction then commit: config = " + config +
      ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_checkpoint(session); });
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Drop in 2nd thread during transaction then commit: config = " + config +
      ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_drop(session, uri); });
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Checkpoint in 2nd thread during transaction then rollback: config = " + config +
      ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_checkpoint(session); });
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->rollback_transaction(session, "") == 0);
    }

    SECTION("Drop then checkpoint in one thread: config = " + config +
      ", close = " + close_as_string)
    {
        check_txn_updates("before close", session_impl);
        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
            check_txn_updates("before drop", session_impl);
            sleep(1);
            REQUIRE(session->drop(session, uri.c_str(), "force=true") == 0);
        } else {
            int result = session->drop(session, uri.c_str(), "force=true");
            REQUIRE(result == EBUSY);
        }
        printf("After drop\n");

        sleep(3);
        check_txn_updates("before checkpoint", session_impl);
        REQUIRE(session->checkpoint(session, nullptr) == EINVAL);
        sleep(1);
        check_txn_updates("before commit", session_impl);

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
        check_txn_updates("after commit", session_impl);
    }
}


static void
multiple_drop_test(std::string const &config, int expected_commit_result, bool do_sleep)
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();
    WT_SESSION *session = &session_impl->iface;
    std::string uri = "table:cursor_test";

    std::string sleep_as_string = do_sleep ? "true" : "false";

    SECTION("Multiple drop test: config = " + config + ", sleep = " + sleep_as_string)
    {
        int count = 0;
        const int limit = 5;

        while (count < limit) {
            count++;

            REQUIRE(session->create(session, uri.c_str(), "key_format=S,value_format=S") == 0);
            REQUIRE(session->begin_transaction(session, "") == 0);

            WT_CURSOR *cursor = nullptr;
            REQUIRE(
              session->open_cursor(session, uri.c_str(), nullptr, config.c_str(), &cursor) == 0);

            insert_sample_values(cursor);

            check_txn_updates("before close", session_impl);
            REQUIRE(cursor->close(cursor) == 0);
            printf("After close\n");

            if (do_sleep)
                sleep(1);

            check_txn_updates("before drop", session_impl);
            REQUIRE(session->drop(session, uri.c_str(), "force=true") == 0);
            printf("After drop\n");

            if (do_sleep)
                sleep(3);

            check_txn_updates("before checkpoint", session_impl);
            REQUIRE(session->checkpoint(session, nullptr) == EINVAL);

            if (do_sleep)
                sleep(1);

            check_txn_updates("before commit", session_impl);
            REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
            check_txn_updates("after commit", session_impl);
        }

        // Confirm the correct number of loops were executed
        REQUIRE(count == limit);
    }
}

TEST_CASE("Cursor: checkpoint during transaction()", "[cursor]")
{
    cursor_test("", false, EINVAL);
    cursor_test("", true, EINVAL);
    cursor_test("bulk", false, 0);
    cursor_test("bulk", true, 0);

    multiple_drop_test("", EINVAL, false);
    multiple_drop_test("", EINVAL, true);
    multiple_drop_test("bulk", 0, false);
    multiple_drop_test("bulk", 0, true);
}
