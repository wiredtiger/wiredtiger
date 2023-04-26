/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <thread>
#include <catch2/catch.hpp>
#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../wrappers/item_wrapper.h"

static void
init_wt_item(WT_ITEM &item)
{
    item.data = nullptr;
    item.size = 0;
    item.mem = nullptr;
    item.memsize = 0;
    item.flags = 0;
}

static int
insert_key_value(WT_CURSOR *cursor, const char *key, const char *value)
{
    item_wrapper item_key(key);
    item_wrapper item_value(value);
    __wt_cursor_set_raw_key(cursor, item_key.get_item());
    __wt_cursor_set_raw_value(cursor, item_value.get_item());
    return cursor->insert(cursor);
}

static bool
check_item(WT_ITEM *item, const char *expected)
{
    bool match = true;
    if (expected != nullptr) {
        const char *key = static_cast<const char *>(item->data);
        REQUIRE(key != nullptr);
        match = strcmp(key, expected) == 0;
    }
    REQUIRE(match);
    return match;
}

static void insert_sample_values(WT_CURSOR *cursor)
{
    REQUIRE(insert_key_value(cursor, "key1", "value1") == 0);
    REQUIRE(insert_key_value(cursor, "key2", "value2") == 0);
    REQUIRE(insert_key_value(cursor, "key3", "value3") == 0);
    REQUIRE(insert_key_value(cursor, "key4", "value4") == 0);
    REQUIRE(insert_key_value(cursor, "key5", "value5") == 0);
}


void thread_function_checkpoint(WT_SESSION *session)
{
    session->checkpoint(session, NULL);
}


void thread_function_drop(WT_SESSION *session, std::string const& uri)
{
    session->drop(session, uri.c_str(), "force=true");
}


void cursor_test(std::string const &config, bool close, int expected_commit_result) {
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

    SECTION("Checkpoint during transaction then commit: config = " + config + ", close = " + close_as_string)
    {
        int result = session->checkpoint(session, NULL);
        REQUIRE(result == EINVAL);

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Checkpoint in 2nd thread during transaction then commit: config = " + config + ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_checkpoint(session); } );
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Drop in 2nd thread during transaction then commit: config = " + config + ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_drop(session, uri); } );
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->commit_transaction(session, "") == expected_commit_result);
    }

    SECTION("Checkpoint in 2nd thread during transaction then rollback: config = " + config + ", close = " + close_as_string)
    {
        std::thread thread([&]() { thread_function_checkpoint(session); } );
        thread.join();

        if (close) {
            REQUIRE(cursor->close(cursor) == 0);
        }

        REQUIRE(session->rollback_transaction(session, "") == 0);
    }
}


TEST_CASE("Cursor: checkpoint during transaction()", "[cursor]")
{
    cursor_test("", false, EINVAL);
    cursor_test("", true, EINVAL);
    cursor_test("bulk", false, 0);
    cursor_test("bulk", true, 0);
}
