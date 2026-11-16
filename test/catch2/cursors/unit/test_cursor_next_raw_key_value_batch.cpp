/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../../utils.h"
#include "../../wrappers/connection_wrapper.h"
#include "../../wrappers/item_wrapper.h"

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
require_get_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value)
{
    const char *key = nullptr;
    const char *value = nullptr;
    REQUIRE(cursor->get_key(cursor, &key) == 0);
    REQUIRE(cursor->get_value(cursor, &value) == 0);

    bool keys_match = strcmp(key, expected_key) == 0;
    bool values_match = strcmp(value, expected_value) == 0;
    REQUIRE(keys_match);
    REQUIRE(values_match);

    return keys_match && values_match;
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

TEST_CASE("Cursor: next_raw_key_value_batch", "[cursor]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, "table:t", "key_format=S,value_format=S") == 0);
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, "table:t", nullptr, nullptr, &cursor) == 0);
    insert_sample_values(cursor);
    REQUIRE(cursor->reset(cursor) == 0);

    WT_ITEM key, value;
    size_t n = 0;
    REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, &value, 1, &n) == 0);
    REQUIRE(n == 1);
    REQUIRE(require_get_key_value(cursor, "key1", "value1"));

    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->close(session, nullptr) == 0);
}
