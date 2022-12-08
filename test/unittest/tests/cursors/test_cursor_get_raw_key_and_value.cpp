/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <iostream>

#include "../utils.h"
#include "wiredtiger.h"
#include "../wrappers/connection_wrapper.h"
#include "../wrappers/item_wrapper.h"
#include "wt_internal.h"


void init_wt_item(WT_ITEM& item) {
    item.data = nullptr;
    item.size = 0;
    item.mem = nullptr;
    item.memsize = 0;
    item.flags = 0;
}

int insert_key_value(WT_CURSOR* cursor, const char* key, const char* value) {
    item_wrapper item_key(key);
    item_wrapper item_value(value);
    __wt_cursor_set_raw_key(cursor, item_key.get_item());
    __wt_cursor_set_raw_value(cursor, item_value.get_item());
    return cursor->insert(cursor);
}


//void require_get_key_value(WT_CURSOR* cursor,
//  const char* expected_key, const char* expected_value) {
//    const char* key   = nullptr;
//    const char* value = nullptr;
//    REQUIRE(cursor->get_key(cursor, &key) == 0);
//    REQUIRE(cursor->get_value(cursor, &value) == 0);
//    std::cout << "key = " << key << ", value = " << value << std::endl;
//
//    REQUIRE(key != nullptr);
//    REQUIRE(value != nullptr);
//    REQUIRE(strcmp(key, expected_key) == 0);
//    REQUIRE(strcmp(value, expected_value) == 0);
//}

bool require_get_raw_key_value(WT_CURSOR* cursor,
  const char* expected_key, const char* expected_value) {
    const char* key   = nullptr;
    const char* value = nullptr;

    WT_ITEM item_key;
    init_wt_item(item_key);
    WT_ITEM item_value;
    init_wt_item(item_value);

    REQUIRE(cursor->get_raw_key_value(cursor, &item_key, &item_value) == 0);
    key = static_cast<const char *>(item_key.data);
    value = static_cast<const char *>(item_value.data);

    REQUIRE(key != nullptr);
    REQUIRE(value != nullptr);

    bool keys_match = strcmp(key, expected_key) == 0;
    bool values_match = strcmp(value, expected_value) == 0;
    REQUIRE(keys_match);
    REQUIRE(values_match);

    return keys_match && values_match;
}


TEST_CASE("Cursor: get_raw_key_and_value()", "[cursor]")
{
    ConnectionWrapper conn("get_raw_key_and_value");
    WT_SESSION_IMPL *session_impl = conn.createSession();
    std::string uri = "table:cursor_test";

    WT_SESSION* session = &session_impl->iface;

    REQUIRE(session->create(session, uri.c_str(), "key_format=S,value_format=S") == 0);

    WT_CURSOR* cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri.c_str(), nullptr, nullptr, &cursor) == 0);

    // Insert some values
    REQUIRE(insert_key_value(cursor, "key1", "value1") == 0);
    REQUIRE(insert_key_value(cursor, "key2", "value2") == 0);
    REQUIRE(insert_key_value(cursor, "key3", "value3") == 0);
    REQUIRE(insert_key_value(cursor, "key4", "value4") == 0);
    REQUIRE(insert_key_value(cursor, "key5", "value5") == 0);

    // Check the values
    REQUIRE(cursor->reset(cursor) == 0);
    REQUIRE(cursor->next(cursor) == 0);
    REQUIRE(require_get_raw_key_value(cursor, "key1", "value1"));
    REQUIRE(cursor->next(cursor) == 0);
    REQUIRE(require_get_raw_key_value(cursor, "key2", "value2"));
    REQUIRE(cursor->next(cursor) == 0);
    REQUIRE(require_get_raw_key_value(cursor, "key3", "value3"));
    REQUIRE(cursor->next(cursor) == 0);
    REQUIRE(require_get_raw_key_value(cursor, "key4", "value4"));
    REQUIRE(cursor->next(cursor) == 0);
    REQUIRE(require_get_raw_key_value(cursor, "key5", "value5"));
    REQUIRE(cursor->next(cursor) == WT_NOTFOUND);
    REQUIRE(cursor->close(cursor) == 0);

    REQUIRE(session->close(session, nullptr) == 0);
}