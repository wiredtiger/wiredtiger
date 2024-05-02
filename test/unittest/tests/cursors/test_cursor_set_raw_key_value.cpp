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
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../wrappers/item_wrapper.h"
#include "validate_key_value.h"

static int
insert_key_value(WT_CURSOR *cursor, const char *key, const char *value)
{
    item_wrapper item_key(key);
    item_wrapper item_value(value);
    __wt_cursor_set_raw_key_value(cursor, item_key.get_item(), item_value.get_item());
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

TEST_CASE("Cursor: set key and value()", "[cursor]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();
    std::string uri = "table:cursor_test";
    std::string file = "file:cursor_test.wt";

    WT_SESSION *session = &session_impl->iface;

    REQUIRE(session->create(session, uri.c_str(), "key_format=S,value_format=S") == 0);

    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri.c_str(), nullptr, nullptr, &cursor) == 0);

    insert_sample_values(cursor);

    SECTION("Check the values using get_key() and get_value()")
    {
        REQUIRE(cursor->reset(cursor) == 0);
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, "key1", "value1"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, "key2", "value2"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, "key3", "value3"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, "key4", "value4"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, "key5", "value5"));
        REQUIRE(cursor->next(cursor) == WT_NOTFOUND);
    }

    SECTION("Check the values using get_raw_key_value()")
    {
        REQUIRE(cursor->reset(cursor) == 0);
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_raw_key_value(cursor, "key1", "value1"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_raw_key_value(cursor, "key2", "value2"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_raw_key_value(cursor, "key3", "value3"));
        REQUIRE(require_get_raw_key_value(cursor, nullptr, "value3"));
        REQUIRE(require_get_raw_key_value(cursor, "key3", nullptr));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_raw_key_value(cursor, "key4", "value4"));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_raw_key_value(cursor, "key5", "value5"));
        REQUIRE(cursor->next(cursor) == WT_NOTFOUND);
    }

    SECTION("Check get_raw_key_value() on a cursor type that does not support it")
    {
        WT_CURSOR *version_cursor = nullptr;
        REQUIRE(session->open_cursor(session, file.c_str(), nullptr, "debug=(dump_version=true)",
                  &version_cursor) == 0);
        WT_ITEM item_key;
        init_wt_item(item_key);
        WT_ITEM item_value;
        init_wt_item(item_value);

        // get_raw_key_value() is not supported on a version cursor
        REQUIRE(
          version_cursor->get_raw_key_value(version_cursor, &item_key, &item_value) == ENOTSUP);

        REQUIRE(version_cursor->close(version_cursor) == 0);
    }

    REQUIRE(cursor->close(cursor) == 0);

    REQUIRE(session->close(session, nullptr) == 0);
}
