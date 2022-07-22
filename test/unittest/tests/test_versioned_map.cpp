/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "wrappers/versioned_map.h"
#include "wrappers/connection_wrapper.h"
#include "wrappers/cursor_wrapper.h"
#include "wrappers/transaction_wrapper.h"

TEST_CASE("std::map iterator")
{
    std::map<int, std::string> testMap;
    testMap[1] = "One";
    testMap[2] = "Two";
    testMap[3] = "Three";
    testMap[4] = "Four";

    REQUIRE(testMap[1] == "One");
    REQUIRE(testMap[2] == "Two");
    REQUIRE(testMap[3] == "Three");
    REQUIRE(testMap[4] == "Four");
    REQUIRE(testMap.size() == 4);

    int count = 0;
    for (const auto &iter : testMap) {
        count++;
        REQUIRE(testMap[iter.first] == iter.second);
    }
    REQUIRE(count == 4);
}

TEST_CASE("VersionedMap", "[versioned_map]")
{
    ConnectionWrapper conn(utils::UnitTestDatabaseHome);
    WT_SESSION_IMPL *sessionImpl = conn.createSession();
    WT_SESSION *session = &(sessionImpl->iface);

    std::string table_name = "table:map_table";
    REQUIRE(session->create(session, table_name.c_str(), "key_format=S,value_format=S") == 0);
    VersionedMap<std::string, std::string> versionedMap(session, table_name);

    const std::string testcase_key1 = "key1";
    const std::string testcase_value1 = "value1";
    const std::string testcase_key2 = "key2";
    const std::string testcase_value2 = "value2";

    SECTION("simple")
    {
        WT_CURSOR *cursor = nullptr;
        REQUIRE(session->open_cursor(session, table_name.c_str(), nullptr, nullptr, &cursor) == 0);
        cursor->set_key(cursor, testcase_key1.c_str());
        cursor->set_value(cursor, testcase_value1.c_str());
        REQUIRE(cursor->insert(cursor) == 0);
        REQUIRE(versionedMap.size() == 1);

        cursor->set_key(cursor, testcase_key2.c_str());
        cursor->set_value(cursor, testcase_value2.c_str());
        REQUIRE(cursor->insert(cursor) == 0);
        REQUIRE(cursor->reset(cursor) == 0);
        REQUIRE(versionedMap.size() == 2);

        std::string value = versionedMap.get(testcase_key1);
        REQUIRE(value == testcase_value1);

        REQUIRE_THROWS(versionedMap.get("fred")); // Key "fred" should not exist.
        REQUIRE_THROWS(versionedMap.get("bill")); // Key "bill" should not exist.
    }

    SECTION("simple with wrappers")
    {
        CursorWrapper cursorWrapper(session, table_name);
        cursorWrapper.setKey(testcase_key1);
        cursorWrapper.setValue(testcase_value1);
        cursorWrapper.insert();
        REQUIRE(versionedMap.size() == 1);

        cursorWrapper.setKey(testcase_key2);
        cursorWrapper.setValue(testcase_value2);
        cursorWrapper.insert();
        cursorWrapper.reset();
        REQUIRE(versionedMap.size() == 2);

        REQUIRE(versionedMap.get(testcase_key1) == testcase_value1);
        REQUIRE(versionedMap.get(testcase_key2) == testcase_value2);

        REQUIRE_THROWS(versionedMap.get("fred")); // Key "fred" should not exist.
        REQUIRE_THROWS(versionedMap.get("bill")); // Key "bill" should not exist.
    }

    SECTION("set() and get()")
    {
        constexpr int numToAdd = 10;
        {
            TransactionWrapper transactionWrapper(session, "");
            for (int i = 0; i < numToAdd; i++) {
                std::string key = std::string("key") + std::to_string(i);
                std::string value = std::string("value") + std::to_string(i);
                versionedMap.set(key, value);
            }
            transactionWrapper.commit("");
        }

        REQUIRE(versionedMap.size() == numToAdd);
        REQUIRE(versionedMap.get("key0") == "value0");
        REQUIRE(versionedMap.get("key1") == "value1");
        REQUIRE(versionedMap.get("key2") == "value2");
        REQUIRE(versionedMap.get("key3") == "value3");
        REQUIRE(versionedMap.get("key4") == "value4");
        REQUIRE(versionedMap.get("key5") == "value5");
        REQUIRE(versionedMap.get("key6") == "value6");
        REQUIRE(versionedMap.get("key7") == "value7");
        REQUIRE(versionedMap.get("key8") == "value8");
        REQUIRE(versionedMap.get("key9") == "value9");
        REQUIRE_THROWS(versionedMap.get("fred"));  // Key "fred" should not exist.
        REQUIRE_THROWS(versionedMap.get("key11")); // Key "key11" should not exist.
    }

    SECTION("set() and get() with timestamps + iterators")
    {
        REQUIRE(
          conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "oldest_timestamp=1") == 0);
        REQUIRE(
          conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "stable_timestamp=1") == 0);

        constexpr int numToAdd = 10;
        {
            TransactionWrapper transactionWrapper(session, "");
            for (int i = 0; i < numToAdd; i++) {
                std::string key = std::string("key") + std::to_string(i);
                std::string value = std::string("value") + std::to_string(i);
                versionedMap.set(key, value);
            }
            transactionWrapper.commit("commit_timestamp=10");
        }

        REQUIRE(versionedMap.get("key3") == "value3");
        REQUIRE(versionedMap.get("key5") == "value5");

        {
            TransactionWrapper transactionWrapper(session, "");
            versionedMap.set("key3", "value3-ts20");
            versionedMap.set("key5", "value5-ts20");
            transactionWrapper.commit("commit_timestamp=20");
        }

        REQUIRE(versionedMap.get("key3") == "value3-ts20");
        REQUIRE(versionedMap.get("key5") == "value5-ts20");

        {
            TransactionWrapper transactionWrapper(session, "");
            versionedMap.set("key5", "value5-ts30");
            transactionWrapper.commit("commit_timestamp=30");
        }

        REQUIRE(versionedMap.size() == numToAdd);

        REQUIRE(versionedMap.get("key0") == "value0");
        REQUIRE(versionedMap.get("key1") == "value1");
        REQUIRE(versionedMap.get("key2") == "value2");
        REQUIRE(versionedMap.get("key3") == "value3-ts20");
        REQUIRE(versionedMap.get("key4") == "value4");
        REQUIRE(versionedMap.get("key5") == "value5-ts30");
        REQUIRE(versionedMap.get("key6") == "value6");
        REQUIRE(versionedMap.get("key7") == "value7");
        REQUIRE(versionedMap.get("key8") == "value8");
        REQUIRE(versionedMap.get("key9") == "value9");

        REQUIRE_THROWS(versionedMap.getTransactionWrapped("key3", "", 0x5));
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x10) == "value3");
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x15) == "value3");
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x20) == "value3-ts20");
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x25) == "value3-ts20");
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x30) == "value3-ts20");
        REQUIRE(versionedMap.getTransactionWrapped("key3", "", 0x35) == "value3-ts20");

        REQUIRE_THROWS(versionedMap.getTransactionWrapped("key5", "", 0x5));
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x10) == "value5");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x15) == "value5");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x20) == "value5-ts20");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x25) == "value5-ts20");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x30) == "value5-ts30");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x35) == "value5-ts30");
        REQUIRE(versionedMap.getTransactionWrapped("key5", "", 0x10) == "value5");

        REQUIRE_THROWS(versionedMap.getTransactionWrapped("key7", "", 0x5));
        REQUIRE(versionedMap.getTransactionWrapped("key7", "", 0x15) == "value7");
        REQUIRE(versionedMap.getTransactionWrapped("key7", "", 0x20) == "value7");
        REQUIRE(versionedMap.getTransactionWrapped("key7", "", 0x25) == "value7");
        REQUIRE(versionedMap.getTransactionWrapped("key7", "", 0x30) == "value7");
        REQUIRE(versionedMap.getTransactionWrapped("key7", "", 0x35) == "value7");

        REQUIRE_THROWS(versionedMap.get("fred"));  // Key "fred" should not exist.
        REQUIRE_THROWS(versionedMap.get("key11")); // Key "key11" should not exist.

        {
            int count = 0;
            auto iterator = versionedMap.begin();
            while (iterator.isOk()) {
                auto value = iterator.get();
                REQUIRE(versionedMap.get(value.first) == value.second);
                iterator.next();
                count++;
            }
            REQUIRE(count == 10);
        }
    }
}
