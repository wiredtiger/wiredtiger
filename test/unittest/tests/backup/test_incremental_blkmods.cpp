/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*	All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#include <catch2/catch.hpp>
#include <iostream>
#include <regex>
#include <sstream>
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

static void
insert_key_value(WT_CURSOR *cursor1, WT_CURSOR *cursor2, std::string const &key, std::string const &value)
{
   insert_key_value(cursor1, key.c_str(), value.c_str());
   insert_key_value(cursor2, key.c_str(), value.c_str());
}

//static bool
//require_get_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value)
//{
//   const char *key = nullptr;
//   const char *value = nullptr;
//   REQUIRE(cursor->get_key(cursor, &key) == 0);
//   REQUIRE(cursor->get_value(cursor, &value) == 0);
//
//   bool keys_match = strcmp(key, expected_key) == 0;
//   bool values_match = strcmp(value, expected_value) == 0;
//   REQUIRE(keys_match);
//   REQUIRE(values_match);
//
//   return keys_match && values_match;
//}

//static bool
//check_item(WT_ITEM *item, const char *expected)
//{
//   bool match = true;
//   if (expected != nullptr) {
//       const char *key = static_cast<const char *>(item->data);
//       REQUIRE(key != nullptr);
//       match = strcmp(key, expected) == 0;
//   }
//   REQUIRE(match);
//   return match;
//}

//static bool
//require_get_raw_key_value(WT_CURSOR *cursor, const char *expected_key, const char *expected_value)
//{
//   WT_ITEM item_key;
//   init_wt_item(item_key);
//   WT_ITEM item_value;
//   init_wt_item(item_value);
//
//   WT_ITEM *p_item_key = (expected_key == nullptr) ? nullptr : &item_key;
//   WT_ITEM *p_item_value = (expected_value == nullptr) ? nullptr : &item_value;
//
//   REQUIRE(cursor->get_raw_key_value(cursor, p_item_key, p_item_value) == 0);
//
//   bool keys_match = check_item(p_item_key, expected_key);
//   bool values_match = check_item(p_item_value, expected_value);
//
//   return keys_match && values_match;
//}

static void
insert_sample_values(WT_CURSOR *cursor1, WT_CURSOR *cursor2, int first_value, int num_values)
{
   for (int loop = 0; loop <= num_values; loop++) {
       int i = first_value + loop;
       std::stringstream key;
       key << "key";
       key << i;
       std::stringstream value;
       value << "value";
       value << i;
       insert_key_value(cursor1, cursor2, key.str(), value.str());
   }
}

static
std::vector<bool> parse_blkmods(WT_SESSION *session, std::string const& uri)
{
   std::vector<bool> result;
   WT_CURSOR *metadata_cursor = nullptr;
   REQUIRE(session->open_cursor(session, "metadata:", nullptr, nullptr, &metadata_cursor) == 0);

   metadata_cursor->set_key(metadata_cursor, uri.c_str());
   REQUIRE(metadata_cursor->search(metadata_cursor) == 0);

   char *file_config;
   REQUIRE(metadata_cursor->get_value(metadata_cursor, &file_config) == 0);
//   std::cout << "file_config for " << uri << " is " << file_config << std::endl;

   std::cmatch match_results;
   REQUIRE(std::regex_search(file_config, match_results, std::regex(",blocks=(\\w+)")));
   std::cout << "Found " << match_results[1] << std::endl;

   REQUIRE(metadata_cursor->close(metadata_cursor) == 0);
   return result;
}


TEST_CASE("Backup: Test blkmods in incremental backup", "[backup]")
{
    std::string create_config = "allocation_size=512,key_format=S,value_format=S";
    std::string backup_config = "incremental=(enabled,granularity=4k,this_id=\"ID1\")";


    std::string uri1 = "backup_test1";
    std::string uri2 = "backup_test2";
    std::string file1_uri = "file:" + uri1 + ".wt";
    std::string file2_uri = "file:" + uri2 + ".wt";
    std::string table1_uri = "table:" + uri1;
    std::string table2_uri = "table:" + uri2;

    const int num_few_keys = 100;
    const int num_more_keys = 5000;

    {
        // Setup
        std::string conn_config = "create,file_manager=(close_handle_minimum=0,close_idle_time=3,close_scan_interval=1),statistics=(fast)";
        ConnectionWrapper conn(DB_HOME, conn_config.c_str());
        conn.clearDoCleanup();
        WT_SESSION_IMPL *session_impl = conn.createSession();

        WT_SESSION *session = &session_impl->iface;

        REQUIRE(session->create(session, table1_uri.c_str(), create_config.c_str()) == 0);
        REQUIRE(session->create(session, table2_uri.c_str(), create_config.c_str()) == 0);

        WT_CURSOR *cursor1 = nullptr;
        REQUIRE(session->open_cursor(session, table1_uri.c_str(), nullptr, nullptr, &cursor1) == 0);

        WT_CURSOR *cursor2 = nullptr;
        REQUIRE(session->open_cursor(session, table2_uri.c_str(), nullptr, nullptr, &cursor2) == 0);

        insert_sample_values(cursor1, cursor2, 0, num_few_keys);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        WT_CURSOR *backup_cursor = nullptr;
        REQUIRE(session->open_cursor(session, "backup:", nullptr, backup_config.c_str(), &backup_cursor) == 0);
        REQUIRE(backup_cursor->close(backup_cursor) == 0);

        insert_sample_values(cursor1, cursor2, num_few_keys, num_more_keys);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        REQUIRE(cursor1->close(cursor1) == 0);
        REQUIRE(cursor2->close(cursor2) == 0);

        REQUIRE(session->checkpoint(session, nullptr) == 0);

        parse_blkmods(session, file1_uri);
        parse_blkmods(session, file2_uri);

//        REQUIRE(session->close(session, nullptr) == 0);
    }

    {
        // Incremental backup and validate
        std::string conn_config = "file_manager=(close_handle_minimum=0,close_idle_time=3,close_scan_interval=1),statistics=(fast)";
        ConnectionWrapper conn(DB_HOME, conn_config.c_str());
        WT_SESSION_IMPL *session_impl = conn.createSession();

        WT_SESSION *session = &session_impl->iface;

        REQUIRE(session->create(session, table1_uri.c_str(), create_config.c_str()) == 0);
        REQUIRE(session->create(session, table2_uri.c_str(), create_config.c_str()) == 0);

        WT_CURSOR *cursor1 = nullptr;
        REQUIRE(session->open_cursor(session, table1_uri.c_str(), nullptr, nullptr, &cursor1) == 0);

        WT_CURSOR *cursor2 = nullptr;
        REQUIRE(session->open_cursor(session, table2_uri.c_str(), nullptr, nullptr, &cursor2) == 0);

        REQUIRE(insert_key_value(cursor1, "key5000", "value5000") == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        parse_blkmods(session, file1_uri);

        REQUIRE(insert_key_value(cursor2, "key5000", "value5000") == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        parse_blkmods(session, file2_uri);

        REQUIRE(cursor1->close(cursor1) == 0);
        REQUIRE(cursor2->close(cursor2) == 0);

//        REQUIRE(session->close(session, nullptr) == 0);
    }

}
