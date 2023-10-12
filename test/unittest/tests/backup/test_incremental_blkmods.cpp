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
#include "../helpers/vector_bool.h"
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
insert_key_value(
  WT_CURSOR *cursor1, WT_CURSOR *cursor2, std::string const &key, std::string const &value)
{
    insert_key_value(cursor1, key.c_str(), value.c_str());
    insert_key_value(cursor2, key.c_str(), value.c_str());
}

static void
insert_sample_values(WT_CURSOR *cursor1, WT_CURSOR *cursor2, int first_value, int num_values)
{
    for (int loop = 0; loop <= num_values; loop++) {
        int i = first_value + loop;
        std::stringstream key;
        key << "key" << i;
        std::stringstream value;
        value << "value" << i;
        insert_key_value(cursor1, cursor2, key.str(), value.str());
    }
}

static std::vector<bool>
parse_blkmods(WT_SESSION *session, std::string const &file_uri)
{
    WT_CURSOR *metadata_cursor = nullptr;
    REQUIRE(session->open_cursor(session, "metadata:", nullptr, nullptr, &metadata_cursor) == 0);

    metadata_cursor->set_key(metadata_cursor, file_uri.c_str());
    REQUIRE(metadata_cursor->search(metadata_cursor) == 0);

    char *file_config;
    REQUIRE(metadata_cursor->get_value(metadata_cursor, &file_config) == 0);

//    printf("file_config = %s\n", file_config);

    std::cmatch match_results;
    REQUIRE(std::regex_search(file_config, match_results, std::regex(",blocks=(\\w+)")));
    std::string hex_blkmod = match_results[1];

    REQUIRE(metadata_cursor->close(metadata_cursor) == 0);

    std::vector<bool> result(vector_bool_from_hex_string(hex_blkmod));
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

    std::vector<bool> orig_blkmod_table1;
    std::vector<bool> orig_blkmod_table2;
    std::vector<bool> new_blkmod_table1;
    std::vector<bool> new_blkmod_table2;

    {
        // Setup
        std::string conn_config =
          "create,file_manager=(close_handle_minimum=0,close_idle_time=3,close_scan_interval=1),"
          "statistics=(fast)";
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
        REQUIRE(session->open_cursor(
                  session, "backup:", nullptr, backup_config.c_str(), &backup_cursor) == 0);
        REQUIRE(backup_cursor->close(backup_cursor) == 0);

        insert_sample_values(cursor1, cursor2, num_few_keys, num_more_keys);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        REQUIRE(cursor1->close(cursor1) == 0);
        REQUIRE(cursor2->close(cursor2) == 0);

        REQUIRE(session->checkpoint(session, nullptr) == 0);

        orig_blkmod_table1 = parse_blkmods(session, file1_uri);
        orig_blkmod_table2 = parse_blkmods(session, file2_uri);
    }

    {
        // Incremental backup and validate
        std::string conn_config =
          "file_manager=(close_handle_minimum=0,close_idle_time=3,close_scan_interval=1),"
          "statistics=(fast)";
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

        REQUIRE(insert_key_value(cursor2, "key5000", "value5000") == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        REQUIRE(insert_key_value(cursor1, "key5000", "value5000") == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        new_blkmod_table1 = parse_blkmods(session, file1_uri);

        REQUIRE(insert_key_value(cursor2, "key5000", "value5000") == 0);
        REQUIRE(session->checkpoint(session, nullptr) == 0);

        new_blkmod_table2 = parse_blkmods(session, file2_uri);

        REQUIRE(cursor1->close(cursor1) == 0);
        REQUIRE(cursor2->close(cursor2) == 0);
    }

    /*
     * If any bits that were 1 in the original blkmod changed, we have an issue. Each of these two
     * check vectors should contain only false values. Any true values indicate a problem.
     */
    std::vector<bool> check_table1 = (orig_blkmod_table1 ^ new_blkmod_table1) & orig_blkmod_table1;
    std::vector<bool> check_table2 = (orig_blkmod_table2 ^ new_blkmod_table2) & orig_blkmod_table2;

    REQUIRE(get_true_count(check_table1) == 0);
    REQUIRE(get_true_count(check_table2) == 0);
}
