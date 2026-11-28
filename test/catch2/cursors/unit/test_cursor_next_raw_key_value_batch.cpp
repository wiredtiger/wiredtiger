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
    REQUIRE(insert_key_value(cursor, "k1", "v1") == 0);
    REQUIRE(insert_key_value(cursor, "k2", "v2") == 0);
    REQUIRE(insert_key_value(cursor, "k3", "v3") == 0);
    REQUIRE(insert_key_value(cursor, "k4", "v4") == 0);
    REQUIRE(insert_key_value(cursor, "k5", "v5") == 0);
}

static void
check_next_raw_key_value_batch(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    insert_sample_values(cursor);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=10") == 0);
    REQUIRE(cursor->reset(cursor) == 0);

    WT_ITEM key{}, value{};
    size_t n = 0;

    /* Reject invalid arguments. */
    REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, &value, 0, &n) == EINVAL);
    REQUIRE(cursor->next_raw_key_value_batch(cursor, nullptr, nullptr, 1, &n) == EINVAL);

    const char *keys[] = {"k1", "k2", "k3", "k4", "k5"};
    const char *values[] = {"v1", "v2", "v3", "v4", "v5"};
    for (int i = 0; i < 5; i++) {
        n = 0;
        REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, &value, 1, &n) == 0);
        REQUIRE(n >= 1);
        REQUIRE(strcmp(static_cast<const char *>(key.data), keys[i]) == 0);
        REQUIRE(strcmp(static_cast<const char *>(value.data), values[i]) == 0);
        REQUIRE(require_get_key_value(cursor, keys[i], values[i]));
    }
    REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, &value, 1, &n) == WT_NOTFOUND);

    /* Only keys, and only values, are both acceptable. */
    REQUIRE(cursor->reset(cursor) == 0);
    REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, nullptr, 1, &n) == 0);
    REQUIRE(strcmp(static_cast<const char *>(key.data), "k1") == 0);
    REQUIRE(cursor->next_raw_key_value_batch(cursor, nullptr, &value, 1, &n) == 0);
    REQUIRE(strcmp(static_cast<const char *>(value.data), "v2") == 0);

    REQUIRE(cursor->close(cursor) == 0);
}

TEST_CASE("next_raw_key_value_batch_table", "[cursor]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, "table:t", "key_format=S,value_format=S") == 0);
    check_next_raw_key_value_batch(session, "table:t");
    REQUIRE(session->close(session, nullptr) == 0);
}

TEST_CASE("next_raw_key_value_batch_table_multiple_colgroups", "[cursor]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, "table:cg",
              "key_format=S,value_format=SS,columns=(k,a,b),colgroups=(g1,g2)") == 0);
    REQUIRE(session->create(session, "colgroup:cg:g1", "columns=(a)") == 0);
    REQUIRE(session->create(session, "colgroup:cg:g2", "columns=(b)") == 0);

    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, "table:cg", nullptr, nullptr, &cursor) == 0);
    cursor->set_key(cursor, "k1");
    cursor->set_value(cursor, "a1", "b1");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(cursor->reset(cursor) == 0);

    /* The API currently only supports one colgroup. */
    WT_ITEM key{}, value{};
    size_t n = 0;
    REQUIRE(cursor->next_raw_key_value_batch(cursor, &key, &value, 1, &n) == ENOTSUP);

    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->close(session, nullptr) == 0);
}

TEST_CASE("next_raw_key_value_batch_file", "[cursor]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, "file:f.wt", "key_format=S,value_format=S") == 0);
    check_next_raw_key_value_batch(session, "file:f.wt");
    REQUIRE(session->close(session, nullptr) == 0);
}

TEST_CASE("next_raw_key_value_batch_file_multiple_results", "[cursor]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    const char *uri = "file:f.wt";
    REQUIRE(session->create(session, uri, "key_format=S,value_format=S") == 0);
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);

    char k[32];
    char v[32];
    int total_written = 100 * 100; /* Must be divisible by 100, just for the txn division. */
    for (int i = 0; i < total_written / 100; i++) {
        REQUIRE(session->begin_transaction(session, nullptr) == 0);
        for (int j = 0; j < 100; j++) {
            std::snprintf(k, sizeof(k), "k%016d", i * 100 + j);
            std::snprintf(v, sizeof(v), "v%016d", i * 100 + j);
            REQUIRE(insert_key_value(cursor, k, v) == 0);
        }
        REQUIRE(session->commit_transaction(session, "commit_timestamp=10") == 0);
    }
    REQUIRE(cursor->reset(cursor) == 0);

    int total_read = 0;
    int ret = 0;
    int high_water_mark = 0;
    for (;;) {
        size_t n;
        WT_ITEM keys[150];
        WT_ITEM values[150];
        ret = cursor->next_raw_key_value_batch(cursor, keys, values, 150, &n);

        if (ret != 0)
            break;

        for (int i = 0; i < n; i++) {
            std::snprintf(k, sizeof(k), "k%016d", total_read + i);
            std::snprintf(v, sizeof(v), "v%016d", total_read + i);
            REQUIRE(strcmp(static_cast<const char *>(keys[i].data), k) == 0);
            REQUIRE(strcmp(static_cast<const char *>(values[i].data), v) == 0);
        }
        total_read += n;
        if (n > high_water_mark)
            high_water_mark = n;
    }
    REQUIRE(ret == WT_NOTFOUND);
    REQUIRE(total_read == total_written);
    REQUIRE(high_water_mark > 1);

    REQUIRE(session->close(session, nullptr) == 0);
}

#ifndef _WIN32
#include "../../misc_tests/layered_disagg_utils.h"

TEST_CASE("next_raw_key_value_batch_layered", "[cursor]")
{
    connection_wrapper conn(DB_HOME, layered_disagg_build_cfg("leader").c_str());
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, "layered:l",
              "key_format=S,value_format=S, block_manager=disagg,type=layered") == 0);
    check_next_raw_key_value_batch(session, "layered:l");
    REQUIRE(session->close(session, nullptr) == 0);
}
#endif

static int64_t
get_dsrc_stat(WT_SESSION *session, const char *uri, int stat_key)
{
    WT_CURSOR *stat_cursor = nullptr;
    const std::string stat_uri = std::string("statistics:") + uri;
    REQUIRE(session->open_cursor(session, stat_uri.c_str(), nullptr, nullptr, &stat_cursor) == 0);

    const char *desc = nullptr, *pvalue = nullptr;
    int64_t value = 0;
    stat_cursor->set_key(stat_cursor, stat_key);
    REQUIRE(stat_cursor->search(stat_cursor) == 0);
    REQUIRE(stat_cursor->get_value(stat_cursor, &desc, &pvalue, &value) == 0);
    REQUIRE(stat_cursor->close(stat_cursor) == 0);
    return value;
}

static void
prefix_key_value_fmt(int i, char *key, size_t key_len, char *value, size_t value_len)
{
    std::snprintf(key, key_len, "shared_key_prefix_%08d", i);
    std::snprintf(value, value_len, "v%08d", i);
}

static void
write_prefix_compressed_table(const char *uri, size_t nkeys)
{
    connection_wrapper conn(DB_HOME, "create,statistics=(all)");
    conn.clear_do_cleanup();
    WT_SESSION *session = &conn.create_session()->iface;
    REQUIRE(session->create(session, uri,
              "key_format=S,value_format=S,prefix_compression=true,prefix_compression_min=1") == 0);

    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    char k[64], v[32];
    for (int i = 0; i < nkeys; i++) {
        prefix_key_value_fmt(i, k, sizeof(k), v, sizeof(v));
        REQUIRE(insert_key_value(cursor, k, v) == 0);
    }
    REQUIRE(cursor->close(cursor) == 0);

    /* Check the keys really were prefix compressed. */
    REQUIRE(session->checkpoint(session, nullptr) == 0);
    int64_t compressed = 0;
    for (int attempt = 0; attempt < 100 && compressed == 0; attempt++) {
        compressed = get_dsrc_stat(session, uri, WT_STAT_DSRC_REC_PREFIX_COMPRESSION_FULL);
        if (compressed == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    REQUIRE(compressed > 0);
}

TEST_CASE("next_raw_key_value_batch_prefix_compression", "[cursor]")
{
    const char *uri = "file:f.wt";
    size_t nkeys = 1000;
    write_prefix_compressed_table(uri, nkeys);

    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);

    char k[64], v[32];
    int total_read = 0;
    int ret = 0;
    size_t high_water_mark = 0;
    for (;;) {
        size_t n = 0;
        WT_ITEM keys[50] = {};
        WT_ITEM values[50] = {};
        ret = cursor->next_raw_key_value_batch(cursor, keys, values, 50, &n);
        if (ret != 0)
            break;

        for (auto i = 0; i < n; i++) {
            prefix_key_value_fmt(total_read + static_cast<int>(i), k, sizeof(k), v, sizeof(v));
            REQUIRE(keys[i].size == strlen(k) + 1);
            REQUIRE(memcmp(keys[i].data, k, keys[i].size) == 0);
            REQUIRE(values[i].size == strlen(v) + 1);
            REQUIRE(memcmp(values[i].data, v, values[i].size) == 0);
        }
        total_read += n;
        if (n > high_water_mark)
            high_water_mark = n;
    }
    REQUIRE(ret == WT_NOTFOUND);
    REQUIRE(total_read == nkeys);
    REQUIRE(high_water_mark > 1);

    REQUIRE(cursor->close(cursor) == 0);
}

TEST_CASE("next_raw_key_value_batch_prefix_compression_then_next", "[cursor]")
{
    const char *uri = "file:f.wt";
    size_t nkeys = 1000;
    write_prefix_compressed_table(uri, nkeys);

    connection_wrapper conn(DB_HOME);
    WT_SESSION *session = &conn.create_session()->iface;
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);

    size_t n = 0;
    WT_ITEM keys[10] = {};
    REQUIRE(cursor->next_raw_key_value_batch(cursor, keys, nullptr, 10, &n) == 0);
    REQUIRE(n >= 1);

    /* The cursor must carry on from where the batch stopped. */
    char k[64], v[32];
    for (int i = static_cast<int>(n); i < nkeys; i++) {
        prefix_key_value_fmt(i, k, sizeof(k), v, sizeof(v));
        REQUIRE(cursor->next(cursor) == 0);
        REQUIRE(require_get_key_value(cursor, k, v));
    }
    REQUIRE(cursor->next(cursor) == WT_NOTFOUND);

    REQUIRE(cursor->close(cursor) == 0);
}
