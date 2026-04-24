/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Test the shared disk hash table get function. [shared_dsk_cache_get]
 */

#include "../shared_dsk_test_env.h"

using namespace utils;

TEST_CASE("shared_dsk_cache_get: miss on empty cache returns NULL",
  "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr[] = {0xde, 0xad, 0xbe, 0xef};

    WT_SHARED_DSK_ITEM *got = reinterpret_cast<WT_SHARED_DSK_ITEM *>(0x1);
    __wt_shared_dsk_cache_get(env.session(), addr, sizeof(addr), &got);

    REQUIRE(got == nullptr);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 1);
    REQUIRE(env.stats()->cache_shared_dsk_hit == 0);
}

TEST_CASE("shared_dsk_cache_get: hit returns inserted item and increments ref_count",
  "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr[] = {0x01, 0x02, 0x03, 0x04};

    WT_SHARED_DSK_ITEM *put_item = env.put(addr, sizeof(addr));
    REQUIRE(put_item->ref_count == 1);

    WT_SHARED_DSK_ITEM *got = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr, sizeof(addr), &got);

    REQUIRE(got == put_item);
    REQUIRE(got->ref_count == 2);
    REQUIRE(got->fid == env.btree_id());
    REQUIRE(got->addr_size == sizeof(addr));
    REQUIRE(memcmp(got->addr, addr, sizeof(addr)) == 0);
    REQUIRE(env.stats()->cache_shared_dsk_hit == 1);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 0);

    env.release_to_zero(got);
}

TEST_CASE(
  "shared_dsk_cache_get: different addr misses", "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr_a[] = {0x01, 0x02, 0x03, 0x04};
    const uint8_t addr_b[] = {0x05, 0x06, 0x07, 0x08};

    WT_SHARED_DSK_ITEM *put_item = env.put(addr_a, sizeof(addr_a));

    WT_SHARED_DSK_ITEM *got = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr_b, sizeof(addr_b), &got);

    REQUIRE(got == nullptr);
    REQUIRE(put_item->ref_count == 1);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 1);

    env.release_to_zero(put_item);
}

TEST_CASE(
  "shared_dsk_cache_get: different addr_size misses", "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr_full[] = {0x01, 0x02, 0x03, 0x04};
    const uint8_t addr_short[] = {0x01, 0x02, 0x03};

    WT_SHARED_DSK_ITEM *put_item = env.put(addr_full, sizeof(addr_full));

    WT_SHARED_DSK_ITEM *got = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr_short, sizeof(addr_short), &got);

    REQUIRE(got == nullptr);
    REQUIRE(put_item->ref_count == 1);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 1);

    env.release_to_zero(put_item);
}

TEST_CASE("shared_dsk_cache_get: different file id misses even with identical addr",
  "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr[] = {0x01, 0x02, 0x03, 0x04};

    WT_SHARED_DSK_ITEM *put_item = env.put(addr, sizeof(addr));

    /* Swap the btree id so the get call sees a different file id but the same hash bucket. */
    uint32_t original_id = S2BT(env.session())->id;
    S2BT(env.session())->id = original_id + 1;

    WT_SHARED_DSK_ITEM *got = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr, sizeof(addr), &got);

    REQUIRE(got == nullptr);
    REQUIRE(put_item->ref_count == 1);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 1);

    S2BT(env.session())->id = original_id;
    env.release_to_zero(put_item);
}

TEST_CASE("shared_dsk_cache_get: repeated hits accumulate ref_count and stats",
  "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr[] = {0xaa, 0xbb};

    WT_SHARED_DSK_ITEM *put_item = env.put(addr, sizeof(addr));

    constexpr int ITERATIONS = 5;
    for (int i = 0; i < ITERATIONS; i++) {
        WT_SHARED_DSK_ITEM *got = nullptr;
        __wt_shared_dsk_cache_get(env.session(), addr, sizeof(addr), &got);
        REQUIRE(got == put_item);
        REQUIRE(got->ref_count == 2 + i);
    }

    REQUIRE(put_item->ref_count == 1 + ITERATIONS);
    REQUIRE(env.stats()->cache_shared_dsk_hit == ITERATIONS);
    REQUIRE(env.stats()->cache_shared_dsk_miss == 0);

    env.release_to_zero(put_item);
}

TEST_CASE("shared_dsk_cache_get: two entries in the same bucket are distinguished by addr",
  "[shared_dsk_cache],[shared_dsk_cache_get]")
{
    shared_dsk_test_env env;
    const uint8_t addr_a[] = {0x10, 0x20};
    const uint8_t addr_b[] = {0x30, 0x40};

    WT_SHARED_DSK_ITEM *item_a = env.put(addr_a, sizeof(addr_a));
    WT_SHARED_DSK_ITEM *item_b = env.put(addr_b, sizeof(addr_b));
    REQUIRE(item_a != item_b);

    WT_SHARED_DSK_ITEM *got_a = nullptr;
    WT_SHARED_DSK_ITEM *got_b = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr_a, sizeof(addr_a), &got_a);
    __wt_shared_dsk_cache_get(env.session(), addr_b, sizeof(addr_b), &got_b);

    REQUIRE(got_a == item_a);
    REQUIRE(got_b == item_b);
    REQUIRE(item_a->ref_count == 2);
    REQUIRE(item_b->ref_count == 2);

    env.release_to_zero(got_a);
    env.release_to_zero(got_b);
}
