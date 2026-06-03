/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Test shared disk cache lifecycle across step-up and step-down. */

#include "../cross_checkpoint_caching_test_env.h"

using namespace utils;

/*
 * Simulate step-up: disable the cache. New get and put calls are blocked by their callers checking
 * enabled, so only in-flight releases from follower-phase pages can still reach the cache.
 */
static void
simulate_step_up(WT_SESSION_IMPL *session)
{
    S2C(session)->cache->shared_dsk_cache.enabled = false;
}

/*
 * Simulate step-down: re-initialize the cache if it has been destroyed, then re-enable it.
 */
static void
simulate_step_down(WT_SESSION_IMPL *session)
{
    WT_SHARED_DSK_CACHE *cache = &S2C(session)->cache->shared_dsk_cache;
    if (cache->hash == NULL) {
        REQUIRE(__wti_shared_dsk_cache_init(session, CROSS_CHECKPOINT_CACHING_TEST_HASH_SIZE) == 0);
    }
    cache->enabled = true;
}

TEST_CASE("step_up_step_down: size increments on new insert and not on collision",
  "[cross_checkpoint_caching],[cross_checkpoint_caching_step_up_step_down]")
{
    cross_checkpoint_caching_test_env env;
    WT_SHARED_DSK_CACHE *cache = &S2C(env.session())->cache->shared_dsk_cache;

    const uint8_t addr_a[] = {0x01, 0x02};
    const uint8_t addr_b[] = {0x03, 0x04};

    REQUIRE(cache->size == 0);

    WT_SHARED_DSK_ITEM *item_a = env.put(addr_a, sizeof(addr_a));
    REQUIRE(cache->size == 1);

    /* Collision on addr_a: ref_count bumps but size stays the same. */
    void *data = nullptr;
    REQUIRE(__wt_calloc(env.session(), 1, CROSS_CHECKPOINT_CACHING_TEST_DATA_SIZE, &data) == 0);
    WT_PAGE_BLOCK_META block_meta;
    memset(&block_meta, 0, sizeof(block_meta));
    WT_SHARED_DSK_ITEM *collision = nullptr;
    bool inserted = false;
    REQUIRE(__wt_shared_dsk_cache_put(env.session(), data, CROSS_CHECKPOINT_CACHING_TEST_DATA_SIZE,
              addr_a, sizeof(addr_a), &block_meta, &collision, &inserted) == 0);
    REQUIRE(!inserted);
    REQUIRE(collision == item_a);
    REQUIRE(cache->size == 1);
    /* Caller retains data on collision; free it. */
    __wt_free(env.session(), data);

    WT_SHARED_DSK_ITEM *item_b = env.put(addr_b, sizeof(addr_b));
    REQUIRE(cache->size == 2);

    /* Releasen item_a extra ref from the collision bump. */
    __wt_shared_dsk_cache_release(env.session(), item_a);
    REQUIRE(cache->size == 2);

    /* Release item_a original ref — removes it from the cache. */
    __wt_shared_dsk_cache_release(env.session(), item_a);
    REQUIRE(cache->size == 1);

    __wt_shared_dsk_cache_release(env.session(), item_b);
    REQUIRE(cache->size == 0);
}

TEST_CASE("step_up_step_down: step-up sets enabled to false",
  "[cross_checkpoint_caching],[cross_checkpoint_caching_step_up_step_down]")
{
    cross_checkpoint_caching_test_env env;
    WT_SHARED_DSK_CACHE *cache = &S2C(env.session())->cache->shared_dsk_cache;

    REQUIRE(cache->enabled);
    simulate_step_up(env.session());
    REQUIRE(!cache->enabled);
}

TEST_CASE(
  "step_up_step_down: releasing last follower-phase reference after step-up destroys the cache",
  "[cross_checkpoint_caching],[cross_checkpoint_caching_step_up_step_down]")
{
    cross_checkpoint_caching_test_env env;
    WT_SHARED_DSK_CACHE *cache = &S2C(env.session())->cache->shared_dsk_cache;

    const uint8_t addr_a[] = {0x01, 0x02};
    const uint8_t addr_b[] = {0x03, 0x04};

    WT_SHARED_DSK_ITEM *item_a = env.put(addr_a, sizeof(addr_a));
    WT_SHARED_DSK_ITEM *item_b = env.put(addr_b, sizeof(addr_b));
    REQUIRE(cache->size == 2);

    simulate_step_up(env.session());
    REQUIRE(!cache->enabled);

    /* Cache is disabled but hash is still alive while references remain. */
    REQUIRE(cache->hash != nullptr);

    __wt_shared_dsk_cache_release(env.session(), item_a);
    REQUIRE(cache->size == 1);
    REQUIRE(cache->hash != nullptr);

    /* Last release destroys the cache structure. */
    __wt_shared_dsk_cache_release(env.session(), item_b);
    REQUIRE(cache->size == 0);
    REQUIRE(cache->hash == nullptr);
}

TEST_CASE("step_up_step_down: step-down reinitialises cache after it has been destroyed",
  "[cross_checkpoint_caching],[cross_checkpoint_caching_step_up_step_down]")
{
    cross_checkpoint_caching_test_env env;
    WT_SHARED_DSK_CACHE *cache = &S2C(env.session())->cache->shared_dsk_cache;

    const uint8_t addr[] = {0xde, 0xad};
    WT_SHARED_DSK_ITEM *item = env.put(addr, sizeof(addr));

    simulate_step_up(env.session());
    __wt_shared_dsk_cache_release(env.session(), item);

    REQUIRE(cache->hash == nullptr);
    REQUIRE(!cache->enabled);

    simulate_step_down(env.session());

    REQUIRE(cache->hash != nullptr);
    REQUIRE(cache->enabled);
    REQUIRE(cache->size == 0);
}

TEST_CASE("step_up_step_down: full cycle populates cache after step-down",
  "[cross_checkpoint_caching],[cross_checkpoint_caching_step_up_step_down]")
{
    cross_checkpoint_caching_test_env env;
    WT_SHARED_DSK_CACHE *cache = &S2C(env.session())->cache->shared_dsk_cache;

    /* Follower phase: populate the cache. */
    const uint8_t addr[] = {0xca, 0xfe};
    WT_SHARED_DSK_ITEM *item = env.put(addr, sizeof(addr));
    REQUIRE(cache->size == 1);

    /* Step-up: disable and drain. */
    simulate_step_up(env.session());
    __wt_shared_dsk_cache_release(env.session(), item);
    REQUIRE(cache->hash == nullptr);

    /* Step-down: reinitialize. */
    simulate_step_down(env.session());
    REQUIRE(cache->enabled);
    REQUIRE(cache->size == 0);

    /* New follower phase: cache is usable again. */
    const uint8_t addr2[] = {0xbe, 0xef};
    WT_SHARED_DSK_ITEM *item2 = env.put(addr2, sizeof(addr2));
    REQUIRE(cache->size == 1);

    WT_SHARED_DSK_ITEM *got = nullptr;
    __wt_shared_dsk_cache_get(env.session(), addr2, sizeof(addr2), &got);
    REQUIRE(got == item2);
    REQUIRE(got->ref_count == 2);

    __wt_shared_dsk_cache_release(env.session(), got);
    __wt_shared_dsk_cache_release(env.session(), item2);
    REQUIRE(cache->size == 0);
}
