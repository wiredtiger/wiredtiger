/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "../include/stat.h"
/* Counter slots are set to be around the expected number of cores */
#define WT_EVICT_EXPECTED_CONTENTION WT_STAT_CONN_COUNTER_SLOTS

#define EVICT_DEBUG_PRINT 0

/*
 * __wt_ref_assign_page --
 *     Must be called every time we associate a new page with a ref. A page must have a back pointer
 *     to its ref. Otherwise eviction won't work properly.
 */
static WT_INLINE void
__wt_ref_assign_page(WT_REF *ref, WT_PAGE *page)
{
    ref->page = page;
    page->ref = ref;
}

/* !!!
 * __wt_evict_aggressive --
 *     Check whether eviction is unable to make any progress for some amount of time.
 *
 *     As eviction continues to struggle, let the caller know that eviction has made no progress.
 *     This helps determine if we need to roll back transactions
 */
static WT_INLINE bool
__wt_evict_aggressive(WT_SESSION_IMPL *session)
{
    return (__wt_atomic_load_uint32_relaxed(&S2C(session)->evict->evict_aggressive_score) >=
      WT_EVICT_SCORE_CUTOFF);
}

/* !!!
 * __wt_evict_cache_stuck --
 *     Check whether eviction has remained inefficient (or made no progress) for a significant
 *     period and that the cache has crossed the trigger thresholds even after significant
 *     efforts towards forceful eviction.
 *
 *     This function represents a more severe state compared to aggressive eviction and servers as a
 *     useful indicator of eviction's health, based on which callers may make certain choices to
 *     reduce cache pressure.
 */
static WT_INLINE bool
__wt_evict_cache_stuck(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;
    uint32_t tmp_evict_aggressive_score;

    evict = S2C(session)->evict;
    tmp_evict_aggressive_score = __wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score);
    WT_ASSERT(session, tmp_evict_aggressive_score <= WT_EVICT_SCORE_MAX);

    return (
      tmp_evict_aggressive_score == WT_EVICT_SCORE_MAX && F_ISSET(evict, WT_EVICT_CACHE_HARD));
}

/*
 * __evict_destination_bucket --
 *     Given the read generation, find the id of its destination bucket.
 *
 * In a single-core world we would compute the home bucket by dividing the read generation by the
 *     read generation step, typically set to 100. So a read generation of 200 would land into
 *     bucket 2, a read generation of 5000 would land into bucket 50 etc.
 *
 * In a multi-core world having only a single bucket for a range of read generations would mean
 *     contention for the bucket's lock. Using smaller ranges per bucket doesn't really help, read
 *     generations of pages touched close together in time will be very similar or the same (e.g.,
 *     many pages with a read generation of, say, 257).
 *
 * Instead we let a range of read generations span many buckets. The size of the span is controlled
 *     by the "expected contention" parameter. So if the expected contention is 10, then the first
 *     10 buckets will be given to read generations 0-99, the next 10 buckets will be given to read
 *     generations 100-199, etc. A thread selects the destination bucket from the available 10 by
 *     adding its session id to the first bucket in the set of 10. If the number of cores is equal
 *     to 10 (the expected contention parameter), we never compete for the buckets.
 *
 * We use a modular division to wrap around to the first bucket when we exceed the length of bucket
 *     array.
 */
static WT_INLINE uint64_t
__evict_destination_bucket(WT_SESSION_IMPL *session, WT_EVICT_BUCKETSET *bucketset, WT_PAGE *page)
{
    uint64_t base_bucket, read_gen;
    uint32_t num_buckets;

    num_buckets = bucketset->num_buckets;
    read_gen = __wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen);

    /*
     * If this is a page we won't need, it goes into a distinct bucketset. In that bucketset all
     * pages have the same read generation, so we place into a randomly selected bucket. Also, read
     * generations only make sense for clean pages. For dirty content, we want to clean ASAP
     * regardless of read generations.
     */
    if (read_gen == WT_READGEN_WONT_NEED || read_gen == WT_READGEN_EVICT_SOON ||
      (bucketset->level != WT_EVICT_LEVEL_CLEAN_LEAF &&
        bucketset->level != WT_EVICT_LEVEL_CLEAN_INTERNAL)) {
        return (uint64_t)__wt_random(&session->rnd_random) % num_buckets;
    }

    /*
     * Each read generation gets many slots, so threads don't compete for the same bucket when we
     * are at that generation
     */
    base_bucket = read_gen / WT_READGEN_STEP * WT_EVICT_EXPECTED_CONTENTION;

    /*
     * Add a random offset to the base bucket so we don't contend on the same bucket for the read
     * generation
     */
    return (base_bucket + __wt_random(&session->rnd_random) % WT_EVICT_EXPECTED_CONTENTION) % num_buckets;
}

/*
 * __wt_evict_get_bucketset_level --
 *     Return the expected bucketset level for a page given its properties.
 */
static WT_INLINE int
__evict_target_bucketset_level(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    uint64_t read_gen;
    if ((read_gen = __wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen)) ==
        WT_READGEN_WONT_NEED ||
      read_gen == WT_READGEN_EVICT_SOON) {
        if (!WT_PAGE_IS_INTERNAL(page))
            return WT_EVICT_LEVEL_WONT_NEED_LEAF;
        else
            return WT_EVICT_LEVEL_WONT_NEED_INTERNAL;
    } else if (!WT_PAGE_IS_INTERNAL(page) && !__wt_page_is_modified(page) && page->modify == NULL)
        return WT_EVICT_LEVEL_CLEAN_LEAF;
    else if (WT_PAGE_IS_INTERNAL(page) && !__wt_page_is_modified(page) && page->modify == NULL)
        return WT_EVICT_LEVEL_CLEAN_INTERNAL;
    else if (!WT_PAGE_IS_INTERNAL(page) && __wt_page_is_modified(page))
        return WT_EVICT_LEVEL_DIRTY_LEAF;
    else if (WT_PAGE_IS_INTERNAL(page) && __wt_page_is_modified(page))
        return WT_EVICT_LEVEL_DIRTY_INTERNAL;
    else if (page->modify != NULL) {
        if (WT_PAGE_IS_INTERNAL(page))
            return WT_EVICT_LEVEL_UPDATES_INTERNAL;
        else
            return WT_EVICT_LEVEL_UPDATES_LEAF;
    }

    /*
     * If we are here, we couldn't determine the bucketset level for a page and this must never
     * happen.
     */
    WT_ASSERT(session, false);
    return 0;
}

/*
 * __evict_read_gen --
 *     Get the current read generation number.
 */
static WT_INLINE uint64_t
__evict_read_gen(WT_SESSION_IMPL *session)
{
    return (__wt_atomic_load_uint64_relaxed(&S2C(session)->evict->read_gen));
}

/*
 * __evict_get_target_bucketset --
 *     Return the target bucket and bucketset for the page given its properties. if the return value
 *     is true, the page's current destination is already equal to the target.
 */
static WT_INLINE bool
__evict_get_target_destination(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_EVICT_BUCKETSET **bucketset, WT_EVICT_BUCKET **bucket)
{
    WT_EVICT *evict;
    WT_EVICT_BUCKET *target_bucket;
    WT_EVICT_BUCKETSET *target_bucketset;
    uint64_t target_bucket_id;
    int target_bucketset_level;

    evict = S2C(session)->evict;

    /* Find the right bucketset level for the page */
    target_bucketset_level = __evict_target_bucketset_level(session, page);

    WT_ASSERT(session, target_bucketset_level >= 0 && target_bucketset_level < WT_EVICT_LEVELS);

    target_bucketset = &evict->evict_bucketset[target_bucketset_level];
    target_bucket_id = __evict_destination_bucket(session, target_bucketset, page);
    target_bucket = &target_bucketset->buckets[target_bucket_id];

    if (bucket != NULL)
        *bucket = target_bucket;
    if (bucketset != NULL)
        *bucketset = target_bucketset;

    if (page->evict_data.bucket != target_bucket) {
        return false; /* page isn't at the right place */
    }
    return true;
}

/*
 * __evict_needs_new_bucket --
 *     A quick check to see if the page will need to be moved into a new bucket.
 */
static WT_INLINE bool
__evict_needs_new_bucket(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    return !__evict_get_target_destination(session, page, NULL, NULL);
}

/*
 * __evict_readgen_is_soon_or_wont_need --
 *     Return whether a read generation value makes a page eligible for forced eviction. Read
 *     generations reserve a range of low numbers for special meanings and currently - with the
 *     exception of the generation not being set - these indicate the page may be evicted
 *     forcefully.
 */
static WT_INLINE bool
__evict_readgen_is_soon_or_wont_need(uint64_t *readgen)
{
    uint64_t gen;

    WT_READ_ONCE(gen, *readgen);
    return (gen != WT_READGEN_NOTSET && gen < WT_READGEN_START_VALUE);
}

/*
 * __wti_evict_read_gen_bump --
 *     Update the page's read generation. Return true if we bumped the read generation.
 */
static WT_INLINE bool
__wti_evict_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    /* Ignore pages set for forcible eviction. */
    if (__evict_readgen_is_soon_or_wont_need(&page->evict_data.read_gen))
        return false;

    /* Ignore pages already in the future. */
    if (__wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen) > __evict_read_gen(session))
        return false;

    /*
     * We set read-generations in the future (where "the future" is measured by increments of the
     * global read generation). The reason is because when acquiring a new hazard pointer for a
     * page, we can check its read generation, and if the read generation isn't less than the
     * current global generation, we don't bother updating the page. In other words, the goal is to
     * avoid some number of updates immediately after each update we have to make.
     */
    __wt_atomic_store_uint64_relaxed(&page->evict_data.read_gen,
                                     __evict_read_gen(session) + WT_READGEN_STEP);
    return true;
}

/* !!!
 * __wt_evict_page_is_soon_or_wont_need --
 *     Check whether a page is a candidate for forced eviction.
 *
 *     Pages marked with lower eviction state (read generation) including `WT_READGEN_EVICT_SOON`
 *     or `WT_READGEN_WONT_NEED` have precedence to be immediately removed from the cache.
 *
 *     At present, this function is called once during the decision of whether an application thread
 *     should perform forced eviction or urgently queue the page for eviction.
 *
 *     Input parameter:
 *       `page`: The page to be checked if it is subject to forced eviction.
 *
 *     Return `true` if the page should be forcefully evicted.
 */
static WT_INLINE bool
__wt_evict_page_is_soon_or_wont_need(WT_PAGE *page)
{
    return (__evict_readgen_is_soon_or_wont_need(&page->evict_data.read_gen));
}

/* !!!
 * __wt_evict_page_is_soon --
 *     Check whether a page is marked with the `WT_READGEN_EVICT_SOON` state, indicating that
 *     it should be evicted as soon as possible.
 *
 *     Currently, this function is called once when deciding whether to unpin the cursor to
 *     facilitate eviction. The `__wt_evict_page_is_soon_or_wont_need` function is not used in this
 *     context because only the `WT_READGEN_EVICT_SOON` state is relevant here (not the
 *     `WT_READGEN_WONT_NEED`).
 *
 *     Input parameter:
 *       `page`: The page to be checked for the evict soon state.
 *
 *     Return `true` if the page is marked to be evicted soon.
 */
static WT_INLINE bool
__wt_evict_page_is_soon(WT_PAGE *page)
{
    return (__wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen) == WT_READGEN_EVICT_SOON);
}

/* !!!
 * __wt_evict_page_init --
 *     Initialize the page's eviction state (read generation) for a newly created page in memory.
 *     Even if the page is evicted and later reallocated, this function will be called to reset
 *     the eviction state. This initialization is essential as it sets the `read_gen` value, which
 *     eviction uses to determine the priority of pages for eviction.
 *
 *     It is called only once when the page is first allocated in memory and should not be called
 *     again for that page.
 *
 *     Input parameter:
 *       `page`: The page for which to initialize the read generation.
 *
 *     We can't put the page into eviction data structures at this point, because we
 *     don't have its reference.
 */
static WT_INLINE void
__wt_evict_page_init(WT_PAGE *page, uint64_t evict_pass_gen)
{
    __wt_atomic_store_uint64_relaxed(&page->evict_data.read_gen, WT_READGEN_NOTSET);
    page->evict_data.cache_create_gen = evict_pass_gen;
}

/* !!!
 * __wt_evict_inherit_page_state --
 *     Initialize the read generation on the new page using the read generation of the original
 *     page, unless this was a forced eviction, in which case we leave the new page with the
 *     default initialization.
 *
 *     Insert the new page in the same bucket as the original page. The new page may not have its
 *     home set after it inherits state, so eviction code will think it's a root page and won't
 *     insert it into the bucket. So we do this here.
 *
 *     It is called when creating a new page from an existing page, for example during split.
 *
 *     Input parameters:
 *       (1) `orig_page`: The page from which to inherit the read generation.
 *       (2) `new_page`: The page for which to set the read generation.
 */
static WT_INLINE void
__wt_evict_inherit_page_state(WT_PAGE *orig_page, WT_PAGE *new_page)
{
    uint64_t orig_read_gen;

    WT_READ_ONCE(orig_read_gen, orig_page->evict_data.read_gen);

    if (!__evict_readgen_is_soon_or_wont_need(&orig_read_gen))
        __wt_atomic_store_uint64_relaxed(&new_page->evict_data.read_gen, orig_read_gen);
}

/* !!!
 * __wt_evict_page_cache_bytes_decr --
 *     Decrement the in-memory byte count for the cache, B-tree, and page to reflect the eviction
 *     of a page.
 *
 *     It is called once each time a page is evicted from memory.
 *
 *     Input parameter:
 *       `page`: The page being evicted, for which byte counts are decremented.
 */
static WT_INLINE void
__wt_evict_page_cache_bytes_decr(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_BTREE *btree;
    WT_CACHE *cache;
    WT_PAGE_MODIFY *modify;
    uint64_t memory_footprint;
    bool is_disagg;

    btree = S2BT(session);
    cache = S2C(session)->cache;
    modify = page->modify;
    memory_footprint = __wt_atomic_load_size_relaxed(&page->memory_footprint);
    is_disagg = __wt_conn_is_disagg(session);

    /* Update the bytes in-memory to reflect the eviction. */
    __wt_cache_decr_check_uint64(
      session, &btree->bytes_inmem, memory_footprint, "WT_BTREE.bytes_inmem");
    __wt_cache_decr_check_uint64(
      session, &cache->bytes_inmem, memory_footprint, "WT_CACHE.bytes_inmem");
    if (is_disagg) {
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
            __wt_cache_decr_check_uint64(
              session, &cache->bytes_inmem_ingest, memory_footprint, "WT_CACHE.bytes_inmem_ingest");
        else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
            __wt_cache_decr_check_uint64(
              session, &cache->bytes_inmem_stable, memory_footprint, "WT_CACHE.bytes_inmem_stable");
    }

    /* Update the bytes_internal value to reflect the eviction */
    if (WT_PAGE_IS_INTERNAL(page)) {
        __wt_cache_decr_check_uint64(
          session, &btree->bytes_internal, memory_footprint, "WT_BTREE.bytes_internal");
        __wt_cache_decr_check_uint64(
          session, &cache->bytes_internal, memory_footprint, "WT_CACHE.bytes_internal");
        if (is_disagg) {
            if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
                __wt_cache_decr_check_uint64(session, &cache->bytes_internal_ingest,
                  memory_footprint, "WT_CACHE.bytes_internal_ingest");
            else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
                __wt_cache_decr_check_uint64(session, &cache->bytes_internal_stable,
                  memory_footprint, "WT_CACHE.bytes_internal_stable");
        }
    }

    /* Update the cache's dirty-byte count. */
    if (modify != NULL && modify->bytes_dirty != 0) {
        if (WT_PAGE_IS_INTERNAL(page)) {
            __wt_cache_decr_check_uint64(
              session, &btree->bytes_dirty_intl, modify->bytes_dirty, "WT_BTREE.bytes_dirty_intl");
            __wt_cache_decr_check_uint64(
              session, &cache->bytes_dirty_intl, modify->bytes_dirty, "WT_CACHE.bytes_dirty_intl");
            if (is_disagg) {
                if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
                    __wt_cache_decr_check_uint64(session, &cache->bytes_dirty_intl_ingest,
                      modify->bytes_dirty, "WT_CACHE.bytes_dirty_intl_ingest");
                else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
                    __wt_cache_decr_check_uint64(session, &cache->bytes_dirty_intl_stable,
                      modify->bytes_dirty, "WT_CACHE.bytes_dirty_intl_stable");
            }
        } else {
            __wt_cache_decr_check_uint64(
              session, &btree->bytes_dirty_leaf, modify->bytes_dirty, "WT_BTREE.bytes_dirty_leaf");
            __wt_cache_decr_check_uint64(
              session, &cache->bytes_dirty_leaf, modify->bytes_dirty, "WT_CACHE.bytes_dirty_leaf");
            if (is_disagg) {
                if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
                    __wt_cache_decr_check_uint64(session, &cache->bytes_dirty_leaf_ingest,
                      modify->bytes_dirty, "WT_CACHE.bytes_dirty_leaf_ingest");
                else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
                    __wt_cache_decr_check_uint64(session, &cache->bytes_dirty_leaf_stable,
                      modify->bytes_dirty, "WT_CACHE.bytes_dirty_leaf_stable");
            }
        }
    }

    /* Update the cache's updates-byte count. */
    if (modify != NULL) {
        __wt_cache_decr_check_uint64(
          session, &btree->bytes_updates, modify->bytes_updates, "WT_BTREE.bytes_updates");
        __wt_cache_decr_check_uint64(
          session, &cache->bytes_updates, modify->bytes_updates, "WT_CACHE.bytes_updates");
        if (is_disagg) {
            if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
                __wt_cache_decr_check_uint64(session, &cache->bytes_updates_ingest,
                  modify->bytes_updates, "WT_CACHE.bytes_updates_ingest");
            else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
                __wt_cache_decr_check_uint64(session, &cache->bytes_updates_stable,
                  modify->bytes_updates, "WT_CACHE.bytes_updates_stable");
        }
    }

    /* Update bytes and pages evicted. */
    (void)__wt_atomic_add_uint64_relaxed(&cache->bytes_evict, memory_footprint);
    (void)__wt_atomic_add_uint64_v_relaxed(&cache->pages_evicted, 1);
    if (is_disagg) {
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
            (void)__wt_atomic_add_uint64_v_relaxed(&cache->pages_evicted_ingest, 1);
        else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
            (void)__wt_atomic_add_uint64_v_relaxed(&cache->pages_evicted_stable, 1);
    }

    /*
     * Track if eviction makes progress. This is used in various places to determine whether
     * eviction is stuck.
     */
    if (!F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_NO_PROGRESS))
        (void)__wt_atomic_add_uint64_v(&S2C(session)->evict->eviction_progress, 1);
}

/* !!!
 * __wt_evict_clean_pressure --
 *     Check whether the cache is approaching or has surpassed its eviction trigger thresholds,
 *     indicating that application threads will soon be required to assist with eviction.
 *
 *     At present, this function is primarily called by the prefetch thread to determine whether it
 *     should avoid prefetching pages, as application threads may soon be involved in eviction.
 *
 *     Return `true` if the cache is nearing the eviction trigger thresholds.
 */
static WT_INLINE bool
__wt_evict_clean_pressure(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;
    double pct_full;

    evict = S2C(session)->evict;
    pct_full = 0;

    /* Eviction should be done if we hit the eviction clean trigger or come close to hitting it. */
    if (__wti_evict_exceeded_clean_trigger(session, &pct_full))
        return (true);
    if (pct_full > evict->eviction_target &&
      pct_full >= WT_EVICT_PRESSURE_THRESHOLD * evict->eviction_trigger)
        return (true);
    return (false);
}

/* !!!
 * __wti_evict_exceeded_clean_target --
 *    Check if the cache exceeded the configured target for clean pages.
 */
static WT_INLINE bool
__wti_evict_exceeded_clean_target(WT_SESSION_IMPL *session)
{
    uint64_t bytes_inuse, bytes_max;

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = __wt_tsan_suppress_load_uint64_v(&S2C(session)->cache_size) + 1;
    bytes_inuse = __wt_cache_bytes_inuse(S2C(session)->cache);

    return (bytes_inuse > (S2C(session)->evict->eviction_target * bytes_max) / 100);
}

/* !!!
 * __wti_evict_exceeded_clean_trigger --
 *     Check whether the configured eviction trigger threshold for the total volume of data in the
 *     cache has been reached. Once this threshold is met, application threads are signaled to
 *     assist with eviction. The eviction trigger threshold is configurable, and defined in
 *     `api_data.py`.
 *
 *     This function is called by the eviction server to determine the cache's current state and to
 *     set the internal flags accordingly.
 *
 *     Input parameter:
 *       `pct_full`: A pointer to store the percentage of cache used, if not NULL.
 *
 *     Return `true` if the cache usage exceeds the eviction trigger threshold.
 */
static WT_INLINE bool
__wti_evict_exceeded_clean_trigger(WT_SESSION_IMPL *session, double *pct_fullp)
{
    uint64_t bytes_inuse, bytes_max;

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = S2C(session)->cache_size + 1;
    bytes_inuse = __wt_cache_bytes_inuse(S2C(session)->cache);

    if (pct_fullp != NULL)
        *pct_fullp = ((100.0 * bytes_inuse) / bytes_max);

    return (bytes_inuse > (S2C(session)->evict->eviction_trigger * bytes_max) / 100);
}

/*
 * __wti_evict_dirty_target --
 *     Return the effective dirty target (including checkpoint scrubbing).
 */
static WT_INLINE double
__wti_evict_dirty_target(WT_EVICT *evict)
{
    double dirty_target, scrub_target;

    dirty_target = __wt_atomic_load_double_relaxed(&evict->eviction_dirty_target);
    scrub_target = __wt_atomic_load_double_relaxed(&evict->eviction_scrub_target);

    return (scrub_target > 0 && scrub_target < dirty_target ? scrub_target : dirty_target);
}

/* !!!
 * __wti_evict_exceeded_dirty_trigger --
 *     Check whether the configured eviction dirty trigger threshold for the total volume
 *     of dirty data in the cache has been reached. Once this threshold is met, application threads
 *     are signaled to assist with the eviction of dirty pages. The eviction dirty trigger threshold
 *     is configurable, and defined in `api_data.py`.
 *
 *     This function is called by the eviction server to determine the cache's current
 *     state and to set the internal flags accordingly.
 *
 *     Input parameter:
 *       `pct_full`: A pointer to store the percentage of the cache used by the dirty leaf pages.
 *
 *     Return `true` if the cache usage exceeds the eviction dirty trigger threshold.
 */
static WT_INLINE bool
__wti_evict_exceeded_dirty_trigger(WT_SESSION_IMPL *session, double *pct_fullp)
{
    uint64_t bytes_dirty, bytes_max;
    double dirty_trigger =
      __wt_atomic_load_double_relaxed(&S2C(session)->evict->eviction_dirty_trigger);

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_dirty = __wt_cache_dirty_leaf_inuse(S2C(session)->cache);
    bytes_max = S2C(session)->cache_size + 1;

    if (pct_fullp != NULL)
        *pct_fullp = (100.0 * bytes_dirty) / bytes_max;

    return (bytes_dirty > (uint64_t)(dirty_trigger * bytes_max) / 100);
}

/* !!!
 * __wti_evict_exceeded_dirty_target --
 *     Check whether the configured eviction dirty target threshold for the total volume
 *     of dirty data in the cache has been reached. Once this threshold is met, eviction threads
 *     begin eviction of dirty pages. The eviction dirty trigger threshold
 *     is configurable, and defined in `api_data.py`.
 *
 *     This function is called by the eviction server to determine the cache's current
 *     state and to set the internal flags accordingly.
 */
static WT_INLINE bool
__wti_evict_exceeded_dirty_target(WT_SESSION_IMPL *session)
{
    double dirty_target;
    uint64_t bytes_dirty, bytes_max;

    dirty_target = __wti_evict_dirty_target(S2C(session)->evict);

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_dirty = __wt_cache_dirty_leaf_inuse(S2C(session)->cache);
    bytes_max = S2C(session)->cache_size + 1;

    return (bytes_dirty > (uint64_t)(dirty_target * bytes_max) / 100);
}

/* !!!
 * __wti_evict_exceeded_updates_trigger --
 *     Check whether the configured eviction update trigger threshold for the total volume of
 *     updates in the cache has been reached. Once this threshold is met, application threads are
 *     signaled to assist with the eviction of pages with updates. The eviction update trigger
 *     threshold is configurable, and defined in `api_data.py`.
 *
 *     This function is called by the eviction server to determine the cache's current
 *     state and to set the internal flags accordingly.
 *
 *     Input parameter:
 *       `pct_full`: A pointer to store the percentage of the cache used by updates.
 *
 *     Returns `true` if the cache usage exceeds the eviction update trigger threshold.
 */
static WT_INLINE bool
__wti_evict_exceeded_updates_trigger(WT_SESSION_IMPL *session, double *pct_fullp)
{
    uint64_t bytes_max, bytes_updates;

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = S2C(session)->cache_size + 1;
    bytes_updates = __wt_cache_bytes_updates(S2C(session)->cache);

    if (pct_fullp != NULL)
        *pct_fullp = (100.0 * bytes_updates) / bytes_max;

    return (bytes_updates >
      (uint64_t)(__wt_atomic_load_double_relaxed(&S2C(session)->evict->eviction_updates_trigger) *
        bytes_max) /
        100);
}

/* !!!
 * __wti_evict_exceeded_updates_target --
 *     Check whether the configured eviction update target threshold for the total volume of
 *     updates in the cache has been reached. Once this threshold is met, eviction threads
 *     begin eviction of pages with updates. The eviction update trigger
 *     threshold is configurable, and defined in `api_data.py`.
 *
 *     This function is called by the eviction server to determine the cache's current
 *     state and to set the internal flags accordingly.
 */
static WT_INLINE bool
__wti_evict_exceeded_updates_target(WT_SESSION_IMPL *session)
{
    double updates_target;
    uint64_t bytes_max, bytes_updates;

    /*
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = S2C(session)->cache_size + 1;
    bytes_updates = __wt_cache_bytes_updates(S2C(session)->cache);
    updates_target = S2C(session)->evict->eviction_updates_target;

    return (bytes_updates > (uint64_t)(updates_target * bytes_max) / 100);
}

/* !!!
 * __wt_evict_needed --
 *     Check whether the configured clean/dirty/update eviction trigger thresholds for the cache
 *     have been reached.
 *
 *     This function is called to determine whether cache is under pressure.
 *
 *     Input parameters:
 *       (1) `busy`: A flag indicating if the session is actively pinning resources, in which
 *            case dirty trigger is ignored.
 *       (2) `readonly`: A flag indicating if the session is read-only, in which case dirty and
 *            update triggers are ignored.
 *       (3) `pct_full`: A pointer to store the calculated cache full percentage, if not NULL.
 *
 *     Return `true` if the cache usage exceeds any of the clean/dirty/update eviction trigger
 *     thresholds.
 */
static WT_INLINE bool
__wt_evict_needed(WT_SESSION_IMPL *session, bool busy, bool readonly, double *pct_fullp)
{
    WT_EVICT *evict;
    double pct_dirty, pct_full, pct_updates;
    bool clean_needed, dirty_needed, updates_needed;

    evict = S2C(session)->evict;

    /*
     * If the connection is closing we do not need eviction from an application thread. The eviction
     * subsystem is already closed.
     */
    if (F_ISSET(S2C(session), WT_CONN_CLOSING))
        return (false);

    clean_needed = __wti_evict_exceeded_clean_trigger(session, &pct_full);
    if (readonly) {
        dirty_needed = updates_needed = false;
        pct_dirty = pct_updates = 0.0;
    } else {
        dirty_needed = __wti_evict_exceeded_dirty_trigger(session, &pct_dirty);
        updates_needed = __wti_evict_exceeded_updates_trigger(session, &pct_updates);
    }

    /*
     * Calculate the cache full percentage.
     */
    if (pct_fullp != NULL)
        *pct_fullp = WT_MAX(0.0,
          100.0 -
            WT_MIN(
              WT_MIN(evict->eviction_trigger - pct_full, evict->eviction_dirty_trigger - pct_dirty),
              evict->eviction_updates_trigger - pct_updates));

    /*
     * Only check the dirty trigger when the session is not busy.
     *
     * In other words, once we are pinning resources, try to finish the operation as quickly as
     * possible without exceeding the cache size. The next transaction in this session will not be
     * able to start until the cache is under the limit.
     */
    return (clean_needed || updates_needed || (!busy && dirty_needed));
}

/* !!!
 * __wt_evict_favor_clearing_dirty_cache --
 *    !!! Use this function with caution as it will significantly impact eviction behavior. !!!
 *
 *    Adjust eviction settings (`dirty_target` and `dirty_trigger`) to aggressively remove dirty
 *    bytes from the cache.
 *
 *    It should only be called once during `WT_CONNECTION::close`.
 */
static WT_INLINE void
__wt_evict_favor_clearing_dirty_cache(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;

    evict = S2C(session)->evict;

    /*
     * Ramp the eviction dirty target down to encourage eviction threads to clear dirty content out
     * of cache.
     */
    __wt_atomic_store_double_relaxed(&evict->eviction_dirty_trigger, 1.0);
    __wt_atomic_store_double_relaxed(&evict->eviction_dirty_target, 0.1);
}

/*
 * __wti_evict_hs_dirty --
 *     Return if a major portion of the cache is dirty due to history store content.
 */
static WT_INLINE bool
__wti_evict_hs_dirty(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    uint64_t bytes_max;
    conn = S2C(session);
    cache = conn->cache;
    bytes_max = conn->cache_size;

    return (__wt_cache_bytes_plus_overhead(
              cache, __wt_atomic_load_uint64_relaxed(&cache->bytes_hs_dirty)) >=
      ((uint64_t)(conn->evict->eviction_dirty_trigger * bytes_max) / 100));
}

/* !!!
 * __wt_evict_check_if_blocking
 *    Check if this session is blocking eviction.
 */
static WT_INLINE int
__wt_evict_check_if_blocking(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    WT_CONNECTION_IMPL *conn = S2C(session);
    WT_EVICT *evict = conn->evict;

    /*
     * If eviction is stuck, check if this thread is likely causing problems and should be
     * rolled back. Ignore if in recovery, those transactions can't be rolled back.
     */
    if (!F_ISSET(conn, WT_CONN_RECOVERING) && __wt_evict_cache_stuck(session)) {
        ret = __wt_txn_is_blocking(session);
        if (ret == WT_ROLLBACK) {
            __wt_atomic_decrement_if_positive(&evict->evict_aggressive_score);
            if (F_ISSET(session, WT_SESSION_SAVE_ERRORS))
                __wt_verbose_debug1(session, WT_VERB_TRANSACTION, "rollback reason: %s",
                                    session->err_info.err_msg);
        }
    }
    if (ret != 0) {
        printf("Transaction blocking eviction\n");
        WT_STAT_CONN_INCR(session, eviction_transaction_blocking);
    }
    return (ret);
}
