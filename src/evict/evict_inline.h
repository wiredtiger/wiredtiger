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

static WT_INLINE bool __evict_page_updates_candidate(WT_PAGE *page);

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
    if (((read_gen = __wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen)) ==
        WT_READGEN_WONT_NEED ||
         read_gen == WT_READGEN_EVICT_SOON)) {
        if (!WT_PAGE_IS_INTERNAL(page)) {
            if (!__wt_page_is_modified(page) && !__evict_page_updates_candidate(page))
                return WT_EVICT_LEVEL_WONT_NEED_CLEAN_LEAF;
            else
                return WT_EVICT_LEVEL_WONT_NEED_DIRTY_LEAF;
        }
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
    else if (__evict_page_updates_candidate(page)) {
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
__wt_evict_needed(WT_SESSION_IMPL *session, bool busy, bool readonly, bool ignore_updates_dirty,
                  double *pct_fullp)
{
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    double pct_dirty, pct_full, pct_updates;
    bool clean_needed, dirty_needed, updates_needed;

    conn = S2C(session);
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

        /*
         * Temporary solution to not do updates and dirty eviction using application threads on
         * followers or during step-up. Log an error and log an error if the cache is full of
         * updates or dirty pages.
         */
        if (ignore_updates_dirty && __wt_conn_is_disagg(session) &&
            (!conn->layered_table_manager.leader || F_ISSET(conn, WT_CONN_RECONFIGURING_STEP_UP))) {
            double cache_full = (evict->eviction_target + evict->eviction_trigger) / 2;
            if (pct_updates > cache_full)
                __wt_verbose_debug1(
                  session, WT_VERB_EVICTION, "cache is full of updates: %f percent", pct_updates);

            if (pct_dirty > cache_full)
                __wt_verbose_debug1(
                  session, WT_VERB_EVICTION, "cache is full of dirty pages: %f percent", pct_dirty);

            if (dirty_needed || updates_needed)
                WT_STAT_CONN_DSRC_INCR(session, cache_eviction_app_threads_skip_updates_dirty_page);

            if (pct_fullp != NULL)
                *pct_fullp = 100.0 - (evict->eviction_trigger - pct_full);
            return (clean_needed);
        }
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

/*
 * __evict_check_user_ok_with_eviction --
 *     Check if the user wants to abort eviction in this session. #private
 */
static bool
__evict_check_user_ok_with_eviction(WT_SESSION_IMPL *session, bool interruptible)
{
    /* Ask the user only if eviction is optional and it's a user session. */
    if (!interruptible || F_ISSET(session, WT_SESSION_INTERNAL))
        return (true);

    WT_EVENT_HANDLER *event_handler = session->event_handler;
    if (event_handler->handle_general != NULL &&
      event_handler->handle_general(
        event_handler, &S2C(session)->iface, &session->iface, WT_EVENT_EVICTION, NULL) != 0) {
        WT_STAT_CONN_INCR(session, eviction_interupted_by_app);
        return (false);
    }

    return (true);
}

/* !!!
 * __evict_is_session_cache_trigger_tolerant --
 *     Check if the session is cache tolerant for the configured trigger values. If tolerant,
 *     session will not perform eviction.
 *
 * Cache tolerance is cache's ability to let application threads perform workload operations above
 *     trigger levels. Cache tolerance is applicable only for dirty_trigger and updates_trigger.
 *     Cache tolerance is expressed in % with respect to the trigger level. If cache tolerance is
 *     configured as 20%, and dirty trigger is 20%, then application threads will be incrementally
 *     involve in eviction as dirty content increases from 20% - 24%.
 *
 * All the application threads will perform eviction only when the dirty content reaches 24% of
 *     cache.
 *
 * Based on the % of cache usage above trigger level with respect to tolerance level,
 *     incrementally involve the app threads for eviction.
 *
 *     Usage between 00% - 20% --> No application threads involve in eviction.
 *     Usage between 20% - 40% --> 20% of application threads involve in eviction.
 *     Usage between 40% - 60% --> 40% of application threads involve in eviction.
 *     Usage between 60% - 80% --> 60% of application threads involve in eviction.
 *     Usage between 80% - 100% --> 80% of application threads involve in eviction.
 *     Usage >= 100%, above tolerance level --> involve all application threads.
 */
static WT_INLINE bool
__evict_is_session_cache_trigger_tolerant(WT_SESSION_IMPL *session, uint8_t cache_tolerance)
{
    double dirty_trigger =
      __wt_atomic_load_double_relaxed(&S2C(session)->evict->eviction_dirty_trigger);
    uint64_t bytes_dirty_trigger = 0, bytes_max = 0;
    uint64_t bytes_dirty = 0, bytes_dirty_tolerance = 0, bytes_over_dirty_trigger = 0;

    bytes_max = S2C(session)->cache_size + 1;
    bytes_dirty = __wt_cache_dirty_leaf_inuse(S2C(session)->cache);
    bytes_dirty_trigger = (uint64_t)(dirty_trigger * bytes_max) / 100;
    /*
     * bytes_dirty_tolerance = number of bytes over the dirty trigger based on configured
     * cache_tolerance
     */
    bytes_dirty_tolerance = (uint64_t)(bytes_dirty_trigger * cache_tolerance) / 100;

    if (bytes_dirty > bytes_dirty_trigger) {
        /* Dirty content is more than dirty trigger. */
        bytes_over_dirty_trigger = bytes_dirty - bytes_dirty_trigger;

        if (bytes_over_dirty_trigger > bytes_dirty_tolerance) {
            /* More than 100% of tolerance level. 100% of the app threads are non-tolerant. */
            return (false);
        } else if (bytes_over_dirty_trigger * 5 > bytes_dirty_tolerance * 4) {
            /* 80% - 100% of tolerance level. 80% of app threads are non-tolerant. */
            if ((session->id % 5) > 0)
                return (false);
        } else if (bytes_over_dirty_trigger * 5 > bytes_dirty_tolerance * 3) {
            /* 60% - 80% of tolerance level. 60% of app threads are non-tolerant. */
            if ((session->id % 5) > 1)
                return (false);
        } else if (bytes_over_dirty_trigger * 5 > bytes_dirty_tolerance * 2) {
            /* 40% - 60% of tolerance level. 40% of app threads are non-tolerant. */
            if ((session->id % 5) > 2)
                return (false);
        } else if (bytes_over_dirty_trigger * 5 > bytes_dirty_tolerance * 1) {
            /* 20% - 40% of tolerance level. 20% of app threads are non-tolerant. */
            if ((session->id % 5) > 3)
                return (false);
        }
    }

    double updates_trigger =
      __wt_atomic_load_double_relaxed(&S2C(session)->evict->eviction_updates_trigger);
    uint64_t bytes_updates_trigger = 0;
    uint64_t bytes_updates = 0, bytes_updates_tolerance = 0, bytes_over_updates_trigger = 0;

    bytes_updates = __wt_cache_bytes_updates(S2C(session)->cache);
    bytes_updates_trigger = (uint64_t)(updates_trigger * bytes_max) / 100;
    /*
     * bytes_updates_tolerance = number of bytes over the update trigger based on configured
     * cache_tolerance
     */
    bytes_updates_tolerance = (uint64_t)(bytes_updates_trigger * cache_tolerance) / 100;

    if (bytes_updates > bytes_updates_trigger) {
        /* Updates content is more than update trigger. */
        bytes_over_updates_trigger = bytes_updates - bytes_updates_trigger;

        if (bytes_over_updates_trigger > bytes_updates_tolerance) {
            /* More than 100% of tolerance level. 100% of the app threads are non-tolerant. */
            return (false);
        } else if (bytes_over_updates_trigger * 5 > bytes_updates_tolerance * 4) {
            /* 80% - 100% of tolerance level. 80% of app threads are non-tolerant. */
            if ((session->id % 5) > 0)
                return (false);
        } else if (bytes_over_updates_trigger * 5 > bytes_updates_tolerance * 3) {
            /* 60% - 80% of tolerance level. 60% of app threads are non-tolerant. */
            if ((session->id % 5) > 1)
                return (false);
        } else if (bytes_over_updates_trigger * 5 > bytes_updates_tolerance * 2) {
            /* 40% - 60% of tolerance level. 40% of app threads are non-tolerant. */
            if ((session->id % 5) > 2)
                return (false);
        } else if (bytes_over_updates_trigger * 5 > bytes_updates_tolerance * 1) {
            /* 20% - 40% of tolerance level. 20% of app threads are non-tolerant. */
            if ((session->id % 5) > 3)
                return (false);
        }
    }
    /* session is tolerant for both the dirty trigger and update trigger. */
    return (true);
}

static WT_INLINE int
__wt_evict_app_assist_worker_check(
  WT_SESSION_IMPL *session, bool busy, bool readonly, bool interruptible, bool *didworkp)
{
    WT_TXN_GLOBAL *txn_global = &(S2C(session))->txn_global;

    WT_STAT_CONN_INCR(session, application_check_evict);
    if (didworkp != NULL)
        *didworkp = false;

    /* It is not safe to proceed if the eviction server threads aren't setup yet. */
    WT_CONNECTION_IMPL *conn = S2C(session);
    if (!__wt_atomic_load_bool_relaxed(&conn->evict_server_running)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_server_not_running);
        return (0);
    }

    if (F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_SCRUB))
        return(0);

    /* Eviction causes reconciliation. So don't evict if we can't reconcile */
    if (F_ISSET(session, WT_SESSION_NO_RECONCILE)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_no_reconcile);
        return (0);
    }

    /* If the transaction is prepared don't evict. */
    if (F_ISSET(session->txn, WT_TXN_PREPARE)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_prepare);
        return (0);
    }

    /* FIXME-WT-12905: Pre-fetch threads are not allowed to be pulled into eviction. */
    if (F_ISSET(session, WT_SESSION_PREFETCH_THREAD)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_prefetch);
        return (0);
    }

    /*
     * If the transaction is a checkpoint cursor transaction, don't try to evict. Because eviction
     * keeps the current transaction snapshot, and the snapshot in a checkpoint cursor transaction
     * can be (and likely is) very old, we won't be able to see anything current to evict and won't
     * be able to accomplish anything useful.
     */
    if (F_ISSET(session->txn, WT_TXN_IS_CHECKPOINT)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_checkpoint_txn);
        return (0);
    }

    /* Setting cache_max_wait_us to 1 effectively means "disable eviction when possible" */
    uint64_t cache_max_wait_us =
      session->cache_max_wait_us != 0 ? session->cache_max_wait_us : conn->evict->cache_max_wait_us;
    if (cache_max_wait_us == 1) {
        WT_STAT_CONN_INCR(session, app_evict_refused_max_wait_disabled);
        return (0);
    }

    /*
     * If the current transaction is keeping the oldest ID pinned, it is in the middle of an
     * operation. This may prevent the oldest ID from moving forward, leading to deadlock, so only
     * evict what we can. Otherwise, we are at a transaction boundary and we can work harder to make
     * sure there is free space in the cache.
     */
    WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session);
    busy = busy || __wt_atomic_load_uint64_v_relaxed(&txn_shared->id) != WT_TXN_NONE ||
      session->hazards.num_active > 0 ||
      (__wt_atomic_load_uint64_v_relaxed(&txn_shared->pinned_id) != WT_TXN_NONE &&
        __wt_atomic_load_uint64_v_relaxed(&txn_global->current) !=
          __wt_atomic_load_uint64_v_relaxed(&txn_global->oldest_id));

    /*
     * Don't block the thread for eviction when holding the handle list, schema or table locks
     * (which can block checkpoints and eviction), or if the "ignore cache size" flag is set. Some
     * internal threads such as the sweep server and background compact will set the "ignore cache
     * size" flag for this reason. Sessions can also set this flag using the "ignore_cache_size"
     * configuration.
     */
    if (F_ISSET(session, WT_SESSION_IGNORE_CACHE_SIZE) ||
      FLD_ISSET(session->lock_flags,
        WT_SESSION_LOCKED_HANDLE_LIST | WT_SESSION_LOCKED_SCHEMA | WT_SESSION_LOCKED_TABLE)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_locks_or_ignore_cache);
        return (0);
    }

    /* In memory configurations don't block when the cache is full. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_in_memory);
        return (0);
    }

    /*
     * Threads operating on cache-resident trees are ignored because they're not contributing to the
     * problem. We also don't block while reading metadata because we're likely to be holding some
     * other resources that could block checkpoints or eviction.
     */
    WT_BTREE *btree = S2BT_SAFE(session);
    if (btree != NULL && (F_ISSET(btree, WT_BTREE_NO_EVICT) || WT_IS_METADATA(session->dhandle))) {
        WT_STAT_CONN_INCR(session, app_evict_refused_no_evict_or_metadata);
        return (0);
    }

    /* Check if eviction is needed. */
    double pct_full;
    if (!__wt_evict_needed(session, busy, readonly, true, &pct_full)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_not_needed);
        return (0);
    }

    /*
     * If the caller is holding shared resources, only evict if the cache is at any of its eviction
     * targets.
     */
    if (busy && pct_full < 100.0) {
        WT_STAT_CONN_INCR(session, app_evict_refused_busy_below_target);
        return (0);
    }

    /* Last check if application wants to prevent the thread from evicting. */
    if (!__evict_check_user_ok_with_eviction(session, interruptible)) {
        WT_STAT_CONN_INCR(session, app_evict_refused_user_not_ok);
        return (0);
    }

    /*
     * If incremental application eviction flag is set, involve application threads proportionally
     * to the aggressiveness score, when the cache usage is less than target. 5% at score 1 to 100%
     * at score 20.
     */
    WT_EVICT *evict = conn->evict;
    if (F_ISSET_ATOMIC_32(
          &(conn->cache->cache_eviction_controls), WT_CACHE_EVICT_INCREMENTAL_APP) &&
      !__wti_evict_exceeded_clean_trigger(session, &pct_full)) {
        if (pct_full <= evict->eviction_target &&
          ((__wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score)) <= (session->id % 20))) {
            WT_STAT_CONN_INCR(session, app_evict_refused_incremental_score);
            return (0);
        }
    }

    /*
     * If the cache tolerance is configured, check if the session can be tolerant. if tolerant,
     * don't involve in eviction.
     */
    if (pct_full <= evict->eviction_trigger) {
        uint8_t cache_tolerance = __wt_atomic_load_uint8_relaxed(
          &conn->cache->cache_eviction_controls.cache_tolerance_for_app_eviction);
        if ((cache_tolerance != 0) &&
          (__evict_is_session_cache_trigger_tolerant(session, cache_tolerance))) {
            WT_STAT_CONN_INCR(session, app_evict_refused_cache_tolerant);
            return (0);
        }
    }

    /*
     * Some callers (those waiting for slow operations), will sleep if there was no cache work to
     * do. After this point, let them skip the sleep.
     */
    if (didworkp != NULL)
        *didworkp = true;

    WT_STAT_CONN_INCR(session, app_evict_accepted);
    return (__wti_evict_app_assist_worker(session, busy, readonly, interruptible));
}

static WT_INLINE bool
__evict_page_updates_candidate(WT_PAGE *page)
{
    if (page == NULL || page->modify == NULL)
        return (false);

    /*
     * Internal pages don't track bytes_updates, but still need to be evicted when updates pressure
     * is active. Evicting and reconciling an internal page frees the underlying disk blocks of any
     * fast-truncate children whose deletions have become globally visible.
     */
    if (WT_PAGE_IS_INTERNAL(page))
        return (true);

    /*
     * For leaf pages, only queue the page if it has non-zero tracked update bytes. Freshly-split
     * child pages start at zero, and evicting a page with no tracked update bytes does not reduce
     * updates cache pressure.
     */
    return (page->modify->bytes_updates != 0);
}

/*
 * __evict_level_is_dirty --
 *     Return true if this bucketset level keeps dirty pages.
 */
static WT_INLINE bool
__evict_level_is_dirty(int level)
{
    return (level == WT_EVICT_LEVEL_WONT_NEED_DIRTY_LEAF || level == WT_EVICT_LEVEL_DIRTY_LEAF
            || level == WT_EVICT_LEVEL_DIRTY_INTERNAL);
}

/*
 * __evict_level_is_internal --
 *     Return true if this bucketset level holds internal pages. Evicting an internal page forces a
 *     re-read of it before anything in its subtree can be reached again, so callers weight these
 *     levels down rather than selecting them in proportion to the pages they hold.
 */
static WT_INLINE bool
__evict_level_is_internal(int level)
{
    return (level == WT_EVICT_LEVEL_DIRTY_INTERNAL || level == WT_EVICT_LEVEL_WONT_NEED_INTERNAL ||
      level == WT_EVICT_LEVEL_UPDATES_INTERNAL || level == WT_EVICT_LEVEL_CLEAN_INTERNAL);
}
