/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __evict_read_gen --
 *     Get the current read generation number.
 */
static WT_INLINE uint64_t
__evict_read_gen(WT_SESSION_IMPL *session)
{
    return (__wt_atomic_load_uint64_relaxed(&WT_EVICT_RANDLRU(S2C(session)->evict)->read_gen));
}

/*
 * __wti_evict_read_gen_bump --
 *     Update the page's read generation.
 */
static WT_INLINE void
__wti_evict_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    /* Ignore pages set for forcible eviction. */
    if (__wt_atomic_load_uint64_relaxed(&page->read_gen) == WT_READGEN_EVICT_SOON)
        return;

    /* Ignore pages already in the future. */
    if (__wt_atomic_load_uint64_relaxed(&page->read_gen) > __evict_read_gen(session))
        return;

    /*
     * We set read-generations in the future (where "the future" is measured by increments of the
     * global read generation). The reason is because when acquiring a new hazard pointer for a
     * page, we can check its read generation, and if the read generation isn't less than the
     * current global generation, we don't bother updating the page. In other words, the goal is to
     * avoid some number of updates immediately after each update we have to make.
     */
    __wt_atomic_store_uint64_relaxed(&page->read_gen, __evict_read_gen(session) + WT_READGEN_STEP);
}

/*
 * __wti_evict_read_gen_new --
 *     Get the read generation for a new page in memory.
 */
static WT_INLINE void
__wti_evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    __wt_atomic_store_uint64_relaxed(
      &page->read_gen, (__evict_read_gen(session) + WT_EVICT_RANDLRU(S2C(session)->evict)->read_gen_oldest) / 2);
}

/*
 * __wti_evict_readgen_is_soon_or_wont_need --
 *     Return whether a read generation value makes a page eligible for forced eviction. Read
 *     generations reserve a range of low numbers for special meanings and currently - with the
 *     exception of the generation not being set - these indicate the page may be evicted
 *     forcefully.
 */
static WT_INLINE bool
__wti_evict_readgen_is_soon_or_wont_need(uint64_t *readgen)
{
    uint64_t gen;

    WT_READ_ONCE(gen, *readgen);
    return (gen != WT_READGEN_NOTSET && gen < WT_READGEN_START_VALUE);
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
 * __wti_evict_updates_needed --
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
__wti_evict_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
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
