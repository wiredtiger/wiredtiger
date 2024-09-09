/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __cache_read_gen_incr --
 *     Increment the current read generation number.
 */
static WT_INLINE void
__cache_read_gen_incr(WT_SESSION_IMPL *session)
{
    (void)__wt_atomic_add64(&S2C(session)->evict->read_gen, 1);
}

/*
 * __eviction_dirty_target --
 *     Return the effective dirty target (including checkpoint scrubbing).
 */
static WT_INLINE double
__eviction_dirty_target(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;
    double dirty_target, scrub_target;

    evict = S2C(session)->evict;

    dirty_target = __wt_read_shared_double(&evict->eviction_dirty_target);
    scrub_target = __wt_read_shared_double(&evict->eviction_scrub_target);

    return (scrub_target > 0 && scrub_target < dirty_target ? scrub_target : dirty_target);
}

/*
 * __btree_dominating_cache --
 *     Return if a single btree is occupying at least half of any of our target's cache usage.
 */
static WT_INLINE bool
__btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_EVICT *evict;
    uint64_t bytes_dirty;
    uint64_t bytes_max;

    evict = S2C(session)->evict;
    bytes_max = S2C(session)->cache_size + 1;

    if (__wt_cache_bytes_plus_overhead(
          S2C(session)->cache, __wt_atomic_load64(&btree->bytes_inmem)) >
      (uint64_t)(0.5 * evict->eviction_target * bytes_max) / 100)
        return (true);

    bytes_dirty =
      __wt_atomic_load64(&btree->bytes_dirty_intl) + __wt_atomic_load64(&btree->bytes_dirty_leaf);
    if (__wt_cache_bytes_plus_overhead(S2C(session)->cache, bytes_dirty) >
      (uint64_t)(0.5 * evict->eviction_dirty_target * bytes_max) / 100)
        return (true);
    if (__wt_cache_bytes_plus_overhead(
          S2C(session)->cache, __wt_atomic_load64(&btree->bytes_updates)) >
      (uint64_t)(0.5 * evict->eviction_updates_target * bytes_max) / 100)
        return (true);

    return (false);
}

/*
 * __cache_hs_dirty --
 *     Return if a major portion of the cache is dirty due to history store content.
 */
static WT_INLINE bool
__cache_hs_dirty(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    uint64_t bytes_max;
    conn = S2C(session);
    evict = conn->evict;
    bytes_max = conn->cache_size;

    return (__wt_cache_bytes_plus_overhead(
              conn->cache, __wt_atomic_load64(&conn->cache->bytes_hs_dirty)) >=
      ((uint64_t)(evict->eviction_dirty_trigger * bytes_max) / 100));
}
