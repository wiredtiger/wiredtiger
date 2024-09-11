/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Tuning constants: I hesitate to call this tuning, but we want to review some number of pages from
 * each file's in-memory tree for each page we evict.
 */
#define WT_EVICT_MAX_TREES WT_THOUSAND /* Maximum walk points */
#define WT_EVICT_WALK_BASE 300         /* Pages tracked across file visits */
#define WT_EVICT_WALK_INCR 100         /* Pages added each walk */

/*
 * WT_EVICT_ENTRY --
 *	Encapsulation of an eviction candidate.
 */
struct __wt_evict_entry {
    WT_BTREE *btree; /* Enclosing btree object */
    WT_REF *ref;     /* Page to flush/evict */
    uint64_t score;  /* Relative eviction priority */
};

#define WT_EVICT_URGENT_QUEUE 2 /* Urgent queue index */

#define WT_WITH_PASS_LOCK(session, op)                                                   \
    do {                                                                                 \
        WT_WITH_LOCK_WAIT(session, &evict->evict_pass_lock, WT_SESSION_LOCKED_PASS, op); \
    } while (0)

/*
 * __wti_cache_read_gen_incr --
 *     Increment the current read generation number.
 */
static WT_INLINE void
__wti_cache_read_gen_incr(WT_SESSION_IMPL *session)
{
    (void)__wt_atomic_add64(&S2C(session)->evict->read_gen, 1);
}

/*
 * __wti_eviction_dirty_target --
 *     Return the effective dirty target (including checkpoint scrubbing).
 */
static WT_INLINE double
__wti_eviction_dirty_target(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;
    double dirty_target, scrub_target;

    evict = S2C(session)->evict;

    dirty_target = __wt_read_shared_double(&evict->eviction_dirty_target);
    scrub_target = __wt_read_shared_double(&evict->eviction_scrub_target);

    return (scrub_target > 0 && scrub_target < dirty_target ? scrub_target : dirty_target);
}

/*
 * __wti_btree_dominating_cache --
 *     Return if a single btree is occupying at least half of any of our target's cache usage.
 */
static WT_INLINE bool
__wti_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
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
 * __wti_cache_hs_dirty --
 *     Return if a major portion of the cache is dirty due to history store content.
 */
static WT_INLINE bool
__wti_cache_hs_dirty(WT_SESSION_IMPL *session)
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

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern void __wti_evict_list_clear_page(WT_SESSION_IMPL *session, WT_REF *ref);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
