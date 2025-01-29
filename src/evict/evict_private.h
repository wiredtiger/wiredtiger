/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * A key structure for eviction is called a bucket set. Each bucket in a set represents a range of
 * read generations, or any other eviction scores we decide to use in the future. Each bucket has a
 * queue of pages that belong to that range of read generations. Each page will be in exactly one
 * queue across all bucket sets and buckets.
 *
 * This data structure keeps all pages in an approximately sorted order. Pages in a higher numbered
 * bucket will generally have higher read generations than pages in a lower numbered buckets. Within
 * each bucket pages will not be sorted according to their read generations, but this is good enough
 * to roughly prioritize eviction of pages with lower-numbered read generations. The benefit of this
 * method is that it avoids walking the tree and refrains from keeping an expensive global order of
 * all pages.
 *
 * We use multiple bucket sets to prioritize eviction. Each tree has its own set of buckets. Leaf
 * pages are in a separate bucket set from internal pages. Clean pages are in a separate bucket set
 * than dirty pages. If contention on bucket queue spinlocks is observed we may introduced a
 * separate bucket set per CPU, similarly to per-CPU statistics counters.
 *
 * The lowest bucket upper range tells us the maximum read generation in the lowest bucket. The
 * upper range of the highest bucket is computed by adding the factor of the bucket range times the
 * number of remaining buckets to the lowest buckets' range. If the highest bucket range becomes too
 * small to accommodate the read generation of any page, we update the lowest bucket's range, and by
 * extension the highest bucket's range is updated accordingly. We won't move the pages between
 * buckets even as we update the read generations, because this is expensive. All we care about is
 * maintaining approximately sorted order or pages by their read generations, and this method does
 * the job.
 */

/*
 * The range of read generations represented by the bucket presents a tradeoff. A higher number
 * means there is more lock contention as we add/remove things to/from buckets, because the higher
 * the range the more pages there are in the bucket. A smaller range number means that we have to
 * move pages between buckets more often as their read generations increase.
 */
#define WT_EVICT_BUCKET_RANGE 300
#define WT_EVICT_NUM_BUCKETS 100
#define WT_EVICT_LEVELS 3
#define WT_EVICT_LEVEL_CLEAN_LEAF 0
#define WT_EVICT_LEVEL_DIRTY_LEAF 1
#define WT_EVICT_LEVEL_INTERNAL 2

#define WT_DHANDLE_TO_BUCKETSET(dhandle, set_number) \
    &dhandle->handle->evict_handle->evict_bucketset[set_number]

struct __wt_evict_bucket {
    WT_SPINLOCK evict_queue_lock;
    TAILQ_HEAD(__wt_evictbucket_qh, __wt_page) evict_queue;
    int id; /* index in the bucket set */
};

/*
 * Per-tree data structure that contains the tree's data needed by eviction.
 *
 * Each tree has its pages organized in several bucket sets: one for internal pages, one for clean
 * leaf pages and one for dirty leaf pages. Clean leaf pages are at the highest priority for
 * eviction, followed by the dirty leaf pages and followed by the internal pages.
 */
struct __wt_evict_bucketset {
    /* the array must be the first thing in the structure for pointer arithmetic to work */
    struct __wt_evict_bucket buckets[WT_EVICT_NUM_BUCKETS];
    uint64_t lowest_bucket_upper_range; /* must be updated atomically */
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double pct_full) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_readgen_is_soon_or_wont_need(uint64_t *readgen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wti_evict_dirty_target(WT_EVICT *evict)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wti_evict_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wti_evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
