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
 * XXX The lowest bucket upper range tells us the maximum read generation in the lowest bucket. The
 * upper range of the highest bucket is computed by adding the factor of the bucket range times the
 * number of remaining buckets to the lowest buckets' range. If the highest bucket range becomes too
 * small to accommodate the read generation of any page, we update the lowest bucket's range, and by
 * extension the highest bucket's range is updated accordingly. We won't move the pages between
 * buckets even as we update the read generations, because this is expensive. All we care about is
 * maintaining approximately sorted order or pages by their read generations, and this method does
 * the job.
 */

#include "../include/stat.h"
/* Statistics counter slots are also set to reflect expected contention, so we reuse that value */
#define WT_EVICT_EXPECTED_CONTENTION WT_STAT_CONN_COUNTER_SLOTS

/*
 * If the database fits entirely in cache, as few as 50 buckets is sufficient.
 * In a degenerate case where all we do is evict, 5000 buckets is about right to avoid contention.
 */
#define WT_EVICT_NUM_BUCKETS (200 * WT_EVICT_EXPECTED_CONTENTION)

#define WT_EVICT_LEVEL_WONT_NEED 0
#define WT_EVICT_LEVEL_CLEAN_LEAF 1
#define WT_EVICT_LEVEL_CLEAN_INTERNAL 2
#define WT_EVICT_LEVEL_DIRTY_LEAF 3
#define WT_EVICT_LEVEL_DIRTY_INTERNAL 4
#define WT_EVICT_LEVELS WT_EVICT_LEVEL_DIRTY_INTERNAL + 1

struct __wt_evict_bucket {
    WT_SPINLOCK evict_queue_lock;
    TAILQ_HEAD(__wt_evictbucket_qh, __wt_page) evict_queue;
    uint64_t id; /* index in the bucket set */
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
    uint32_t bucket_last_considered; /* must be updated atomically */
};

/*
 * Data handle evict data
 */
struct __wt_evict_handle_data {
    struct __wt_evict_bucketset evict_bucketset[WT_EVICT_LEVELS];
    bool initialized;
    uint64_t evict_priority;                   /* Relative priority of cached pages */
    wt_shared int32_t evict_disabled;          /* Eviction disabled count */
    bool evict_disabled_open;                  /* Eviction disabled on open */
    wt_shared volatile uint32_t evict_busy;    /* Count of threads in eviction */
};

/*
 * Page evict data
 */
struct __wt_evict_page_data {
    TAILQ_ENTRY(__wt_page) evict_q; /* Link to the next item in the evict queue */
    struct __wt_data_handle *dhandle;
    struct __wt_evict_bucket *bucket; /* Bucket containing this page */
    /*
     * The page's read generation acts as an LRU value for each page in the
     * tree; it is used by the eviction server thread to select pages to be
     * discarded from the in-memory tree.
     *
     * The read generation is a 64-bit value, if incremented frequently, a
     * 32-bit value could overflow.
     *
     * The read generation is a piece of shared memory potentially read
     * by many threads.  We don't want to update page read generations for
     * in-cache workloads and suffer the cache misses, so we don't simply
     * increment the read generation value on every access.  Instead, the
     * read generation is incremented by the eviction server each time it
     * becomes active.  To avoid incrementing a page's read generation too
     * frequently, it is set to a future point.
     *
     * Because low read generation values have special meaning, and there
     * are places where we manipulate the value, use an initial value well
     * outside of the special range.
     */
    uint64_t read_gen;
    uint64_t cache_create_gen; /* Page create timestamp */
    uint64_t evict_pass_gen;   /* Eviction pass generation */
    bool evict_skip;           /* Skip this page once for eviction */
    bool destroying;           /* Sticky flag set once when the page is being destroyed */
};
