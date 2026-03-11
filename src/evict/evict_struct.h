/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_evict_bucketset;

struct __wt_evict_bucket {
    WT_SPINLOCK evict_queue_lock;
    TAILQ_HEAD(__wt_evictbucket_qh, __wt_page) evict_queue;
    uint64_t id; /* index in the bucket set */
    struct __wt_evict_bucketset *bucketset;
};

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
 * than dirty pages.
 *
 * There is a pair of bucketsets dedicated to pages scheduled for forced eviction. Those pages have
 * the same read generation, so they are placed in a randomly selected bucket in the bucketset.
 *
 * The number of buckets is set at initialization. It is important to get it right. If we don't have
 * enough buckets we will compete on bucket locks. If we have too many we will spend a long time
 * looking for non-empty buckets. If the cache is very small and the tree is very large, all we do
 * is evict; quickly finding evictable pages is our priority, so we set the number of buckets to a
 * low number. If the cache is well-sized relative to the data, bucket lock contention will dominate
 * as we move pages between buckets, so we need to have many buckets. The values shown below were
 * determined experimentally. For most workloads the default value of 9200 will work well.
 *
 * If the ratio of tree size to cache size is below 100, set the number of buckets to 9200. If the
 * ratio is in the range 100-1000, set to 230. If the ratio is 1000 or above set to the expected
 * number of cores in the system.
 */
struct __wt_evict_bucketset {
    WT_CACHE_LINE_PAD_BEGIN
    struct __wt_evict_bucket *buckets;
    uint32_t bucket_last_considered; /* must be updated atomically */
    uint64_t bucketset_num_items;    /* must be updated atomically */
    int level;
    uint32_t num_buckets;
    WT_CACHE_LINE_PAD_END
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

/*
 * Data handle evict data
 */
struct __wt_evict_handle_data {
    bool initialized;
    uint64_t evict_priority;                /* Relative priority of cached pages */
    wt_shared int32_t evict_disabled;       /* Eviction disabled count */
    bool evict_disabled_open;               /* Eviction disabled on open */
    wt_shared volatile uint32_t evict_busy; /* Count of threads in eviction */
    /*
     * Track the number of obsolete time window pages that are changed into dirty page
     * reconciliation by the eviction.
     */
    wt_shared uint32_t eviction_obsolete_tw_pages;
};
