/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_evict_bucketset;
struct __wt_evict_dhandle_hash_entry;
struct __wt_evict_dhandle_subqueue;

/*
 * Head type for a queue of pages held in an eviction bucket or a per-tree subqueue. Defined once
 * here so that both __wt_evict_bucket and __wt_evict_dhandle_subqueue can share the same type
 * (both are passed to __evict_scan_queue()).
 */
TAILQ_HEAD(__wt_evictbucket_qh, __wt_page);

struct __wt_evict_bucket {
    uint64_t id; /* index in the bucket set */
    struct __wt_evict_bucketset *bucketset;

    /* Every bucket, at every level, holds per-tree queues in a hashtable. */
    struct __wt_evict_dhandle_hash_entry *pertree_hashtable;
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
     * The per-tree subqueue containing this page, or NULL if the page is not currently queued.
     * Every level uses subqueues now, so a queued page is always in one.
     *
     * This is a cache of the subqueue that would otherwise be located by hashing the dhandle and
     * walking the bucket's hash chain. It lets a caller holding the page go straight to the
     * subqueue lock, without computing the hash, walking the chain, or acquiring the hash chain
     * lock at all.
     *
     * Invariant: subq is non-NULL exactly when the page is linked into that subqueue. Both the
     * pointer and the TAILQ linkage are updated while holding the subqueue lock, so a reader sees
     * either "in the queue with a valid pointer" or "not in the queue with NULL".
     *
     * Lifetime: a subqueue is only freed when its dhandle is closed, and the close path discards
     * every page of the tree first (each discard removes the page from its queue and clears this
     * pointer). A non-NULL subq therefore always refers to a live subqueue. If subqueues ever
     * become reclaimable while the dhandle is open, this cache is no longer safe.
     */
    struct __wt_evict_dhandle_subqueue *subq;
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
    uint16_t evict_page_attempts; /* Tried to evict page but failed */
};

/*
 * Data handle evict data
 */
struct __wt_evict_handle_data {
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


struct __wt_evict_dhandle_subqueue {
    struct __wt_data_handle *dhandle; /* Dhandle owning this queue */
    TAILQ_ENTRY(__wt_evict_dhandle_subqueue) dhandle_subq;
	WT_SPINLOCK evict_queue_lock;
    struct __wt_evictbucket_qh evict_queue; /* Pages in this queue */
};

/*
 * Hash table entry used for dirty syncing bucket.
 */
struct __wt_evict_dhandle_hash_entry {
	/* Locks the entire hash chain. Must be acquired before the subqueue lock. */
    WT_SPINLOCK evict_hashchain_lock;
    /* List of per-tree queues to resolve hash collisions */
    TAILQ_HEAD(__wt_hashchain_dhandle_qh,  __wt_evict_dhandle_subqueue) dhandle_hashchain;
    /* Number of subqueues on the chain. Maintained under evict_hashchain_lock. */
    uint32_t chain_len;
    /*
     * Unlocked hint: zero means no subqueue on this chain held a page the last time a sweep looked.
     * Read without the chain lock so a drained slot can be skipped before paying for the trylock
     * and the chain walk.
     *
     * Set to one when a page is enqueued and cleared by a sweep that walked the whole chain and
     * found every subqueue empty. Both happen under evict_hashchain_lock, which is why neither
     * needs a read-modify-write: this hint costs the enqueue and dequeue paths a single relaxed
     * store between them, and costs the dequeue paths nothing at all.
     *
     * The raciness is deliberately one-sided. A stale one costs a wasted walk that then clears it.
     * A stale zero would cost a missed page, so the clear is the only conditional side and it runs
     * under the same lock that enqueue holds, which is what makes a lost update impossible.
     */
    wt_shared uint32_t maybe_nonempty;
};
