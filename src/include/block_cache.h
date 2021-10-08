/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WiredTiger's block cache. It is used to cache blocks identical to those that live on disk in a
 * faster storage medium, such as NVRAM.
 */

#ifdef HAVE_LIBMEMKIND
#include <memkind.h>
#endif /* HAVE_LIBMEMKIND */

#define BLKCACHE_HASHSIZE_DEFAULT 32768
#define BLKCACHE_HASHSIZE_MIN 512
#define BLKCACHE_HASHSIZE_MAX WT_GIGABYTE

#define BLKCACHE_TRACE 0

#define WT_BLKCACHE_FULL -2
#define WT_BLKCACHE_BYPASS -3

/*
 * WT_BLKCACHE_ID --
 *    Checksum, offset and size uniquely identify a block.
 *    These are the same items used to compute the cookie.
 */
struct __wt_blkcache_id {
    uint64_t checksum;
    uint64_t offset;
    uint64_t size;
};

/*
 * WT_BLKCACHE_ITEM --
 *     Block cache item. It links with other items in the same hash bucket.
 */
struct __wt_blkcache_item {

    struct __wt_blkcache_id id;
    TAILQ_ENTRY(__wt_blkcache_item) hashq;
    void *data;
    uint32_t num_references;

    /*
     * This counter is incremented every time a block is referenced and decremented every time the
     * eviction thread sweeps through the cache. This counter will be low for blocks that have not
     * been reused or for blocks that were reused in the past but lost their appeal. In this sense,
     * this counter is a metric combining frequency and recency, and hence its name.
     */
    int32_t freq_rec_counter;
};

/*
 * WT_BLKCACHE --
 *     Block cache metadata includes the hashtable of cached items, number of cached data blocks
 * and the total amount of space they occupy.
 */
struct __wt_blkcache {
    /* Locked: Block manager cache. Locks are per-bucket. */
    TAILQ_HEAD(__wt_blkcache_hash, __wt_blkcache_item) * hash;
    WT_SPINLOCK *hash_locks;
    WT_CONDVAR *blkcache_cond;
    wt_thread_t evict_thread_tid;

    volatile bool blkcache_exiting;
    bool chkpt_write_bypass;
    bool eviction_on;
    int32_t evict_aggressive;
    bool write_allocate;
    char *nvram_device_path;
    double full_target;
    double overhead_pct;
    float fraction_in_dram;
    int refs_since_filesize_estimated;
    volatile size_t bytes_used;
    size_t estimated_file_size;
    size_t hash_size;
    size_t num_data_blocks;
    size_t max_bytes;
    size_t system_ram;
    u_int type;
    uint32_t min_freq_counter;

    /*
     * Various metrics helping us measure the overhead and decide if to bypass the cache. We access
     * some of them without synchronization despite races. These serve as heuristics, and we don't
     * need precise values for them to be useful. If, because of races, we lose updates of these
     * values, assuming that we lose them at the same rate for all variables, the ratio should
     * remain roughly accurate. We care about the ratio.
     */
    size_t lookups;
    size_t inserts;
    size_t removals;

#ifdef HAVE_LIBMEMKIND
    struct memkind *pmem_kind;
#endif /* HAVE_LIBMEMKIND */

    /* Histograms keeping track of number of references to each block */
#define BLKCACHE_HIST_BUCKETS 11
#define BLKCACHE_HIST_BOUNDARY 10
    uint32_t cache_references[BLKCACHE_HIST_BUCKETS];
    uint32_t cache_references_removed_blocks[BLKCACHE_HIST_BUCKETS];
    uint32_t cache_references_evicted_blocks[BLKCACHE_HIST_BUCKETS];
};

#define BLKCACHE_UNCONFIGURED 0
#define BLKCACHE_DRAM 1
#define BLKCACHE_NVRAM 2

#define BLKCACHE_RM_EXIT 1
#define BLKCACHE_RM_FREE 2
#define BLKCACHE_RM_EVICTION 3
