/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* Cache consumption rankings maintained at runtime. */
typedef enum {
    WT_CACHE_TOP_UPDATES = 0, /* Update bytes held by the tree. */
    WT_CACHE_TOP_DIRTY,       /* Dirty leaf bytes held by the tree. */
    WT_CACHE_TOP_INMEM,       /* Total resident bytes held by the tree. */
    WT_CACHE_TOP_READ,        /* Recent bytes read into cache by the tree. */
    WT_CACHE_TOP_EVICT,       /* Recent bytes evicted from the tree. */
    WT_CACHE_TOP_METRICS      /* Number of rankings, not a ranking. */
} WT_CACHE_TOP_METRIC;

/*
 * A tracked metric is bounded by the cache size (the read and eviction metrics are decayed to make
 * them so), which is what makes the ranking complete rather than a sample: with a threshold of the
 * cache size divided by the slot count, no more than that many trees can be above the threshold at
 * once, and a tree missing from the list is provably below it. The threshold is adjusted to keep
 * the list roughly full so a workload whose cache usage is spread thinly still produces a ranking.
 */
#define WT_CACHE_TOP_SLOTS 32

/*
 * Growth that earns a tree another look at the rankings, as a divisor of the threshold. Trades how
 * closely the rankings follow growth against how often trees touch the shared ranking state.
 */
#define WT_CACHE_TOP_RECHECK_DIVISOR 8

/* Half-life of the metrics that track a flow rather than a level. */
#define WT_CACHE_TOP_FLOW_HALFLIFE_US (30 * WT_MILLION)

struct __wt_cache_top_entry {
    WT_BTREE *btree; /* Tracked tree, NULL if the slot is unused. */
    uint64_t value;  /* Metric value as of the last refresh. */
};

struct __wt_cache_top_list {
    WT_SPINLOCK lock; /* Protects everything below, including the slots' trees. */

    /* Read and adjusted outside the lock, so a stale read only costs a wasted visit. */
    wt_shared uint64_t threshold;
    WT_CACHE_TOP_ENTRY slots[WT_CACHE_TOP_SLOTS];
};

struct __wt_cache_top {
    WT_CACHE_TOP_LIST lists[WT_CACHE_TOP_METRICS];

    /* The flow half-life in the units __wt_clock returns, so decay needs no unit conversion. */
    uint64_t halflife_ticks;
};
