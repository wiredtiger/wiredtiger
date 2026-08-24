/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* The rankings this file maintains, one metric per ranking. */
typedef enum {
    WT_CACHE_TOP_UPDATES = 0, /* Update bytes held by the tree. */
    WT_CACHE_TOP_DIRTY,       /* Dirty leaf bytes held by the tree. */
    WT_CACHE_TOP_INMEM,       /* Total resident bytes held by the tree. */
    WT_CACHE_TOP_READ,        /* Recent bytes read into cache by the tree. */
    WT_CACHE_TOP_EVICT        /* Recent bytes evicted from the tree. */
} WT_CACHE_TOP_METRIC;

/*
 * The number of rankings above. Kept as a plain constant rather than a trailing enumerator, so that
 * a switch over WT_CACHE_TOP_METRIC only ever has to list values that can actually occur, instead
 * of also handling a sentinel that -Wswitch-enum would otherwise require every such switch to name.
 */
#define WT_CACHE_TOP_METRICS (WT_CACHE_TOP_EVICT + 1)

/*
 * Every metric tracked here is bounded by the cache size (bytes read and bytes evicted are decayed
 * so that this stays true for them too). That bound is what lets a fixed number of slots hold every
 * tree worth reporting, instead of just a sample of them: set the threshold to cache size divided
 * by the slot count, and at most that many trees can ever be above it, so a tree that is not in the
 * ranking is provably below the threshold. The threshold is adjusted over time to keep the ranking
 * reasonably full, so a workload where cache usage is spread thin across many tables still produces
 * a useful ranking.
 */
#define WT_CACHE_TOP_SLOTS 32

/*
 * The value WT_BTREE.cache_top_slot holds for a metric the tree is not currently part of. Equal to
 * the slot count, so every valid slot index (0 to WT_CACHE_TOP_SLOTS - 1) is distinguishable from
 * it. A tree's slot fields must be set to this explicitly when the tree is opened: zero is a valid
 * slot index, so relying on the struct's zero-initialization would wrongly claim slot 0.
 */
#define WT_CACHE_TOP_NOT_TRACKED WT_CACHE_TOP_SLOTS

/*
 * A tree is reconsidered for a ranking once it has grown by threshold / this value since its last
 * check. A smaller divisor means the rankings track growth more closely, but trees touch the shared
 * ranking state more often; a larger divisor means the opposite.
 */
#define WT_CACHE_TOP_RECHECK_DIVISOR 8

/* How long it takes a flow metric's value (bytes read, bytes evicted) to decay by half. */
#define WT_CACHE_TOP_FLOW_HALFLIFE_US (30 * WT_MILLION)

/*
 * The lowest a ranking's threshold is ever allowed to go. Not just cosmetic: a threshold of 0 would
 * make a tree's recheck spacing 0 too, so every single byte of growth would call back into the
 * tracking function, defeating the point of tracking a recheck value at all. Deliberately small,
 * not the megabyte-scale figures elsewhere in this file, so a connection whose real tables are
 * genuinely this size is not permanently unable to produce a nonempty ranking.
 */
#define WT_CACHE_TOP_THRESHOLD_FLOOR (64 * WT_KILOBYTE)

/*
 * The width, in bits, of the counters this decay operates on. Shifting a value right by this many
 * bits or more is undefined behavior in C, so it is also the point past which decay stops trying to
 * compute a shift and just reports zero; any real value reaches zero in far fewer halvings than
 * this.
 */
#define WT_CACHE_TOP_DECAY_MAX_HALVINGS 64

struct __wt_cache_top_entry {
    WT_BTREE *btree; /* The tree in this slot, or NULL if the slot is unused. */
    uint64_t value;  /* That tree's value, as of the last time this slot was updated. */
};

struct __wt_cache_top_array {
    WT_SPINLOCK lock; /* Guards everything below, including which tree each slot points at. */

    /* Read and updated without the lock: a caller that reads a stale value wastes one visit. */
    wt_shared uint64_t threshold;
    WT_CACHE_TOP_ENTRY slots[WT_CACHE_TOP_SLOTS];
};

struct __wt_cache_top {
    WT_CACHE_TOP_ARRAY arrays[WT_CACHE_TOP_METRICS];

    /* WT_CACHE_TOP_FLOW_HALFLIFE_US converted into the same units __wt_clock returns. */
    uint64_t halflife_ticks;
};
