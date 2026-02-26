/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Tuning constants: I hesitate to call this tuning, but we want to review some number of pages from
 * each file's in-memory tree for each page we evict.
 */
#define WTI_EVICT_MAX_TREES WT_THOUSAND /* Maximum walk points */
#define WTI_EVICT_WALK_BASE 300         /* Pages tracked across file visits */
#define WTI_EVICT_WALK_INCR 100         /* Pages added each walk */

/*
 * WTI_EVICT_ENTRY --
 *	Encapsulation of an eviction candidate.
 */
struct __wti_evict_entry {
    WT_BTREE *btree; /* Enclosing btree object */
    WT_REF *ref;     /* Page to flush/evict */
    uint64_t score;  /* Relative eviction priority */
};

#define WTI_EVICT_QUEUE_MAX 3    /* Two ordinary queues plus urgent */
#define WTI_EVICT_URGENT_QUEUE 2 /* Urgent queue index */

/*
 * WTI_EVICT_QUEUE --
 *	Encapsulation of an eviction candidate queue.
 */
struct __wti_evict_queue {
    WT_SPINLOCK evict_lock;                /* Eviction LRU queue */
    WTI_EVICT_ENTRY *evict_queue;          /* LRU pages being tracked */
    WTI_EVICT_ENTRY *evict_current;        /* LRU current page to be evicted */
    uint32_t evict_candidates;             /* LRU list pages to evict */
    uint32_t evict_entries;                /* LRU entries in the queue */
    wt_shared volatile uint32_t evict_max; /* LRU maximum eviction slot used */
};

#define WTI_WITH_PASS_LOCK(session, op)                                                    \
    do {                                                                                   \
        WT_WITH_LOCK_WAIT(                                                                 \
          session, &WT_EVICT_RANDLRU(evict)->evict_pass_lock, WT_SESSION_LOCKED_PASS, op); \
    } while (0)
