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

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_cache_eviction_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double pct_full) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_evict_list_clear_page(WT_SESSION_IMPL *session, WT_REF *ref);
static WT_INLINE bool __wti_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_cache_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_eviction_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_readgen_evict_soon(uint64_t *read_gen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wti_eviction_dirty_target(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wti_cache_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wti_cache_read_gen_incr(WT_SESSION_IMPL *session);
static WT_INLINE void __wti_cache_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
