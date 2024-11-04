/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * The range of read generations represented by the bucket presents a tradeoff.
 * A higher number means there is more lock contention as we add/remove things
 * to/from buckets, because the higher the range the more pages there are in
 * the bucket. A smaller range number means that we have to move pages between
 * buckets more often as their read generations increase.
 */
#define WT_EVICT_BUCKET_RANGE 300
#define WT_EVICT_NUM_BUCKETS 100

struct __wt_evict_bucket {
	WT_SPINLOCK evict_queue_lock;
	TAILQ_HEAD(__wt_evictbucket_queue, __wt_page) evict_queue;
};



/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double pct_full) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_evict_list_clear_page(WT_SESSION_IMPL *session, WT_REF *ref);
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
