/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once
#define WT_EVICT_DISABLED(btree) btree->evict_data.evict_disabled
#define WT_EVICT_PAGE_CLEARED(page) (page->evict_data.bucket == NULL)

#define WT_DHANDLE_TO_BUCKETSET(dhandle, set_number) \
    &((WT_BTREE *)(dhandle->handle))->evict_data.evict_bucketset[set_number]

#define WT_READGEN_NOTSET 0
#define WT_READGEN_EVICT_SOON 1
#define WT_READGEN_WONT_NEED 2
#define WT_READGEN_START_VALUE 100
#define WT_READGEN_STEP 100

/*
 * How much preference we give to leaf vs internal dirty pages upon eviction.
 * A higher value means more preference to leaf vs internal pages.
 */
#define WT_EVICT_INTERNAL_WEIGHT_DIVISOR 256

/*
 * The number of trees a checkpoint may be syncing at once and still have their dirty leaf bytes
 * discounted from the dirty thresholds. Checkpoints sync trees one at a time per worker, so this
 * only needs to cover the checkpoint worker count. Overflowing the array is not an error: a tree
 * that cannot be registered simply keeps counting towards the thresholds, which is the behaviour
 * before this change.
 */
#define WT_EVICT_CKPT_TREES_MAX 16

/*
 * The most dirty leaf data, as a percentage of the cache, that may be hidden from the dirty
 * thresholds because a checkpoint is syncing the tree that owns it.
 *
 * Discounting is what lets application threads keep working while a checkpoint holds the dominant
 * tree, but it must be bounded: if a checkpoint stalls, an unbounded discount would disable the
 * dirty thresholds entirely and let the cache fill. This is the absolute ceiling; the discount is
 * additionally tapered as the cache fills, see __wti_evict_ckpt_dirty_discount.
 */
#define WT_EVICT_CKPT_DIRTY_DISCOUNT_MAX_PCT 15

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool interruptible) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_clean_target(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_clean_trigger(
  WT_SESSION_IMPL *session, double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_dirty_target(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_dirty_trigger(
  WT_SESSION_IMPL *session, double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_updates_target(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_exceeded_updates_trigger(
  WT_SESSION_IMPL *session, double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wti_evict_dirty_target(WT_EVICT *evict)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
