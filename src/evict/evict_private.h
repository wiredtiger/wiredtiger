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

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */
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
