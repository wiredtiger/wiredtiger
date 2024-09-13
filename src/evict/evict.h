/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once
#include "evict_private.h"

/*
 * Helper: in order to read without any calls to eviction, we have to ignore the cache size and
 * disable splits.
 */
#define WT_READ_NO_EVICT (WT_READ_IGNORE_CACHE_SIZE | WT_READ_NO_SPLIT)

struct __wt_evict {
    WT_EVICT_PRIV priv;
    wt_shared volatile uint64_t eviction_progress; /* Eviction progress count */

    uint64_t app_waits;  /* User threads waited for cache */
    uint64_t app_evicts; /* Pages evicted by user threads */

    wt_shared uint64_t evict_max_page_size; /* Largest page seen at eviction */
    wt_shared uint64_t evict_max_ms;        /* Longest milliseconds spent at a single eviction */
    uint64_t reentry_hs_eviction_ms;        /* Total milliseconds spent inside a nested eviction */

    uint64_t evict_pass_gen; /* Number of eviction passes */

    /*
     * Eviction threshold percentages use double type to allow for specifying percentages less than
     * one.
     */
    wt_shared double eviction_dirty_target;  /* Percent to allow dirty */
    wt_shared double eviction_dirty_trigger; /* Percent to trigger dirty eviction */
    double eviction_target;                  /* Percent to end eviction */
    double eviction_trigger;                 /* Percent to trigger eviction */

    double eviction_checkpoint_target; /* Percent to reduce dirty to during checkpoint scrubs */
    wt_shared double eviction_scrub_target; /* Current scrub target */

    /*
     * Pass interrupt counter.
     */
    wt_shared volatile uint32_t pass_intr; /* Interrupt eviction pass. */

    WT_DATA_HANDLE *walk_tree; /* LRU walk current tree */

/*
 * Flags.
 */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CACHE_EVICT_CLEAN 0x001u        /* Evict clean pages */
#define WT_CACHE_EVICT_CLEAN_HARD 0x002u   /* Clean % blocking app threads */
#define WT_CACHE_EVICT_DIRTY 0x004u        /* Evict dirty pages */
#define WT_CACHE_EVICT_DIRTY_HARD 0x008u   /* Dirty % blocking app threads */
#define WT_CACHE_EVICT_NOKEEP 0x010u       /* Don't add read pages to cache */
#define WT_CACHE_EVICT_SCRUB 0x020u        /* Scrub dirty pages */
#define WT_CACHE_EVICT_UPDATES 0x040u      /* Evict pages with updates */
#define WT_CACHE_EVICT_UPDATES_HARD 0x080u /* Update % blocking app threads */
#define WT_CACHE_EVICT_URGENT 0x100u       /* Pages are in the urgent queue */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
#define WT_CACHE_EVICT_ALL (WT_CACHE_EVICT_CLEAN | WT_CACHE_EVICT_DIRTY | WT_CACHE_EVICT_UPDATES)
#define WT_CACHE_EVICT_HARD \
    (WT_CACHE_EVICT_CLEAN_HARD | WT_CACHE_EVICT_DIRTY_HARD | WT_CACHE_EVICT_UPDATES_HARD)
    uint32_t flags;
};

/* Flags used with __wt_evict */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_EVICT_CALL_CLOSING 0x1u  /* Closing connection or tree */
#define WT_EVICT_CALL_NO_SPLIT 0x2u /* Splits not allowed */
#define WT_EVICT_CALL_URGENT 0x4u   /* Urgent eviction */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern bool __wt_page_evict_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_threads_create(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_threads_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_eviction_config(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_eviction_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_eviction_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_curstat_cache_walk(WT_SESSION_IMPL *session);
extern void __wt_evict_file_exclusive_off(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_clear(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v);
extern void __wt_evict_server_wake(WT_SESSION_IMPL *session);
extern void __wt_eviction_stats_update(WT_SESSION_IMPL *session);
static WT_INLINE bool __wt_cache_aggressive(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cache_stuck(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_page_is_soon(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_clean_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_clean_pressure(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_dirty_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_needed(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cache_eviction_check(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool *didworkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wt_evict_copy_page_state(WT_PAGE *orig_page, WT_PAGE *new_page);
static WT_INLINE void __wt_evict_page_init(WT_PAGE *page);
static WT_INLINE void __wt_evict_page_needed(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_evict_touch_page(
  WT_SESSION_IMPL *session, WT_PAGE *page, bool init_only, bool wont_need);
static WT_INLINE void __wt_page_evict_soon(WT_SESSION_IMPL *session, WT_REF *ref);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
