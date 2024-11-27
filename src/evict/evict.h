/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "evict_private.h"


/*
 * Per-dhandle evict data
 */
struct __wt_evict_handle_data {
	WT_EVICT_BUCKETSET evict_bucketset[WT_EVICT_LEVELS];
};

/*
 * Page evict data
 */
struct __wt_evict_page_data {
	TAILQ_ENTRY(__wt_page) evict_q; /* Link to the next item in the evict queue */
	WT_EVICT_BUCKET *bucket; /* Bucket containing this page */
};

/*
 * Connection evict data.
 */
struct __wt_evict {
    wt_shared volatile uint64_t eviction_progress; /* Eviction progress count */
    uint64_t last_eviction_progress;               /* Tracked eviction progress */

    uint64_t app_waits;  /* User threads waited for eviction */
    uint64_t app_evicts; /* Pages evicted by user threads */

    wt_shared uint64_t evict_max_page_size; /* Largest page seen at eviction */
    wt_shared uint64_t evict_max_ms;        /* Longest milliseconds spent at a single eviction */
    uint64_t reentry_hs_eviction_ms;        /* Total milliseconds spent inside a nested eviction */
    struct timespec stuck_time;             /* Stuck time */

    /*
     * Read information.
     */
    uint64_t read_gen;        /* Current page read generation */
    uint64_t read_gen_oldest; /* Oldest read generation the eviction
                               * server saw in its last queue load */
    /*
     * Eviction thread information.
     */
    WT_CONDVAR *evict_cond;      /* Eviction server condition */

    /*
     * Eviction threshold percentages use double type to allow for specifying percentages less than
     * one.
     */
    wt_shared double eviction_dirty_target;  /* Percent to allow dirty */
    wt_shared double eviction_dirty_trigger; /* Percent to trigger dirty eviction */
    double eviction_trigger;                 /* Percent to trigger eviction */
    double eviction_target;                  /* Percent to end eviction */
    double eviction_updates_target;          /* Percent to allow for updates */
    double eviction_updates_trigger;         /* Percent of updates to trigger eviction */

    double eviction_checkpoint_target; /* Percent to reduce dirty to during checkpoint scrubs */
    wt_shared double eviction_scrub_target; /* Current scrub target */

    uint64_t cache_max_wait_us;      /* Maximum time an operation waits for space in cache */
    uint64_t cache_stuck_timeout_ms; /* Maximum time the cache can be stuck for in diagnostic mode
                                        before timing out */

    /*
     * Eviction thread tuning information.
     */
    uint32_t evict_tune_datapts_needed;          /* Data needed to tune */
    struct timespec evict_tune_last_action_time; /* Time of last action */
    struct timespec evict_tune_last_time;        /* Time of last check */
    uint32_t evict_tune_num_points;              /* Number of values tried */
    uint64_t evict_tune_progress_last;           /* Progress counter */
    uint64_t evict_tune_progress_rate_max;       /* Max progress rate */
    bool evict_tune_stable;                      /* Are we stable? */
    uint32_t evict_tune_workers_best;            /* Best performing value */

/*
 * Flags.
 */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_EVICT_CACHE_CLEAN 0x001u        /* Evict clean pages */
#define WT_EVICT_CACHE_CLEAN_HARD 0x002u   /* Clean % blocking app threads */
#define WT_EVICT_CACHE_DIRTY 0x004u        /* Evict dirty pages */
#define WT_EVICT_CACHE_DIRTY_HARD 0x008u   /* Dirty % blocking app threads */
#define WT_EVICT_CACHE_NOKEEP 0x010u       /* Don't add read pages to cache */
#define WT_EVICT_CACHE_SCRUB 0x020u        /* Scrub dirty pages */
#define WT_EVICT_CACHE_UPDATES 0x040u      /* Evict pages with updates */
#define WT_EVICT_CACHE_UPDATES_HARD 0x080u /* Update % blocking app threads */
#define WT_EVICT_CACHE_URGENT 0x100u       /* Pages are in the urgent queue */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
#define WT_EVICT_CACHE_ALL (WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_UPDATES)
#define WT_EVICT_CACHE_HARD \
    (WT_EVICT_CACHE_CLEAN_HARD | WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD)
    uint32_t flags;
};

/* Flags used with __wt_evict */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_EVICT_CALL_CLOSING 0x1u  /* Closing connection or tree */
#define WT_EVICT_CALL_NO_SPLIT 0x2u /* Splits not allowed */
#define WT_EVICT_CALL_URGENT 0x4u   /* Urgent eviction */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern bool __wt_evict_page_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_config(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_threads_create(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_threads_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_evict_cache_stat_walk(WT_SESSION_IMPL *session);
extern void __wt_evict_file_exclusive_off(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_clear(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v);
extern void __wt_evict_server_wake(WT_SESSION_IMPL *session);
extern void __wt_evict_stats_update(WT_SESSION_IMPL *session);
static WT_INLINE bool __wt_evict_aggressive(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_cache_stuck(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_clean_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_clean_pressure(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_dirty_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_needed(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_page_is_soon(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_evict_page_is_soon_or_wont_need(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_evict_app_assist_worker_check(WT_SESSION_IMPL *session, bool busy,
  bool readonly, bool *didworkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wt_evict_clear_npos(WT_BTREE *btree);
static WT_INLINE void __wt_evict_favor_clearing_dirty_cache(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_evict_inherit_page_state(WT_PAGE *orig_page, WT_PAGE *new_page);
static WT_INLINE void __wt_evict_page_cache_bytes_decr(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_evict_page_first_dirty(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_evict_page_init(WT_PAGE *page);
static WT_INLINE void __wt_evict_page_soon(WT_SESSION_IMPL *session, WT_REF *ref);
static WT_INLINE void __wt_evict_touch_page(
  WT_SESSION_IMPL *session, WT_PAGE *page, bool internal_only, bool wont_need);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
