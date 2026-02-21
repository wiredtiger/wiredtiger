/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "evict_private.h"

struct __wt_evict {
    /* Methods -- function pointer vtable for eviction dispatch. */
    int (*evict_page)(WT_EVICT *, WT_SESSION_IMPL *, WT_REF *, WT_REF_STATE, uint32_t);
    int (*evict_file)(WT_EVICT *, WT_SESSION_IMPL *, WT_CACHE_OP);
    int (*config)(WT_EVICT *, WT_SESSION_IMPL *, const char *[], bool);
    int (*destroy)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*stats_update)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*stats_init)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*server_wake)(WT_EVICT *, WT_SESSION_IMPL *);
    int (*threads_create)(WT_EVICT *, WT_SESSION_IMPL *);
    int (*threads_destroy)(WT_EVICT *, WT_SESSION_IMPL *);
    int (*file_exclusive_on)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*file_exclusive_off)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*page_urgent)(WT_EVICT *, WT_SESSION_IMPL *, WT_REF *);
    void (*priority_set)(WT_EVICT *, WT_SESSION_IMPL *, uint64_t);
    void (*priority_clear)(WT_EVICT *, WT_SESSION_IMPL *);
    int (*verbose_dump_cache)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*cache_stat_walk)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*aggressive)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*cache_stuck)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*clean_needed)(WT_EVICT *, WT_SESSION_IMPL *, double *);
    bool (*clean_pressure)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*dirty_needed)(WT_EVICT *, WT_SESSION_IMPL *, double *);
    bool (*needed)(WT_EVICT *, WT_SESSION_IMPL *, bool, bool, bool, double *);
    void (*favor_clearing_dirty)(WT_EVICT *, WT_SESSION_IMPL *);
    int (*app_assist_worker_check)(WT_EVICT *, WT_SESSION_IMPL *, bool, bool, bool, bool *);
    void (*page_init)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*touch_page)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *, bool, bool);
    void (*page_soon)(WT_EVICT *, WT_SESSION_IMPL *, WT_REF *);
    bool (*page_is_soon)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    bool (*page_is_soon_or_wont_need)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*page_first_dirty)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*inherit_page_state)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *, WT_PAGE *);
    void (*page_cache_bytes_decr)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*clear_npos)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    void (*reset_checkpoint_stats)(WT_EVICT *, WT_SESSION_IMPL *);
    WT_DATA_HANDLE *(*get_walk_tree)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*pass_interrupt_inc)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*pass_interrupt_dec)(WT_EVICT *, WT_SESSION_IMPL *);
    uint64_t (*get_evict_pass_gen)(WT_EVICT *, WT_SESSION_IMPL *);
    uint64_t (*get_page_evict_pass_gen)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*save_evict_state)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE_MODIFY *);
    void (*copy_evict_state_to_mod)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE_MODIFY *, WT_PAGE_MODIFY *);
    bool (*page_evict_retry)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    void (*page_set_cache_create_gen)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    uint64_t (*page_get_cache_create_gen)(WT_EVICT *, WT_SESSION_IMPL *, WT_PAGE *);
    uint64_t (*btree_get_priority)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*btree_save_walk_period)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*btree_restore_walk_period)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*btree_is_eviction_disabled)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*btree_set_disabled_open)(WT_EVICT *, WT_SESSION_IMPL *);
    bool (*btree_is_disabled_open)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*btree_clear_disabled_open)(WT_EVICT *, WT_SESSION_IMPL *);
    void (*btree_evict_busy_inc)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    void (*btree_evict_busy_dec)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    void (*btree_prefetch_busy_inc)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    void (*btree_prefetch_busy_dec)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    void (*btree_prefetch_busy_wait)(WT_EVICT *, WT_SESSION_IMPL *, WT_BTREE *);
    WT_REF *(*btree_get_evict_ref)(WT_EVICT *, WT_SESSION_IMPL *);

    /* Common data -- used by all eviction implementations. */
    uint64_t app_waits;  /* User threads waited for eviction */
    uint64_t app_evicts; /* Pages evicted by user threads */

    wt_shared uint64_t evict_max_clean_page_size_per_checkpoint;   /* Largest clean page seen at
                                                                      eviction per checkpoint */
    wt_shared uint64_t evict_max_dirty_page_size_per_checkpoint;   /* Largest dirty page seen at
                                                                      eviction per checkpoint */
    wt_shared uint64_t evict_max_updates_page_size_per_checkpoint; /* Largest updates page seen at
                                                                      eviction per checkpoint */
    wt_shared uint64_t evict_max_ms; /* Longest milliseconds spent at a single eviction */
    wt_shared uint64_t
      evict_max_ms_per_checkpoint;   /* Longest milliseconds spent at a single eviction */
    uint64_t reentry_hs_eviction_ms; /* Total milliseconds spent inside a nested eviction */
    struct timespec stuck_time;      /* Stuck time */

    wt_shared uint64_t evict_lock_wait_time; /* Time spent waiting for locks during eviction */

    /*
     * Eviction threshold percentages use double type to allow for specifying percentages less than
     * one.
     */
    wt_shared double eviction_dirty_target;    /* Percent to allow dirty */
    wt_shared double eviction_dirty_trigger;   /* Percent to trigger dirty eviction */
    double eviction_trigger;                   /* Percent to trigger eviction */
    double eviction_target;                    /* Percent to end eviction */
    double eviction_updates_target;            /* Percent to allow for updates */
    wt_shared double eviction_updates_trigger; /* Percent of updates to trigger eviction */

    double eviction_checkpoint_target; /* Percent to reduce dirty to during checkpoint scrubs */
    wt_shared double eviction_scrub_target; /* Current scrub target */

    uint64_t cache_max_wait_us;      /* Maximum time an operation waits for space in cache */
    uint64_t cache_stuck_timeout_ms; /* Maximum time the cache can be stuck for in diagnostic mode
                                        before timing out */

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

    /* Algorithm identifier for the active eviction implementation. */
#define WT_EVICT_ALGO_RANDLRU 1
    uint32_t algo_id;

    /* Implementation-specific data. */
    union {
        struct {
            wt_shared volatile uint64_t eviction_progress; /* Eviction progress count */
            uint64_t last_eviction_progress;               /* Tracked eviction progress */

            uint64_t evict_pass_gen; /* Number of eviction passes */

            /*
             * Score of how aggressive eviction should be about selecting eviction candidates. If
             * eviction is struggling to make progress, this score rises (up to a maximum of
             * WT_EVICT_SCORE_MAX), at which point the cache is "stuck" and transactions will be
             * rolled back.
             */
            wt_shared uint32_t evict_aggressive_score;

            /*
             * Read information.
             */
            uint64_t read_gen;        /* Current page read generation */
            uint64_t read_gen_oldest; /* Oldest read generation the eviction
                                       * server saw in its last queue load */
            wt_shared uint64_t
              evict_max_unvisited_gen_gap; /* Maximum gap between page and connection evict
                                             pass generation of unvisited pages */
            wt_shared uint64_t
              evict_max_unvisited_gen_gap_per_checkpoint; /* Maximum gap between page and
                                             connection evict pass generation of unvisited pages */
            wt_shared uint64_t
              evict_max_visited_gen_gap; /* Maximum gap between page and connection evict
                                             pass generation of visited pages */
            wt_shared uint64_t
              evict_max_visited_gen_gap_per_checkpoint; /* Maximum gap between page and
                                             connection evict pass generation of visited pages */

            /*
             * Eviction thread information.
             */
            WT_CONDVAR *evict_cond;      /* Eviction server condition */
            WT_SPINLOCK evict_walk_lock; /* Eviction walk location */

            /*
             * Eviction thread tuning information.
             */
            uint32_t evict_tune_datapts_needed;                   /* Data needed to tune */
            wt_shared uint16_t evict_max_eviction_queue_attempts; /* Maximum number of attempts to
                                                                     add a page to eviction queue */
            wt_shared uint16_t evict_max_evict_page_attempts;     /* Maximum number of attempts
                                                                     to evict a page */

            struct timespec evict_tune_last_action_time; /* Time of last action */
            struct timespec evict_tune_last_time;        /* Time of last check */
            uint64_t evict_tune_progress_last;           /* Progress counter */
            uint64_t evict_tune_progress_rate_max;       /* Max progress rate */
            uint32_t evict_tune_workers_best;            /* Best performing value */
            uint32_t evict_tune_num_points;              /* Number of values tried */

            /*
             * LRU eviction list information.
             */
            WT_SPINLOCK evict_pass_lock;   /* Eviction pass lock */
            WT_SESSION_IMPL *walk_session; /* Eviction pass session */
            WT_DATA_HANDLE *walk_tree;     /* LRU walk current tree */

            WT_SPINLOCK evict_queue_lock; /* Eviction current queue lock */
            WTI_EVICT_QUEUE evict_queues[WTI_EVICT_QUEUE_MAX];
            WTI_EVICT_QUEUE *evict_current_queue; /* LRU current queue in use */
            WTI_EVICT_QUEUE *evict_fill_queue;    /* LRU next queue to fill.
                                                    This is usually the same as the
                                                    "other" queue but under heavy
                                                    load the eviction server will
                                                    start filling the current queue
                                                    before it switches. */
            WTI_EVICT_QUEUE *evict_other_queue;   /* LRU queue not in use */
            WTI_EVICT_QUEUE *evict_urgent_queue;  /* LRU urgent queue */

            /*
             * Pass interrupt counter.
             */
            wt_shared volatile uint32_t pass_intr; /* Interrupt eviction pass. */
            uint32_t evict_slots;                  /* LRU list eviction slots */

#define WT_EVICT_PRESSURE_THRESHOLD 0.95
#define WT_EVICT_SCORE_BUMP 10
#define WT_EVICT_SCORE_CUTOFF 10
#define WT_EVICT_SCORE_MAX 100
            /*
             * Score of how often LRU queues are empty on refill. This score varies between 0 (if
             * the queue hasn't been empty for a long time) and 100 (if the queue has been empty the
             * last 10 times we filled up.
             */
            uint32_t evict_empty_score;

            bool evict_tune_stable; /* Are we stable? */
            bool use_npos_in_pass;  /* Cached value of conn->evict_use_npos for the run of eviction
                                       server */
        } randlru;
    } impl;
};

/* Flags used with __wt_evict */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_EVICT_CALL_CLOSING 0x1u  /* Closing connection or tree */
#define WT_EVICT_CALL_NO_SPLIT 0x2u /* Splits not allowed */
#define WT_EVICT_CALL_URGENT 0x4u   /* Urgent eviction */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */

#define WT_EVICT_MAX_WORKERS 64

/*
 * Dispatch macros -- preserve existing call-site syntax while routing through the vtable.
 */
/* clang-format off */
#define __wt_evict_page(s, ref, state, flags) \
    (S2C(s)->evict->evict_page(S2C(s)->evict, (s), (ref), (state), (flags)))
#define __wt_evict_file(s, syncop) \
    (S2C(s)->evict->evict_file(S2C(s)->evict, (s), (syncop)))
#define __wt_evict_config(s, cfg, reconfig) \
    (S2C(s)->evict->config(S2C(s)->evict, (s), (cfg), (reconfig)))
#define __wt_evict_destroy(s) \
    (S2C(s)->evict->destroy(S2C(s)->evict, (s)))
#define __wt_evict_stats_update(s) \
    (S2C(s)->evict->stats_update(S2C(s)->evict, (s)))
#define __wt_evict_stats_init(s) \
    (S2C(s)->evict->stats_init(S2C(s)->evict, (s)))
#define __wt_evict_server_wake(s) \
    (S2C(s)->evict->server_wake(S2C(s)->evict, (s)))
#define __wt_evict_threads_create(s) \
    (S2C(s)->evict->threads_create(S2C(s)->evict, (s)))
#define __wt_evict_threads_destroy(s) \
    (S2C(s)->evict->threads_destroy(S2C(s)->evict, (s)))
#define __wt_evict_file_exclusive_on(s) \
    (S2C(s)->evict->file_exclusive_on(S2C(s)->evict, (s)))
#define __wt_evict_file_exclusive_off(s) \
    (S2C(s)->evict->file_exclusive_off(S2C(s)->evict, (s)))
#define __wt_evict_page_urgent(s, ref) \
    (S2C(s)->evict->page_urgent(S2C(s)->evict, (s), (ref)))
#define __wt_evict_priority_set(s, v) \
    (S2C(s)->evict->priority_set(S2C(s)->evict, (s), (v)))
#define __wt_evict_priority_clear(s) \
    (S2C(s)->evict->priority_clear(S2C(s)->evict, (s)))
#define __wt_verbose_dump_cache(s) \
    (S2C(s)->evict->verbose_dump_cache(S2C(s)->evict, (s)))
#define __wt_evict_cache_stat_walk(s) \
    (S2C(s)->evict->cache_stat_walk(S2C(s)->evict, (s)))
#define __wt_evict_aggressive(s) \
    (S2C(s)->evict->aggressive(S2C(s)->evict, (s)))
#define __wt_evict_cache_stuck(s) \
    (S2C(s)->evict->cache_stuck(S2C(s)->evict, (s)))
#define __wt_evict_clean_needed(s, pct) \
    (S2C(s)->evict->clean_needed(S2C(s)->evict, (s), (pct)))
#define __wt_evict_clean_pressure(s) \
    (S2C(s)->evict->clean_pressure(S2C(s)->evict, (s)))
#define __wt_evict_dirty_needed(s, pct) \
    (S2C(s)->evict->dirty_needed(S2C(s)->evict, (s), (pct)))
#define __wt_evict_needed(s, busy, readonly, ignore_updates_dirty, pct) \
    (S2C(s)->evict->needed(S2C(s)->evict, (s), (busy), (readonly), (ignore_updates_dirty), (pct)))
#define __wt_evict_favor_clearing_dirty_cache(s) \
    (S2C(s)->evict->favor_clearing_dirty(S2C(s)->evict, (s)))
#define __wt_evict_app_assist_worker_check(s, busy, readonly, interruptible, didworkp) \
    (S2C(s)->evict->app_assist_worker_check(                                          \
      S2C(s)->evict, (s), (busy), (readonly), (interruptible), (didworkp)))
#define __wt_evict_page_init(s, page) \
    (S2C(s)->evict->page_init(S2C(s)->evict, (s), (page)))
#define __wt_evict_touch_page(s, page, internal_only, wont_need) \
    (S2C(s)->evict->touch_page(S2C(s)->evict, (s), (page), (internal_only), (wont_need)))
#define __wt_evict_page_soon(s, ref) \
    (S2C(s)->evict->page_soon(S2C(s)->evict, (s), (ref)))
#define __wt_evict_page_is_soon(s, page) \
    (S2C(s)->evict->page_is_soon(S2C(s)->evict, (s), (page)))
#define __wt_evict_page_is_soon_or_wont_need(s, page) \
    (S2C(s)->evict->page_is_soon_or_wont_need(S2C(s)->evict, (s), (page)))
#define __wt_evict_page_first_dirty(s, page) \
    (S2C(s)->evict->page_first_dirty(S2C(s)->evict, (s), (page)))
#define __wt_evict_inherit_page_state(s, orig, new_page) \
    (S2C(s)->evict->inherit_page_state(S2C(s)->evict, (s), (orig), (new_page)))
#define __wt_evict_page_cache_bytes_decr(s, page) \
    (S2C(s)->evict->page_cache_bytes_decr(S2C(s)->evict, (s), (page)))
#define __wt_evict_clear_npos(s, btree) \
    (S2C(s)->evict->clear_npos(S2C(s)->evict, (s), (btree)))
#define __wt_evict_reset_checkpoint_stats(s) \
    (S2C(s)->evict->reset_checkpoint_stats(S2C(s)->evict, (s)))
#define __wt_evict_get_walk_tree(s) \
    (S2C(s)->evict->get_walk_tree(S2C(s)->evict, (s)))
#define __wt_evict_pass_interrupt_inc(s) \
    (S2C(s)->evict->pass_interrupt_inc(S2C(s)->evict, (s)))
#define __wt_evict_pass_interrupt_dec(s) \
    (S2C(s)->evict->pass_interrupt_dec(S2C(s)->evict, (s)))
#define __wt_evict_get_pass_gen(s) \
    (S2C(s)->evict->get_evict_pass_gen(S2C(s)->evict, (s)))
#define __wt_evict_get_page_pass_gen(s, page) \
    (S2C(s)->evict->get_page_evict_pass_gen(S2C(s)->evict, (s), (page)))
#define __wt_evict_save_evict_state(s, mod) \
    (S2C(s)->evict->save_evict_state(S2C(s)->evict, (s), (mod)))
#define __wt_evict_copy_evict_state_to_mod(s, dst, src) \
    (S2C(s)->evict->copy_evict_state_to_mod(S2C(s)->evict, (s), (dst), (src)))
#define __wt_evict_page_evict_retry(s, page) \
    (S2C(s)->evict->page_evict_retry(S2C(s)->evict, (s), (page)))
#define __wt_evict_page_set_cache_create_gen(s, page) \
    (S2C(s)->evict->page_set_cache_create_gen(S2C(s)->evict, (s), (page)))
#define __wt_evict_page_get_cache_create_gen(s, page) \
    (S2C(s)->evict->page_get_cache_create_gen(S2C(s)->evict, (s), (page)))
#define __wt_evict_btree_get_priority(s) \
    (S2C(s)->evict->btree_get_priority(S2C(s)->evict, (s)))
#define __wt_evict_btree_save_walk_period(s) \
    (S2C(s)->evict->btree_save_walk_period(S2C(s)->evict, (s)))
#define __wt_evict_btree_restore_walk_period(s) \
    (S2C(s)->evict->btree_restore_walk_period(S2C(s)->evict, (s)))
#define __wt_evict_btree_is_eviction_disabled(s) \
    (S2C(s)->evict->btree_is_eviction_disabled(S2C(s)->evict, (s)))
#define __wt_evict_btree_set_disabled_open(s) \
    (S2C(s)->evict->btree_set_disabled_open(S2C(s)->evict, (s)))
#define __wt_evict_btree_is_disabled_open(s) \
    (S2C(s)->evict->btree_is_disabled_open(S2C(s)->evict, (s)))
#define __wt_evict_btree_clear_disabled_open(s) \
    (S2C(s)->evict->btree_clear_disabled_open(S2C(s)->evict, (s)))
#define __wt_evict_btree_busy_inc(s, btree) \
    (S2C(s)->evict->btree_evict_busy_inc(S2C(s)->evict, (s), (btree)))
#define __wt_evict_btree_busy_dec(s, btree) \
    (S2C(s)->evict->btree_evict_busy_dec(S2C(s)->evict, (s), (btree)))
#define __wt_evict_btree_prefetch_busy_inc(s, btree) \
    (S2C(s)->evict->btree_prefetch_busy_inc(S2C(s)->evict, (s), (btree)))
#define __wt_evict_btree_prefetch_busy_dec(s, btree) \
    (S2C(s)->evict->btree_prefetch_busy_dec(S2C(s)->evict, (s), (btree)))
#define __wt_evict_btree_prefetch_busy_wait(s, btree) \
    (S2C(s)->evict->btree_prefetch_busy_wait(S2C(s)->evict, (s), (btree)))
#define __wt_evict_btree_get_evict_ref(s) \
    (S2C(s)->evict->btree_get_evict_ref(S2C(s)->evict, (s)))
/* clang-format on */

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern WT_DATA_HANDLE *__wt_evict_randlru_get_walk_tree(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern WT_REF *__wt_evict_randlru_btree_get_evict_ref(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_aggressive(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_btree_is_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_btree_is_eviction_disabled(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_cache_stuck(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_clean_needed(WT_EVICT *evict, WT_SESSION_IMPL *session,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_clean_pressure(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_dirty_needed(WT_EVICT *evict, WT_SESSION_IMPL *session,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, bool busy,
  bool readonly, bool ignore_updates_dirty, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_page_evict_retry(WT_EVICT *evict, WT_SESSION_IMPL *session,
  WT_PAGE *page) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_page_is_soon(WT_EVICT *evict, WT_SESSION_IMPL *session,
  WT_PAGE *page) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_page_is_soon_or_wont_need(WT_EVICT *evict, WT_SESSION_IMPL *session,
  WT_PAGE *page) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_evict_randlru_page_urgent(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_app_assist_worker_check(WT_EVICT *evict, WT_SESSION_IMPL *session,
  bool busy, bool readonly, bool interruptible, bool *didworkp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_config(WT_EVICT *evict, WT_SESSION_IMPL *session, const char *cfg[],
  bool reconfig) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_destroy(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_file(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_file_exclusive_on(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_page(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref,
  WT_REF_STATE previous_state, uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_threads_create(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_threads_destroy(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_randlru_verbose_dump_cache(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_evict_randlru_btree_get_priority(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_evict_randlru_get_evict_pass_gen(WT_EVICT *evict, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_evict_randlru_get_page_evict_pass_gen(WT_EVICT *evict,
  WT_SESSION_IMPL *session, WT_PAGE *page) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_evict_randlru_page_get_cache_create_gen(WT_EVICT *evict,
  WT_SESSION_IMPL *session, WT_PAGE *page) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_evict_randlru_btree_busy_dec(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_btree_busy_inc(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_btree_clear_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_btree_prefetch_busy_dec(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_btree_prefetch_busy_inc(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_btree_prefetch_busy_wait(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_btree_restore_walk_period(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_btree_save_walk_period(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_btree_set_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_cache_stat_walk(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_clear_npos(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree);
extern void __wt_evict_randlru_copy_evict_state(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE_MODIFY *dst, WT_PAGE_MODIFY *src);
extern void __wt_evict_randlru_favor_clearing_dirty(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_file_exclusive_off(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_inherit_page_state(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *orig_page, WT_PAGE *new_page);
extern void __wt_evict_randlru_page_cache_bytes_decr(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page);
extern void __wt_evict_randlru_page_first_dirty(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page);
extern void __wt_evict_randlru_page_init(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page);
extern void __wt_evict_randlru_page_set_cache_create_gen(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page);
extern void __wt_evict_randlru_page_soon(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref);
extern void __wt_evict_randlru_pass_interrupt_dec(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_pass_interrupt_inc(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_priority_clear(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_priority_set(WT_EVICT *evict, WT_SESSION_IMPL *session, uint64_t v);
extern void __wt_evict_randlru_reset_checkpoint_stats(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_save_evict_state(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE_MODIFY *mod);
extern void __wt_evict_randlru_server_wake(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_stats_init(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_stats_update(WT_EVICT *evict, WT_SESSION_IMPL *session);
extern void __wt_evict_randlru_touch_page(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page, bool internal_only, bool wont_need);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
