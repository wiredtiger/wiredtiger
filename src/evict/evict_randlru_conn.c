/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/* !!!
 * __wt_evict_randlru_config --
 *     Parses eviction-related configuration strings during `wiredtiger_open` or
 *     `WT_CONNECTION::reconfigure` to set eviction parameters.
 *
 *     Input parameters:
 *       (1) `cfg[]`: a stack of configuration strings, where each string specifies a configuration
 *           option (e.g., `eviction.threads_max`). The full list of valid eviction configurations
 *           are defined in `api_data.py`.
 *       (2) `reconfig`: a boolean that indicates whether this function is being called during
 *           `WT_CONNECTION::reconfigure`.
 *
 *     Return an error code for invalid configurations.
 */
int
__wt_evict_randlru_config(WT_EVICT *evict, WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
{
    WT_CACHE *cache;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    uint32_t evict_threads_max, evict_threads_min;

    conn = S2C(session);
    cache = conn->cache;

    WT_ASSERT(session, evict != NULL);

    WT_RET(__wt_evict_validate_config(session, cfg));

    WT_RET(__wt_config_gets(session, cfg, "eviction.threads_max", &cval));
    WT_ASSERT(session, cval.val > 0);
    evict_threads_max = (uint32_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "eviction.threads_min", &cval));
    WT_ASSERT(session, cval.val > 0);
    evict_threads_min = (uint32_t)cval.val;

    if (evict_threads_min > evict_threads_max)
        WT_RET_MSG(
          session, EINVAL, "eviction=(threads_min) cannot be greater than eviction=(threads_max)");
    conn->evict_threads_max = evict_threads_max;
    conn->evict_threads_min = evict_threads_min;

    WT_RET(__wt_config_gets(session, cfg, "eviction.evict_sample_inmem", &cval));
    conn->evict_sample_inmem = cval.val != 0;

    WT_RET(__wt_config_gets(session, cfg, "eviction.evict_use_softptr", &cval));
    __wt_atomic_store_bool_relaxed(&conn->evict_use_npos, cval.val != 0);

    WT_RET(__wt_config_gets(session, cfg, "eviction.legacy_page_visit_strategy", &cval));
    conn->evict_legacy_page_visit_strategy = cval.val != 0;

    /* Retrieve the wait time and convert from milliseconds */
    WT_RET(__wt_config_gets(session, cfg, "cache_max_wait_ms", &cval));
    if (cval.val > 1)
        evict->cache_max_wait_us = (uint64_t)(cval.val * WT_THOUSAND);
    else if (cval.val == 1)
        evict->cache_max_wait_us = 1;
    else
        evict->cache_max_wait_us = 0;

    /* Retrieve the timeout value and convert from seconds */
    WT_RET(__wt_config_gets(session, cfg, "cache_stuck_timeout_ms", &cval));
    evict->cache_stuck_timeout_ms = (uint64_t)cval.val;

    /*
     * The cache tolerance is a percentage value with range 0 - 100, inclusive.
     * Given input percentage is considered in multiples of 10 only, by applying floor().
     * 00 < value < 10  -> 00
     * 10 < value < 20  -> 10
     * 20 < value < 30  -> 20
     * ...
     * 90 < value < 100 -> 90
     * value is 100     -> 100
     */
    WT_RET(__wt_config_gets(session, cfg, "eviction.cache_tolerance_for_app_eviction", &cval));
    __wt_atomic_store_uint8_relaxed(
      &cache->cache_eviction_controls.cache_tolerance_for_app_eviction,
      (((uint8_t)cval.val / 10) * 10));

    WT_RET(__wt_config_gets(session, cfg, "eviction.incremental_app_eviction", &cval));
    if (cval.val != 0)
        F_SET_ATOMIC_32(&(cache->cache_eviction_controls), WT_CACHE_EVICT_INCREMENTAL_APP);

    WT_RET(__wt_config_gets(session, cfg, "eviction.prefer_scrub_eviction", &cval));
    if (cval.val != 0)
        F_SET_ATOMIC_32(&(cache->cache_eviction_controls), WT_CACHE_PREFER_SCRUB_EVICTION);

    WT_RET(__wt_config_gets(session, cfg, "eviction.skip_update_obsolete_check", &cval));
    if (cval.val != 0)
        F_SET_ATOMIC_32(&(cache->cache_eviction_controls), WT_CACHE_SKIP_UPDATE_OBSOLETE_CHECK);

    WT_RET(__wt_config_gets(session, cfg, "eviction.app_eviction_min_cache_fill_ratio", &cval));
    __wt_atomic_store_uint8_relaxed(
      &cache->cache_eviction_controls.app_eviction_min_cache_fill_ratio, (uint8_t)cval.val);

    /*
     * Resize the thread group if reconfiguring, otherwise the thread group will be initialized as
     * part of creating the connection workers.
     */
    if (reconfig)
        WT_RET(__wt_thread_group_resize(session, &conn->evict_threads, conn->evict_threads_min,
          conn->evict_threads_max, WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL));

    return (0);
}

/*
 * __wt_evict_randlru_reset_checkpoint_stats --
 *     Reset per-checkpoint eviction statistics.
 */
void
__wt_evict_randlru_reset_checkpoint_stats(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(session);

    __wt_atomic_store_uint64_relaxed(
      &WT_EVICT_RANDLRU(evict)->evict_max_unvisited_gen_gap_per_checkpoint, 0);
    __wt_atomic_store_uint64_relaxed(
      &WT_EVICT_RANDLRU(evict)->evict_max_visited_gen_gap_per_checkpoint, 0);
    __wt_atomic_store_uint64_relaxed(&evict->evict_max_clean_page_size_per_checkpoint, 0);
    __wt_atomic_store_uint64_relaxed(&evict->evict_max_dirty_page_size_per_checkpoint, 0);
    __wt_atomic_store_uint64_relaxed(&evict->evict_max_updates_page_size_per_checkpoint, 0);
    __wt_atomic_store_uint64_relaxed(&evict->evict_max_ms_per_checkpoint, 0);
    __wt_atomic_store_uint16_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_eviction_queue_attempts, 0);
    __wt_atomic_store_uint16_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_evict_page_attempts, 0);
    __wt_atomic_store_uint64_relaxed(&evict->reentry_hs_eviction_ms, 0);
}

static size_t
__evict_randlru_evict_extra_size(void)
{
    return (sizeof(WT_EVICT_RANDLRU_DATA));
}

static size_t
__evict_randlru_btree_extra_size(void)
{
    return (sizeof(WT_BTREE_RANDLRU_DATA));
}

static size_t
__evict_randlru_page_extra_size(void)
{
    return (sizeof(WT_PAGE_RANDLRU_DATA));
}

static size_t
__evict_randlru_page_modify_extra_size(void)
{
    return (sizeof(WT_PAGE_MODIFY_RANDLRU_DATA));
}

/*
 * __wt_evict_randlru_method_set --
 *     Wire up the vtable entries for LRU eviction.
 */
void
__wt_evict_randlru_method_set(WT_EVICT *evict)
{
    evict->algo_id = WT_EVICT_ALGO_RANDLRU;
    evict->evict_page = __wt_evict_randlru_page;
    evict->evict_file = __wt_evict_randlru_file;
    evict->config = __wt_evict_randlru_config;
    evict->destroy = __wt_evict_randlru_destroy;
    evict->stats_update = __wt_evict_randlru_stats_update;
    evict->stats_init = __wt_evict_randlru_stats_init;
    evict->server_wake = __wt_evict_randlru_server_wake;
    evict->threads_create = __wt_evict_randlru_threads_create;
    evict->threads_destroy = __wt_evict_randlru_threads_destroy;
    evict->file_exclusive_on = __wt_evict_randlru_file_exclusive_on;
    evict->file_exclusive_off = __wt_evict_randlru_file_exclusive_off;
    evict->page_urgent = __wt_evict_randlru_page_urgent;
    evict->priority_set = __wt_evict_randlru_priority_set;
    evict->priority_clear = __wt_evict_randlru_priority_clear;
    evict->verbose_dump_cache = __wt_evict_randlru_verbose_dump_cache;
    evict->cache_stat_walk = __wt_evict_randlru_cache_stat_walk;
    evict->aggressive = __wt_evict_randlru_aggressive;
    evict->cache_stuck = __wt_evict_randlru_cache_stuck;
    evict->clean_needed = __wt_evict_randlru_clean_needed;
    evict->clean_pressure = __wt_evict_randlru_clean_pressure;
    evict->dirty_needed = __wt_evict_randlru_dirty_needed;
    evict->needed = __wt_evict_randlru_needed;
    evict->favor_clearing_dirty = __wt_evict_randlru_favor_clearing_dirty;
    evict->app_assist_worker_check = __wt_evict_randlru_app_assist_worker_check;
    evict->page_init = __wt_evict_randlru_page_init;
    evict->touch_page = __wt_evict_randlru_touch_page;
    evict->page_soon = __wt_evict_randlru_page_soon;
    evict->page_is_soon = __wt_evict_randlru_page_is_soon;
    evict->page_is_soon_or_wont_need = __wt_evict_randlru_page_is_soon_or_wont_need;
    evict->page_first_dirty = __wt_evict_randlru_page_first_dirty;
    evict->inherit_page_state = __wt_evict_randlru_inherit_page_state;
    evict->page_cache_bytes_decr = __wt_evict_randlru_page_cache_bytes_decr;
    evict->clear_npos = __wt_evict_randlru_clear_npos;
    evict->reset_checkpoint_stats = __wt_evict_randlru_reset_checkpoint_stats;
    evict->get_walk_tree = __wt_evict_randlru_get_walk_tree;
    evict->pass_interrupt_inc = __wt_evict_randlru_pass_interrupt_inc;
    evict->pass_interrupt_dec = __wt_evict_randlru_pass_interrupt_dec;
    evict->get_evict_pass_gen = __wt_evict_randlru_get_evict_pass_gen;
    evict->get_page_evict_pass_gen = __wt_evict_randlru_get_page_evict_pass_gen;
    evict->save_evict_state = __wt_evict_randlru_save_evict_state;
    evict->copy_evict_state_to_mod = __wt_evict_randlru_copy_evict_state;
    evict->page_evict_retry = __wt_evict_randlru_page_evict_retry;
    evict->page_set_cache_create_gen = __wt_evict_randlru_page_set_cache_create_gen;
    evict->page_get_cache_create_gen = __wt_evict_randlru_page_get_cache_create_gen;
    evict->btree_get_priority = __wt_evict_randlru_btree_get_priority;
    evict->btree_save_walk_period = __wt_evict_randlru_btree_save_walk_period;
    evict->btree_restore_walk_period = __wt_evict_randlru_btree_restore_walk_period;
    evict->btree_is_eviction_disabled = __wt_evict_randlru_btree_is_eviction_disabled;
    evict->btree_set_disabled_open = __wt_evict_randlru_btree_set_disabled_open;
    evict->btree_is_disabled_open = __wt_evict_randlru_btree_is_disabled_open;
    evict->btree_clear_disabled_open = __wt_evict_randlru_btree_clear_disabled_open;
    evict->btree_evict_busy_inc = __wt_evict_randlru_btree_busy_inc;
    evict->btree_evict_busy_dec = __wt_evict_randlru_btree_busy_dec;
    evict->btree_prefetch_busy_inc = __wt_evict_randlru_btree_prefetch_busy_inc;
    evict->btree_prefetch_busy_dec = __wt_evict_randlru_btree_prefetch_busy_dec;
    evict->btree_prefetch_busy_wait = __wt_evict_randlru_btree_prefetch_busy_wait;
    evict->btree_get_evict_ref = __wt_evict_randlru_btree_get_evict_ref;
    evict->evict_extra_size = __evict_randlru_evict_extra_size;
    evict->btree_extra_size = __evict_randlru_btree_extra_size;
    evict->page_extra_size = __evict_randlru_page_extra_size;
    evict->page_modify_extra_size = __evict_randlru_page_modify_extra_size;
}

/* !!!
 * __wt_evict_randlru_destroy --
 *     Release all memory and locks related to eviction, ensuring the eviction system is properly
 *     destroyed. It must be called exactly once during `WT_CONNECTION::close`, and must be called
 *     after all the eviction threads are destroyed (via `__wt_evict_threads_destroy`).
 *
 *     Return an error code if the internal eviction session cannot be closed.
 */
int
__wt_evict_randlru_destroy(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    int i;

    conn = S2C(session);

    if (evict == NULL)
        return (0);

    __wt_cond_destroy(session, &WT_EVICT_RANDLRU(evict)->evict_cond);
    __wt_spin_destroy(session, &WT_EVICT_RANDLRU(evict)->evict_pass_lock);
    __wt_spin_destroy(session, &WT_EVICT_RANDLRU(evict)->evict_queue_lock);
    __wt_spin_destroy(session, &WT_EVICT_RANDLRU(evict)->evict_walk_lock);
    if (WT_EVICT_RANDLRU(evict)->walk_session != NULL)
        WT_TRET(__wt_session_close_internal(WT_EVICT_RANDLRU(evict)->walk_session));

    for (i = 0; i < WTI_EVICT_QUEUE_MAX; ++i) {
        __wt_spin_destroy(session, &WT_EVICT_RANDLRU(evict)->evict_queues[i].evict_lock);
        __wt_free(session, WT_EVICT_RANDLRU(evict)->evict_queues[i].evict_queue);
    }
    __wt_free(session, conn->evict);
    return (ret);
}

/*
 * __wt_evict_randlru_stats_update --
 *     Update eviction stats.
 */
void
__wt_evict_randlru_stats_update(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    __wt_evict_set_cache_threshold_stats(session);
}

/* !!!
 * __wt_evict_randlru_stats_init --
 *     Initialize eviction stats, ensuring they start with initial values during the startup
 *     process. It should be called exactly once when initializing eviction. Running it outside
 *     of startup will not cause functional failures, but it will reset eviction-related stats.
 *
 *     FIXME-WT-13666: Investigate whether this function should be internal to prevent unintended
 *     stat resets.
 */
void
__wt_evict_randlru_stats_init(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_STATS **stats;

    conn = S2C(session);
    stats = conn->stats;

    WT_STATP_CONN_SET(session, stats, eviction_maximum_clean_page_size_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(&evict->evict_max_clean_page_size_per_checkpoint));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_dirty_page_size_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(&evict->evict_max_dirty_page_size_per_checkpoint));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_updates_page_size_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(&evict->evict_max_updates_page_size_per_checkpoint));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_milliseconds,
      __wt_atomic_load_uint64_relaxed(&evict->evict_max_ms));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_milliseconds_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(&evict->evict_max_ms_per_checkpoint));
    WT_STATP_CONN_SET(session, stats, eviction_reentry_hs_eviction_milliseconds,
      __wt_atomic_load_uint64_relaxed(&evict->reentry_hs_eviction_ms));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_unvisited_gen_gap,
      __wt_atomic_load_uint64_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_unvisited_gen_gap));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_unvisited_gen_gap_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(
        &WT_EVICT_RANDLRU(evict)->evict_max_unvisited_gen_gap_per_checkpoint));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_visited_gen_gap,
      __wt_atomic_load_uint64_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_visited_gen_gap));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_visited_gen_gap_per_checkpoint,
      __wt_atomic_load_uint64_relaxed(
        &WT_EVICT_RANDLRU(evict)->evict_max_visited_gen_gap_per_checkpoint));
    WT_STATP_CONN_SET(
      session, stats, eviction_state, __wt_atomic_load_uint32_relaxed(&evict->flags));
    WT_STATP_CONN_SET(session, stats, eviction_aggressive_set,
      __wt_atomic_load_uint32_relaxed(&WT_EVICT_RANDLRU(evict)->evict_aggressive_score));
    WT_STATP_CONN_SET(
      session, stats, eviction_empty_score, WT_EVICT_RANDLRU(evict)->evict_empty_score);

    WT_STATP_CONN_SET(session, stats, eviction_active_workers,
      __wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads));
    WT_STATP_CONN_SET(session, stats, eviction_stable_state_workers,
      __wt_atomic_load_uint32_relaxed(&WT_EVICT_RANDLRU(evict)->evict_tune_workers_best));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_attempts_to_queue_page,
      __wt_atomic_load_uint16_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_eviction_queue_attempts));
    WT_STATP_CONN_SET(session, stats, eviction_maximum_attempts_to_evict_page,
      __wt_atomic_load_uint16_relaxed(&WT_EVICT_RANDLRU(evict)->evict_max_evict_page_attempts));

    WT_STATP_CONN_SET(session, stats, eviction_worker_lock_wait_time,
      __wt_atomic_load_uint64_relaxed(&evict->evict_lock_wait_time));

    /*
     * The number of files with active walks ~= number of hazard pointers in the walk session. Note:
     * reading without locking.
     */
    if (__wt_atomic_load_bool_relaxed(&conn->evict_server_running))
        WT_STATP_CONN_SET(session, stats, eviction_walks_active,
          WT_EVICT_RANDLRU(evict)->walk_session->hazards.num_active);

    /* Update eviction threshold stats. */
    __wt_evict_randlru_stats_update(evict, session);
}
