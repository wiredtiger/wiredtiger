/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __evict_config_abs_to_pct --
 *     Evict configuration values can be either a percentage or an absolute size, this function
 *     converts an absolute size to a percentage.
 */
static WT_INLINE int
__evict_config_abs_to_pct(
  WT_SESSION_IMPL *session, double *param, const char *param_name, uint64_t cache_size, bool shared)
{
    double input;

    WT_ASSERT(session, param != NULL);
    input = *param;

    /*
     * Anything above 100 is an absolute value; convert it to percentage.
     */
    if (input > 100.0) {
        /*
         * In a shared cache configuration the cache size changes regularly. Therefore, we require a
         * percentage setting and do not allow an absolute size setting.
         */
        if (shared)
            WT_RET_MSG(session, EINVAL,
              "Shared cache configuration requires a percentage value for %s", param_name);
        /* An absolute value can't exceed the cache size. */
        if (input > cache_size)
            WT_RET_MSG(session, EINVAL, "%s should not exceed cache size", param_name);

        *param = (input * 100.0) / cache_size;
    }

    return (0);
}

/*
 * __evict_validate_config --
 *     Validate trigger and target values of given configs.
 */
static int
__evict_validate_config(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    bool shared;

    conn = S2C(session);
    evict = conn->evict;

    WT_RET(__wt_config_gets_none(session, cfg, "shared_cache.name", &cval));
    shared = cval.len != 0;

    /* Debug flags are not yet set when this function runs during connection open. Set it now. */
    WT_RET(__wt_config_gets(session, cfg, "debug_mode.configuration", &cval));
    if (cval.val)
        FLD_SET(conn->debug_flags, WT_CONN_DEBUG_CONFIGURATION);
    else
        FLD_CLR(conn->debug_flags, WT_CONN_DEBUG_CONFIGURATION);

    WT_RET(__wt_config_gets(session, cfg, "eviction_target", &cval));
    evict->eviction_target = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(
      session, &(evict->eviction_target), "eviction target", conn->cache_size, shared));

    WT_RET(__wt_config_gets(session, cfg, "eviction_trigger", &cval));
    evict->eviction_trigger = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(
      session, &(evict->eviction_trigger), "eviction trigger", conn->cache_size, shared));

    WT_RET(__wt_config_gets(session, cfg, "eviction_dirty_target", &cval));
    evict->eviction_dirty_target = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(
      session, &(evict->eviction_dirty_target), "eviction dirty target", conn->cache_size, shared));

    WT_RET(__wt_config_gets(session, cfg, "eviction_dirty_trigger", &cval));
    evict->eviction_dirty_trigger = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(session, &(evict->eviction_dirty_trigger),
      "eviction dirty trigger", conn->cache_size, shared));

    WT_RET(__wt_config_gets(session, cfg, "eviction_updates_target", &cval));
    evict->eviction_updates_target = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(session, &(evict->eviction_updates_target),
      "eviction updates target", conn->cache_size, shared));

    WT_RET(__wt_config_gets(session, cfg, "eviction_updates_trigger", &cval));
    double updates_trigger_val = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(
      session, &updates_trigger_val, "eviction updates trigger", conn->cache_size, shared));
    __wt_atomic_store_double_relaxed(&evict->eviction_updates_trigger, updates_trigger_val);

    WT_RET(__wt_config_gets(session, cfg, "eviction_checkpoint_target", &cval));
    evict->eviction_checkpoint_target = (double)cval.val;
    WT_RET(__evict_config_abs_to_pct(session, &(evict->eviction_checkpoint_target),
      "eviction checkpoint target", conn->cache_size, shared));

    /* Check for invalid configurations and automatically fix them to suitable values. */
    if (evict->eviction_dirty_target > evict->eviction_target) {
        WT_CONFIG_DEBUG(session,
          "config eviction_dirty_target=%f cannot exceed eviction_target=%f. Setting "
          "eviction_dirty_target to %f.",
          evict->eviction_dirty_target, evict->eviction_target, evict->eviction_target);
        evict->eviction_dirty_target = evict->eviction_target;
    }

    if (evict->eviction_checkpoint_target > 0 &&
      evict->eviction_checkpoint_target < evict->eviction_dirty_target) {
        WT_CONFIG_DEBUG(session,
          "config eviction_checkpoint_target=%f cannot be less than eviction_dirty_target=%f. "
          "Setting "
          "eviction_checkpoint_target to %f.",
          evict->eviction_checkpoint_target, evict->eviction_dirty_target,
          evict->eviction_dirty_target);
        evict->eviction_checkpoint_target = evict->eviction_dirty_target;
    }

    if (evict->eviction_dirty_trigger > evict->eviction_trigger) {
        WT_CONFIG_DEBUG(session,
          "config eviction_dirty_trigger=%f cannot exceed eviction_trigger=%f. Setting "
          "eviction_dirty_trigger to %f.",
          evict->eviction_dirty_trigger, evict->eviction_trigger, evict->eviction_trigger);
        evict->eviction_dirty_trigger = evict->eviction_trigger;
    }

    bool precise_checkpoint = F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT);
    if (evict->eviction_updates_target < DBL_EPSILON) {
        if (precise_checkpoint) {
            /*
             * If we are running with precise checkpoint enabled we want to discourage update based
             * eviction. To do this we set the updates target to the dirty target by default. This
             * change improves performance with regards to history store eviction as previously we
             * were evicting history store pages ahead of the checkpoint as they would have updates
             * on them. These pages would then be read back in due to checkpoint moving updates to
             * the history store.
             */
            WT_CONFIG_DEBUG(session,
              "config eviction_updates_target (%f) cannot be zero. Setting "
              "to eviction_dirty_target (%f) for precise checkpoint.",
              evict->eviction_updates_target, evict->eviction_dirty_target);
            evict->eviction_updates_target = evict->eviction_dirty_target;
        } else {
            WT_CONFIG_DEBUG(session,
              "config eviction_updates_target (%f) cannot be zero. Setting "
              "to 50%% of eviction_dirty_target (%f).",
              evict->eviction_updates_target, evict->eviction_dirty_target / 2);
            evict->eviction_updates_target = evict->eviction_dirty_target / 2;
        }
    }

    double updates_trigger = __wt_atomic_load_double_relaxed(&evict->eviction_updates_trigger);
    if (updates_trigger < DBL_EPSILON) {
        /*
         * Generally we want to allow a reasonable amount of updates content, the default dirty
         * targets of 5% target and 20% dirty would result in a 2.5% dirty target which is lower
         * than ideal when precise checkpoints are configured. Allow more updates content to remain
         * in cache, but handle cases where non-default dirty configurations would cause updates
         * target to exceed the trigger value with an asymmetric formula.
         */
        if (precise_checkpoint &&
          evict->eviction_dirty_trigger / 2 < evict->eviction_updates_target) {
            WT_CONFIG_DEBUG(session,
              "config eviction_updates_trigger (%f) cannot be zero. Setting "
              "to eviction_dirty_trigger (%f) for precise checkpoint.",
              updates_trigger, evict->eviction_dirty_trigger);
            updates_trigger = evict->eviction_dirty_trigger;
        } else {
            WT_CONFIG_DEBUG(session,
              "config eviction_updates_trigger (%f) cannot be zero. Setting "
              "to 50%% of eviction_dirty_trigger (%f).",
              updates_trigger, evict->eviction_dirty_trigger / 2);
            updates_trigger = evict->eviction_dirty_trigger / 2;
        }
    }

    /* Don't allow the trigger to be larger than the overall trigger. */
    if (updates_trigger > evict->eviction_trigger) {
        WT_CONFIG_DEBUG(session,
          "config eviction_updates_trigger=%f cannot exceed eviction_trigger=%f. Setting "
          "eviction_updates_trigger to %f.",
          updates_trigger, evict->eviction_trigger, evict->eviction_trigger);
        updates_trigger = evict->eviction_trigger;
    }

    /* The target size must be lower than the trigger size or we will never get any work done. */
    if (evict->eviction_target >= evict->eviction_trigger)
        WT_RET_MSG(session, EINVAL, "eviction target must be lower than the eviction trigger");
    if (evict->eviction_dirty_target >= evict->eviction_dirty_trigger)
        WT_RET_MSG(
          session, EINVAL, "eviction dirty target must be lower than the eviction dirty trigger");
    if (evict->eviction_updates_target >= updates_trigger)
        WT_RET_MSG(session, EINVAL,
          "eviction updates target must be lower than the eviction updates trigger");

    /* Store the value back to eviction updates trigger after we have validated it. */
    __wt_atomic_store_double_relaxed(&evict->eviction_updates_trigger, updates_trigger);
    return (0);
}

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

    WT_RET(__evict_validate_config(session, cfg));

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
 * __evict_randlru_method_set --
 *     Wire up the vtable entries for LRU eviction.
 */
static void
__evict_randlru_method_set(WT_EVICT *evict)
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
 * __wt_evict_create --
 *     Set up eviction's internal structures and stats during `wiredtiger_open` to manage eviction.
 *     It must be called exactly once during `wiredtiger_open` and must be called before any
 *     eviction threads are spawned.
 *
 *     Input parameter:
 *       `cfg[]`: An array of configuration strings. This is passed to `__evict_config`, which
 *       handles all eviction-related configs (i.e., `eviction.*`) as part of the eviction
 *       setup process.
 *
 *     Return an error code for invalid configurations, memory allocation, or spinlock
 *     initialization failures.
 */
int
__wt_evict_create(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    int i;

    conn = S2C(session);

    WT_ASSERT(session, conn->evict == NULL);
    WT_RET(__wt_calloc(session, 1, sizeof(WT_EVICT) + sizeof(WT_EVICT_RANDLRU_DATA), &conn->evict));

    evict = conn->evict;
    __evict_randlru_method_set(evict);

    /* Use a common routine for run-time configuration options. */
    WT_RET(__wt_evict_randlru_config(evict, session, cfg, false));

    /*
     * The lowest possible page read-generation has a special meaning, it marks a page for forcible
     * eviction; don't let it happen by accident.
     */
    WT_EVICT_RANDLRU(evict)->read_gen_oldest = WT_READGEN_START_VALUE;
    __wt_atomic_store_uint64_relaxed(&WT_EVICT_RANDLRU(evict)->read_gen, WT_READGEN_START_VALUE);

    WT_RET(__wt_cond_auto_alloc(
      session, "evict server", 10 * WT_THOUSAND, WT_MILLION, &WT_EVICT_RANDLRU(evict)->evict_cond));
    WT_RET(__wt_spin_init(session, &WT_EVICT_RANDLRU(evict)->evict_pass_lock, "evict pass"));
    WT_RET(__wt_spin_init(session, &WT_EVICT_RANDLRU(evict)->evict_queue_lock, "evict queues"));
    WT_RET(__wt_spin_init(session, &WT_EVICT_RANDLRU(evict)->evict_walk_lock, "evict walk"));
    if ((ret = __wt_open_internal_session(conn, "evict pass", false, WT_SESSION_NO_DATA_HANDLES, 0,
           &WT_EVICT_RANDLRU(evict)->walk_session)) != 0)
        WT_RET_MSG(NULL, ret, "Failed to create session for eviction walks");

    /* Allocate the LRU eviction queue. */
    WT_EVICT_RANDLRU(evict)->evict_slots = WTI_EVICT_WALK_BASE + WTI_EVICT_WALK_INCR;
    for (i = 0; i < WTI_EVICT_QUEUE_MAX; ++i) {
        WT_RET(__wt_calloc_def(
          session, WT_EVICT_RANDLRU(evict)->evict_slots, &WT_EVICT_RANDLRU(evict)->evict_queues[i].evict_queue));
        WT_RET(
          __wt_spin_init(session, &WT_EVICT_RANDLRU(evict)->evict_queues[i].evict_lock, "evict queue"));
    }

    /* Ensure there are always non-NULL queues. */
    WT_EVICT_RANDLRU(evict)->evict_current_queue = WT_EVICT_RANDLRU(evict)->evict_fill_queue =
      &WT_EVICT_RANDLRU(evict)->evict_queues[0];
    WT_EVICT_RANDLRU(evict)->evict_other_queue = &WT_EVICT_RANDLRU(evict)->evict_queues[1];
    WT_EVICT_RANDLRU(evict)->evict_urgent_queue = &WT_EVICT_RANDLRU(evict)->evict_queues[WTI_EVICT_URGENT_QUEUE];
    evict->evict_lock_wait_time = 0;

    /*
     * We get/set some values in the evict statistics (rather than have two copies), configure them.
     */
    __wt_evict_randlru_stats_init(evict, session);
    return (0);
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
 * __evict_set_cache_threshold_stats --
 *     Set the cache threshold stats.
 */
static void
__evict_set_cache_threshold_stats(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn = S2C(session);

    /*
     * It is possible for this function to be called before the eviction system is created, so we
     * need to check for that.
     */
    if (conn->evict == NULL)
        return;

    WT_EVICT *evict = conn->evict;
    WT_CONNECTION_STATS **stats = conn->stats;

    /*
     * WiredTiger's cache thresholds are percentages but the stats are integers, so we convert to
     * integers by multiplying by 100. This gives us 2 decimal places of precision. The expectation
     * is that tooling will display this as a percentage.
     */
    WT_STATP_CONN_SET(session, stats, eviction_threshold_cache_full_target,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_target)));
    WT_STATP_CONN_SET(session, stats, eviction_threshold_cache_full_trigger,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_trigger)));
    WT_STATP_CONN_SET(session, stats, eviction_threshold_dirty_target,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_dirty_target)));
    WT_STATP_CONN_SET(session, stats, eviction_threshold_dirty_trigger,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_dirty_trigger)));
    WT_STATP_CONN_SET(session, stats, eviction_threshold_updates_target,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_updates_target)));
    WT_STATP_CONN_SET(session, stats, eviction_threshold_updates_trigger,
      (int64_t)(WT_HUNDRED * __wt_atomic_load_double_relaxed(&evict->eviction_updates_trigger)));
}

/*
 * __wt_evict_randlru_stats_update --
 *     Update eviction stats.
 */
void
__wt_evict_randlru_stats_update(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    __evict_set_cache_threshold_stats(session);
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

/*
 * __wt_evict_randlru_aggressive --
 *     Return if the eviction is in aggressive mode.
 */
bool
__wt_evict_randlru_aggressive(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    return (__wt_atomic_load_uint32_relaxed(&WT_EVICT_RANDLRU(S2C(session)->evict)->evict_aggressive_score) >=
      WT_EVICT_SCORE_CUTOFF);
}

/*
 * __wt_evict_randlru_cache_stuck --
 *     Return if the eviction cache is stuck.
 */
bool
__wt_evict_randlru_cache_stuck(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    uint32_t tmp_evict_aggressive_score;

    tmp_evict_aggressive_score = __wt_atomic_load_uint32_relaxed(&WT_EVICT_RANDLRU(evict)->evict_aggressive_score);
    WT_ASSERT(session, tmp_evict_aggressive_score <= WT_EVICT_SCORE_MAX);
    return (
      tmp_evict_aggressive_score == WT_EVICT_SCORE_MAX && F_ISSET(evict, WT_EVICT_CACHE_HARD));
}

/*
 * __wt_evict_randlru_clean_needed --
 *     Return if the eviction clean bytes threshold has been reached.
 */
bool
__wt_evict_randlru_clean_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, double *pct_fullp)
{
    uint64_t bytes_inuse, bytes_max;

    bytes_max = S2C(session)->cache_size + 1;
    bytes_inuse = __wt_cache_bytes_inuse(S2C(session)->cache);

    if (pct_fullp != NULL)
        *pct_fullp = ((100.0 * bytes_inuse) / bytes_max);

    return (bytes_inuse > (evict->eviction_trigger * bytes_max) / 100);
}

/*
 * __wt_evict_randlru_dirty_needed --
 *     Return if the eviction dirty bytes threshold has been reached.
 */
bool
__wt_evict_randlru_dirty_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, double *pct_fullp)
{
    uint64_t bytes_dirty, bytes_max;
    double dirty_trigger = __wt_atomic_load_double_relaxed(&evict->eviction_dirty_trigger);

    bytes_dirty = __wt_cache_dirty_leaf_inuse(S2C(session)->cache);
    bytes_max = S2C(session)->cache_size + 1;

    if (pct_fullp != NULL)
        *pct_fullp = (100.0 * bytes_dirty) / bytes_max;

    return (bytes_dirty > (uint64_t)(dirty_trigger * bytes_max) / 100);
}

/*
 * __wt_evict_randlru_clean_pressure --
 *     Return if the eviction clean pressure threshold has been reached.
 */
bool
__wt_evict_randlru_clean_pressure(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    double pct_full;

    pct_full = 0;

    if (__wt_evict_randlru_clean_needed(evict, session, &pct_full))
        return (true);
    if (pct_full > evict->eviction_target &&
      pct_full >= WT_EVICT_PRESSURE_THRESHOLD * evict->eviction_trigger)
        return (true);
    return (false);
}

/*
 * __wt_evict_randlru_favor_clearing_dirty --
 *     Favor clearing dirty content.
 */
void
__wt_evict_randlru_favor_clearing_dirty(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(session);

    __wt_atomic_store_double_relaxed(&evict->eviction_dirty_trigger, 1.0);
    __wt_atomic_store_double_relaxed(&evict->eviction_dirty_target, 0.1);
}
