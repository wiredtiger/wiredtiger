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
 * __wt_evict_validate_config --
 *     Validate trigger and target values of given configs.
 */
int
__wt_evict_validate_config(WT_SESSION_IMPL *session, const char *cfg[])
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

/*
 * __wt_evict_set_cache_threshold_stats --
 *     Set the cache threshold stats.
 */
void
__wt_evict_set_cache_threshold_stats(WT_SESSION_IMPL *session)
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
    __wt_evict_randlru_method_set(evict);

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
