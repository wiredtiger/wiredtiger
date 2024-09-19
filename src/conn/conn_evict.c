/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_evict_create --
 *     Initialize eviction.
 */
int
__wti_evict_create(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    int i;

    conn = S2C(session);
    cache = conn->cache;

    /*
     * The lowest possible page read-generation has a special meaning, it marks a page for forcible
     * eviction; don't let it happen by accident.
     */
    cache->read_gen_oldest = WT_READGEN_START_VALUE;
    __wt_atomic_store64(&cache->read_gen, WT_READGEN_START_VALUE);

    WT_RET(__wt_cond_auto_alloc(
      session, "cache eviction server", 10 * WT_THOUSAND, WT_MILLION, &cache->evict_cond));
    WT_RET(__wt_spin_init(session, &cache->evict_pass_lock, "evict pass"));
    WT_RET(__wt_spin_init(session, &cache->evict_queue_lock, "cache eviction queue"));
    WT_RET(__wt_spin_init(session, &cache->evict_walk_lock, "cache walk"));
    if ((ret = __wt_open_internal_session(
           conn, "evict pass", false, WT_SESSION_NO_DATA_HANDLES, 0, &cache->walk_session)) != 0)
        WT_RET_MSG(NULL, ret, "Failed to create session for eviction walks");

    /* Allocate the LRU eviction queue. */
    cache->evict_slots = WT_EVICT_WALK_BASE + WT_EVICT_WALK_INCR;
    for (i = 0; i < WT_EVICT_QUEUE_MAX; ++i) {
        WT_RET(__wt_calloc_def(session, cache->evict_slots, &cache->evict_queues[i].evict_queue));
        WT_RET(__wt_spin_init(session, &cache->evict_queues[i].evict_lock, "cache eviction"));
    }

    /* Ensure there are always non-NULL queues. */
    cache->evict_current_queue = cache->evict_fill_queue = &cache->evict_queues[0];
    cache->evict_other_queue = &cache->evict_queues[1];
    cache->evict_urgent_queue = &cache->evict_queues[WT_EVICT_URGENT_QUEUE];

    /*
     * We get/set some values in the evict statistics (rather than have two copies), configure them.
     */
    __wti_evict_stats_update(session);
    return (0);
}

/*
 * __wti_evict_destroy --
 *     Destroy Eviction
 */
int
__wti_evict_destroy(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_DECL_RET;
    int i;

    cache = S2C(session)->cache;

    if (cache == NULL)
        return (0);

    __wt_cond_destroy(session, &cache->evict_cond);
    __wt_spin_destroy(session, &cache->evict_pass_lock);
    __wt_spin_destroy(session, &cache->evict_queue_lock);
    __wt_spin_destroy(session, &cache->evict_walk_lock);
    if (cache->walk_session != NULL)
        WT_TRET(__wt_session_close_internal(cache->walk_session));

    for (i = 0; i < WT_EVICT_QUEUE_MAX; ++i) {
        __wt_spin_destroy(session, &cache->evict_queues[i].evict_lock);
        __wt_free(session, cache->evict_queues[i].evict_queue);
    }

    return (ret);
}

/*
 * __wti_evict_stats_update --
 *     Update the eviction statistics for return to the application.
 */
void
__wti_evict_stats_update(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_STATS **stats;

    conn = S2C(session);
    cache = conn->cache;
    stats = conn->stats;

    WT_STATP_CONN_SET(session, stats, cache_eviction_maximum_page_size,
      __wt_atomic_load64(&cache->evict_max_page_size));
    WT_STATP_CONN_SET(session, stats, cache_eviction_maximum_milliseconds,
      __wt_atomic_load64(&cache->evict_max_ms));
    WT_STATP_CONN_SET(
      session, stats, cache_reentry_hs_eviction_milliseconds, cache->reentry_hs_eviction_ms);

    WT_STATP_CONN_SET(session, stats, cache_eviction_state, __wt_atomic_load32(&cache->flags));
    WT_STATP_CONN_SET(session, stats, cache_eviction_aggressive_set, cache->evict_aggressive_score);
    WT_STATP_CONN_SET(session, stats, cache_eviction_empty_score, cache->evict_empty_score);

    WT_STATP_CONN_SET(session, stats, cache_eviction_active_workers,
      __wt_atomic_load32(&conn->evict_threads.current_threads));
    WT_STATP_CONN_SET(
      session, stats, cache_eviction_stable_state_workers, cache->evict_tune_workers_best);

    /*
     * The number of files with active walks ~= number of hazard pointers in the walk session. Note:
     * reading without locking.
     */
    if (__wt_atomic_loadbool(&conn->evict_server_running))
        WT_STATP_CONN_SET(
          session, stats, cache_eviction_walks_active, cache->walk_session->hazards.num_active);
}
