/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

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
