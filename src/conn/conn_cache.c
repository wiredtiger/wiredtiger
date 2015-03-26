/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __cache_config_local --
 *	Configure the underlying cache.
 */
static int
__cache_config_local(WT_SESSION_IMPL *session, int shared, const char *cfg[])
{
	WT_CACHE *cache;
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	uint32_t evict_workers_max, evict_workers_min;

	conn = S2C(session);
	cache = conn->cache;

	/*
	 * If not using a shared cache configure the cache size, otherwise
	 * check for a reserved size. All other settings are independent of
	 * whether we are using a shared cache or not.
	 */
	if (!shared) {
		WT_RET(__wt_config_gets(session, cfg, "cache_size", &cval));
		conn->cache_size = (uint64_t)cval.val;
	}

	WT_RET(__wt_config_gets(session, cfg, "cache_overhead", &cval));
	cache->overhead_pct = (u_int)cval.val;

	/*
	 * Historically the eviction.{dirty_target,target,trigger} configuration
	 * values were eviction_{dirty_target,target,trigger}, check for the old
	 * values before looking for the new values. The old values have illegal
	 * defaults (of 0) so we can detect if they were actually set or not.
	 */
	WT_RET(__wt_config_gets(session, cfg, "eviction_target", &cval));
	if (cval.val == 0)
		WT_RET(__wt_config_gets(
		    session, cfg, "eviction.target", &cval));
	cache->evict_target = (u_int)cval.val;

	WT_RET(__wt_config_gets(session, cfg, "eviction_trigger", &cval));
	if (cval.val == 0)
		WT_RET(__wt_config_gets(
		    session, cfg, "eviction.trigger", &cval));
	cache->evict_trigger = (u_int)cval.val;

	WT_RET(__wt_config_gets(session, cfg, "eviction_dirty_target", &cval));
	if (cval.val == 0)
		WT_RET(__wt_config_gets(
		    session, cfg, "eviction.dirty_target", &cval));
	cache->evict_dirty_target = (u_int)cval.val;

	WT_RET(__wt_config_gets(session, cfg, "eviction.threads_max", &cval));
	evict_workers_max = (uint32_t)cval.val - 1;

	WT_RET(__wt_config_gets(session, cfg, "eviction.threads_min", &cval));
	evict_workers_min = (uint32_t)cval.val - 1;

	if (evict_workers_min > evict_workers_max)
		WT_RET_MSG(session, EINVAL,
		    "eviction=(threads_min) cannot be greater than "
		    "eviction=(threads_max)");
	conn->evict_workers_max = evict_workers_max;
	conn->evict_workers_min = evict_workers_min;

	WT_RET(__wt_config_gets(session, cfg, "eviction.walk_base", &cval));
	cache->evict_walk_base = (u_int)cval.val;

	WT_RET(__wt_config_gets(
	    session, cfg, "eviction.walk_base_incr", &cval));
	cache->evict_walk_base_incr = (u_int)cval.val;

	WT_RET(__wt_config_gets(
	    session, cfg, "eviction.walk_queue_per_file", &cval));
	cache->evict_walk_queue_per_file = (u_int)cval.val;

	WT_RET(__wt_config_gets(
	    session, cfg, "eviction.walk_visit_per_file", &cval));
	cache->evict_walk_visit_per_file = (u_int)cval.val;

	return (0);
}

/*
 * __wt_cache_config --
 *	Configure or reconfigure the current cache and shared cache.
 */
int
__wt_cache_config(WT_SESSION_IMPL *session, int reconfigure, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	int now_shared, was_shared;

	conn = S2C(session);

	WT_ASSERT(session, conn->cache != NULL);

	WT_RET(__wt_config_gets_none(session, cfg, "shared_cache.name", &cval));
	now_shared = cval.len != 0;
	was_shared = F_ISSET(conn, WT_CONN_CACHE_POOL);

	/* Cleanup if reconfiguring */
	if (reconfigure && was_shared && !now_shared)
		/* Remove ourselves from the pool if necessary */
		WT_RET(__wt_conn_cache_pool_destroy(session));
	else if (reconfigure && !was_shared && now_shared)
		/*
		 * Cache size will now be managed by the cache pool - the
		 * start size always needs to be zero to allow the pool to
		 * manage how much memory is in-use.
		 */
		conn->cache_size = 0;

	/*
	 * Always setup the local cache - it's used even if we are
	 * participating in a shared cache.
	 */
	WT_RET(__cache_config_local(session, now_shared, cfg));
	if (now_shared) {
		WT_RET(__wt_cache_pool_config(session, cfg));
		WT_ASSERT(session, F_ISSET(conn, WT_CONN_CACHE_POOL));
		if (!was_shared)
			WT_RET(__wt_conn_cache_pool_open(session));
	}

	return (0);
}

/*
 * __wt_cache_create --
 *	Create the underlying cache.
 */
int
__wt_cache_create(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	conn = S2C(session);

	WT_ASSERT(session, conn->cache == NULL);

	WT_RET(__wt_calloc_one(session, &conn->cache));

	cache = conn->cache;

	/* Use a common routine for run-time configuration options. */
	WT_RET(__wt_cache_config(session, 0, cfg));

	/*
	 * The target size must be lower than the trigger size or we will never
	 * get any work done.
	 */
	if (cache->evict_target >= cache->evict_trigger)
		WT_ERR_MSG(session, EINVAL,
		    "eviction target must be lower than the eviction trigger");

	WT_ERR(__wt_cond_alloc(session,
	    "cache eviction server", 0, &cache->evict_cond));
	WT_ERR(__wt_cond_alloc(session,
	    "eviction waiters", 0, &cache->evict_waiter_cond));
	WT_ERR(__wt_spin_init(session, &cache->evict_lock, "cache eviction"));
	WT_ERR(__wt_spin_init(session, &cache->evict_walk_lock, "cache walk"));

	/* Allocate the LRU eviction queue. */
	cache->evict_slots =
	    cache->evict_walk_base + cache->evict_walk_base_incr;
	WT_ERR(__wt_calloc_def(session, cache->evict_slots, &cache->evict));

	/*
	 * We get/set some values in the cache statistics (rather than have
	 * two copies), configure them.
	 */
	__wt_cache_stats_update(session);
	return (0);

err:	WT_RET(__wt_cache_destroy(session));
	return (ret);
}

/*
 * __wt_cache_stats_update --
 *	Update the cache statistics for return to the application.
 */
void
__wt_cache_stats_update(WT_SESSION_IMPL *session)
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	WT_CONNECTION_STATS *stats;

	conn = S2C(session);
	cache = conn->cache;
	stats = &conn->stats;

	WT_STAT_SET(stats, cache_bytes_max, conn->cache_size);
	WT_STAT_SET(stats, cache_bytes_inuse, __wt_cache_bytes_inuse(cache));

	WT_STAT_SET(stats, cache_overhead, cache->overhead_pct);
	WT_STAT_SET(stats, cache_pages_inuse, __wt_cache_pages_inuse(cache));
	WT_STAT_SET(stats, cache_bytes_dirty, __wt_cache_dirty_inuse(cache));
	WT_STAT_SET(stats,
	    cache_eviction_maximum_page_size, cache->evict_max_page_size);
	WT_STAT_SET(stats, cache_pages_dirty, cache->pages_dirty);

	/* Figure out internal, leaf and overflow stats */
	WT_STAT_SET(stats, cache_bytes_internal, cache->bytes_internal);
	WT_STAT_SET(stats, cache_bytes_leaf,
	    conn->cache_size - (cache->bytes_internal + cache->bytes_overflow));
	WT_STAT_SET(stats, cache_bytes_overflow, cache->bytes_overflow);
}

/*
 * __wt_cache_destroy --
 *	Discard the underlying cache.
 */
int
__wt_cache_destroy(WT_SESSION_IMPL *session)
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	conn = S2C(session);
	cache = conn->cache;

	if (cache == NULL)
		return (0);

	WT_TRET(__wt_cond_destroy(session, &cache->evict_cond));
	WT_TRET(__wt_cond_destroy(session, &cache->evict_waiter_cond));
	__wt_spin_destroy(session, &cache->evict_lock);
	__wt_spin_destroy(session, &cache->evict_walk_lock);

	__wt_free(session, cache->evict);
	__wt_free(session, conn->cache);
	return (ret);
}
