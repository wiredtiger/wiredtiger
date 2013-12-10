/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __lsm_stat_init --
 *	Initialize a LSM statistics structure.
 */
static int
__lsm_stat_init(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR_STAT *cst)
{
	WT_CURSOR *stat_cursor;
	WT_DECL_ITEM(uribuf);
	WT_DECL_RET;
	WT_DSRC_STATS *new, *stats;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	u_int i;
	int locked;
	char config[64];
	const char *cfg[] = {
	    WT_CONFIG_BASE(session, session_open_cursor), NULL, NULL };
	const char *disk_cfg[] = {
	   WT_CONFIG_BASE(session, session_open_cursor),
	   "checkpoint=" WT_CHECKPOINT, NULL, NULL };

	locked = 0;
	WT_RET(__wt_lsm_tree_get(session, uri, 0, &lsm_tree));
	WT_ERR(__wt_scr_alloc(session, 0, &uribuf));

	/* Propagate all, fast and/or clear to the cursors we open. */
	if (cst->stat_clear || cst->stat_all || cst->stat_fast) {
		(void)snprintf(config, sizeof(config),
		    "statistics=(%s%s%s)",
		    cst->stat_clear ? "clear," : "",
		    cst->stat_all ? "all," : "",
		    cst->stat_fast ? "fast," : "");
		cfg[1] = disk_cfg[1] = config;
	}

	/*
	 * Set the cursor to reference the data source statistics; we don't
	 * initialize it, instead we copy (rather than aggregate), the first
	 * chunk's statistics, which has the same effect.
	 */
	stats = &cst->u.dsrc_stats;

	/* Hold the LSM lock so that we can safely walk through the chunks. */
	WT_ERR(__wt_lsm_tree_lock(session, lsm_tree, 0));
	locked = 1;

	/*
	 * For each chunk, aggregate its statistics, as well as any associated
	 * bloom filter statistics, into the total statistics.
	 */
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];

		/*
		 * Get the statistics for the chunk's underlying object.
		 *
		 * XXX kludge: we may have an empty chunk where no checkpoint
		 * was written.  If so, try to open the ordinary handle on that
		 * chunk instead.
		 */
		WT_ERR(__wt_buf_fmt(
		    session, uribuf, "statistics:%s", chunk->uri));
		ret = __wt_curstat_open(session, uribuf->data,
		    F_ISSET(chunk, WT_LSM_CHUNK_ONDISK) ? disk_cfg : cfg,
		    &stat_cursor);
		if (ret == WT_NOTFOUND && F_ISSET(chunk, WT_LSM_CHUNK_ONDISK))
			ret = __wt_curstat_open(
			    session, uribuf->data, cfg, &stat_cursor);
		WT_ERR(ret);

		/*
		 * The underlying statistics have now been initialized; fill in
		 * values from the chunk's information, then aggregate into the
		 * top-level.
		 */
		new = (WT_DSRC_STATS *)WT_CURSOR_STATS(stat_cursor);
		WT_STAT_SET(new, lsm_generation_max, chunk->generation);

		/*
		 * We want to aggregate the table's statistics.  Get a base set
		 * of statistics from the first chunk, then aggregate statistics
		 * from each new chunk.
		 */
		if (i == 0)
			*stats = *new;
		else
			__wt_stat_aggregate_dsrc_stats(new, stats);
		WT_ERR(stat_cursor->close(stat_cursor));

		if (!F_ISSET(chunk, WT_LSM_CHUNK_BLOOM))
			continue;

		/* Maintain a count of bloom filters. */
		WT_STAT_INCR(&lsm_tree->stats, bloom_count);

		/* Get the bloom filter's underlying object. */
		WT_ERR(__wt_buf_fmt(
		    session, uribuf, "statistics:%s", chunk->bloom_uri));
		WT_ERR(__wt_curstat_open(
		    session, uribuf->data, cfg, &stat_cursor));

		/*
		 * The underlying statistics have now been initialized; fill in
		 * values from the bloom filter's information, then aggregate
		 * into the top-level.
		 */
		new = (WT_DSRC_STATS *)WT_CURSOR_STATS(stat_cursor);
		WT_STAT_SET(new,
		    bloom_size, (chunk->count * lsm_tree->bloom_bit_count) / 8);
		WT_STAT_SET(new, bloom_page_evict,
		    WT_STAT(new, cache_eviction_clean) +
		    WT_STAT(new, cache_eviction_dirty));
		WT_STAT_SET(new, bloom_page_read, WT_STAT(new, cache_read));

		__wt_stat_aggregate_dsrc_stats(new, stats);
		WT_ERR(stat_cursor->close(stat_cursor));
	}

	/* Set statistics that aren't aggregated directly into the cursor */
	WT_STAT_SET(stats, lsm_chunk_count, lsm_tree->nchunks);

	/* Aggregate, and optionally clear, LSM-level specific information. */
	__wt_stat_aggregate_dsrc_stats(&lsm_tree->stats, stats);
	if (cst->stat_clear)
		__wt_stat_refresh_dsrc_stats(&lsm_tree->stats);

	__wt_curstat_dsrc_final(cst);

err:	if (locked)
		WT_TRET(__wt_lsm_tree_unlock(session, lsm_tree));
	__wt_lsm_tree_release(session, lsm_tree);
	__wt_scr_free(&uribuf);

	return (ret);
}

/*
 * __wt_curstat_lsm_init --
 *	Initialize the statistics for a LSM tree.
 */
int
__wt_curstat_lsm_init(
    WT_SESSION_IMPL *session, const char *uri, WT_CURSOR_STAT *cst)
{
	WT_DECL_RET;

	WT_WITH_SCHEMA_LOCK(session, ret = __lsm_stat_init(session, uri, cst));

	return (ret);
}
