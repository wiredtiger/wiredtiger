/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __wt_stats {
	const char	*desc;				/* text description */
	uint64_t	 v;				/* 64-bit value */
};

/*
 * Read/write statistics without any test for statistics configuration.
 */
#define	WT_STAT(stats, fld)						\
	((stats)->fld.v)
#define	WT_STAT_DECR(stats, fld) do {					\
	--(stats)->fld.v;						\
} while (0)
#define	WT_STAT_ATOMIC_DECR(stats, fld) do {				\
	(void)WT_ATOMIC_SUB(WT_STAT(stats, fld), 1);			\
} while (0)
#define	WT_STAT_INCR(stats, fld) do {					\
	++(stats)->fld.v;						\
} while (0)
#define	WT_STAT_ATOMIC_INCR(stats, fld) do {				\
	(void)WT_ATOMIC_ADD(WT_STAT(stats, fld), 1);			\
} while (0)
#define	WT_STAT_INCRV(stats, fld, value) do {				\
	(stats)->fld.v += (value);					\
} while (0)
#define	WT_STAT_SET(stats, fld, value) do {				\
	(stats)->fld.v = (uint64_t)(value);				\
} while (0)

/*
 * Read/write statistics if "fast" statistics are configured.
 */
#define	WT_STAT_FAST_DECR(session, stats, fld) do {			\
	if (S2C(session)->stat_fast)					\
		WT_STAT_DECR(stats, fld);				\
} while (0)
#define	WT_STAT_FAST_ATOMIC_DECR(session, stats, fld) do {		\
	if (S2C(session)->stat_fast)					\
		WT_STAT_ATOMIC_DECR(stats, fld);			\
} while (0)
#define	WT_STAT_FAST_INCR(session, stats, fld) do {			\
	if (S2C(session)->stat_fast)					\
		WT_STAT_INCR(stats, fld);				\
} while (0)
#define	WT_STAT_FAST_ATOMIC_INCR(session, stats, fld) do {		\
	if (S2C(session)->stat_fast)					\
		WT_STAT_ATOMIC_INCR(stats, fld);			\
} while (0)
#define	WT_STAT_FAST_INCRV(session, stats, fld, value) do {		\
	if (S2C(session)->stat_fast)					\
		WT_STAT_INCRV(stats, fld, value);			\
} while (0)
#define	WT_STAT_FAST_SET(session, stats, fld, value) do {		\
	if (S2C(session)->stat_fast)					\
		WT_STAT_SET(stats, fld, value);				\
} while (0)

/*
 * Read/write connection handle statistics if "fast" statistics are configured.
 */
#define	WT_STAT_FAST_CONN_DECR(session, fld)				\
	WT_STAT_FAST_DECR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_ATOMIC_DECR(session, fld)			\
	WT_STAT_FAST_ATOMIC_DECR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_INCR(session, fld)				\
	WT_STAT_FAST_INCR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_ATOMIC_INCR(session, fld)			\
	WT_STAT_FAST_ATOMIC_INCR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_INCRV(session, fld, v)			\
	WT_STAT_FAST_INCRV(session, &S2C(session)->stats, fld, v)
#define	WT_STAT_FAST_CONN_SET(session, fld, v)				\
	WT_STAT_FAST_SET(session, &S2C(session)->stats, fld, v)

/*
 * Read/write data-source handle statistics if the data-source handle is set
 * and "fast" statistics are configured.
 *
 * XXX
 * We shouldn't have to check if the data-source handle is NULL, but it's
 * useful until everything is converted to using data-source handles.
 */
#define	WT_STAT_FAST_DATA_DECR(session, fld) do {			\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_DECR(					\
		    session, &(session)->dhandle->stats, fld);		\
} while (0)
#define	WT_STAT_FAST_DATA_INCR(session, fld) do {			\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_INCR(					\
		    session, &(session)->dhandle->stats, fld);		\
} while (0)
#define	WT_STAT_FAST_DATA_INCRV(session, fld, v) do {			\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_INCRV(					\
		    session, &(session)->dhandle->stats, fld, v);	\
} while (0)
#define	WT_STAT_FAST_DATA_SET(session, fld, v) do {			\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_SET(					\
		   session, &(session)->dhandle->stats, fld, v);	\
} while (0)

/*
 * DO NOT EDIT: automatically built by dist/stat.py.
 */
/* Statistics section: BEGIN */

/*
 * Statistics entries for connections.
 */
#define	WT_CONNECTION_STATS_BASE	1000
struct __wt_connection_stats {
	WT_STATS block_byte_map_read;
	WT_STATS block_byte_read;
	WT_STATS block_byte_write;
	WT_STATS block_map_read;
	WT_STATS block_preload;
	WT_STATS block_read;
	WT_STATS block_write;
	WT_STATS cache_bytes_dirty;
	WT_STATS cache_bytes_inuse;
	WT_STATS cache_bytes_max;
	WT_STATS cache_bytes_read;
	WT_STATS cache_bytes_write;
	WT_STATS cache_eviction_checkpoint;
	WT_STATS cache_eviction_clean;
	WT_STATS cache_eviction_dirty;
	WT_STATS cache_eviction_fail;
	WT_STATS cache_eviction_force;
	WT_STATS cache_eviction_force_fail;
	WT_STATS cache_eviction_hazard;
	WT_STATS cache_eviction_internal;
	WT_STATS cache_eviction_merge;
	WT_STATS cache_eviction_merge_fail;
	WT_STATS cache_eviction_merge_levels;
	WT_STATS cache_eviction_slow;
	WT_STATS cache_eviction_walk;
	WT_STATS cache_inmem_split;
	WT_STATS cache_pages_dirty;
	WT_STATS cache_pages_inuse;
	WT_STATS cache_read;
	WT_STATS cache_write;
	WT_STATS cond_wait;
	WT_STATS cursor_create;
	WT_STATS cursor_insert;
	WT_STATS cursor_next;
	WT_STATS cursor_prev;
	WT_STATS cursor_remove;
	WT_STATS cursor_reset;
	WT_STATS cursor_search;
	WT_STATS cursor_search_near;
	WT_STATS cursor_update;
	WT_STATS dh_conn_handles;
	WT_STATS dh_conn_sweeps;
	WT_STATS dh_session_handles;
	WT_STATS dh_session_sweeps;
	WT_STATS dh_sweep_evict;
	WT_STATS file_open;
	WT_STATS log_buffer_grow;
	WT_STATS log_buffer_size;
	WT_STATS log_bytes_user;
	WT_STATS log_bytes_written;
	WT_STATS log_max_filesize;
	WT_STATS log_reads;
	WT_STATS log_scan_records;
	WT_STATS log_scan_rereads;
	WT_STATS log_scans;
	WT_STATS log_slot_closes;
	WT_STATS log_slot_consolidated;
	WT_STATS log_slot_joins;
	WT_STATS log_slot_races;
	WT_STATS log_slot_ready_wait_timeout;
	WT_STATS log_slot_release_wait_timeout;
	WT_STATS log_slot_switch_fails;
	WT_STATS log_slot_toobig;
	WT_STATS log_slot_toosmall;
	WT_STATS log_slot_transitions;
	WT_STATS log_sync;
	WT_STATS log_writes;
	WT_STATS lsm_rows_merged;
	WT_STATS memory_allocation;
	WT_STATS memory_free;
	WT_STATS memory_grow;
	WT_STATS read_io;
	WT_STATS rec_pages;
	WT_STATS rec_pages_eviction;
	WT_STATS rec_skipped_update;
	WT_STATS rwlock_read;
	WT_STATS rwlock_write;
	WT_STATS session_cursor_open;
	WT_STATS txn_begin;
	WT_STATS txn_checkpoint;
	WT_STATS txn_checkpoint_running;
	WT_STATS txn_commit;
	WT_STATS txn_fail_cache;
	WT_STATS txn_rollback;
	WT_STATS write_io;
};

/*
 * Statistics entries for data sources.
 */
#define	WT_DSRC_STATS_BASE	2000
struct __wt_dsrc_stats {
	WT_STATS allocation_size;
	WT_STATS block_alloc;
	WT_STATS block_checkpoint_size;
	WT_STATS block_extension;
	WT_STATS block_free;
	WT_STATS block_magic;
	WT_STATS block_major;
	WT_STATS block_minor;
	WT_STATS block_reuse_bytes;
	WT_STATS block_size;
	WT_STATS bloom_count;
	WT_STATS bloom_false_positive;
	WT_STATS bloom_hit;
	WT_STATS bloom_miss;
	WT_STATS bloom_page_evict;
	WT_STATS bloom_page_read;
	WT_STATS bloom_size;
	WT_STATS btree_column_deleted;
	WT_STATS btree_column_fix;
	WT_STATS btree_column_internal;
	WT_STATS btree_column_variable;
	WT_STATS btree_compact_rewrite;
	WT_STATS btree_entries;
	WT_STATS btree_fixed_len;
	WT_STATS btree_maximum_depth;
	WT_STATS btree_maxintlitem;
	WT_STATS btree_maxintlpage;
	WT_STATS btree_maxleafitem;
	WT_STATS btree_maxleafpage;
	WT_STATS btree_overflow;
	WT_STATS btree_row_internal;
	WT_STATS btree_row_leaf;
	WT_STATS cache_bytes_read;
	WT_STATS cache_bytes_write;
	WT_STATS cache_eviction_checkpoint;
	WT_STATS cache_eviction_clean;
	WT_STATS cache_eviction_dirty;
	WT_STATS cache_eviction_fail;
	WT_STATS cache_eviction_hazard;
	WT_STATS cache_eviction_internal;
	WT_STATS cache_eviction_merge;
	WT_STATS cache_eviction_merge_fail;
	WT_STATS cache_eviction_merge_levels;
	WT_STATS cache_inmem_split;
	WT_STATS cache_overflow_value;
	WT_STATS cache_read;
	WT_STATS cache_read_overflow;
	WT_STATS cache_write;
	WT_STATS compress_raw_fail;
	WT_STATS compress_raw_fail_temporary;
	WT_STATS compress_raw_ok;
	WT_STATS compress_read;
	WT_STATS compress_write;
	WT_STATS compress_write_fail;
	WT_STATS compress_write_too_small;
	WT_STATS cursor_create;
	WT_STATS cursor_insert;
	WT_STATS cursor_insert_bulk;
	WT_STATS cursor_insert_bytes;
	WT_STATS cursor_next;
	WT_STATS cursor_prev;
	WT_STATS cursor_remove;
	WT_STATS cursor_remove_bytes;
	WT_STATS cursor_reset;
	WT_STATS cursor_search;
	WT_STATS cursor_search_near;
	WT_STATS cursor_update;
	WT_STATS cursor_update_bytes;
	WT_STATS lsm_chunk_count;
	WT_STATS lsm_generation_max;
	WT_STATS lsm_lookup_no_bloom;
	WT_STATS rec_dictionary;
	WT_STATS rec_overflow_key_internal;
	WT_STATS rec_overflow_key_leaf;
	WT_STATS rec_overflow_value;
	WT_STATS rec_page_delete;
	WT_STATS rec_page_merge;
	WT_STATS rec_pages;
	WT_STATS rec_pages_eviction;
	WT_STATS rec_skipped_update;
	WT_STATS rec_split_internal;
	WT_STATS rec_split_leaf;
	WT_STATS rec_split_max;
	WT_STATS session_compact;
	WT_STATS session_cursor_open;
	WT_STATS txn_update_conflict;
};

/* Statistics section: END */
