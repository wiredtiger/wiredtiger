/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * ARRAY COUNTERS: DESIGN DESCRIPTION.
 *
 * Instead of a single counter we use an array of values.
 * Threads update different values in an array to avoid writing the same
 * cache line and incurring the cache coherency overheads, which can
 * dramatically slowdown fast and otherwise read-mostly
 * workloads. Upon reading a value, items in individual array cells
 * are merged and returned to the caller.
 * Aggregation is performed without locking, so the counter read may be
 * slightly inconsistent. Existing use cases tolerate eventual consistency.
 * We are not making things worse relative to non-array implementation.
 * The read operation now takes longer, because of aggregation, so we
 * are slightly increasing the window when things may be inconsistent,
 * but that's about it.
 *
 * We used a fixed number of slots in an array. Picking the number of slots
 * is not as straightforward as it should be.
 * Ideally, if our application is running on the system alone is CPU-intensive,
 * and is using all CPUs on the system, we want to use the same number of slots
 * as there are CPUs (because their L1 caches are the units of coherency).
 * However, in practice we cannot easily determine how many CPUs are actually
 * available for the application.
 * Our next best option is to use the number of threads in the application as
 * a heuristic for the number of CPUs. Unfortunately, However, inside WT we do
 * not know when the application creates its threads.
 *
 * Our solution is to simply use a fixed number of slots that roughly
 * approximates the largest number of cores we expect to see on the machine
 * where WT is used. All we want is to avoid all threads writing the same
 * variable at the same time, and that solution does the trick.
 *
 */
#define	WT_COUNTER_SLOTS 24

/* This is a cache-line-padded counter value. 
 * It is here to effectively support array counters.
 * Padding is needed, otherwise cache coherency messages
 * will be triggered because of false sharing.
 *
 * The actual counter value v must be signed, because it is possible
 * that one thread incremented the counter in its own slot, and then
 * another thread decremented the same counter in another slot,
 * which was initially zero. We need to allow the value is the second
 * thread's slot to be negative. When the values are aggregated, we get
 * the correct total value.
 */
typedef struct{
	int64_t v;
	char padding[WT_CACHE_LINE_ALIGNMENT-sizeof(uint64_t)];
} padded_counter_t;

struct __wt_stats {
	const char	*desc;				/* text description */
//  	uint64_t	 v;				/* 64-bit value */
	padded_counter_t array_v[WT_COUNTER_SLOTS];     /* Array counter */
};

/*
 * This macro determines the slot id for the array of counters.
 *
 * Ideally, we want a slot per CPU, and we want each thread to index the slot
 * corresponding to the CPU it runs on. Unfortunately, getting the id of the
 * current CPU is difficult. Some operating systems provide a system call for
 * that, but not all. Further, invoking a system call every time we need to
 * increment a stats counter is expensive. Another option would be to use the
 * rdtscp instruction, but it's x86 specific, has a ~50 cycle overhead and is
 * not supported on some older processors.
 *
 * Our second-best option is to use the thread ID. Unfortunately, there is no
 * portable way to obtain a thread ID that is a small-enough number that could
 * be used as an array index.
 *
 * Our solution is to use the session ID, because there is a session per thread
 * and the id itself is a small monotonically increasing number, so this results
 * in threads writing into different array slots, which is ultimately what we
 * want.
 */
#define	WT_STATS_SLOT_ID	(session->id) % WT_COUNTER_SLOTS

/* Set all the values in the array counter slots to zero. We are
 * doing more work than needed by using memset, because we are setting
 * both the values and the padding. However, resetting the counters is
 * not a common case operation, so we use memset for compactness.
 */
#define	WT_STAT_ALL_RESET(stats, fld)  do {				\
		memset((stats)->fld.array_v, 0,                         \
		       sizeof(padded_counter_t) * WT_COUNTER_SLOTS);	\
} while (0)

/* Aggregate the counter values from all slots into the 
 * "master" value "v". Return v.
 * We may race here, which appears to be okay for most
 * counter uses. If there are situations where we want to be
 * precise, we must create a separate version that uses locks.
 */
static inline uint64_t
__wt_stats_aggregate_and_return(struct __wt_stats *stats)
{
	int i;
	int64_t aggr_v = 0;

	for (i = 0; i < WT_COUNTER_SLOTS; i++)
		aggr_v += stats->array_v[i].v;

	/* This can race. However, the previous implementation
	 * allowed the same races as well: different threads could
	 * set the same counter value simultaneously. Therefore,
	 * we are not weakening the isolation semantics of the previous
	 * implementation.
	 */
	return (uint64_t) aggr_v;
}

/*
 * Read/write statistics without any test for statistics configuration.
 */
/* These two macros are a replacement for WT_STAT. WT_STAT used to return
 * the stats field: ((stats)->fld.v). The caller could either read this
 * field, or set it (or both). In our case, reading and setting the field
 * require different actions: reading must aggregate the values across
 * array slots, setting must update the counter in one slot only.
 * So we need to have different macros depending on whether the intent
 * is to read or to set the field.
 */
#define	WT_STAT_READ(stats, fld)				        \
	__wt_stats_aggregate_and_return(&((stats)->fld))
/* Just return the field that the caller will set */
#define	WT_STAT_WRITE(session, stats, fld)		                \
	((stats)->fld.array_v[WT_STATS_SLOT_ID].v)
/* This is for situations where we just want to update a value
 * in any array slot. This is for infrequent update operations,
 * where it's inconvenient for us to use the coherency-aware
 * macro WT_STAT_WRITE, i.e., in cases where we don't know the
 * session id. We just return the first array slot.
 */
#define	WT_STAT_WRITE_SIMPLE(stats, fld)		                 \
	((stats)->fld.array_v[0].v)
/* For atomic macros we don't want races, so we always update the
 * same slot (slot 0).
 */
#define	WT_STAT_ATOMIC_DECRV(stats, fld, value) do {			  \
		(void)WT_ATOMIC_SUB8((stats)->fld.array_v[0].v, (value)); \
} while (0)
#define	WT_STAT_ATOMIC_DECR(stats, fld) WT_STAT_ATOMIC_DECRV(stats, fld, 1)
#define	WT_STAT_ATOMIC_INCRV(stats, fld, value) do {			  \
		(void)WT_ATOMIC_ADD8((stats)->fld.array_v[0].v, (value)); \
} while (0)
#define	WT_STAT_ATOMIC_INCR(stats, fld) WT_STAT_ATOMIC_INCRV(stats, fld, 1)

#define	WT_STAT_DECRV(session, stats, fld, value) do {		      \
		(stats)->fld.array_v[WT_STATS_SLOT_ID].v -= (value);  \
} while (0)
#define	WT_STAT_DECR(session, stats, fld) WT_STAT_DECRV(session, stats, fld, 1)
#define	WT_STAT_INCRV(session, stats, fld, value) do {		      \
		(stats)->fld.array_v[WT_STATS_SLOT_ID].v += (value);  \
} while (0)
#define	WT_STAT_INCR(session, stats, fld) WT_STAT_INCRV(session, stats, fld, 1)
#define	WT_STAT_SET(session, stats, fld, value) do {			\
	WT_STAT_ALL_RESET(stats,fld);                                   \
	WT_STAT_WRITE(session, stats, fld) = (int64_t)(value);		\
} while (0)
/* END DONE */

/*
 * Read/write statistics if "fast" statistics are configured.
 */
#define	WT_STAT_FAST_ATOMIC_DECRV(session, stats, fld, value) do {	\
	if (FLD_ISSET(S2C(session)->stat_flags, WT_CONN_STAT_FAST))	\
		WT_STAT_ATOMIC_DECRV(stats, fld, value);		\
} while (0)
#define	WT_STAT_FAST_ATOMIC_DECR(session, stats, fld)			\
	WT_STAT_FAST_ATOMIC_DECRV(session, stats, fld, 1)
#define	WT_STAT_FAST_ATOMIC_INCRV(session, stats, fld, value) do {	\
	if (FLD_ISSET(S2C(session)->stat_flags, WT_CONN_STAT_FAST))	\
		WT_STAT_ATOMIC_INCRV(stats, fld, value);		\
} while (0)
#define	WT_STAT_FAST_ATOMIC_INCR(session, stats, fld)			\
	WT_STAT_FAST_ATOMIC_INCRV(session, stats, fld, 1)
#define	WT_STAT_FAST_DECRV(session, stats, fld, value) do {		\
	if (FLD_ISSET(S2C(session)->stat_flags, WT_CONN_STAT_FAST))	\
		WT_STAT_DECRV(session, stats, fld, value);		\
} while (0)
#define	WT_STAT_FAST_DECR(session, stats, fld)				\
	WT_STAT_FAST_DECRV(session, stats, fld, 1)
#define	WT_STAT_FAST_INCRV(session, stats, fld, value) do {		\
	if (FLD_ISSET(S2C(session)->stat_flags, WT_CONN_STAT_FAST))	\
		WT_STAT_INCRV(session, stats, fld, value);		\
} while (0)
#define	WT_STAT_FAST_INCR(session, stats, fld)				\
	WT_STAT_FAST_INCRV(session, stats, fld, 1)
#define	WT_STAT_FAST_SET(session, stats, fld, value) do {		\
	if (FLD_ISSET(S2C(session)->stat_flags, WT_CONN_STAT_FAST))	\
		WT_STAT_SET(session, stats, fld, value);		\
} while (0)

/*
 * Read/write connection handle statistics if "fast" statistics are configured.
 */
#define	WT_STAT_FAST_CONN_ATOMIC_DECRV(session, fld, value)		\
	WT_STAT_FAST_ATOMIC_DECRV(session, &S2C(session)->stats, fld, value)
#define	WT_STAT_FAST_CONN_ATOMIC_DECR(session, fld)			\
	WT_STAT_FAST_ATOMIC_DECR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_ATOMIC_INCRV(session, fld, value)		\
	WT_STAT_FAST_ATOMIC_INCRV(session, &S2C(session)->stats, fld, value)
#define	WT_STAT_FAST_CONN_ATOMIC_INCR(session, fld)			\
	WT_STAT_FAST_ATOMIC_INCR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_DECR(session, fld)				\
	WT_STAT_FAST_DECR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_DECRV(session, fld, value)			\
	WT_STAT_FAST_DECRV(session, &S2C(session)->stats, fld, value)
#define	WT_STAT_FAST_CONN_INCR(session, fld)				\
	WT_STAT_FAST_INCR(session, &S2C(session)->stats, fld)
#define	WT_STAT_FAST_CONN_INCRV(session, fld, value)			\
	WT_STAT_FAST_INCRV(session, &S2C(session)->stats, fld, value)
#define	WT_STAT_FAST_CONN_SET(session, fld, value)			\
	WT_STAT_FAST_SET(session, &S2C(session)->stats, fld, value)

/*
 * Read/write data-source handle statistics if the data-source handle is set
 * and "fast" statistics are configured.
 *
 * XXX
 * We shouldn't have to check if the data-source handle is NULL, but it's
 * useful until everything is converted to using data-source handles.
 */
#define	WT_STAT_FAST_DATA_DECRV(session, fld, value) do {		\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_DECRV(					\
		    session, &(session)->dhandle->stats, fld, value);	\
} while (0)
#define	WT_STAT_FAST_DATA_DECR(session, fld)				\
	WT_STAT_FAST_DATA_DECRV(session, fld, 1)
#define	WT_STAT_FAST_DATA_INCRV(session, fld, value) do {		\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_INCRV(					\
		    session, &(session)->dhandle->stats, fld, value);	\
} while (0)
#define	WT_STAT_FAST_DATA_INCR(session, fld)				\
	WT_STAT_FAST_DATA_INCRV(session, fld, 1)
#define	WT_STAT_FAST_DATA_SET(session, fld, value) do {			\
	if ((session)->dhandle != NULL)					\
		WT_STAT_FAST_SET(					\
		   session, &(session)->dhandle->stats, fld, value);	\
} while (0)

/* Connection handle statistics value. */
#define	WT_CONN_STAT(session, fld)	/* XXX: REMOVE */ 	        \
	WT_STAT_WRITE(session, &S2C(session)->stats, fld)
#define	WT_CONN_STAT_GET(session, fld)					\
	WT_STAT_READ(&S2C(session)->stats, fld)
#define	WT_CONN_STAT_SET(session, fld)					\
	WT_STAT_WRITE(&S2C(session)->stats, fld)

/*
 * DO NOT EDIT: automatically built by dist/stat.py.
 */
/* Statistics section: BEGIN */

/*
 * Statistics entries for connections.
 */
#define	WT_CONNECTION_STATS_BASE	1000
struct __wt_connection_stats {
	WT_STATS async_alloc_race;
	WT_STATS async_alloc_view;
	WT_STATS async_cur_queue;
	WT_STATS async_flush;
	WT_STATS async_full;
	WT_STATS async_max_queue;
	WT_STATS async_nowork;
	WT_STATS async_op_alloc;
	WT_STATS async_op_compact;
	WT_STATS async_op_insert;
	WT_STATS async_op_remove;
	WT_STATS async_op_search;
	WT_STATS async_op_update;
	WT_STATS block_byte_map_read;
	WT_STATS block_byte_read;
	WT_STATS block_byte_write;
	WT_STATS block_map_read;
	WT_STATS block_preload;
	WT_STATS block_read;
	WT_STATS block_write;
	WT_STATS cache_bytes_dirty;
	WT_STATS cache_bytes_internal;
	WT_STATS cache_bytes_inuse;
	WT_STATS cache_bytes_leaf;
	WT_STATS cache_bytes_max;
	WT_STATS cache_bytes_overflow;
	WT_STATS cache_bytes_read;
	WT_STATS cache_bytes_write;
	WT_STATS cache_eviction_app;
	WT_STATS cache_eviction_checkpoint;
	WT_STATS cache_eviction_clean;
	WT_STATS cache_eviction_deepen;
	WT_STATS cache_eviction_dirty;
	WT_STATS cache_eviction_fail;
	WT_STATS cache_eviction_force;
	WT_STATS cache_eviction_force_delete;
	WT_STATS cache_eviction_force_fail;
	WT_STATS cache_eviction_hazard;
	WT_STATS cache_eviction_internal;
	WT_STATS cache_eviction_maximum_page_size;
	WT_STATS cache_eviction_queue_empty;
	WT_STATS cache_eviction_queue_not_empty;
	WT_STATS cache_eviction_server_evicting;
	WT_STATS cache_eviction_server_not_evicting;
	WT_STATS cache_eviction_slow;
	WT_STATS cache_eviction_split;
	WT_STATS cache_eviction_walk;
	WT_STATS cache_eviction_worker_evicting;
	WT_STATS cache_inmem_split;
	WT_STATS cache_overhead;
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
	WT_STATS dh_conn_ref;
	WT_STATS dh_conn_sweeps;
	WT_STATS dh_conn_tod;
	WT_STATS dh_session_handles;
	WT_STATS dh_session_sweeps;
	WT_STATS file_open;
	WT_STATS log_buffer_grow;
	WT_STATS log_buffer_size;
	WT_STATS log_bytes_payload;
	WT_STATS log_bytes_written;
	WT_STATS log_close_yields;
	WT_STATS log_compress_len;
	WT_STATS log_compress_mem;
	WT_STATS log_compress_small;
	WT_STATS log_compress_write_fails;
	WT_STATS log_compress_writes;
	WT_STATS log_max_filesize;
	WT_STATS log_prealloc_files;
	WT_STATS log_prealloc_max;
	WT_STATS log_prealloc_used;
	WT_STATS log_release_write_lsn;
	WT_STATS log_scan_records;
	WT_STATS log_scan_rereads;
	WT_STATS log_scans;
	WT_STATS log_slot_closes;
	WT_STATS log_slot_consolidated;
	WT_STATS log_slot_joins;
	WT_STATS log_slot_races;
	WT_STATS log_slot_switch_fails;
	WT_STATS log_slot_toobig;
	WT_STATS log_slot_toosmall;
	WT_STATS log_slot_transitions;
	WT_STATS log_sync;
	WT_STATS log_sync_dir;
	WT_STATS log_write_lsn;
	WT_STATS log_writes;
	WT_STATS lsm_checkpoint_throttle;
	WT_STATS lsm_merge_throttle;
	WT_STATS lsm_rows_merged;
	WT_STATS lsm_work_queue_app;
	WT_STATS lsm_work_queue_manager;
	WT_STATS lsm_work_queue_max;
	WT_STATS lsm_work_queue_switch;
	WT_STATS lsm_work_units_created;
	WT_STATS lsm_work_units_discarded;
	WT_STATS lsm_work_units_done;
	WT_STATS memory_allocation;
	WT_STATS memory_free;
	WT_STATS memory_grow;
	WT_STATS page_busy_blocked;
	WT_STATS page_forcible_evict_blocked;
	WT_STATS page_locked_blocked;
	WT_STATS page_read_blocked;
	WT_STATS page_sleep;
	WT_STATS read_io;
	WT_STATS rec_pages;
	WT_STATS rec_pages_eviction;
	WT_STATS rec_split_stashed_bytes;
	WT_STATS rec_split_stashed_objects;
	WT_STATS rwlock_read;
	WT_STATS rwlock_write;
	WT_STATS session_cursor_open;
	WT_STATS session_open;
	WT_STATS txn_begin;
	WT_STATS txn_checkpoint;
	WT_STATS txn_checkpoint_generation;
	WT_STATS txn_checkpoint_running;
	WT_STATS txn_checkpoint_time_max;
	WT_STATS txn_checkpoint_time_min;
	WT_STATS txn_checkpoint_time_recent;
	WT_STATS txn_checkpoint_time_total;
	WT_STATS txn_commit;
	WT_STATS txn_fail_cache;
	WT_STATS txn_pinned_checkpoint_range;
	WT_STATS txn_pinned_range;
	WT_STATS txn_rollback;
	WT_STATS txn_sync;
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
	WT_STATS btree_checkpoint_generation;
	WT_STATS btree_column_deleted;
	WT_STATS btree_column_fix;
	WT_STATS btree_column_internal;
	WT_STATS btree_column_variable;
	WT_STATS btree_compact_rewrite;
	WT_STATS btree_entries;
	WT_STATS btree_fixed_len;
	WT_STATS btree_maximum_depth;
	WT_STATS btree_maxintlkey;
	WT_STATS btree_maxintlpage;
	WT_STATS btree_maxleafkey;
	WT_STATS btree_maxleafpage;
	WT_STATS btree_maxleafvalue;
	WT_STATS btree_overflow;
	WT_STATS btree_row_internal;
	WT_STATS btree_row_leaf;
	WT_STATS cache_bytes_read;
	WT_STATS cache_bytes_write;
	WT_STATS cache_eviction_checkpoint;
	WT_STATS cache_eviction_clean;
	WT_STATS cache_eviction_deepen;
	WT_STATS cache_eviction_dirty;
	WT_STATS cache_eviction_fail;
	WT_STATS cache_eviction_hazard;
	WT_STATS cache_eviction_internal;
	WT_STATS cache_eviction_split;
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
	WT_STATS lsm_checkpoint_throttle;
	WT_STATS lsm_chunk_count;
	WT_STATS lsm_generation_max;
	WT_STATS lsm_lookup_no_bloom;
	WT_STATS lsm_merge_throttle;
	WT_STATS rec_dictionary;
	WT_STATS rec_multiblock_internal;
	WT_STATS rec_multiblock_leaf;
	WT_STATS rec_multiblock_max;
	WT_STATS rec_overflow_key_internal;
	WT_STATS rec_overflow_key_leaf;
	WT_STATS rec_overflow_value;
	WT_STATS rec_page_delete;
	WT_STATS rec_page_match;
	WT_STATS rec_pages;
	WT_STATS rec_pages_eviction;
	WT_STATS rec_prefix_compression;
	WT_STATS rec_suffix_compression;
	WT_STATS session_compact;
	WT_STATS session_cursor_open;
	WT_STATS txn_update_conflict;
};

/* Statistics section: END */
