#pragma once

/*
 * __wt_background_compact_end --
 *     Fill resulting compact statistics in the background compact tracking list for the file being
 *     compacted by the current session.
 */
extern int __wt_background_compact_end(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_background_compact_signal --
 *     Signal the compact thread. Return an error if the background compaction server has not
 *     processed a previous signal yet or because of an invalid configuration.
 */
extern int __wt_background_compact_signal(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_background_compact_start --
 *     Pre-fill compact related statistics for the file being compacted by the current session.
 */
extern int __wt_background_compact_start(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_calc_modify --
 *     Calculate a set of WT_MODIFY operations to represent an update.
 */
extern int __wt_calc_modify(WT_SESSION_IMPL *wt_session, const WT_ITEM *oldv, const WT_ITEM *newv,
  size_t maxdiff, WT_MODIFY *entries, int *nentriesp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_capacity_throttle --
 *     Reserve a time to perform a write operation for the subsystem, and wait until that time. The
 *     concept is that each write to a subsystem reserves a time slot to do its write, and
 *     atomically adjusts the reservation marker to point past the reserved slot. The size of the
 *     adjustment (i.e. the length of time represented by the slot in nanoseconds) is chosen to be
 *     proportional to the number of bytes to be written, and the proportion is a simple calculation
 *     so that we can fit reservations for exactly the configured capacity in a second. Reservation
 *     times are in nanoseconds since the epoch.
 */
extern void __wt_capacity_throttle(WT_SESSION_IMPL *session, uint64_t bytes, WT_THROTTLE_TYPE type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_signal --
 *     Signal the checkpoint thread if sufficient log has been written.
 */
extern void __wt_checkpoint_signal(WT_SESSION_IMPL *session, wt_off_t logsize)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_collator_config --
 *     Configure a custom collator.
 */
extern int __wt_collator_config(WT_SESSION_IMPL *session, const char *uri, WT_CONFIG_ITEM *cname,
  WT_CONFIG_ITEM *metadata, WT_COLLATOR **collatorp, int *ownp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_compressor_config --
 *     Given a configuration, configure the compressor.
 */
extern int __wt_compressor_config(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cval,
  WT_COMPRESSOR **compressorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_btree_apply --
 *     Apply a function to all open btree handles with the given URI.
 */
extern int __wt_conn_btree_apply(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_dhandle_alloc --
 *     Allocate a new data handle and return it linked into the connection's list.
 */
extern int __wt_conn_dhandle_alloc(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_dhandle_close --
 *     Sync and close the underlying btree handle.
 */
extern int __wt_conn_dhandle_close(WT_SESSION_IMPL *session, bool final, bool mark_dead,
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_dhandle_close_all --
 *     Close all data handles with matching name (including all checkpoint handles).
 */
extern int __wt_conn_dhandle_close_all(WT_SESSION_IMPL *session, const char *uri, bool removed,
  bool mark_dead, bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_dhandle_find --
 *     Find a previously opened data handle.
 */
extern int __wt_conn_dhandle_find(WT_SESSION_IMPL *session, const char *uri, const char *checkpoint)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_dhandle_open --
 *     Open the current data handle.
 */
extern int __wt_conn_dhandle_open(WT_SESSION_IMPL *session, const char *cfg[], uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_prefetch_clear_tree --
 *     Clear pages from the pre-fetch queue, either all pages on the queue or pages from the current
 *     btree - depending on input parameters.
 */
extern int __wt_conn_prefetch_clear_tree(WT_SESSION_IMPL *session, bool all)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_prefetch_queue_push --
 *     Push a ref onto the pre-fetch queue.
 */
extern int __wt_conn_prefetch_queue_push(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_stat_init --
 *     Initialize the per-connection statistics.
 */
extern void __wt_conn_stat_init(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dhandle_update_write_gens --
 *     Update the open dhandles write generation, run write generation and base write generation
 *     number.
 */
extern int __wt_dhandle_update_write_gens(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_encryptor_config --
 *     Given a configuration, configure the encryptor.
 */
extern int __wt_encryptor_config(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cval,
  WT_CONFIG_ITEM *keyid, WT_CONFIG_ARG *cfg_arg, WT_KEYED_ENCRYPTOR **kencryptorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_extractor_config --
 *     Given a configuration, configure the extractor.
 */
extern int __wt_extractor_config(WT_SESSION_IMPL *session, const char *uri, const char *config,
  WT_EXTRACTOR **extractorp, int *ownp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_truncate_files --
 *     Truncate log files via remove once. Requires that the server is not currently running.
 */
extern int __wt_log_truncate_files(WT_SESSION_IMPL *session, WT_CURSOR *cursor, bool force)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_wrlsn --
 *     Process written log slots and attempt to coalesce them if the LSNs are contiguous. The
 *     purpose of this function is to advance the write_lsn in LSN order after the buffer is written
 *     to the log file.
 */
extern void __wt_log_wrlsn(WT_SESSION_IMPL *session, int *yield)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_config --
 *     Set verbose configuration.
 */
extern int __wt_verbose_config(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_sessions --
 *     Print out debugging information about sessions. Skips internal sessions but does count them.
 */
extern int __wt_verbose_dump_sessions(WT_SESSION_IMPL *session, bool show_cursors)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_wiredtiger_error --
 *     Return a constant string for POSIX-standard and WiredTiger errors.
 */
extern const char *__wt_wiredtiger_error(int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
