#pragma once

/*
 * __wt_checkpoint --
 *     Checkpoint a file.
 */
extern int __wt_checkpoint(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_close --
 *     Checkpoint a single file as part of closing the handle.
 */
extern int __wt_checkpoint_close(WT_SESSION_IMPL *session, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_get_handles --
 *     Get a list of handles to flush.
 */
extern int __wt_checkpoint_get_handles(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_progress --
 *     Output a checkpoint progress message.
 */
extern void __wt_checkpoint_progress(WT_SESSION_IMPL *session, bool closing)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_sync --
 *     Sync a file that has been checkpointed, and wait for the result.
 */
extern int __wt_checkpoint_sync(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_tree_reconcile_update --
 *     Update a checkpoint based on reconciliation results.
 */
extern void __wt_checkpoint_tree_reconcile_update(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_active --
 *     Check if a transaction is still active. If not, it is either committed, prepared, or rolled
 *     back. It is possible that we race with commit, prepare or rollback and a transaction is still
 *     active before the start of the call is eventually reported as resolved.
 */
extern bool __wt_txn_active(WT_SESSION_IMPL *session, uint64_t txnid)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_activity_drain --
 *     Wait for transactions to quiesce.
 */
extern int __wt_txn_activity_drain(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_bump_snapshot --
 *     Uncommon case, allocate a snapshot but skip updating our shared ids.
 */
extern void __wt_txn_bump_snapshot(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_checkpoint --
 *     Checkpoint a database or a list of objects in the database.
 */
extern int __wt_txn_checkpoint(WT_SESSION_IMPL *session, const char *cfg[], bool waiting)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_checkpoint_log --
 *     Write a log record for a checkpoint operation.
 */
extern int __wt_txn_checkpoint_log(WT_SESSION_IMPL *session, bool full, uint32_t flags,
  WT_LSN *lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_close_checkpoint_cursor --
 *     Dispose of the private transaction object in a checkpoint cursor.
 */
extern void __wt_txn_close_checkpoint_cursor(WT_SESSION_IMPL *session, WT_TXN **txn_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_commit --
 *     Commit the current transaction.
 */
extern int __wt_txn_commit(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_config --
 *     Configure a transaction.
 */
extern int __wt_txn_config(WT_SESSION_IMPL *session, WT_CONF *conf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_destroy --
 *     Destroy a session's transaction data.
 */
extern void __wt_txn_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_get_snapshot --
 *     Common case, allocate a snapshot and update our shared ids.
 */
extern void __wt_txn_get_snapshot(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_global_destroy --
 *     Destroy the global transaction state.
 */
extern void __wt_txn_global_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_global_init --
 *     Initialize the global transaction state.
 */
extern int __wt_txn_global_init(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_global_set_timestamp --
 *     Set a global transaction timestamp.
 */
extern int __wt_txn_global_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_global_shutdown --
 *     Shut down the global transaction state.
 */
extern int __wt_txn_global_shutdown(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_init --
 *     Initialize a session's transaction data.
 */
extern int __wt_txn_init(WT_SESSION_IMPL *session, WT_SESSION_IMPL *session_ret)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_init_checkpoint_cursor --
 *     Create a transaction object for a checkpoint cursor. On success, takes charge of the snapshot
 *     array passed down, which should have been allocated separately, and nulls the pointer. (On
 *     failure, the caller must destroy it.)
 */
extern int __wt_txn_init_checkpoint_cursor(WT_SESSION_IMPL *session, WT_CKPT_SNAPSHOT *snapinfo,
  WT_TXN **txn_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_is_blocking --
 *     Return an error if this transaction is likely blocking eviction because of a pinned
 *     transaction ID, called by eviction to determine if a worker thread should be released from
 *     eviction.
 */
extern int __wt_txn_is_blocking(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_log_op --
 *     Write the last logged operation into the in-memory buffer.
 */
extern int __wt_txn_log_op(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_free --
 *     Free memory associated with a transactional operation.
 */
extern void __wt_txn_op_free(WT_SESSION_IMPL *session, WT_TXN_OP *op)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_parse_timestamp --
 *     Decodes and sets a timestamp checking it is non-zero.
 */
extern int __wt_txn_parse_timestamp(WT_SESSION_IMPL *session, const char *name,
  wt_timestamp_t *timestamp, WT_CONFIG_ITEM *cval) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_parse_timestamp_raw --
 *     Decodes and sets a timestamp. Don't do any checking.
 */
extern int __wt_txn_parse_timestamp_raw(WT_SESSION_IMPL *session, const char *name,
  wt_timestamp_t *timestamp, WT_CONFIG_ITEM *cval) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_prepare --
 *     Prepare the current transaction.
 */
extern int __wt_txn_prepare(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_printlog --
 *     Print the log in a human-readable format.
 */
extern int __wt_txn_printlog(WT_SESSION *wt_session, const char *ofile, uint32_t flags,
  WT_LSN *start_lsn, WT_LSN *end_lsn) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_query_timestamp --
 *     Query a timestamp. The caller may query the global transaction or the session's transaction.
 */
extern int __wt_txn_query_timestamp(WT_SESSION_IMPL *session, char *hex_timestamp,
  const char *cfg[], bool global_txn) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_reconfigure --
 *     WT_SESSION::reconfigure for transactions.
 */
extern int __wt_txn_reconfigure(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_recover --
 *     Run recovery.
 */
extern int __wt_txn_recover(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_release_resources --
 *     Release resources for a session's transaction data.
 */
extern void __wt_txn_release_resources(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_release_snapshot --
 *     Release the snapshot in the current transaction.
 */
extern void __wt_txn_release_snapshot(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_rollback --
 *     Roll back the current transaction.
 */
extern int __wt_txn_rollback(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_rollback_required --
 *     Prepare to log a reason if the user attempts to use the transaction to do anything other than
 *     rollback.
 */
extern int __wt_txn_rollback_required(WT_SESSION_IMPL *session, const char *reason)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_set_timestamp --
 *     Parse a request to set a timestamp in a transaction.
 */
extern int __wt_txn_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[], bool commit)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_set_timestamp_uint --
 *     Directly set the commit timestamp in a transaction, bypassing parsing logic. Prefer this to
 *     __wt_txn_set_timestamp when string parsing is a performance bottleneck.
 */
extern int __wt_txn_set_timestamp_uint(WT_SESSION_IMPL *session, WT_TS_TXN_TYPE which,
  wt_timestamp_t ts) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_snapshot_release_and_restore --
 *     Switch back to the original snapshot.
 */
extern void __wt_txn_snapshot_release_and_restore(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_snapshot_save_and_refresh --
 *     Save the existing snapshot and allocate a new snapshot.
 */
extern int __wt_txn_snapshot_save_and_refresh(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_stats_update --
 *     Update the transaction statistics for return to the application.
 */
extern void __wt_txn_stats_update(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_truncate_end --
 *     Finish truncating a range of a file.
 */
extern void __wt_txn_truncate_end(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_truncate_log --
 *     Begin truncating a range of a file.
 */
extern int __wt_txn_truncate_log(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_update_oldest --
 *     Sweep the running transactions to update the oldest ID required.
 */
extern int __wt_txn_update_oldest(WT_SESSION_IMPL *session, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_txn --
 *     Output diagnostic information about the global transaction state.
 */
extern int __wt_verbose_dump_txn(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_txn_one --
 *     Output diagnostic information about a transaction structure.
 */
extern int __wt_verbose_dump_txn_one(WT_SESSION_IMPL *session, WT_SESSION_IMPL *txn_session,
  int error_code, const char *error_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST
extern int WT_CDECL __ut_txn_mod_compare(const void *a, const void *b)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif
