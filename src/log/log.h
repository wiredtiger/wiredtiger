#pragma once

/*
 * __wt_log_allocfile --
 *     Given a log number, create a new log file by writing the header, pre-allocating the file and
 *     moving it to the destination name.
 */
extern int __wt_log_allocfile(WT_SESSION_IMPL *session, uint32_t lognum, const char *dest)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_ckpt --
 *     Record the given LSN as the checkpoint LSN and signal the removal thread as needed.
 */
extern void __wt_log_ckpt(WT_SESSION_IMPL *session, WT_LSN *ckpt_lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_close --
 *     Close the log file.
 */
extern int __wt_log_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_compat_verify --
 *     Verify the last log when opening for the compatibility settings. This is separate because we
 *     need to do it very early in the startup process.
 */
extern int __wt_log_compat_verify(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_extract_lognum --
 *     Given a log file name, extract out the log number.
 */
extern int __wt_log_extract_lognum(WT_SESSION_IMPL *session, const char *name, uint32_t *id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_filename --
 *     Given a log number, return a WT_ITEM of a generated log file name of the given prefix type.
 */
extern int __wt_log_filename(WT_SESSION_IMPL *session, uint32_t id, const char *file_prefix,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_flush --
 *     Forcibly flush the log to the synchronization level specified. Wait until it has been
 *     completed.
 */
extern int __wt_log_flush(WT_SESSION_IMPL *session, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_flush_lsn --
 *     Force out buffered records and return the LSN, either the write_start_lsn or write_lsn
 *     depending on the argument.
 */
extern int __wt_log_flush_lsn(WT_SESSION_IMPL *session, WT_LSN *lsn, bool start)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_force_sync --
 *     Force a sync of the log and files.
 */
extern int __wt_log_force_sync(WT_SESSION_IMPL *session, WT_LSN *min_lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_force_write --
 *     Force a switch and release and write of the current slot. Wrapper function that takes the
 *     lock.
 */
extern int __wt_log_force_write(WT_SESSION_IMPL *session, bool retry, bool *did_work)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_get_backup_files --
 *     Retrieve the list of log files for taking a backup, either all of them or only the active
 *     ones (those that are not candidates for removal). The caller is responsible for freeing the
 *     directory list returned.
 */
extern int __wt_log_get_backup_files(WT_SESSION_IMPL *session, char ***filesp, u_int *countp,
  uint32_t *maxid, bool active_only) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_needs_recovery --
 *     Return 0 if we encounter a clean shutdown and 1 if recovery must be run in the given
 *     variable.
 */
extern int __wt_log_needs_recovery(WT_SESSION_IMPL *session, WT_LSN *ckp_lsn, bool *recp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_open --
 *     Open the appropriate log file for the connection. The purpose is to find the last log file
 *     that exists, open it and set our initial LSNs to the end of that file. If none exist, call
 *     __log_newfile to create it.
 */
extern int __wt_log_open(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_printf --
 *     Write a text message to the log.
 */
extern int __wt_log_printf(WT_SESSION_IMPL *session, const char *format, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_remove --
 *     Given a log number, remove that log file.
 */
extern int __wt_log_remove(WT_SESSION_IMPL *session, const char *file_prefix, uint32_t lognum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_reset --
 *     Reset the existing log file to after the given file number. Called from recovery when
 *     toggling logging back on, it was off the previous open but it was on earlier before that
 *     toggle.
 */
extern int __wt_log_reset(WT_SESSION_IMPL *session, uint32_t lognum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_scan --
 *     Scan the logs, calling a function on each record found.
 */
extern int __wt_log_scan(WT_SESSION_IMPL *session, WT_LSN *start_lsnp, WT_LSN *end_lsnp,
  uint32_t flags,
  int (*func)(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp, WT_LSN *next_lsnp,
    void *cookie, int firstrecord),
  void *cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_set_version --
 *     Change the version number in logging. Will be done with locking. We need to force the log
 *     file to advance and remove all old pre-allocated files.
 */
extern int __wt_log_set_version(WT_SESSION_IMPL *session, uint16_t version, uint32_t first_rec,
  bool downgrade, bool live_chg, uint32_t *lognump)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_slot_destroy --
 *     Clean up the slot array on shutdown.
 */
extern int __wt_log_slot_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_slot_free --
 *     Free a slot back into the pool.
 */
extern void __wt_log_slot_free(WT_SESSION_IMPL *session, WT_LOGSLOT *slot)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_slot_init --
 *     Initialize the slot array.
 */
extern int __wt_log_slot_init(WT_SESSION_IMPL *session, bool alloc)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_system_backup_id --
 *     Write a system log record for the incremental backup IDs.
 */
extern int __wt_log_system_backup_id(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_vprintf --
 *     Write a message into the log.
 */
extern int __wt_log_vprintf(WT_SESSION_IMPL *session, const char *fmt, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_write --
 *     Write a record into the log, compressing as necessary.
 */
extern int __wt_log_write(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_written_reset --
 *     Interface to reset the amount of log written during this checkpoint period. Called from the
 *     checkpoint code.
 */
extern void __wt_log_written_reset(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_backup_id_print --
 *     Print the log operation backup_id.
 */
extern int __wt_logop_backup_id_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_backup_id_unpack --
 *     Unpack the log operation backup_id.
 */
extern int __wt_logop_backup_id_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *indexp, uint64_t *granularityp, const char **idp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_checkpoint_start_print --
 *     Print the log operation checkpoint_start.
 */
extern int __wt_logop_checkpoint_start_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_checkpoint_start_unpack --
 *     Unpack the log operation checkpoint_start.
 */
extern int __wt_logop_checkpoint_start_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_modify_print --
 *     Print the log operation col_modify.
 */
extern int __wt_logop_col_modify_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_modify_unpack --
 *     Unpack the log operation col_modify.
 */
extern int __wt_logop_col_modify_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_put_print --
 *     Print the log operation col_put.
 */
extern int __wt_logop_col_put_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_put_unpack --
 *     Unpack the log operation col_put.
 */
extern int __wt_logop_col_put_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_remove_print --
 *     Print the log operation col_remove.
 */
extern int __wt_logop_col_remove_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_remove_unpack --
 *     Unpack the log operation col_remove.
 */
extern int __wt_logop_col_remove_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_truncate_print --
 *     Print the log operation col_truncate.
 */
extern int __wt_logop_col_truncate_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_col_truncate_unpack --
 *     Unpack the log operation col_truncate.
 */
extern int __wt_logop_col_truncate_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *startp, uint64_t *stopp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_prev_lsn_print --
 *     Print the log operation prev_lsn.
 */
extern int __wt_logop_prev_lsn_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_prev_lsn_unpack --
 *     Unpack the log operation prev_lsn.
 */
extern int __wt_logop_prev_lsn_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_LSN *prev_lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_read --
 *     Peek at the operation type.
 */
extern int __wt_logop_read(WT_SESSION_IMPL *session, const uint8_t **pp_peek, const uint8_t *end,
  uint32_t *optypep, uint32_t *opsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_modify_print --
 *     Print the log operation row_modify.
 */
extern int __wt_logop_row_modify_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_modify_unpack --
 *     Unpack the log operation row_modify.
 */
extern int __wt_logop_row_modify_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_put_print --
 *     Print the log operation row_put.
 */
extern int __wt_logop_row_put_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_put_unpack --
 *     Unpack the log operation row_put.
 */
extern int __wt_logop_row_put_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_remove_print --
 *     Print the log operation row_remove.
 */
extern int __wt_logop_row_remove_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_remove_unpack --
 *     Unpack the log operation row_remove.
 */
extern int __wt_logop_row_remove_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_truncate_print --
 *     Print the log operation row_truncate.
 */
extern int __wt_logop_row_truncate_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_row_truncate_unpack --
 *     Unpack the log operation row_truncate.
 */
extern int __wt_logop_row_truncate_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *startp, WT_ITEM *stopp, uint32_t *modep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_txn_timestamp_print --
 *     Print the log operation txn_timestamp.
 */
extern int __wt_logop_txn_timestamp_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_txn_timestamp_unpack --
 *     Unpack the log operation txn_timestamp.
 */
extern int __wt_logop_txn_timestamp_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint64_t *time_secp, uint64_t *time_nsecp, uint64_t *commit_tsp,
  uint64_t *durable_tsp, uint64_t *first_commit_tsp, uint64_t *prepare_tsp, uint64_t *read_tsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_unpack --
 *     Read the operation type.
 */
extern int __wt_logop_unpack(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  uint32_t *optypep, uint32_t *opsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logop_write --
 *     Write the operation type.
 */
extern int __wt_logop_write(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end, uint32_t optype,
  uint32_t opsize) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logrec_alloc --
 *     Allocate a new WT_ITEM structure.
 */
extern int __wt_logrec_alloc(WT_SESSION_IMPL *session, size_t size, WT_ITEM **logrecp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logrec_free --
 *     Free the given WT_ITEM structure.
 */
extern void __wt_logrec_free(WT_SESSION_IMPL *session, WT_ITEM **logrecp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_logrec_read --
 *     Read the record type.
 */
extern int __wt_logrec_read(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  uint32_t *rectypep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_printlog --
 *     Print operation from a log cookie.
 */
extern int __wt_txn_op_printlog(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_log --
 *     Dump information about the logging subsystem.
 */
extern int __wt_verbose_dump_log(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
