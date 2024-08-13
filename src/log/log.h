#pragma once

extern int __wt_log_allocfile(WT_SESSION_IMPL *session, uint32_t lognum, const char *dest)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_compat_verify(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_extract_lognum(WT_SESSION_IMPL *session, const char *name, uint32_t *id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_filename(WT_SESSION_IMPL *session, uint32_t id, const char *file_prefix,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_flush(WT_SESSION_IMPL *session, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_flush_lsn(WT_SESSION_IMPL *session, WT_LSN *lsn, bool start)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_force_sync(WT_SESSION_IMPL *session, WT_LSN *min_lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_force_write(WT_SESSION_IMPL *session, bool retry, bool *did_work)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_get_backup_files(WT_SESSION_IMPL *session, char ***filesp, u_int *countp,
  uint32_t *maxid, bool active_only) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_needs_recovery(WT_SESSION_IMPL *session, WT_LSN *ckp_lsn, bool *recp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_open(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_printf(WT_SESSION_IMPL *session, const char *format, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_remove(WT_SESSION_IMPL *session, const char *file_prefix, uint32_t lognum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_reset(WT_SESSION_IMPL *session, uint32_t lognum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_scan(WT_SESSION_IMPL *session, WT_LSN *start_lsnp, WT_LSN *end_lsnp,
  uint32_t flags,
  int (*func)(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp, WT_LSN *next_lsnp,
    void *cookie, int firstrecord),
  void *cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_set_version(WT_SESSION_IMPL *session, uint16_t version, uint32_t first_rec,
  bool downgrade, bool live_chg, uint32_t *lognump)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_slot_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_slot_init(WT_SESSION_IMPL *session, bool alloc)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_system_backup_id(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_vprintf(WT_SESSION_IMPL *session, const char *fmt, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_write(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_backup_id_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t index,
  uint64_t granularity, const char *id) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_backup_id_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_backup_id_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *indexp, uint64_t *granularityp, const char **idp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_checkpoint_start_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_checkpoint_start_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_checkpoint_start_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_modify_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  uint64_t recno, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_modify_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_modify_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_put_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  uint64_t recno, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_put_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_put_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_remove_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  uint64_t recno) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_remove_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_remove_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *recnop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_truncate_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  uint64_t start, uint64_t stop) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_truncate_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_col_truncate_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, uint64_t *startp, uint64_t *stopp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_prev_lsn_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, WT_LSN *prev_lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_prev_lsn_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_prev_lsn_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_LSN *prev_lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_read(WT_SESSION_IMPL *session, const uint8_t **pp_peek, const uint8_t *end,
  uint32_t *optypep, uint32_t *opsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_modify_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  WT_ITEM *key, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_modify_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_modify_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_put_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  WT_ITEM *key, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_put_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_put_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp, WT_ITEM *valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_remove_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  WT_ITEM *key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_remove_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_remove_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *keyp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_truncate_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec, uint32_t fileid,
  WT_ITEM *start, WT_ITEM *stop, uint32_t mode) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_truncate_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_row_truncate_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint32_t *fileidp, WT_ITEM *startp, WT_ITEM *stopp, uint32_t *modep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_txn_timestamp_pack(WT_SESSION_IMPL *session, WT_ITEM *logrec,
  uint64_t time_sec, uint64_t time_nsec, uint64_t commit_ts, uint64_t durable_ts,
  uint64_t first_commit_ts, uint64_t prepare_ts, uint64_t read_ts)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_txn_timestamp_print(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_txn_timestamp_unpack(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, uint64_t *time_secp, uint64_t *time_nsecp, uint64_t *commit_tsp,
  uint64_t *durable_tsp, uint64_t *first_commit_tsp, uint64_t *prepare_tsp, uint64_t *read_tsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_unpack(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  uint32_t *optypep, uint32_t *opsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logop_write(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end, uint32_t optype,
  uint32_t opsize) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logrec_alloc(WT_SESSION_IMPL *session, size_t size, WT_ITEM **logrecp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_logrec_read(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  uint32_t *rectypep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_op_printlog(WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
  WT_TXN_PRINTLOG_ARGS *args) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_log(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_log_ckpt(WT_SESSION_IMPL *session, WT_LSN *ckpt_lsn);
extern void __wt_log_slot_free(WT_SESSION_IMPL *session, WT_LOGSLOT *slot);
extern void __wt_log_written_reset(WT_SESSION_IMPL *session);
extern void __wt_logrec_free(WT_SESSION_IMPL *session, WT_ITEM **logrecp);

#ifdef HAVE_UNITTEST

#endif
