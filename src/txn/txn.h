#pragma once

extern bool __wt_txn_active(WT_SESSION_IMPL *session, uint64_t txnid)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint_close(WT_SESSION_IMPL *session, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint_get_handles(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint_sync(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_activity_drain(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_checkpoint(WT_SESSION_IMPL *session, const char *cfg[], bool waiting)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_checkpoint_log(WT_SESSION_IMPL *session, bool full, uint32_t flags,
  WT_LSN *lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_commit(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_config(WT_SESSION_IMPL *session, WT_CONF *conf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_global_init(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_global_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_global_shutdown(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_init(WT_SESSION_IMPL *session, WT_SESSION_IMPL *session_ret)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_init_checkpoint_cursor(WT_SESSION_IMPL *session, WT_CKPT_SNAPSHOT *snapinfo,
  WT_TXN **txn_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_is_blocking(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_log_op(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_parse_timestamp(WT_SESSION_IMPL *session, const char *name,
  wt_timestamp_t *timestamp, WT_CONFIG_ITEM *cval) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_parse_timestamp_raw(WT_SESSION_IMPL *session, const char *name,
  wt_timestamp_t *timestamp, WT_CONFIG_ITEM *cval) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_prepare(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_printlog(WT_SESSION *wt_session, const char *ofile, uint32_t flags,
  WT_LSN *start_lsn, WT_LSN *end_lsn) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_query_timestamp(WT_SESSION_IMPL *session, char *hex_timestamp,
  const char *cfg[], bool global_txn) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_reconfigure(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_recover(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_rollback(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_rollback_required(WT_SESSION_IMPL *session, const char *reason)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[], bool commit)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_set_timestamp_uint(WT_SESSION_IMPL *session, WT_TS_TXN_TYPE which,
  wt_timestamp_t ts) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_snapshot_save_and_refresh(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_truncate_log(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_txn_update_oldest(WT_SESSION_IMPL *session, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_txn(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_txn_one(WT_SESSION_IMPL *session, WT_SESSION_IMPL *txn_session,
  int error_code, const char *error_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_checkpoint_progress(WT_SESSION_IMPL *session, bool closing);
extern void __wt_checkpoint_tree_reconcile_update(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta);
extern void __wt_txn_bump_snapshot(WT_SESSION_IMPL *session);
extern void __wt_txn_close_checkpoint_cursor(WT_SESSION_IMPL *session, WT_TXN **txn_arg);
extern void __wt_txn_destroy(WT_SESSION_IMPL *session);
extern void __wt_txn_get_snapshot(WT_SESSION_IMPL *session);
extern void __wt_txn_global_destroy(WT_SESSION_IMPL *session);
extern void __wt_txn_op_free(WT_SESSION_IMPL *session, WT_TXN_OP *op);
extern void __wt_txn_release_resources(WT_SESSION_IMPL *session);
extern void __wt_txn_release_snapshot(WT_SESSION_IMPL *session);
extern void __wt_txn_snapshot_release_and_restore(WT_SESSION_IMPL *session);
extern void __wt_txn_stats_update(WT_SESSION_IMPL *session);
extern void __wt_txn_truncate_end(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST
extern int WT_CDECL __ut_txn_mod_compare(const void *a, const void *b)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif
