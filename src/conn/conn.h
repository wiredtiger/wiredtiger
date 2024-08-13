#pragma once

extern const char *__wt_wiredtiger_error(int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_background_compact_end(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_background_compact_signal(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_background_compact_start(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_calc_modify(WT_SESSION_IMPL *wt_session, const WT_ITEM *oldv, const WT_ITEM *newv,
  size_t maxdiff, WT_MODIFY *entries, int *nentriesp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_collator_config(WT_SESSION_IMPL *session, const char *uri, WT_CONFIG_ITEM *cname,
  WT_CONFIG_ITEM *metadata, WT_COLLATOR **collatorp, int *ownp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_compressor_config(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cval,
  WT_COMPRESSOR **compressorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_btree_apply(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_dhandle_alloc(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_dhandle_close(WT_SESSION_IMPL *session, bool final, bool mark_dead,
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_dhandle_close_all(WT_SESSION_IMPL *session, const char *uri, bool removed,
  bool mark_dead, bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_dhandle_find(WT_SESSION_IMPL *session, const char *uri, const char *checkpoint)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_dhandle_open(WT_SESSION_IMPL *session, const char *cfg[], uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_prefetch_clear_tree(WT_SESSION_IMPL *session, bool all)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_prefetch_queue_push(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_dhandle_update_write_gens(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_encryptor_config(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cval,
  WT_CONFIG_ITEM *keyid, WT_CONFIG_ARG *cfg_arg, WT_KEYED_ENCRYPTOR **kencryptorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_extractor_config(WT_SESSION_IMPL *session, const char *uri, const char *config,
  WT_EXTRACTOR **extractorp, int *ownp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_log_truncate_files(WT_SESSION_IMPL *session, WT_CURSOR *cursor, bool force)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_config(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_sessions(WT_SESSION_IMPL *session, bool show_cursors)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_capacity_throttle(WT_SESSION_IMPL *session, uint64_t bytes, WT_THROTTLE_TYPE type);
extern void __wt_checkpoint_signal(WT_SESSION_IMPL *session, wt_off_t logsize);
extern void __wt_conn_stat_init(WT_SESSION_IMPL *session);
extern void __wt_log_wrlsn(WT_SESSION_IMPL *session, int *yield);

#ifdef HAVE_UNITTEST

#endif
