#pragma once

extern int __wt_tiered_close(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_conn_config(WT_SESSION_IMPL *session, const char **cfg, bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_discard(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_name(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, uint32_t id,
  uint32_t flags, const char **retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_open(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_put_flush_finish(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_put_remove_local(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_set_metadata(WT_SESSION_IMPL *session, WT_TIERED *tiered, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_switch(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_tree_close(WT_SESSION_IMPL *session, WT_TIERED_TREE *tiered_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tiered_tree_open(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_tiered_flush_work_wait(WT_SESSION_IMPL *session, uint32_t timeout);
extern void __wt_tiered_get_flush(
  WT_SESSION_IMPL *session, uint64_t generation, WT_TIERED_WORK_UNIT **entryp);
extern void __wt_tiered_get_flush_finish(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT **entryp);
extern void __wt_tiered_get_remove_local(
  WT_SESSION_IMPL *session, uint64_t now, WT_TIERED_WORK_UNIT **entryp);
extern void __wt_tiered_remove_work(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool locked);
extern void __wt_tiered_requeue_work(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT *entry);
extern void __wt_tiered_work_free(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT *entry);

#ifdef HAVE_UNITTEST

#endif
