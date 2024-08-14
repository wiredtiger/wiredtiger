#pragma once

extern int __wt_clsm_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_lsm_init(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR_STAT *cst)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_compact(WT_SESSION_IMPL *session, const char *name, bool *skipp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_manager_config(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_manager_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_manager_reconfig(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_create(WT_SESSION_IMPL *session, const char *uri, bool exclusive,
  const char *config) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_drop(WT_SESSION_IMPL *session, const char *name, const char *cfg[],
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_get(WT_SESSION_IMPL *session, const char *uri, bool exclusive,
  WT_LSM_TREE **treep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_rename(WT_SESSION_IMPL *session, const char *olduri, const char *newuri,
  const char *cfg[], bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_truncate(WT_SESSION_IMPL *session, const char *name, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_lsm_tree_worker(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[], uint32_t open_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_lsm_tree_release(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);

#ifdef HAVE_UNITTEST

#endif
