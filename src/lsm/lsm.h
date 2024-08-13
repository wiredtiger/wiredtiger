#pragma once

/*
 * __wt_clsm_open --
 *     WT_SESSION->open_cursor method for LSM cursors.
 */
extern int __wt_clsm_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_lsm_init --
 *     Initialize the statistics for a LSM tree.
 */
extern int __wt_curstat_lsm_init(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR_STAT *cst)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_compact --
 *     Compact an LSM tree called via __wt_schema_worker.
 */
extern int __wt_lsm_compact(WT_SESSION_IMPL *session, const char *name, bool *skipp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_manager_config --
 *     Configure the LSM manager.
 */
extern int __wt_lsm_manager_config(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_manager_destroy --
 *     Destroy the LSM manager threads and subsystem.
 */
extern int __wt_lsm_manager_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_manager_reconfig --
 *     Re-configure the LSM manager.
 */
extern int __wt_lsm_manager_reconfig(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_create --
 *     Create an LSM tree structure for the given name.
 */
extern int __wt_lsm_tree_create(WT_SESSION_IMPL *session, const char *uri, bool exclusive,
  const char *config) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_drop --
 *     Drop an LSM tree.
 */
extern int __wt_lsm_tree_drop(WT_SESSION_IMPL *session, const char *name, const char *cfg[],
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_get --
 *     Find an LSM tree handle or open a new one.
 */
extern int __wt_lsm_tree_get(WT_SESSION_IMPL *session, const char *uri, bool exclusive,
  WT_LSM_TREE **treep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_release --
 *     Release an LSM tree structure.
 */
extern void __wt_lsm_tree_release(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_rename --
 *     Rename an LSM tree.
 */
extern int __wt_lsm_tree_rename(WT_SESSION_IMPL *session, const char *olduri, const char *newuri,
  const char *cfg[], bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_truncate --
 *     Truncate an LSM tree.
 */
extern int __wt_lsm_tree_truncate(WT_SESSION_IMPL *session, const char *name, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsm_tree_worker --
 *     Run a schema worker operation on each level of a LSM tree.
 */
extern int __wt_lsm_tree_worker(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[], uint32_t open_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
