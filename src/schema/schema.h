#pragma once

extern WT_DATA_SOURCE *__wt_schema_get_source(WT_SESSION_IMPL *session, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_colgroup_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_index_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_table_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_direct_io_size_check(WT_SESSION_IMPL *session, const char **cfg,
  const char *config_name, uint32_t *allocsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_find_import_metadata(WT_SESSION_IMPL *session, const char *uri, const char **config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_name_check(WT_SESSION_IMPL *session, const char *str, size_t len, bool check_uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_range_truncate(WT_CURSOR *start, WT_CURSOR *stop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_alter(WT_SESSION_IMPL *session, const char *uri, const char *newcfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_close_table(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_create(WT_SESSION_IMPL *session, const char *uri, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_drop(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_get_colgroup(WT_SESSION_IMPL *session, const char *uri, bool quiet,
  WT_TABLE **tablep, WT_COLGROUP **colgroupp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_get_table(WT_SESSION_IMPL *session, const char *name, size_t namelen,
  bool ok_incomplete, uint32_t flags, WT_TABLE **tablep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_get_table_uri(WT_SESSION_IMPL *session, const char *uri, bool ok_incomplete,
  uint32_t flags, WT_TABLE **tablep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_open_index(WT_SESSION_IMPL *session, WT_TABLE *table, const char *idxname,
  size_t len, WT_INDEX **indexp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_open_indices(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_open_table(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_project_in(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_project_merge(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  const char *vformat, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_project_out(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_project_slice(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  bool key_only, const char *vformat, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_release_table(WT_SESSION_IMPL *session, WT_TABLE **tablep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_rename(WT_SESSION_IMPL *session, const char *uri, const char *newuri,
  const char *cfg[], bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_tiered_shared_colgroup_name(WT_SESSION_IMPL *session, const char *tablename,
  bool active, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_truncate(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_schema_worker(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[], uint32_t open_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_str_name_check(WT_SESSION_IMPL *session, const char *str)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_plan(WT_SESSION_IMPL *session, WT_TABLE *table, const char *columns,
  size_t len, bool value_only, WT_ITEM *plan) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_reformat(WT_SESSION_IMPL *session, WT_TABLE *table, const char *columns,
  size_t len, const char *extra_cols, bool value_only, WT_ITEM *format)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
