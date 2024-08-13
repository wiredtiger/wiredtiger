#pragma once

extern int __wti_execute_handle_operation(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[], uint32_t open_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_backup_check(WT_SESSION_IMPL *session, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_colcheck(WT_SESSION_IMPL *session, const char *key_format,
  const char *value_format, WT_CONFIG_ITEM *colconf, u_int *kcolsp, u_int *vcolsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_colgroup_source(
  WT_SESSION_IMPL *session, WT_TABLE *table, const char *cgname, const char *config, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_destroy_index(WT_SESSION_IMPL *session, WT_INDEX **idxp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_get_index(WT_SESSION_IMPL *session, const char *uri, bool invalidate,
  bool quiet, WT_INDEX **indexp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_get_tiered_uri(WT_SESSION_IMPL *session, const char *uri, uint32_t flags,
  WT_TIERED **tieredp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_index_source(WT_SESSION_IMPL *session, WT_TABLE *table, const char *idxname,
  const char *config, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_internal_session(WT_SESSION_IMPL *session, WT_SESSION_IMPL **int_sessionp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_open_colgroups(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_release_table_gen(WT_SESSION_IMPL *session, WT_TABLE **tablep,
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_release_tiered(WT_SESSION_IMPL *session, WT_TIERED **tieredp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_schema_session_release(WT_SESSION_IMPL *session, WT_SESSION_IMPL *int_session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_struct_truncate(WT_SESSION_IMPL *session, const char *input_fmt, u_int ncols,
  WT_ITEM *format) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_table_check(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_schema_destroy_colgroup(WT_SESSION_IMPL *session, WT_COLGROUP **colgroupp);

#ifdef HAVE_UNITTEST

#endif
