#pragma once

/*
 * __wt_curstat_colgroup_init --
 *     Initialize the statistics for a column group.
 */
extern int __wt_curstat_colgroup_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_index_init --
 *     Initialize the statistics for an index.
 */
extern int __wt_curstat_index_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_table_init --
 *     Initialize the statistics for a table.
 */
extern int __wt_curstat_table_init(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_direct_io_size_check --
 *     Return a size from the configuration, complaining if it's insufficient for direct I/O.
 */
extern int __wt_direct_io_size_check(WT_SESSION_IMPL *session, const char **cfg,
  const char *config_name, uint32_t *allocsizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_find_import_metadata --
 *     Find metadata entry by URI in session's import list. The list must already be sorted by uri.
 */
extern int __wt_find_import_metadata(WT_SESSION_IMPL *session, const char *uri, const char **config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_name_check --
 *     Disallow any use of the WiredTiger name space.
 */
extern int __wt_name_check(WT_SESSION_IMPL *session, const char *str, size_t len, bool check_uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_range_truncate --
 *     Truncate of a cursor range, default implementation. This truncate takes explicit cursors
 *     rather than a truncate information structure since it is used to implement truncate for
 *     column groups within a complex table, and those use different cursors than the API level
 *     truncate tracks.
 */
extern int __wt_range_truncate(WT_CURSOR *start, WT_CURSOR *stop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_alter --
 *     Alter an object.
 */
extern int __wt_schema_alter(WT_SESSION_IMPL *session, const char *uri, const char *newcfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_close_table --
 *     Close a table handle.
 */
extern int __wt_schema_close_table(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_create --
 *     Process a WT_SESSION::create operation for all supported types.
 */
extern int __wt_schema_create(WT_SESSION_IMPL *session, const char *uri, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_drop --
 *     Process a WT_SESSION::drop operation for all supported types.
 */
extern int __wt_schema_drop(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_get_colgroup --
 *     Find a column group by URI.
 */
extern int __wt_schema_get_colgroup(WT_SESSION_IMPL *session, const char *uri, bool quiet,
  WT_TABLE **tablep, WT_COLGROUP **colgroupp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_get_source --
 *     Find a matching data source or report an error.
 */
extern WT_DATA_SOURCE *__wt_schema_get_source(WT_SESSION_IMPL *session, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_get_table --
 *     Get the table handle for the named table.
 */
extern int __wt_schema_get_table(WT_SESSION_IMPL *session, const char *name, size_t namelen,
  bool ok_incomplete, uint32_t flags, WT_TABLE **tablep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_get_table_uri --
 *     Get the table handle for the named table.
 */
extern int __wt_schema_get_table_uri(WT_SESSION_IMPL *session, const char *uri, bool ok_incomplete,
  uint32_t flags, WT_TABLE **tablep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_open_index --
 *     Open one or more indices for a table.
 */
extern int __wt_schema_open_index(WT_SESSION_IMPL *session, WT_TABLE *table, const char *idxname,
  size_t len, WT_INDEX **indexp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_open_indices --
 *     Open the indices for a table.
 */
extern int __wt_schema_open_indices(WT_SESSION_IMPL *session, WT_TABLE *table)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_open_table --
 *     Open a named table.
 */
extern int __wt_schema_open_table(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_project_in --
 *     Given list of cursors and a projection, read columns from the application into the dependent
 *     cursors.
 */
extern int __wt_schema_project_in(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_project_merge --
 *     Given list of cursors and a projection, build a buffer containing the column values read from
 *     the cursors.
 */
extern int __wt_schema_project_merge(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  const char *vformat, WT_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_project_out --
 *     Given list of cursors and a projection, read columns from the dependent cursors and return
 *     them to the application.
 */
extern int __wt_schema_project_out(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_project_slice --
 *     Given list of cursors and a projection, read columns from a raw buffer.
 */
extern int __wt_schema_project_slice(WT_SESSION_IMPL *session, WT_CURSOR **cp, const char *proj_arg,
  bool key_only, const char *vformat, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_range_truncate --
 *     WT_SESSION::truncate with a range.
 */
extern int __wt_schema_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_release_table --
 *     Release a table handle.
 */
extern int __wt_schema_release_table(WT_SESSION_IMPL *session, WT_TABLE **tablep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_rename --
 *     WT_SESSION::rename.
 */
extern int __wt_schema_rename(WT_SESSION_IMPL *session, const char *uri, const char *newuri,
  const char *cfg[], bool check_visibility) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_tiered_shared_colgroup_name --
 *     Get the URI for a tiered storage shared column group. This is used for metadata lookups.
 */
extern int __wt_schema_tiered_shared_colgroup_name(WT_SESSION_IMPL *session, const char *tablename,
  bool active, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_truncate --
 *     WT_SESSION::truncate without a range.
 */
extern int __wt_schema_truncate(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_schema_worker --
 *     Get Btree handles for the object and cycle through calls to an underlying worker function
 *     with each handle.
 */
extern int __wt_schema_worker(WT_SESSION_IMPL *session, const char *uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[], uint32_t open_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_str_name_check --
 *     Disallow any use of the WiredTiger name space.
 */
extern int __wt_str_name_check(WT_SESSION_IMPL *session, const char *str)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_plan --
 *     Given a table cursor containing a complete table, build the "projection plan" to distribute
 *     the columns to dependent stores. A string representing the plan will be appended to the plan
 *     buffer.
 */
extern int __wt_struct_plan(WT_SESSION_IMPL *session, WT_TABLE *table, const char *columns,
  size_t len, bool value_only, WT_ITEM *plan) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_reformat --
 *     Given a table and a list of columns (which could be values in a column group or index keys),
 *     calculate the resulting new format string. The result will be appended to the format buffer.
 */
extern int __wt_struct_reformat(WT_SESSION_IMPL *session, WT_TABLE *table, const char *columns,
  size_t len, const char *extra_cols, bool value_only, WT_ITEM *format)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
