#pragma once

extern int __wti_curbackup_free_incr(WT_SESSION_IMPL *session, WT_CURSOR_BACKUP *cb)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_curbackup_open_incr(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  WT_CURSOR *cursor, const char *cfg[], WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_curbulk_close(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_curbulk_init(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool bitmap,
  bool skip_sort_check) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_curdump_create(WT_CURSOR *child, WT_CURSOR *owner, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_bound(WT_CURSOR *cursor, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_cache(WT_CURSOR *cursor, WT_DATA_HANDLE *dhandle)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_cache_release(WT_SESSION_IMPL *session, WT_CURSOR *cursor, bool *released)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_compare_notsup(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_equals_notsup(WT_CURSOR *cursor, WT_CURSOR *other, int *equalp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_get_keyv(WT_CURSOR *cursor, uint64_t flags, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_get_raw_key_value_notsup(WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_get_value_notsup(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_get_valuev(WT_CURSOR *cursor, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_largest_key(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_modify_notsup(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_noop(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_search_near_notsup(WT_CURSOR *cursor, int *exact)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_set_keyv(WT_CURSOR *cursor, uint64_t flags, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_set_valuev(WT_CURSOR *cursor, const char *fmt, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_json_alloc_unpack(WT_SESSION_IMPL *session, const void *buffer, size_t size,
  const char *fmt, WT_CURSOR_JSON *json, bool iskey, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_json_column_init(WT_CURSOR *cursor, const char *uri, const char *keyformat,
  const WT_CONFIG_ITEM *idxconf, const WT_CONFIG_ITEM *colconf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_json_to_item(WT_SESSION_IMPL *session, const char *jstr, const char *format,
  WT_CURSOR_JSON *json, bool iskey, WT_ITEM *item) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_cursor_reopen(WT_CURSOR *cursor, WT_DATA_HANDLE *dhandle);
extern void __wti_cursor_set_key_notsup(WT_CURSOR *cursor, ...);
extern void __wti_cursor_set_value_notsup(WT_CURSOR *cursor, ...);
extern void __wti_json_close(WT_SESSION_IMPL *session, WT_CURSOR *cursor);

#ifdef HAVE_UNITTEST

#endif
