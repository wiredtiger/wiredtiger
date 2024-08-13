#pragma once

extern int __wt_hs_config(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_delete_key(WT_SESSION_IMPL *session, WT_CURSOR *hs_cursor, uint32_t btree_id,
  const WT_ITEM *key, bool reinsert, bool error_on_ts_ordering)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_delete_updates(WT_SESSION_IMPL *session, WT_RECONCILE *r)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_find_upd(WT_SESSION_IMPL *session, uint32_t btree_id, WT_ITEM *key,
  const char *value_format, uint64_t recno, WT_UPDATE_VALUE *upd_value, WT_ITEM *base_value_buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_get_btree(WT_SESSION_IMPL *session, WT_BTREE **hs_btreep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_insert_updates(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_MULTI *multi)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_modify(WT_CURSOR_BTREE *hs_cbt, WT_UPDATE *hs_upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_open(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_verify(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hs_verify_one(WT_SESSION_IMPL *session, uint32_t btree_id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_hs_close(WT_SESSION_IMPL *session);
extern void __wt_hs_upd_time_window(WT_CURSOR *hs_cursor, WT_TIME_WINDOW **twp);

#ifdef HAVE_UNITTEST

#endif
