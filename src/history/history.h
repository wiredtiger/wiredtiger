#pragma once

/*
 * __wt_hs_close --
 *     Destroy the database's history store.
 */
extern void __wt_hs_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_config --
 *     Configure the history store table.
 */
extern int __wt_hs_config(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_delete_key --
 *     Delete history store content of a given key and optionally reinsert them with 0 timestamp.
 */
extern int __wt_hs_delete_key(WT_SESSION_IMPL *session, WT_CURSOR *hs_cursor, uint32_t btree_id,
  const WT_ITEM *key, bool reinsert, bool error_on_ts_ordering)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_delete_updates --
 *     Delete the updates from the history store.
 */
extern int __wt_hs_delete_updates(WT_SESSION_IMPL *session, WT_RECONCILE *r)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_find_upd --
 *     Scan the history store for a record the btree cursor wants to position on. Create an update
 *     for the record and return to the caller.
 */
extern int __wt_hs_find_upd(WT_SESSION_IMPL *session, uint32_t btree_id, WT_ITEM *key,
  const char *value_format, uint64_t recno, WT_UPDATE_VALUE *upd_value, WT_ITEM *base_value_buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_get_btree --
 *     Get the history store btree by opening a history store cursor.
 */
extern int __wt_hs_get_btree(WT_SESSION_IMPL *session, WT_BTREE **hs_btreep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_insert_updates --
 *     Copy one set of saved updates into the database's history store table if they haven't been
 *     moved there. Whether the function fails or succeeds, if there is a successful write to
 *     history, cache_write_hs is set to true.
 */
extern int __wt_hs_insert_updates(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_MULTI *multi)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_modify --
 *     Make an update to the history store.
 *
 * History store updates don't use transactions as those updates should be immediately visible and
 *     don't follow normal transaction semantics. For this reason, history store updates are
 *     directly modified using the low level api instead of the ordinary cursor api.
 */
extern int __wt_hs_modify(WT_CURSOR_BTREE *hs_cbt, WT_UPDATE *hs_upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_open --
 *     Initialize the database's history store.
 */
extern int __wt_hs_open(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_upd_time_window --
 *     Get the underlying time window of the update history store cursor is positioned at.
 */
extern void __wt_hs_upd_time_window(WT_CURSOR *hs_cursor, WT_TIME_WINDOW **twp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_verify --
 *     Verify the history store. There can't be an entry in the history store without having the
 *     latest value for the respective key in the data store.
 */
extern int __wt_hs_verify(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hs_verify_one --
 *     Verify the history store for a given btree. This must be called when we are known to have
 *     exclusive access to the btree.
 */
extern int __wt_hs_verify_one(WT_SESSION_IMPL *session, uint32_t btree_id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
