#pragma once

extern bool __wti_rts_visibility_has_stable_update(WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wti_rts_visibility_page_needs_abort(WT_SESSION_IMPL *session, WT_REF *ref,
  wt_timestamp_t rollback_timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wti_rts_visibility_txn_visible_id(WT_SESSION_IMPL *session, uint64_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_btree_abort_updates(WT_SESSION_IMPL *session, WT_REF *ref,
  wt_timestamp_t rollback_timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_btree_apply_all(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_btree_walk_btree(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_btree_walk_btree_apply(
  WT_SESSION_IMPL *session, const char *uri, const char *config, wt_timestamp_t rollback_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_btree_work_unit(WT_SESSION_IMPL *session, WT_RTS_WORK_UNIT *entry)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_history_btree_hs_truncate(WT_SESSION_IMPL *session, uint32_t btree_id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_history_delete_hs(WT_SESSION_IMPL *session, WT_ITEM *key, wt_timestamp_t ts)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rts_history_final_pass(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_rts_pop_work(WT_SESSION_IMPL *session, WT_RTS_WORK_UNIT **entryp);
extern void __wti_rts_progress_msg(WT_SESSION_IMPL *session, WT_TIMER *rollback_start,
  uint64_t rollback_count, uint64_t max_count, uint64_t *rollback_msg_count, bool walk);
extern void __wti_rts_work_free(WT_SESSION_IMPL *session, WT_RTS_WORK_UNIT *entry);

#ifdef HAVE_UNITTEST

#endif
