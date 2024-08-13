#pragma once

extern int __wti_txn_checkpoint_logread(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_LSN *ckpt_lsn) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_txn_log_commit(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_txn_set_read_timestamp(WT_SESSION_IMPL *session, wt_timestamp_t read_ts)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_txn_ts_log(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_txn_clear_durable_timestamp(WT_SESSION_IMPL *session);
extern void __wti_txn_clear_read_timestamp(WT_SESSION_IMPL *session);
extern void __wti_txn_get_pinned_timestamp(
  WT_SESSION_IMPL *session, wt_timestamp_t *tsp, uint32_t flags);
extern void __wti_txn_update_pinned_timestamp(WT_SESSION_IMPL *session, bool force);

#ifdef HAVE_UNITTEST

#endif
