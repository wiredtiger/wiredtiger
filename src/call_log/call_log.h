#pragma once

extern int __wt_call_log_begin_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_close_session(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_commit_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_open_session(WT_SESSION_IMPL *session, int ret_val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_prepare_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_print_return(WT_CONNECTION_IMPL *conn, WT_SESSION_IMPL *session,
  int ret_val, const char *err_msg) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_query_timestamp(
  WT_SESSION_IMPL *session, const char *config, const char *hex_timestamp, int ret_val, bool global)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_rollback_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_set_timestamp(WT_SESSION_IMPL *session, const char *config, int ret_val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_timestamp_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_call_log_timestamp_transaction_uint(WT_SESSION_IMPL *session, WT_TS_TXN_TYPE which,
  uint64_t ts, int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_call_log_setup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_call_log_teardown(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
