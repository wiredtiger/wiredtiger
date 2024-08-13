#pragma once

/*
 * __wt_call_log_begin_transaction --
 *     Print the call log entry for the begin transaction API call.
 */
extern int __wt_call_log_begin_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_close_session --
 *     Print the call log entry for the close session API call.
 */
extern int __wt_call_log_close_session(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_commit_transaction --
 *     Print the call log entry for the commit transaction API call.
 */
extern int __wt_call_log_commit_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_open_session --
 *     Print the call log entry for the open session API call.
 */
extern int __wt_call_log_open_session(WT_SESSION_IMPL *session, int ret_val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_prepare_transaction --
 *     Print the call log entry for the prepare transaction API call.
 */
extern int __wt_call_log_prepare_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_print_return --
 *     Helper function for printing the return section of the call log entry. Each return section of
 *     the call log entry expects to have the return value of the API call and an error message if
 *     it exists.
 */
extern int __wt_call_log_print_return(WT_CONNECTION_IMPL *conn, WT_SESSION_IMPL *session,
  int ret_val, const char *err_msg) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_query_timestamp --
 *     Print the call log entry for the query timestamp API call.
 */
extern int __wt_call_log_query_timestamp(
  WT_SESSION_IMPL *session, const char *config, const char *hex_timestamp, int ret_val, bool global)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_rollback_transaction --
 *     Print the call log entry for the rollback transaction API call.
 */
extern int __wt_call_log_rollback_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_set_timestamp --
 *     Print the call log entry for the set timestamp API call.
 */
extern int __wt_call_log_set_timestamp(WT_SESSION_IMPL *session, const char *config, int ret_val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_timestamp_transaction --
 *     Print the call log entry for the timestamp_transaction API call.
 */
extern int __wt_call_log_timestamp_transaction(WT_SESSION_IMPL *session, const char *config,
  int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_call_log_timestamp_transaction_uint --
 *     Print the call log entry for the timestamp_transaction_uint API call.
 */
extern int __wt_call_log_timestamp_transaction_uint(WT_SESSION_IMPL *session, WT_TS_TXN_TYPE which,
  uint64_t ts, int ret_val) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_call_log_setup --
 *     Setup the resources for call log tracking.
 */
extern int __wt_conn_call_log_setup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_call_log_teardown --
 *     Clean up the resources used for the call log.
 */
extern int __wt_conn_call_log_teardown(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
