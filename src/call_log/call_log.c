/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef HAVE_CALL_LOG
/*
 * __wt_conn_call_log_setup --
 *     Setup the resources for call log tracking.
 */
int
__wt_conn_call_log_setup(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(file_name);
    WT_DECL_RET;

    conn = S2C(session);

    WT_RET(__wt_scr_alloc(session, 0, &file_name));
    WT_ERR(__wt_filename_construct(
      session, "", "wt_call_log", __wt_process_id(), UINT32_MAX, file_name));
    WT_ERR(__wt_fopen(session, (const char *)file_name->data, WT_FS_OPEN_CREATE, WT_STREAM_APPEND,
      &conn->call_log_fst));

    F_SET(session, WT_CONN_CALL_LOG_ENABLED);

    WT_RET(__wt_call_log_print_start(
      session, "global", "wiredtiger_open", 2, "\"Input1\": hello1", "\"Input1\": Hello2"));
    WT_RET(__wt_call_log_print_finish(
      session, 0, "no error", 2, "\"Output1\": hello1", "\"Output2\": Hello2"));

err:
    if (!F_ISSET(session, WT_CONN_CALL_LOG_ENABLED))
        WT_ERR_MSG(session, WT_ERROR, "Failed to open call log.");
    __wt_scr_free(session, &file_name);
    return (ret);
}

/*
 * __wt_conn_call_log_teardown --
 *     Clean up the resources used for the call log.
 */
int
__wt_conn_call_log_teardown(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    if (!F_ISSET(session, WT_CONN_CALL_LOG_ENABLED))
        return (0);

    return (__wt_fclose(session, &conn->call_log_fst));
}

/*
 * __wt_call_log_print --
 *     Helper function for printing the JSON formatted call log entry.
 */
int
__wt_call_log_print(
  WT_SESSION_IMPL *session, const char *class_name, const char *operation, int ret_val)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    WT_ERR(__wt_fprintf(session, conn->call_log_fst,
      "{\n"
      "    \"Operation\" : {\n"
      "        \"ClassName\" : \"%s\",\n"
      "        \"methodName\" : \"%s\",\n"
      "        \"Input\" : {\n"
      "            \"ObjectId\" :"
      "        },\n"
      "        \"Output\" : {\n"
      "        },\n"
      "        \"Return\" : {\n"
      "            \"ReturnVal\" : \"%d\",\n"
      "            \"errMsg\" : \" \",\n"
      "        },\n"
      "    },\n"
      "},",
      class_name, operation, ret_val));
err:

    return (ret);
}

/*
 * __wt_call_log_print --
 *     Helper function for printing the JSON formatted call log entry.
 */
int
__wt_call_log_print_start(
  WT_SESSION_IMPL *session, const char *class_name, const char *method_name, int n, ...)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    va_list valist;

    conn = S2C(session);

    va_start(valist, n);

    WT_ERR(__wt_fprintf(session, conn->call_log_fst,
      "{\n"
      "    \"Operation\" : {\n"
      "        \"ClassName\" : \"%s\",\n"
      "        \"methodName\" : \"%s\",\n"
      "        \"Input\" : {\n",
      class_name, method_name));

    for (int i = 0; i < n; i++) {
        WT_ERR(
          __wt_fprintf(session, conn->call_log_fst, "            %s,\n", va_arg(valist, char *)));
    }

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        },\n"));
err:

    return (ret);
}

/*
 * __wt_call_log_print --
 *     Helper function for printing the JSON formatted call log entry.
 */
int
__wt_call_log_print_finish(WT_SESSION_IMPL *session, int retVal, const char *errMsg, int n, ...)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    va_list valist;

    conn = S2C(session);

    va_start(valist, n);

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        \"Output\" : {\n"));

    for (int i = 0; i < n; i++) {
        WT_ERR(
          __wt_fprintf(session, conn->call_log_fst, "            %s,\n", va_arg(valist, char *)));
    }

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        },\n"));

    WT_ERR(__wt_fprintf(session, conn->call_log_fst,
      "        \"Return\" : {\n"
      "            \"ReturnVal\" : \"%d\",\n"
      "            \"errMsg\" : \"%s\",\n"
      "        },\n"
      "    },\n"
      "},",
      retVal, errMsg));
err:

    return (ret);
}
#endif
