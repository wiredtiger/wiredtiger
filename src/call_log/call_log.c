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
    if ((ret = __wt_fopen(session, (const char *)file_name->data, WT_FS_OPEN_CREATE,
           WT_STREAM_APPEND, &conn->call_log_fst)) != 0)
        WT_ERR_MSG(session, ret, "Failed to open call log.");

    F_SET(conn, WT_CONN_CALL_LOG_ENABLED);

err:
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

    if (!F_ISSET(conn, WT_CONN_CALL_LOG_ENABLED))
        return (0);

    return (__wt_fclose(session, &conn->call_log_fst));
}

/*
 * __call_log_print_start --
 *     Helper function for printing the beginning section of the call log entry. Each entry will
 *     specify the class and method names corresponding to the timestamp simulator.
 */
static int
__call_log_print_start(WT_SESSION_IMPL *session, const char *class_name, const char *method_name)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    WT_RET(__wt_fprintf(session, conn->call_log_fst,
      "{\n"
      "    \"Operation\" : {\n"
      "        \"ClassName\" : \"%s\",\n"
      "        \"MethodName\" : \"%s\",\n",
      class_name, method_name));

    return (0);
}

/*
 * __call_log_print_input --
 *     Helper function for printing the input section of the call log entry. A variable number of
 *     arguments is accepted since each API call may use a different number of inputs. Inputs are
 *     expected to be a string in JSON format to be appended to the input list in the JSON API call
 *     entry.
 */
static int
__call_log_print_input(WT_SESSION_IMPL *session, int n, ...)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    va_list valist;

    conn = S2C(session);

    va_start(valist, n);

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        \"Input\" : {\n"));

    for (int i = 0; i < n; i++) {
        WT_ERR(
          __wt_fprintf(session, conn->call_log_fst, "            %s,\n", va_arg(valist, char*)));
    }

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        },\n"));

err:
    va_end(valist);

    return (ret);
}

/*
 * __call_log_print_output --
 *     Helper function for printing the output section of the call log entry. A variable number of
 *     arguments is accepted since each API call may use a different number of outputs. Outputs are
 *     expected to be a string in JSON format to be appended to the output list in the JSON API call
 *     entry.
 */
static int
__call_log_print_output(WT_SESSION_IMPL *session, int n, ...)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    va_list valist;

    conn = S2C(session);

    va_start(valist, n);

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        \"Output\" : {\n"));

    for (int i = 0; i < n; i++) {
        WT_ERR(
          __wt_fprintf(session, conn->call_log_fst, "            %s,\n", va_arg(valist, char*)));
    }

    WT_ERR(__wt_fprintf(session, conn->call_log_fst, "        },\n"));

err:
    va_end(valist);

    return (ret);
}

/*
 * __call_log_print_return --
 *     Helper function for printing the return section of the call log entry. Each return section of
 *     the call log entry expects to have the return value of the API call and an error message if
 *     it exists.
 */
static int
__call_log_print_return(WT_SESSION_IMPL *session, int ret_val, const char *err_msg)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    WT_RET(__wt_fprintf(session, conn->call_log_fst,
      "        \"Return\" : {\n"
      "            \"ReturnVal\" : %d,\n"
      "            \"errMsg\" : \"%s\"\n"
      "        }\n"
      "    }\n"
      "}",
      ret_val, err_msg));

    return (0);
}

/*
 * __wt_call_log_wiredtiger_open_start --
 *     Print the first half of the call log entry for the wiredtiger_open API call. The call log
 *     entry is split in two so that the api call will always be logged regardless of a failure.
 */
int
__wt_call_log_wiredtiger_open_start(WT_SESSION_IMPL *session)
{

    WT_RET(__call_log_print_start(session, "global", "wiredtiger_open"));
    WT_RET(__call_log_print_input(session, 0));

    return (0);
}

/*
 * __wt_call_log_wiredtiger_open_end --
 *     Print the second half of the call log entry for the wiredtiger_open API call.
 */
int
__wt_call_log_wiredtiger_open_end(WT_SESSION_IMPL *session, int ret_val)
{
    WT_CONNECTION_IMPL *conn;
    char buf[128];

    conn = S2C(session);

    WT_RET(__wt_snprintf(buf, sizeof(buf), "\"objectId\": %p", conn));
    WT_RET(__call_log_print_output(session, 1, buf));
    WT_RET(__call_log_print_return(session, ret_val, ""));

    return (0);
}

#endif
