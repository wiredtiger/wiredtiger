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
#endif
