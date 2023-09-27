/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_array_walk --
 *     Walk the connection sessions array, calling a function for every session in the array.
 *     Callers can exit the walk early if desired. Arguments to the function are provided by a
 *     customizable cookie.
 */
void
__wt_session_array_walk(WT_SESSION_IMPL *session,
  void (*walk_func)(WT_SESSION_IMPL *, bool *exit_walkp, void *cookiep), bool skip_internal,
  void *cookiep)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *array_session;
    uint32_t session_cnt, i;
    u_int active;
    bool exit_walk;

    exit_walk = false;
    conn = S2C(session);
    /* Ensure we read the value once. */
    session_cnt = *(volatile uint32_t *)&(conn->session_array.cnt);

    for (i = 0, array_session = WT_CONN_SESSIONS_GET(conn); i < session_cnt; i++, array_session++) {
        WT_ORDERED_READ(active, array_session->active);
        /* Skip inactive sessions. */
        if (!active)
            continue;

        /* If configured skip internal sessions. */
        if (skip_internal && F_ISSET(array_session, WT_SESSION_INTERNAL))
            continue;

        walk_func(array_session, &exit_walk, cookiep);
        /* Early exit the walk if possible. */
        if (exit_walk)
            break;
    }
}
