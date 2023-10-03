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
 *     Walk the connections session array, calling a function for every active session in the array.
 *     Callers can exit the walk early if desired. Arguments to the walk function are provided by a
 *     customizable cookie.
 */
void
__wt_session_array_walk(WT_CONNECTION_IMPL *conn,
  void (*walk_func)(WT_SESSION_IMPL *, bool *exit_walkp, void *cookiep), bool skip_internal,
  void *cookiep)
{
    WT_SESSION_IMPL *array_session;
    uint32_t session_cnt, i;
    u_int active;
    bool exit_walk;

    exit_walk = false;

    /*
     * Ensure we read the session count only once. We want to iterate over all sessions that were
     * active at this point in time. Sessions in the array may open, close, or be have their
     * contents change during traversal. We expect the calling code to handle this. See the slotted
     * sessions docs for further details. FIXME-WT-10946 Add link to docs once they're added.
     */
    session_cnt = *(volatile uint32_t *)&(conn->session_array.cnt);

    for (i = 0, array_session = WT_CONN_SESSIONS_GET(conn); i < session_cnt; i++, array_session++) {
        /*
         * This ordered read is paired with a WT_PUBLISH from the session create logic, and
         * guarantees that by the time this thread sees active == 1 all other fields in the session
         * have been initialized properly. Any other ordering constraints, such as ensure this loop
         * occurs in-order, are not intentional.
         */
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
