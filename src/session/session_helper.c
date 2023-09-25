/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

void
__wt_session_array_walk(WT_SESSION_IMPL *session,
  void (*walk_func)(WT_SESSION_IMPL *, bool *exit_walk, void *cookiep), void *cookiep)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *array_session;
    uint32_t session_cnt, i;
    u_int active;
    bool exit_walk;

    exit_walk = false;
    conn = S2C(session);
    /* Ensure we read the value once. */
    session_cnt = *(volatile uint32_t *)&(conn->session_cnt);

    for (i = 0, array_session = conn->sessions; i < session_cnt; i++, array_session++) {
        WT_ORDERED_READ(active, array_session->active);
        if (!active)
            continue;

        walk_func(array_session, &exit_walk, cookiep);
        if (exit_walk)
            break;
    }
}
