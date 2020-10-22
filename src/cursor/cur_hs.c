
/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __hs_cursor_open_int --
 *     Open a new history store table cursor, internal function.
 */
static int
__hs_cursor_open_int(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *open_cursor_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

    WT_WITHOUT_DHANDLE(
      session, ret = __wt_open_cursor(session, WT_HS_URI, NULL, open_cursor_cfg, &cursor));
    WT_RET(ret);

    /* History store cursors should always ignore tombstones. */
    F_SET(cursor, WT_CURSTD_IGNORE_TOMBSTONE);

    *cursorp = cursor;
    return (0);
}

/*
 * __wt_hs_cursor_cache --
 *     Cache a new history store table cursor. Open and then close a history store cursor without
 *     saving it in the session.
 */
int
__wt_hs_cursor_cache(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;

    conn = S2C(session);
    if (F_ISSET(conn, WT_CONN_IN_MEMORY) || F_ISSET(session, WT_SESSION_NO_RECONCILE) ||
      (session->dhandle != NULL && WT_IS_METADATA(S2BT(session)->dhandle)) ||
      session == conn->default_session)
        return (0);
    WT_RET(__hs_cursor_open_int(session, &cursor));
    WT_RET(cursor->close(cursor));
    return (0);
}

/*
 * __wt_hs_cursor_open --
 *     Open a new history store table cursor wrapper function.
 */
int
__wt_hs_cursor_open(WT_SESSION_IMPL *session)
{
    /* Not allowed to open a cursor if you already have one */
    WT_ASSERT(session, session->hs_cursor == NULL);

    return (__hs_cursor_open_int(session, &session->hs_cursor));
}

/*
 * __wt_hs_cursor_close --
 *     Discard a history store cursor.
 */
int
__wt_hs_cursor_close(WT_SESSION_IMPL *session)
{
    /* Should only be called when session has an open history store cursor */
    WT_ASSERT(session, session->hs_cursor != NULL);

    WT_RET(session->hs_cursor->close(session->hs_cursor));
    session->hs_cursor = NULL;
    return (0);
}

/*
 * __wt_hs_cursor_next --
 *     Execute a next operation on a history store cursor with the appropriate isolation level.
 */
int
__wt_hs_cursor_next(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
{
    WT_DECL_RET;

    WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED, ret = cursor->next(cursor));
    return (ret);
}

/*
 * __wt_hs_cursor_prev --
 *     Execute a prev operation on a history store cursor with the appropriate isolation level.
 */
int
__wt_hs_cursor_prev(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
{
    WT_DECL_RET;

    WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED, ret = cursor->prev(cursor));
    return (ret);
}

/*
 * __wt_hs_cursor_search_near --
 *     Execute a search near operation on a history store cursor with the appropriate isolation
 *     level.
 */
int
__wt_hs_cursor_search_near(WT_SESSION_IMPL *session, WT_CURSOR *cursor, int *exactp)
{
    WT_DECL_RET;

    WT_WITH_TXN_ISOLATION(
      session, WT_ISO_READ_UNCOMMITTED, ret = cursor->search_near(cursor, exactp));
    return (ret);
}
