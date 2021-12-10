/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __hs_cleanup_las --
 *     Drop the lookaside file if it exists.
 */
static int
__hs_cleanup_las(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    const char *drop_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_drop), "force=true", NULL};

    conn = S2C(session);

    /* Read-only and in-memory configurations won't drop the lookaside. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY))
        return (0);

    /* The LAS table may exist on upgrade. Discard it. */
    WT_WITH_SCHEMA_LOCK(
      session, ret = __wt_schema_drop(session, "file:WiredTigerLAS.wt", drop_cfg));

    return (ret);
}

/*
 * __wt_hs_get_btree --
 *     Get the history store btree by opening a history store cursor.
 */
int
__wt_hs_get_btree(WT_SESSION_IMPL *session, WT_BTREE **hs_btreep)
{
    WT_CURSOR *hs_cursor;
    WT_DECL_RET;

    *hs_btreep = NULL;

    WT_RET(__wt_curhs_open(session, NULL, &hs_cursor));
    *hs_btreep = __wt_curhs_get_btree(hs_cursor);
    WT_ASSERT(session, *hs_btreep != NULL);
    WT_TRET(hs_cursor->close(hs_cursor));
    return (ret);
}

/*
 * __wt_hs_uri --
 *     Given an ID value, generate a history store uri
 */
int
__wt_hs_uri(WT_SESSION_IMPL *session, uint32_t id, char **retp)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;

    *retp = NULL;

    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(__wt_buf_fmt(session, tmp, "file:" WT_HS_PREFIX "%010" PRIu32 ".wt", id));
    WT_ERR(__wt_strndup(session, tmp->data, tmp->size, retp));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_hs_open --
 *     Initialize the database's history store.
 */
int
__wt_hs_open(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONNECTION_IMPL *conn;

    WT_UNUSED(cfg);

    conn = S2C(session);

    /* This function opens the single, database-wide history store file. */
    if (WT_HS_MULTI)
        return (0);

    /* Read-only and in-memory configurations don't need the history store table. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY))
        return (0);

    /* Drop the lookaside file if it still exists. */
    WT_RET(__hs_cleanup_las(session));

    /* Create the table. */
    WT_RET(__wt_session_create(session, WT_HS_URI, WT_HS_CONFIG));

    /* The statistics server is already running, make sure we don't race. */
    WT_WRITE_BARRIER();
    F_SET(conn, WT_CONN_HS_OPEN);

    return (0);
}

/*
 * __wt_hs_close --
 *     Destroy the database's history store.
 */
void
__wt_hs_close(WT_SESSION_IMPL *session)
{
    F_CLR(S2C(session), WT_CONN_HS_OPEN);
}
