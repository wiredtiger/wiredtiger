/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curversion_set_key --
 *     WT_CURSOR->set_key implementation for version cursors.
 */
static void
__curversion_set_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    table_cursor->set_key(table_cursor);
}

/*
 * __curversion_get_key --
 *     WT_CURSOR->get_key implementation for version cursors.
 */
static int
__curversion_get_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    va_list ap;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    va_start(ap, cursor);
    WT_ERR(__wt_cursor_get_keyv(cursor, cursor->flags, ap));
    WT_ERR(__wt_cursor_get_keyv(table_cursor, table_cursor->flags, ap));

err:
    va_end(ap);
    return (ret);
}

/*
 * __curversion_next --
 *     WT_CURSOR->next method for version cursors. The next function will position the cursor on the
 *     next update of the key it is positioned at. We traverse through updates on the update chain,
 *     then the ondisk value, and finally from the history store.
 */
static int
__curversion_next(WT_CURSOR *cursor)
{
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_TIME_WINDOW tw;
    WT_UPDATE *upd;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    cbt = (WT_CURSOR_BTREE *)table_cursor;
    CURSOR_API_CALL(cursor, session, next, NULL);

    /* The cursor should be positioned. */
    if (!F_ISSET(cursor, WT_CURSTD_KEY_INT))
        goto err;

    upd = version_cursor->next_upd;

    /* 
     * If the update is a tombstone, we still want to record the stop information
     * but we also need traverse to the next update to get the full value.
     */
    version_cursor->upd_txnid = upd->txnid;
    version_cursor->upd_durable_stop_ts = upd->durable_ts;
    version_cursor->upd_stop_ts = upd->start_ts;
    if (upd->type == WT_UPDATE_TOMBSTONE)
        upd = upd->next;

    if (upd != NULL && !F_ISSET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED)) {
        /*
         * Set the version cursor's key, which contains all the record metadata
         * for that particular version of the update. 
         */
        __wt_cursor_set_key(cursor, upd->txnid, upd->durable_ts, upd->start_ts,
          version_cursor->upd_txnid, version_cursor->upd_durable_stop_ts,
          version_cursor->upd_stop_ts, upd->type, upd->prepare_state, upd->flags);

        /*
         * Set the version cursor's value to be the update's value. If the update
         * is a modify, reconstruct the value.
         */
        if (upd->type != WT_UPDATE_MODIFY)
            __wt_upd_value_assign(cbt->upd_value, upd);
        else
            __wt_modify_reconstruct_from_upd_list(session, cbt, upd, cbt->upd_value);

        __wt_cursor_set_value(cursor, cbt->upd_value->buf);

        upd = upd->next;
        if (upd == NULL)
            F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
    } else if (!F_ISSET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED)) {
        /* Get the ondisk value. */
        WT_RET(__wt_value_return_buf(cbt, cbt->ref, &cbt->upd_value->buf, &tw));

        __wt_cursor_set_key(cursor, tw.start_txn, tw.durable_start_ts, tw.start_ts,
          tw.stop_txn, tw.durable_stop_ts, tw.stop_ts, cbt->upd_value->type, 0, 0);
        __wt_cursor_set_value(cursor, cbt->upd_value->buf);

        F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
    } else if (!F_ISSET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED)) {
        /* Use the history store cursor to traverse history store records. */
        /* TODO: Implement iterating through history store records. */

        if (ret == WT_NOTFOUND)
            F_SET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED);
    } else {
        /* We have exhausted all versions of the key. */
        ret = WT_NOTFOUND;
    }

    if (0) {
err:
        WT_TRET(cursor->reset(cursor));
    }
    API_END_RET(session, ret);
}

/*
 * __curversion_reset --
 *     WT_CURSOR::reset for version cursors.
 */
static int
__curversion_reset(WT_CURSOR *cursor)
{
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    CURSOR_API_CALL(cursor, session, reset, NULL);

    if (table_cursor != NULL)
        WT_TRET(table_cursor->reset(table_cursor));
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->reset(hs_cursor));
    version_cursor->next_upd = NULL;
    version_cursor->flags = 0;
    F_CLR(cursor, WT_CURSTD_KEY_SET);
    F_CLR(cursor, WT_CURSTD_VALUE_SET);

err:
    API_END_RET(session, ret);
}

/*
 * __curversion_search --
 *     WT_CURSOR->search method for version cursors.
 */
static int
__curversion_search(WT_CURSOR *cursor)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_INSERT *ins;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;
    bool key_only;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    key_only = F_ISSET(cursor, WT_CURSTD_KEY_ONLY);

    /*
     * For now, we assume that we are using simple cursors only.
     */
    cbt = (WT_CURSOR_BTREE *)table_cursor;
    CURSOR_API_CALL(cursor, session, search, CUR2BT(cbt));
    WT_ERR(__cursor_checkkey(table_cursor));

    /* Do a search and position on they key if it is found */
    F_SET(cursor, WT_CURSTD_KEY_ONLY);
    WT_ERR(__wt_btcur_search(cbt));
    if (!F_ISSET(cbt, WT_CURSTD_KEY_SET))
        return (WT_NOTFOUND);

    /*
     * If we position on a key, set next update of the version cursor to be the first update on the
     * key if any.
     */
    page = cbt->ref->page;
    rip = &page->pg_row[cbt->slot];
    if (cbt->ins != NULL)
        version_cursor->next_upd = ins->upd;
    else if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
        version_cursor->next_upd = upd;
    else {
        version_cursor->next_upd = NULL;
        F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
    }

err:
    if (!key_only)
        F_CLR(cursor, WT_CURSTD_KEY_ONLY);
    API_END_RET(session, ret);
}

/*
 * __curversion_close --
 *     WT_CURSOR->close method for version cursors.
 */
static int
__curversion_close(WT_CURSOR *cursor)
{
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    CURSOR_API_CALL(cursor, session, close, NULL);
err:
    version_cursor->next_upd = NULL;
    if (table_cursor != NULL)
        WT_TRET(table_cursor->close(table_cursor));
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));
    __wt_cursor_close(cursor);

    API_END_RET(session, ret);
}

/*
 * __wt_curversion_open --
 *     Initialize a version cursor.
 */
int
__wt_curversion_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CURSOR_STATIC_INIT(iface, __curversion_get_key, /* get-key */
      __wt_cursor_get_value,                           /* get-value */
      __curversion_set_key,                            /* set-key */
      __wt_cursor_set_value_notsup,                    /* set-value */
      __wt_cursor_compare_notsup,                      /* compare */
      __wt_cursor_equals_notsup,                       /* equals */
      __curversion_next,                               /* next */
      __wt_cursor_notsup,                              /* prev */
      __curversion_reset,                              /* reset */
      __curversion_search,                             /* search */
      __wt_cursor_search_near_notsup,                  /* search-near */
      __wt_cursor_notsup,                              /* insert */
      __wt_cursor_modify_notsup,                       /* modify */
      __wt_cursor_notsup,                              /* update */
      __wt_cursor_notsup,                              /* remove */
      __wt_cursor_notsup,                              /* reserve */
      __wt_cursor_reconfigure_notsup,                  /* reconfigure */
      __wt_cursor_notsup,                              /* largest_key */
      __wt_cursor_notsup,                              /* cache */
      __wt_cursor_reopen_notsup,                       /* reopen */
      __curversion_close);                             /* close */

    WT_CURSOR *cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    /* The table cursor is read only. */
    const char *table_cursor_cfg[] = {
      WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "read_only=true", NULL};

    *cursorp = NULL;
    WT_RET(__wt_calloc_one(session, &version_cursor));
    cursor = (WT_CURSOR *)version_cursor;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;

    /* Open the table cursor. */
    WT_ERR(__wt_open_cursor(session, uri, cursor, table_cursor_cfg, &version_cursor->table_cursor));
    cursor->key_format = WT_UNCHECKED_STRING(QQQQQQBBB);
    cursor->value_format = version_cursor->table_cursor->value_format;
    WT_ERR(__wt_strdup(session, uri, &cursor->uri));

    /* Open the history store cursor for operations on the regular history store .*/
    WT_ERR(__wt_curhs_open(session, cursor, &version_cursor->hs_cursor));

    /* Initialize information used to track update metadata. */
    version_cursor->upd_txnid = 0;
    version_cursor->upd_durable_stop_ts = WT_TS_MAX;
    version_cursor->upd_stop_ts = WT_TS_MAX;

    WT_ERR(__wt_cursor_init(cursor, cursor->uri, owner, cfg, cursorp));

    if (0) {
err:
        WT_TRET(cursor->close(cursor));
        *cursorp = NULL;
    }
    return (ret);
}
