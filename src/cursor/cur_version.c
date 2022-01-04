/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define VERSION_CURSOR_METADATA_FORMAT WT_UNCHECKED_STRING(QQQQQQBBBB)
/*
 * __curversion_set_key --
 *     WT_CURSOR->set_key implementation for version cursors.
 */
static void
__curversion_set_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    uint32_t flags;
    va_list ap;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    va_start(ap, cursor);
    flags = table_cursor->flags;
    if (F_ISSET(cursor, WT_CURSTD_RAW))
        flags |= WT_CURSTD_RAW;
    WT_IGNORE_RET(__wt_cursor_set_keyv(table_cursor, flags, ap));
    va_end(ap);
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
    uint32_t flags;
    va_list ap;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    va_start(ap, cursor);
    flags = table_cursor->flags;
    if (F_ISSET(cursor, WT_CURSTD_RAW))
        flags |= WT_CURSTD_RAW;
    WT_ERR(__wt_cursor_get_keyv(table_cursor, flags, ap));

err:
    va_end(ap);
    return (ret);
}

/*
 * __curversion_get_value --
 *     WT_CURSOR->get_value implementation for version cursors.
 */
static int
__curversion_get_value(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    va_list ap;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    va_start(ap, cursor);
    if (F_ISSET(cursor, WT_CURSTD_RAW)) {
        WT_ERR(__wt_cursor_get_valuev(cursor, "u", ap));
        WT_ERR(__wt_cursor_get_valuev(table_cursor, "u", ap));
    } else {
        WT_ERR(__wt_cursor_get_valuev(cursor, VERSION_CURSOR_METADATA_FORMAT, ap));
        WT_ERR(__wt_cursor_get_valuev(table_cursor, table_cursor->value_format, ap));
    }

err:
    va_end(ap);
    return (ret);
}

/*
 * __curversion_next_int --
 *     internal implementation for version cursor next
 */
static int
__curversion_next_int(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
{
    WT_CELL *cell;
    WT_CELL_UNPACK_KV *vpack, _vpack;
    WT_COL *cip;
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_ITEM hs_value;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_TIME_WINDOW *twp;
    WT_UPDATE *upd;
    wt_timestamp_t durable_stop_ts, stop_ts;
    uint64_t stop_txn;
    uint32_t hs_upd_type;
    uint32_t raw;
    uint8_t version_prepare_state;
    bool upd_found;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    cbt = (WT_CURSOR_BTREE *)table_cursor;
    twp = NULL;
    upd_found = false;
    vpack = &_vpack;
    raw = F_MASK(cursor, WT_CURSTD_RAW);
    F_CLR(cursor, WT_CURSTD_RAW);

    /* The cursor should be positioned, otherwise the next call will fail. */
    if (!F_ISSET(table_cursor, WT_CURSTD_KEY_SET)) {
        WT_IGNORE_RET(__wt_msg(
          session, "WT_ROLLBACK: rolling back version_cursor->next due to no initial position"));
        WT_ERR(WT_ROLLBACK);
    }

    upd = version_cursor->next_upd;

    if (!F_ISSET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED)) {
        /*
         * If the update is an aborted update, we want to skip to the next update immediately or get
         * the ondisk value if the update is the last one in the update chain.
         */
        while (upd != NULL && upd->txnid == WT_TXN_ABORTED)
            upd = upd->next;

        if (upd == NULL)
            F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
        else {
            if (upd->type == WT_UPDATE_TOMBSTONE) {
                /*
                 * If the update is a tombstone, we still want to record the stop information but we
                 * also need traverse to the next update to get the full value. If the tombstone was
                 * the last update in the update list, retrieve the ondisk value.
                 */
                version_cursor->upd_stop_txnid = upd->txnid;
                version_cursor->upd_durable_stop_ts = upd->durable_ts;
                version_cursor->upd_stop_ts = upd->start_ts;

                upd = upd->next;

                /* Make sure the next update is not an aborted update. */
                while (upd != NULL && upd->txnid == WT_TXN_ABORTED)
                    upd = upd->next;
            }

            if (upd == NULL)
                F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
            else {
                if (upd->prepare_state == WT_PREPARE_INPROGRESS ||
                  upd->prepare_state == WT_PREPARE_LOCKED)
                    version_prepare_state = 1;
                else
                    version_prepare_state = 0;

                /*
                 * Copy the update value into the version cursor as we don't know the value format.
                 * If the update is a modify, reconstruct the value.
                 */
                if (upd->type != WT_UPDATE_MODIFY)
                    __wt_upd_value_assign(cbt->upd_value, upd);
                else
                    WT_ERR(
                      __wt_modify_reconstruct_from_upd_list(session, cbt, upd, cbt->upd_value));

                /*
                 * Set the version cursor's value, which also contains all the record metadata for
                 * that particular version of the update.
                 */
                __wt_cursor_set_value(cursor, upd->txnid, upd->start_ts, upd->durable_ts,
                  version_cursor->upd_stop_txnid, version_cursor->upd_stop_ts,
                  version_cursor->upd_durable_stop_ts, upd->type, version_prepare_state, upd->flags,
                  WT_VERSION_UPDATE_CHAIN);
                table_cursor->value.data = cbt->upd_value->buf.data;
                table_cursor->value.size = cbt->upd_value->buf.size;

                version_cursor->upd_stop_txnid = upd->txnid;
                version_cursor->upd_durable_stop_ts = upd->durable_ts;
                version_cursor->upd_stop_ts = upd->start_ts;

                upd_found = true;
                version_cursor->next_upd = upd->next;
            }
        }
    }

    if (!upd_found && !F_ISSET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED)) {
        WT_TIME_WINDOW_INIT(&vpack->tw);
        /*
         * For row store, if the key is on an insert list only there is no ondisk value nor history
         * store value.
         */
        page = cbt->ref->page;
        if (page->type == WT_PAGE_ROW_LEAF && cbt->ins != NULL) {
            F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
            F_SET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED);
        } else {
            /* Retrieve the value for each type of underlying table structure. */
            switch (page->type) {
            case WT_PAGE_ROW_LEAF:
                rip = &page->pg_row[cbt->slot];
                __wt_row_leaf_value_cell(session, cbt->ref->page, rip, vpack);
                break;
            case WT_PAGE_COL_FIX:
            case WT_PAGE_COL_VAR:
                cip = &page->pg_var[cbt->slot];
                cell = WT_COL_PTR(page, cip);
                __wt_cell_unpack_kv(session, page->dsk, cell, vpack);

                if (vpack->type == WT_CELL_DEL) {
                    F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
                    return (WT_NOTFOUND);
                }
                break;
            default:
                WT_ERR(__wt_illegal_value(session, page->type));
            }

            if (page->type == WT_PAGE_COL_FIX)
                WT_ERR(
                  __wt_col_fix_get_time_window(session, cbt->ref, cbt->ref->ref_recno, &vpack->tw));

            if (!WT_TIME_WINDOW_HAS_STOP(&vpack->tw)) {
                durable_stop_ts = version_cursor->upd_durable_stop_ts;
                stop_ts = version_cursor->upd_stop_ts;
                stop_txn = version_cursor->upd_stop_txnid;
            } else {
                durable_stop_ts = vpack->tw.durable_stop_ts;
                stop_ts = vpack->tw.stop_ts;
                stop_txn = vpack->tw.stop_txn;
            }

            __wt_cursor_set_value(cursor, vpack->tw.start_txn, vpack->tw.start_ts,
              vpack->tw.durable_start_ts, stop_txn, stop_ts, durable_stop_ts, WT_UPDATE_STANDARD,
              vpack->tw.prepare, 0, WT_VERSION_DISK_IMAGE);
            table_cursor->value.data = vpack->data;
            table_cursor->value.size = vpack->size;

            upd_found = true;
            F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
        }
    }

    if (!upd_found && !F_ISSET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED)) {
        /* Ensure we can see all the content in the history store. */
        F_SET(hs_cursor, WT_CURSTD_HS_READ_COMMITTED);
        /*
         * If the history store cursor is not yet positioned, then we are traversing the history
         * store versions for the first time.
         */
        if (!F_ISSET(hs_cursor, WT_CURSTD_KEY_INT)) {
            hs_cursor->set_key(
              hs_cursor, 4, S2BT(session)->id, &cursor->key, WT_TS_MAX, UINT64_MAX);
            WT_ERR(__wt_curhs_search_near_before(session, hs_cursor));
        } else
            WT_ERR(hs_cursor->prev(hs_cursor));

        /*
         * If there are no history store records for the given key or if we have iterated through
         * all the records already, we have exhausted the history store.
         */
        WT_ASSERT(session, ret == 0);

        __wt_hs_upd_time_window(hs_cursor, &twp);
        WT_ERR(hs_cursor->get_value(
          hs_cursor, twp->stop_ts, twp->durable_start_ts, &hs_upd_type, &hs_value));

        __wt_cursor_set_value(cursor, twp->start_txn, twp->start_ts, twp->durable_start_ts,
          twp->stop_txn, twp->stop_ts, twp->durable_stop_ts, hs_upd_type, 0, 0,
          WT_VERSION_HISTORY_STORE);

        /*
         * Reconstruct the history store value if needed. Since we save the value inside the version
         * cursor every time we traverse a version, we can simply apply the modify onto the latest
         * value.
         */
        if (hs_upd_type == WT_UPDATE_MODIFY) {
            WT_ERR(__wt_modify_apply_item(
              session, table_cursor->value_format, &table_cursor->value, hs_value.data));
        } else {
            WT_ASSERT(session, hs_upd_type == WT_UPDATE_STANDARD);
            table_cursor->value.data = hs_value.data;
            table_cursor->value.size = hs_value.size;
        }
        upd_found = true;
    }

    if (!upd_found)
        ret = WT_NOTFOUND;
    else
        F_SET(table_cursor, WT_CURSTD_VALUE_INT);

err:
    F_SET(cursor, raw);
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
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    CURSOR_API_CALL(cursor, session, next, NULL);
    WT_ERR(__curversion_next_int(session, cursor));

err:
    if (ret != 0)
        WT_TRET(cursor->reset(cursor));
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
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    cbt = (WT_CURSOR_BTREE *)table_cursor;

    /*
     * For now, we assume that we are using simple cursors only.
     */
    CURSOR_API_CALL(cursor, session, search, CUR2BT(cbt));
    WT_ERR(__cursor_checkkey(table_cursor));

    /* Do a search and position on they key if it is found */
    F_SET(table_cursor, WT_CURSTD_KEY_ONLY);
    WT_ERR(__wt_btcur_search(cbt));
    WT_ASSERT(session, F_ISSET(table_cursor, WT_CURSTD_KEY_SET));

    /*
     * If we position on a key, set next update of the version cursor to be the first update on the
     * key if any.
     */
    page = cbt->ref->page;
    switch (page->type) {
    case WT_PAGE_ROW_LEAF:
        if (cbt->ins != NULL)
            version_cursor->next_upd = cbt->ins->upd;
        else {
            rip = &page->pg_row[cbt->slot];
            upd = WT_ROW_UPDATE(page, rip);
            version_cursor->next_upd = upd;
        }
        break;
    case WT_PAGE_COL_FIX:
    case WT_PAGE_COL_VAR:
        if (cbt->ins != NULL)
            version_cursor->next_upd = cbt->ins->upd;
        else
            version_cursor->next_upd = NULL;
        break;
    default:
        WT_ERR(__wt_illegal_value(session, page->type));
    }

    /* Point to the newest version. */
    WT_ERR(__curversion_next_int(session, cursor));

err:
    if (ret != 0)
        WT_TRET(cursor->reset(cursor));
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
    if (table_cursor != NULL) {
        WT_TRET(table_cursor->close(table_cursor));
        version_cursor->table_cursor = NULL;
    }
    if (hs_cursor != NULL) {
        WT_TRET(hs_cursor->close(hs_cursor));
        version_cursor->hs_cursor = NULL;
    }
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
      __curversion_get_value,                          /* get-value */
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
    char *version_cursor_value_format;
    size_t format_len;

    *cursorp = NULL;
    WT_RET(__wt_calloc_one(session, &version_cursor));
    cursor = (WT_CURSOR *)version_cursor;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    version_cursor_value_format = NULL;

    /* Open the table cursor. */
    WT_ERR(__wt_open_cursor(session, uri, cursor, table_cursor_cfg, &version_cursor->table_cursor));
    cursor->key_format = version_cursor->table_cursor->key_format;
    format_len = strlen(VERSION_CURSOR_METADATA_FORMAT) +
      strlen(version_cursor->table_cursor->value_format) + 1;
    WT_ERR(__wt_malloc(session, format_len, &version_cursor_value_format));
    WT_ERR(__wt_snprintf(version_cursor_value_format, format_len, "%s%s",
      VERSION_CURSOR_METADATA_FORMAT, version_cursor->table_cursor->value_format));
    cursor->value_format = version_cursor_value_format;
    version_cursor_value_format = NULL;

    WT_ERR(__wt_strdup(session, uri, &cursor->uri));
    WT_ERR(__wt_cursor_init(cursor, cursor->uri, owner, cfg, cursorp));

    /* Open the history store cursor for operations on the regular history store .*/
    if (F_ISSET(S2C(session), WT_CONN_HS_OPEN)) {
        WT_ERR(__wt_curhs_open(session, cursor, &version_cursor->hs_cursor));
        F_SET(version_cursor->hs_cursor, WT_CURSTD_HS_READ_COMMITTED);
    } else
        F_SET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED);

    /* Initialize information used to track update metadata. */
    version_cursor->upd_stop_txnid = WT_TXN_MAX;
    version_cursor->upd_durable_stop_ts = WT_TS_MAX;
    version_cursor->upd_stop_ts = WT_TS_MAX;

    /* Mark the cursor as version cursor for python api. */
    F_SET(cursor, WT_CURSTD_VERSION_CURSOR);

    if (0) {
err:
        __wt_free(session, version_cursor_value_format);
        WT_TRET(cursor->close(cursor));
        *cursorp = NULL;
    }
    return (ret);
}
