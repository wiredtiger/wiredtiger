
/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_hs_cursor_open --
 *     Open a new history store table cursor.
 */
int
__wt_hs_cursor_open(WT_SESSION_IMPL *session)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *open_cursor_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

    /* Not allowed to open a cursor if you already have one */
    WT_ASSERT(session, session->hs_cursor == NULL);

    WT_WITHOUT_DHANDLE(
      session, ret = __wt_open_cursor(session, WT_HS_URI, NULL, open_cursor_cfg, &cursor));
    WT_RET(ret);

    /* History store cursors should always ignore tombstones. */
    F_SET(cursor, WT_CURSTD_IGNORE_TOMBSTONE);

    session->hs_cursor = cursor;
    return (0);
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
 * __hs_cursor_position_int --
 *     Internal function to position a history store cursor at the end of a set of updates for a
 *     given btree id, record key and timestamp.
 */
static int
__hs_cursor_position_int(WT_SESSION_IMPL *session, WT_CURSOR *cursor, uint32_t btree_id,
  const WT_ITEM *key, wt_timestamp_t timestamp, WT_ITEM *user_srch_key)
{
    WT_DECL_ITEM(srch_key);
    WT_DECL_RET;
    int cmp, exact;

    if (user_srch_key == NULL)
        WT_RET(__wt_scr_alloc(session, 0, &srch_key));
    else
        srch_key = user_srch_key;

    /*
     * Because of the special visibility rules for the history store, a new key can appear in
     * between our search and the set of updates that we're interested in. Keep trying until we find
     * it.
     *
     * There may be no history store entries for the given btree id and record key if they have been
     * removed by WT_CONNECTION::rollback_to_stable.
     *
     * Note that we need to compare the raw key off the cursor to determine where we are in the
     * history store as opposed to comparing the embedded data store key since the ordering is not
     * guaranteed to be the same.
     */
    cursor->set_key(
      cursor, btree_id, key, timestamp != WT_TS_NONE ? timestamp : WT_TS_MAX, UINT64_MAX);
    /* Copy the raw key before searching as a basis for comparison. */
    WT_ERR(__wt_buf_set(session, srch_key, cursor->key.data, cursor->key.size));
    WT_ERR(cursor->search_near(cursor, &exact));
    if (exact > 0) {
        /*
         * It's possible that we may race with a history store insert for another key. So we may be
         * more than one record away the end of our target key/timestamp range. Keep iterating
         * backwards until we land on our key.
         */
        while ((ret = cursor->prev(cursor)) == 0) {
            WT_STAT_CONN_INCR(session, cursor_skip_hs_cur_position);
            WT_STAT_DATA_INCR(session, cursor_skip_hs_cur_position);

            WT_ERR(__wt_compare(session, NULL, &cursor->key, srch_key, &cmp));
            if (cmp <= 0)
                break;
        }
    }
#ifdef HAVE_DIAGNOSTIC
    if (ret == 0) {
        WT_ERR(__wt_compare(session, NULL, &cursor->key, srch_key, &cmp));
        WT_ASSERT(session, cmp <= 0);
    }
#endif
err:
    if (user_srch_key == NULL)
        __wt_scr_free(session, &srch_key);
    return (ret);
}

/*
 * __wt_hs_cursor_position --
 *     Position a history store cursor at the end of a set of updates for a given btree id, record
 *     key and timestamp. There may be no history store entries for the given btree id and record
 *     key if they have been removed by WT_CONNECTION::rollback_to_stable. There is an optional
 *     argument to store the key that we used to position the cursor which can be used to assess
 *     where the cursor is relative to it. The function executes with isolation level set as
 *     WT_ISO_READ_UNCOMMITTED.
 */
int
__wt_hs_cursor_position(WT_SESSION_IMPL *session, WT_CURSOR *cursor, uint32_t btree_id,
  const WT_ITEM *key, wt_timestamp_t timestamp, WT_ITEM *user_srch_key)
{
    WT_DECL_RET;
    WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
      ret = __hs_cursor_position_int(session, cursor, btree_id, key, timestamp, user_srch_key));
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
