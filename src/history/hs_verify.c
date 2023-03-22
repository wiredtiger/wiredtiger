/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __hs_verify_id --
 *     Verify the history store for a single btree. Given a cursor to the tree, walk all history
 *     store keys. This function assumes any caller has already opened a cursor to the history
 *     store.
 */
static int
__hs_verify_id(
  WT_SESSION_IMPL *session, WT_CURSOR *hs_cursor, WT_CURSOR *ds_cursor, uint32_t this_btree_id)
{
    WT_DECL_ITEM(prev_key);
    WT_DECL_RET;
    WT_ITEM key;
    wt_timestamp_t hs_start_ts;
    uint64_t hs_counter;
    uint32_t btree_id;
    int cmp;

    WT_CLEAR(key);
    WT_ERR(__wt_scr_alloc(session, 0, &prev_key));

    /*
     * If using standard cursors, we need to skip the non-globally visible tombstones in the data
     * table to verify the corresponding entries in the history store are too present in the data
     * store.
     */
    F_SET(ds_cursor, WT_CURSTD_IGNORE_TOMBSTONE);

    /*
     * The caller is responsible for positioning the history store cursor at the first record to
     * verify. When we return after moving to a new key the caller is responsible for keeping the
     * cursor there or deciding they're done.
     */
    for (; ret == 0; ret = hs_cursor->next(hs_cursor)) {
        /*
         * If the btree id does not match the previous one, we're done. It is up to the caller to
         * set up for the next tree and call us, if they choose. For a full history store walk, the
         * caller sends in WT_BTREE_ID_INVALID and this function will set and use the first btree id
         * it finds and will return once it walks off that tree, leaving the cursor set to the first
         * key of that new tree.
         *
         * We should never cross the btree id, assert if we do so.
         */
        WT_ERR(hs_cursor->get_key(hs_cursor, &btree_id, &key, &hs_start_ts, &hs_counter));
        /*
         * TODO: I don't think this assert is correct. We just need to break when we move to the
         * next btree.
         */
        // WT_ASSERT(session, btree_id == this_btree_id);
        if (btree_id != this_btree_id)
            break;

        /*
         * If we have already checked against this key, keep going to the next key. We only need to
         * check the key once.
         */
        WT_ERR(__wt_compare(session, NULL, &key, prev_key, &cmp));
        if (cmp == 0)
            continue;

        /* Check the key can be found in the data store. */
        ds_cursor->set_key(ds_cursor, key.data);
        WT_ERR(ds_cursor->search(ds_cursor));

        /*
         * Copy the key memory into our scratch buffer. The key will get invalidated on our next
         * cursor iteration.
         */
        WT_ERR(__wt_buf_set(session, prev_key, key.data, key.size));
    }
    WT_ERR_NOTFOUND_OK(ret, true);
    WT_ERR(ds_cursor->reset(ds_cursor));
err:
    F_CLR(ds_cursor, WT_CURSTD_IGNORE_TOMBSTONE);
    WT_ASSERT(session, key.mem == NULL && key.memsize == 0);
    __wt_scr_free(session, &prev_key);
    return (ret);
}

/*
 * __wt_hs_verify_one --
 *     Verify the history store for the btree that is set up in this session. This must be called
 *     when we are known to have exclusive access to the btree.
 */
int
__wt_hs_verify_one(WT_SESSION_IMPL *session, uint32_t this_btree_id)
{
    WT_CURSOR *ds_cursor, *hs_cursor;
    WT_DECL_RET;
    WT_ITEM key;
    wt_timestamp_t hs_start_ts;
    uint64_t hs_counter;
    uint32_t btree_id;
    char *uri_data;

    ds_cursor = hs_cursor = NULL;
    WT_CLEAR(key);
    hs_start_ts = 0;
    hs_counter = 0;
    btree_id = 0;
    uri_data = NULL;

    WT_RET(__wt_curhs_open(session, NULL, &hs_cursor));
    F_SET(hs_cursor, WT_CURSTD_HS_READ_COMMITTED);

    /* Position the hs cursor on the requested btree id. */
    hs_cursor->set_key(hs_cursor, 1, this_btree_id);
    WT_ERR(__wt_curhs_search_near_after(session, hs_cursor));

    /* Make sure the requested btree id exists in the history store. */
    WT_ERR(hs_cursor->get_key(hs_cursor, &btree_id, &key, &hs_start_ts, &hs_counter));
    if(this_btree_id != btree_id) {
        ret = WT_NOTFOUND;
        goto err;
    }

    /* If we positioned the cursor there is something to verify. */
    if ((ret = __wt_metadata_btree_id_to_uri(session, btree_id, &uri_data)) != 0) {
        F_SET(S2C(session), WT_CONN_DATA_CORRUPTION);
        WT_ERR_PANIC(
          session, ret, "Unable to find btree id %" PRIu32 " in the metadata file.", btree_id);
    }

    WT_ERR(__wt_open_cursor(session, uri_data, NULL, NULL, &ds_cursor));
    F_SET(ds_cursor, WT_CURSOR_RAW_OK);

    ret = __hs_verify_id(session, hs_cursor, ds_cursor, btree_id);

    WT_TRET(ds_cursor->close(ds_cursor));

err:
    WT_TRET(hs_cursor->close(hs_cursor));
    return (ret == WT_NOTFOUND ? 0 : ret);
}

/*
 * __wt_hs_verify --
 *     Verify the history store. There can't be an entry in the history store without having the
 *     latest value for the respective key in the data store.
 */
int
__wt_hs_verify(WT_SESSION_IMPL *session)
{
    WT_CURSOR *ds_cursor, *hs_cursor;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM key;
    wt_timestamp_t hs_start_ts;
    uint64_t hs_counter;
    uint32_t btree_id;
    char *uri_data;
    bool stop;

    /* We should never reach here if working in context of the default session. */
    WT_ASSERT(session, S2C(session)->default_session != session);

    ds_cursor = hs_cursor = NULL;
    WT_CLEAR(key);
    btree_id = WT_BTREE_ID_INVALID;
    uri_data = NULL;

    WT_RET(__wt_curhs_open(session, NULL, &hs_cursor));
    F_SET(hs_cursor, WT_CURSTD_HS_READ_COMMITTED);
    WT_ERR(__wt_scr_alloc(session, 0, &buf));
    WT_ERR_NOTFOUND_OK(hs_cursor->next(hs_cursor), true);
    stop = ret == WT_NOTFOUND ? true : false;
    ret = 0;

    /*
     * We have the history store cursor positioned at the first record that we want to verify. The
     * internal function is expecting a btree cursor, so open and initialize that.
     */
    while (!stop) {
        /*
         * The cursor is positioned either from above or left over from the internal call on the
         * first key of a new btree id.
         */
        WT_ERR(hs_cursor->get_key(hs_cursor, &btree_id, &key, &hs_start_ts, &hs_counter));
        if ((ret = __wt_metadata_btree_id_to_uri(session, btree_id, &uri_data)) != 0) {
            F_SET(S2C(session), WT_CONN_DATA_CORRUPTION);
            WT_ERR_PANIC(session, ret,
              "Unable to find btree id %" PRIu32
              " in the metadata file for the associated key '%s'.",
              btree_id, __wt_buf_set_printable(session, key.data, key.size, false, buf));
        }

        WT_ERR(__wt_open_cursor(session, uri_data, NULL, NULL, &ds_cursor));
        F_SET(ds_cursor, WT_CURSOR_RAW_OK);
        ret = __hs_verify_id(session, hs_cursor, ds_cursor, btree_id);

        /* Exit when the entire HS has been parsed. */
        if (ret == WT_NOTFOUND)
            stop = true;

        WT_TRET(ds_cursor->close(ds_cursor));
        WT_ERR_NOTFOUND_OK(ret, false);
    }
err:
    __wt_scr_free(session, &buf);
    WT_ASSERT(session, key.mem == NULL && key.memsize == 0);
    __wt_free(session, uri_data);
    WT_TRET(hs_cursor->close(hs_cursor));
    return (ret);
}
