/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * When an operation is accessing the history store table, it should ignore the cache size (since
 * the cache is already full), and the operation can't reenter reconciliation.
 */
#define WT_HS_SESSION_FLAGS (WT_SESSION_IGNORE_CACHE_SIZE | WT_SESSION_NO_RECONCILE)

/*
 * __hs_store_time_pair --
 *     Store the time pair to use for the history store inserts.
 */
static void
__hs_store_time_pair(WT_SESSION_IMPL *session, wt_timestamp_t timestamp, uint64_t txnid)
{
    session->orig_timestamp_to_las = timestamp;
    session->orig_txnid_to_las = txnid;
}

/*
 * __wt_hs_get_btree --
 *     Get the history store btree. Open a history store cursor if needed to get the btree.
 */
int
__wt_hs_get_btree(WT_SESSION_IMPL *session, WT_BTREE **hs_tree)
{
    WT_DECL_RET;
    uint32_t session_flags;
    bool close_hs_cursor;

    *hs_tree = NULL;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    close_hs_cursor = false;

    if (!F_ISSET(session, WT_SESSION_HS_CURSOR)) {
        WT_RET(__wt_hs_cursor(session, &session_flags));
        close_hs_cursor = true;
    }

    *hs_tree = ((WT_CURSOR_BTREE *)session->hs_cursor)->btree;
    WT_ASSERT(session, *hs_tree != NULL);

    if (close_hs_cursor)
        WT_TRET(__wt_hs_cursor_close(session, session_flags));

    return (ret);
}

/*
 * __wt_hs_config --
 *     Configure the history store table.
 */
int
__wt_hs_config(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_BTREE *btree;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    bool reset_no_dh_flag;

    conn = S2C(session);
    reset_no_dh_flag = false;

    WT_ERR(__wt_config_gets(session, cfg, "history_store.file_max", &cval));
    if (cval.val != 0 && cval.val < WT_HS_FILE_MIN)
        WT_ERR_MSG(session, EINVAL, "max history store size %" PRId64 " below minimum %d", cval.val,
          WT_HS_FILE_MIN);

    /* in-memory or readonly configurations do not have a history store. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY))
        return (0);

    /*
     * Retrieve the btree from the history store cursor. This function might need to open a cursor
     * from the default session and hence need to flip the no-dhandle flag temporarily in case it is
     * set. TODO: WT-5501 We should find a way to do this without opening a history store cursor
     * from the default session.
     */
    if (F_ISSET(session, WT_SESSION_NO_DATA_HANDLES)) {
        reset_no_dh_flag = true;
        F_CLR(session, WT_SESSION_NO_DATA_HANDLES);
    }
    WT_ERR(__wt_hs_get_btree(session, &btree));

    /* Track the history store file ID. */
    if (conn->cache->hs_fileid == 0)
        conn->cache->hs_fileid = btree->id;

    /*
     * Set special flags for the history store table: the history store flag (used, for example, to
     * avoid writing records during reconciliation), also turn off checkpoints and logging.
     *
     * Test flags before setting them so updates can't race in subsequent opens (the first update is
     * safe because it's single-threaded from wiredtiger_open).
     */
    if (!F_ISSET(btree, WT_BTREE_HS))
        F_SET(btree, WT_BTREE_HS);
    if (!F_ISSET(btree, WT_BTREE_NO_LOGGING))
        F_SET(btree, WT_BTREE_NO_LOGGING);

    /*
     * We need to set file_max on the btree associated with one of the history store sessions.
     */
    btree->file_max = (uint64_t)cval.val;
    WT_STAT_CONN_SET(session, cache_hs_ondisk_max, btree->file_max);

err:
    if (reset_no_dh_flag)
        F_SET(session, WT_SESSION_NO_DATA_HANDLES);

    return (ret);
}

/*
 * __wt_hs_stats_update --
 *     Update the history store table statistics for return to the application.
 */
int
__wt_hs_stats_update(WT_SESSION_IMPL *session)
{
    WT_BTREE *hs_btree;
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_STATS **cstats;
    WT_DSRC_STATS **dstats;
    int64_t v;

    conn = S2C(session);

    /*
     * History store table statistics are copied from the underlying history store table data-source
     * statistics. If there's no history store table, values remain 0.
     */
    if (!F_ISSET(conn, WT_CONN_HS_OPEN))
        return (0);

    /* Set the connection-wide statistics. */
    cstats = conn->stats;

    /*
     * Get a history store cursor, we need the underlying data handle; we can get to it by way of
     * the underlying btree handle, but it's a little ugly.
     */
    WT_RET(__wt_hs_get_btree(session, &hs_btree));

    dstats = hs_btree->dhandle->stats;

    v = WT_STAT_READ(dstats, cursor_update);
    WT_STAT_SET(session, cstats, cache_hs_insert, v);

    /*
     * If we're clearing stats we need to clear the cursor values we just read. This does not clear
     * the rest of the statistics in the history store data source stat cursor, but we own that
     * namespace so we don't have to worry about users seeing inconsistent data source information.
     */
    if (FLD_ISSET(conn->stat_flags, WT_STAT_CLEAR)) {
        WT_STAT_SET(session, dstats, cursor_update, 0);
        WT_STAT_SET(session, dstats, cursor_remove, 0);
    }

    return (0);
}

/*
 * __wt_hs_create --
 *     Initialize the database's history store.
 */
int
__wt_hs_create(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    /* Read-only and in-memory configurations don't need the history store table. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY))
        return (0);

    /* Re-create the table. */
    WT_RET(__wt_session_create(session, WT_HS_URI, WT_HS_CONFIG));

    WT_RET(__wt_hs_config(session, cfg));

    /* The statistics server is already running, make sure we don't race. */
    WT_WRITE_BARRIER();
    F_SET(conn, WT_CONN_HS_OPEN);

    return (0);
}

/*
 * __wt_hs_destroy --
 *     Destroy the database's history store.
 */
void
__wt_hs_destroy(WT_SESSION_IMPL *session)
{
    F_CLR(S2C(session), WT_CONN_HS_OPEN);
}

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

    WT_WITHOUT_DHANDLE(
      session, ret = __wt_open_cursor(session, WT_HS_URI, NULL, open_cursor_cfg, &cursor));
    WT_RET(ret);

    session->hs_cursor = cursor;
    F_SET(session, WT_SESSION_HS_CURSOR);

    return (0);
}

/*
 * __wt_hs_cursor --
 *     Return a history store cursor, open one if not already open.
 */
int
__wt_hs_cursor(WT_SESSION_IMPL *session, uint32_t *session_flags)
{
    /*
     * We don't want to get tapped for eviction after we start using the history store cursor; save
     * a copy of the current eviction state, we'll turn eviction off before we return.
     *
     * Don't cache history store table pages, we're here because of eviction problems and there's no
     * reason to believe history store pages will be useful more than once.
     */
    *session_flags = F_MASK(session, WT_HS_SESSION_FLAGS);

    /* Open a cursor if this session doesn't already have one. */
    if (!F_ISSET(session, WT_SESSION_HS_CURSOR))
        WT_RET(__wt_hs_cursor_open(session));

    WT_ASSERT(session, session->hs_cursor != NULL);

    /* Configure session to access the history store table. */
    F_SET(session, WT_HS_SESSION_FLAGS);

    return (0);
}

/*
 * __wt_hs_cursor_close --
 *     Discard a history store cursor.
 */
int
__wt_hs_cursor_close(WT_SESSION_IMPL *session, uint32_t session_flags)
{
    /* Nothing to do if the session doesn't have a HS cursor opened. */
    if (!F_ISSET(session, WT_SESSION_HS_CURSOR)) {
        WT_ASSERT(session, session->hs_cursor == NULL);
        return (0);
    }
    WT_ASSERT(session, session->hs_cursor != NULL);

    /*
     * We turned off caching and eviction while the history store cursor was in use, restore the
     * session's flags.
     */
    F_CLR(session, WT_HS_SESSION_FLAGS);
    F_SET(session, session_flags);

    WT_RET(session->hs_cursor->close(session->hs_cursor));
    session->hs_cursor = NULL;
    F_CLR(session, WT_SESSION_HS_CURSOR);

    return (0);
}

/*
 * __hs_insert_updates_verbose --
 *     Display a verbose message once per checkpoint with details about the cache state when
 *     performing a history store table write.
 */
static void
__hs_insert_updates_verbose(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    double pct_dirty, pct_full;
    uint64_t ckpt_gen_current, ckpt_gen_last;
    uint32_t btree_id;

    btree_id = btree->id;

    if (!WT_VERBOSE_ISSET(session, WT_VERB_HS | WT_VERB_HS_ACTIVITY))
        return;

    conn = S2C(session);
    cache = conn->cache;
    ckpt_gen_current = __wt_gen(session, WT_GEN_CHECKPOINT);
    ckpt_gen_last = cache->hs_verb_gen_write;

    /*
     * Print a message if verbose history store, or once per checkpoint if only reporting activity.
     * Avoid an expensive atomic operation as often as possible when the message rate is limited.
     */
    if (WT_VERBOSE_ISSET(session, WT_VERB_HS) ||
      (ckpt_gen_current > ckpt_gen_last &&
          __wt_atomic_casv64(&cache->hs_verb_gen_write, ckpt_gen_last, ckpt_gen_current))) {
        WT_IGNORE_RET_BOOL(__wt_eviction_clean_needed(session, &pct_full));
        WT_IGNORE_RET_BOOL(__wt_eviction_dirty_needed(session, &pct_dirty));

        __wt_verbose(session, WT_VERB_HS | WT_VERB_HS_ACTIVITY,
          "Page reconciliation triggered history store write: file ID %" PRIu32
          ". "
          "Current history store file size: %" PRId64
          ", "
          "cache dirty: %2.3f%% , "
          "cache use: %2.3f%%",
          btree_id, WT_STAT_READ(conn->stats, cache_hs_ondisk), pct_dirty, pct_full);
    }

    /* Never skip updating the tracked generation */
    if (WT_VERBOSE_ISSET(session, WT_VERB_HS))
        cache->hs_verb_gen_write = ckpt_gen_current;
}

/*
 * __hs_insert_record --
 *     A helper function to insert the record into the history store including stop time pair.
 */
static int
__hs_insert_record(WT_SESSION_IMPL *session, WT_CURSOR *cursor, const uint32_t btree_id,
  const WT_ITEM *key, const WT_UPDATE *upd, const uint8_t type, const WT_ITEM *hs_value,
  WT_TIME_PAIR stop_ts_pair)
{
    /*
     * Only deltas or full updates should be written to the history store. More specifically, we
     * should NOT be writing tombstone records in the history store table.
     */
    WT_ASSERT(session, type == WT_UPDATE_STANDARD || type == WT_UPDATE_MODIFY);

    cursor->set_key(
      cursor, btree_id, key, upd->start_ts, upd->txnid, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Set the current update start time pair as the commit time pair to the history store record.
     */
    __hs_store_time_pair(session, upd->start_ts, upd->txnid);

    cursor->set_value(cursor, upd->durable_ts, upd->prepare_state, type, hs_value);

    /*
     * Using update instead of insert so the page stays pinned and can be searched before the tree.
     */
    WT_RET(cursor->update(cursor));

    /* Append a delete record to represent stop time pair for the above insert record */
    cursor->set_key(
      cursor, btree_id, key, upd->start_ts, upd->txnid, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Set the stop time pair as the commit time pair of the history store delete record. */
    __hs_store_time_pair(session, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Remove the inserted record with stop timestamp. */
    WT_RET(cursor->remove(cursor));

    return (0);
}

/*
 * __wt_hs_insert_updates --
 *     Copy one set of saved updates into the database's history store table.
 */
int
__wt_hs_insert_updates(WT_CURSOR *cursor, WT_BTREE *btree, WT_RECONCILE *r, WT_MULTI *multi)
{
    WT_DECL_ITEM(full_value);
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(modify_value);
    WT_DECL_ITEM(prev_full_value);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
/* If the limit is exceeded, we will insert a full update to the history store */
#define MAX_REVERSE_MODIFY_NUM 16
    WT_MODIFY entries[MAX_REVERSE_MODIFY_NUM];
    WT_MODIFY_VECTOR modifies;
    WT_PAGE *page;
    WT_SAVE_UPD *list;
    WT_SESSION_IMPL *session;
    WT_UPDATE *prev_upd, *upd;
    WT_TIME_PAIR stop_ts_pair;
    wt_off_t hs_size;
    uint64_t insert_cnt, max_hs_size;
    uint32_t btree_id, i;
    uint8_t *p;
    int nentries;
    bool local_txn, squashed;

    page = r->page;
    prev_upd = NULL;
    session = (WT_SESSION_IMPL *)cursor->session;
    insert_cnt = 0;
    local_txn = false;
    btree_id = btree->id;
    __wt_modify_vector_init(session, &modifies);

    if (!btree->hs_entries)
        btree->hs_entries = true;

    /*
     * Wrap all the updates in a transaction:
     * 1. We should already have a running application transaction, or
     * 2. We create one.
     * TODO: WT-5478 Ideally we should not need transaction semantics (or a txn mod structure) for a
     * write to history store.
     */
    if (!F_ISSET(&session->txn, WT_TXN_RUNNING)) {
        WT_ERR(__wt_txn_begin(session, NULL));
        local_txn = true;
    }

    /* Ensure enough room for a column-store key without checking. */
    WT_ERR(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

    WT_ERR(__wt_scr_alloc(session, 0, &full_value));

    WT_ERR(__wt_scr_alloc(session, 0, &prev_full_value));

    /* Inserts should be on the same page absent a split, search any pinned leaf page. */
    F_SET(cursor, WT_CURSTD_UPDATE_LOCAL);

    /* Enter each update in the boundary's list into the history store. */
    for (i = 0, list = multi->supd; i < multi->supd_entries; ++i, ++list) {
        /* If no onpage_upd is selected, we don't need to insert anything into the history store. */
        if (list->onpage_upd == NULL)
            continue;

        /* onpage_upd now is always from the update chain */
        WT_ASSERT(session, !F_ISSET(list->onpage_upd, WT_UPDATE_RESTORED_FROM_DISK));

        /* History store table key component: source key. */
        switch (page->type) {
        case WT_PAGE_COL_FIX:
        case WT_PAGE_COL_VAR:
            p = key->mem;
            WT_ERR(__wt_vpack_uint(&p, 0, WT_INSERT_RECNO(list->ins)));
            key->size = WT_PTRDIFF(p, key->data);
            break;
        case WT_PAGE_ROW_LEAF:
            if (list->ins == NULL) {
                WT_WITH_BTREE(
                  session, btree, ret = __wt_row_leaf_key(session, page, list->ripcip, key, false));
                WT_ERR(ret);
            } else {
                key->data = WT_INSERT_KEY(list->ins);
                key->size = WT_INSERT_KEY_SIZE(list->ins);
            }
            break;
        default:
            WT_ERR(__wt_illegal_value(session, page->type));
        }

        /*
         * Trim any updates before writing to history store. This saves wasted work, but is also
         * necessary because the reconciliation only resolves existing birthmarks if they aren't
         * obsolete.
         */
        WT_WITH_BTREE(
          session, btree, upd = __wt_update_obsolete_check(session, page, list->onpage_upd, true));
        __wt_free_update_list(session, &upd);
        upd = list->onpage_upd;

        /*
         * It's not OK for the update list to contain a birthmark on entry - we will generate one
         * below if necessary.
         */
        WT_ASSERT(session, __wt_count_birthmarks(upd) == 0);

        /*
         * The algorithm assumes the oldest update on the update chain in memory is either a full
         * update or a tombstone.
         *
         * This is guaranteed by __wt_rec_upd_select appends the original onpage value at the end of
         * the chain. It also assumes the onpage_upd selected cannot be a TOMBSTONE and the update
         * newer to a TOMBSTONE must be a full update.
         *
         * The algorithm walks from the oldest update, or the most recently inserted into history
         * store update. To the newest update and build full updates along the way. It sets the stop
         * time pair of the update to the start time pair of the next update, squashes the updates
         * that are from the same transaction and of the same start timestamp, calculates reverse
         * modification if prev_upd is a MODIFY, and inserts the update to the history store.
         *
         * It deals with the following scenarios:
         * 1) We only have full updates on the chain and we only insert full updates to
         * the history store.
         * 2) We have modifies on the chain, i.e., U (selected onpage value) -> M -> M ->U. We
         * reverse the modifies and insert the reversed modifies to the history store if it is not
         * the newest update written to the history store and the reverse operation is successful.
         * With regard to the example, we insert U -> RM -> U to the history store.
         * 3) We have tombstones in the middle of the chain, i.e.
         * U (selected onpage value) -> U -> T -> M -> U.
         * We write the stop time pair of M with the start time pair of the tombstone and skip the
         * tombstone.
         * 4) We have a tombstone at the end of the chain with transaction id WT_TXN_NONE
         * and start timestamp WT_TS_NONE, it is simply ignored.
         */
        for (; upd != NULL; upd = upd->next) {
            if (upd->txnid == WT_TXN_ABORTED)
                continue;
            WT_ERR(__wt_modify_vector_push(&modifies, upd));
            /*
             * If we've reached a full update and its in the history store we don't need to continue
             * as anything beyond this point won't help with calculating deltas.
             */
            if (upd->type == WT_UPDATE_STANDARD && F_ISSET(upd, WT_UPDATE_HS))
                break;
        }

        upd = NULL;

        /*
         * Get the oldest full update on chain. It is either the oldest update or the second oldest
         * update if the oldest update is a TOMBSTONE.
         */
        WT_ASSERT(session, modifies.size > 0);
        __wt_modify_vector_pop(&modifies, &upd);
        /* Skip TOMBSTONE at the end of the update chain. */
        if (upd->type == WT_UPDATE_TOMBSTONE) {
            if (modifies.size > 0) {
                __wt_modify_vector_pop(&modifies, &upd);
            } else
                continue;
        }

        /* The key didn't exist back then, which is globally visible. */
        WT_ASSERT(session, upd->type == WT_UPDATE_STANDARD);
        full_value->data = upd->data;
        full_value->size = upd->size;

        squashed = false;

        /*
         * Flush the updates on stack. Stopping once we run out or we reach the onpage upd start
         * time pair, we can squash modifies with the same start time pair as the onpage upd away.
         */
        for (; modifies.size > 0 &&
             !(upd->txnid == list->onpage_upd->txnid &&
                 upd->start_ts == list->onpage_upd->start_ts);
             tmp = full_value, full_value = prev_full_value, prev_full_value = tmp,
             upd = prev_upd) {
            WT_ASSERT(session, upd->type == WT_UPDATE_STANDARD || upd->type == WT_UPDATE_MODIFY);

            __wt_modify_vector_pop(&modifies, &prev_upd);
            stop_ts_pair.timestamp = prev_upd->start_ts;
            stop_ts_pair.txnid = prev_upd->txnid;

            if (prev_upd->type == WT_UPDATE_TOMBSTONE) {
                WT_ASSERT(session, modifies.size > 0);
                __wt_modify_vector_pop(&modifies, &prev_upd);

                /* The update newer to a TOMBSTONE must be a full update. */
                WT_ASSERT(session, prev_upd->type == WT_UPDATE_STANDARD);
            }

            if (prev_upd->type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_buf_set(session, prev_full_value, full_value->data, full_value->size));
                WT_ERR(__wt_modify_apply_item(session, prev_full_value, prev_upd->data, false));
            } else {
                WT_ASSERT(session, prev_upd->type == WT_UPDATE_STANDARD);

                prev_full_value->data = prev_upd->data;
                prev_full_value->size = prev_upd->size;
            }

            /*
             * Skip the updates have the same start timestamp and transaction id
             *
             * Modifies that have the same start time pair as the onpage_upd can be squashed away.
             */
            if (upd->start_ts != prev_upd->start_ts || upd->txnid != prev_upd->txnid) {
                /*
                 * Calculate reverse delta. Insert full update for the newest historical record even
                 * it's a MODIFY.
                 *
                 * It is not correct to check prev_upd == list->onpage_upd as we may have aborted
                 * updates in the middle.
                 */
                nentries = MAX_REVERSE_MODIFY_NUM;
                if (!F_ISSET(upd, WT_UPDATE_HS)) {
                    if (upd->type == WT_UPDATE_MODIFY &&
                      __wt_calc_modify(session, prev_full_value, full_value,
                        prev_full_value->size / 10, entries, &nentries) == 0) {
                        WT_ERR(__wt_modify_pack(cursor, entries, nentries, &modify_value));
                        WT_ERR(__hs_insert_record(session, cursor, btree_id, key, upd,
                          WT_UPDATE_MODIFY, modify_value, stop_ts_pair));
                        __wt_scr_free(session, &modify_value);
                    } else {
                        WT_ERR(__hs_insert_record(session, cursor, btree_id, key, upd,
                          WT_UPDATE_STANDARD, full_value, stop_ts_pair));
                        /*
                         * If we are evicting, we can now free older updates which have already been
                         * written to the history store. However we can only free updates after an
                         * original full update is inserted as a full update.
                         */
                        if (F_ISSET(r, WT_REC_EVICT) && upd->type == WT_UPDATE_STANDARD)
                            __wt_free_update_list(session, &upd->next);
                    }

                    /* Flag the update as now in the history store. */
                    F_SET(upd, WT_UPDATE_HS);
                    ++insert_cnt;
                    if (squashed) {
                        WT_STAT_CONN_INCR(session, cache_hs_write_squash);
                        squashed = false;
                    }
                }
            } else
                squashed = true;
        }

        if (modifies.size > 0)
            WT_STAT_CONN_INCR(session, cache_hs_write_squash);

        /*
         * If we are evicting, we can now free older updates since they have already been written to
         * the history store and can't be rolled back anyway.
         */
        if (F_ISSET(r, WT_REC_EVICT))
            __wt_free_update_list(session, &upd->next);
    }

    WT_ERR(__wt_block_manager_named_size(session, WT_HS_FILE, &hs_size));
    WT_STAT_CONN_SET(session, cache_hs_ondisk, hs_size);
    max_hs_size = ((WT_CURSOR_BTREE *)cursor)->btree->file_max;
    if (max_hs_size != 0 && (uint64_t)hs_size > max_hs_size)
        WT_PANIC_ERR(session, WT_PANIC, "WiredTigerHS: file size of %" PRIu64
                                        " exceeds maximum "
                                        "size %" PRIu64,
          (uint64_t)hs_size, max_hs_size);

err:
    /* Resolve the transaction. */
    if (local_txn) {
        /* We commit the HS updates irrespective of a failure, because HS updates can't rollback. */
        ret = __wt_txn_commit(session, NULL);
        F_CLR(cursor, WT_CURSTD_UPDATE_LOCAL);
    }

    if (ret == 0 && insert_cnt > 0)
        __hs_insert_updates_verbose(session, btree);

    __wt_scr_free(session, &key);
    /* modify_value is allocated in __wt_modify_pack. Free it if it is allocated. */
    if (modify_value != NULL)
        __wt_scr_free(session, &modify_value);
    __wt_modify_vector_free(&modifies);
    __wt_scr_free(session, &full_value);
    __wt_scr_free(session, &prev_full_value);
    return (ret);
}

/*
 * __wt_hs_cursor_position --
 *     Position a history store cursor at the end of a set of updates for a given btree id, record
 *     key and timestamp. There may be no history store entries for the given btree id and record
 *     key if they have been removed by WT_CONNECTION::rollback_to_stable.
 */
int
__wt_hs_cursor_position(WT_SESSION_IMPL *session, WT_CURSOR *cursor, uint32_t btree_id,
  WT_ITEM *key, wt_timestamp_t timestamp)
{
    WT_ITEM hs_key;
    WT_TIME_PAIR hs_start, hs_stop;
    uint32_t hs_btree_id;
    int cmp, exact;

    /*
     * Because of the special visibility rules for the history store, a new key can appear in
     * between our search and the set of updates that we're interested in. Keep trying until we find
     * it.
     */
    for (;;) {
        cursor->set_key(cursor, btree_id, key, timestamp, WT_TXN_MAX, WT_TS_MAX, WT_TXN_MAX);
        WT_RET(cursor->search_near(cursor, &exact));
        if (exact > 0)
            WT_RET(cursor->prev(cursor));

        /*
         * Because of the special visibility rules for the history store, a new key can appear in
         * between our search and the set of updates we're interested in. Keep trying while we have
         * a key lower than we expect.
         *
         * There may be no history store entries for the given btree id and record key if they have
         * been removed by WT_CONNECTION::rollback_to_stable.
         */
        WT_CLEAR(hs_key);
        WT_RET(cursor->get_key(cursor, &hs_btree_id, &hs_key, &hs_start.timestamp, &hs_start.txnid,
          &hs_stop.timestamp, &hs_stop.txnid));
        if (hs_btree_id < btree_id)
            return (0);
        else if (hs_btree_id == btree_id) {
            WT_RET(__wt_compare(session, NULL, &hs_key, key, &cmp));
            if (cmp < 0)
                return (0);
            if (cmp == 0 && hs_start.timestamp <= timestamp)
                return (0);
        }
    }

    /* NOTREACHED */
}

/*
 * __hs_save_read_timestamp --
 *     Save the currently running transaction's read timestamp into a variable.
 */
static void
__hs_save_read_timestamp(WT_SESSION_IMPL *session, wt_timestamp_t *saved_timestamp)
{
    *saved_timestamp = session->txn.read_timestamp;
}

/*
 * __hs_restore_read_timestamp --
 *     Reset the currently running transaction's read timestamp with a previously saved one.
 */
static void
__hs_restore_read_timestamp(WT_SESSION_IMPL *session, wt_timestamp_t saved_timestamp)
{
    session->txn.read_timestamp = saved_timestamp;
}

/*
 * __wt_find_hs_upd --
 *     Scan the history store for a record the btree cursor wants to position on. Create an update
 *     for the record and return to the caller. The caller may choose to optionally allow prepared
 *     updates to be returned regardless of whether prepare is being ignored globally. Otherwise, a
 *     prepare conflict will be returned upon reading a prepared update.
 */
int
__wt_find_hs_upd(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE **updp,
  bool allow_prepare, WT_ITEM *on_disk_buf, WT_TIME_PAIR *on_disk_start)
{
    WT_CURSOR *hs_cursor;
    WT_DECL_ITEM(hs_key);
    WT_DECL_ITEM(hs_value);
    WT_DECL_ITEM(orig_hs_value_buf);
    WT_DECL_RET;
    WT_ITEM *key, _key;
    WT_MODIFY_VECTOR modifies;
    WT_TIME_PAIR hs_start, hs_start_tmp, hs_stop, hs_stop_tmp;
    WT_TXN *txn;
    WT_UPDATE *mod_upd, *upd;
    wt_timestamp_t durable_timestamp, durable_timestamp_tmp, read_timestamp, saved_timestamp;
    size_t notused, size;
    uint64_t recno;
    uint32_t hs_btree_id, session_flags;
    uint8_t prepare_state, prepare_state_tmp, *p, recno_key[WT_INTPACK64_MAXSIZE], upd_type;
    const uint8_t *recnop;
    int cmp;
    bool modify;

    *updp = NULL;
    hs_cursor = NULL;
    key = NULL;
    mod_upd = upd = NULL;
    orig_hs_value_buf = NULL;
    __wt_modify_vector_init(session, &modifies);
    txn = &session->txn;
    __hs_save_read_timestamp(session, &saved_timestamp);
    notused = size = 0;
    hs_btree_id = S2BT(session)->id;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    WT_NOT_READ(modify, false);

    /* Row-store has the key available, create the column-store key on demand. */
    switch (cbt->btree->type) {
    case BTREE_ROW:
        key = &cbt->iface.key;
        break;
    case BTREE_COL_FIX:
    case BTREE_COL_VAR:
        p = recno_key;
        WT_RET(__wt_vpack_uint(&p, 0, cbt->recno));
        WT_CLEAR(_key);
        _key.data = recno_key;
        _key.size = WT_PTRDIFF(p, recno_key);
        key = &_key;
    }

    /* Allocate buffers for the history store key/value. */
    WT_ERR(__wt_scr_alloc(session, 0, &hs_key));
    WT_ERR(__wt_scr_alloc(session, 0, &hs_value));

    /* Open a history store table cursor. */
    WT_ERR(__wt_hs_cursor(session, &session_flags));
    hs_cursor = session->hs_cursor;

    /*
     * After positioning our cursor, we're stepping backwards to find the correct update. Since the
     * timestamp is part of the key, our cursor needs to go from the newest record (further in the
     * las) to the oldest (earlier in the las) for a given key.
     */
    read_timestamp = allow_prepare ? txn->prepare_timestamp : txn->read_timestamp;
    ret = __wt_hs_cursor_position(session, hs_cursor, hs_btree_id, key, read_timestamp);
    for (; ret == 0; ret = hs_cursor->prev(hs_cursor)) {
        WT_ERR(hs_cursor->get_key(hs_cursor, &hs_btree_id, hs_key, &hs_start.timestamp,
          &hs_start.txnid, &hs_stop.timestamp, &hs_stop.txnid));

        /* Stop before crossing over to the next btree */
        if (hs_btree_id != S2BT(session)->id)
            break;

        /*
         * Keys are sorted in an order, skip the ones before the desired key, and bail out if we
         * have crossed over the desired key and not found the record we are looking for.
         */
        WT_ERR(__wt_compare(session, NULL, hs_key, key, &cmp));
        if (cmp != 0)
            break;

        /*
         * It is safe to assume that we're reading the updates newest to the oldest. We can quit
         * searching after finding the newest visible record.
         */
        if (!__wt_txn_visible(session, hs_start.txnid, hs_start.timestamp))
            continue;

        WT_ERR(
          hs_cursor->get_value(hs_cursor, &durable_timestamp, &prepare_state, &upd_type, hs_value));

        /* We do not have prepared updates in the history store anymore */
        WT_ASSERT(session, prepare_state != WT_PREPARE_INPROGRESS);

        /*
         * Found a visible record, return success unless it is prepared and we are not ignoring
         * prepared.
         *
         * It's necessary to explicitly signal a prepare conflict so that the callers don't fallback
         * to using something from the update list.
         *
         * FIXME-PM-1521: review the code in future
         */
        if (prepare_state == WT_PREPARE_INPROGRESS &&
          !F_ISSET(&session->txn, WT_TXN_IGNORE_PREPARE) && !allow_prepare) {
            ret = WT_PREPARE_CONFLICT;
            break;
        }

        /* We do not have birthmarks and tombstones in the history store anymore. */
        WT_ASSERT(session, upd_type != WT_UPDATE_BIRTHMARK && upd_type != WT_UPDATE_TOMBSTONE);

        /*
         * Keep walking until we get a non-modify update. Once we get to that point, squash the
         * updates together.
         */
        if (upd_type == WT_UPDATE_MODIFY) {
            WT_NOT_READ(modify, true);
            /* Store this so that we don't have to make a special case for the first modify. */
            hs_stop_tmp.timestamp = hs_stop.timestamp;
            while (upd_type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_update_alloc(session, hs_value, &mod_upd, &notused, upd_type));
                WT_ERR(__wt_modify_vector_push(&modifies, mod_upd));
                mod_upd = NULL;

                /*
                 * Each entry in the lookaside is written with the actual start and stop time pair
                 * embedded in the key. In order to traverse a sequence of modifies, we're going to
                 * have to manipulate our read timestamp to see records we wouldn't otherwise be
                 * able to see.
                 *
                 * In this case, we want to read the next update in the chain meaning that its start
                 * timestamp should be equivalent to the stop timestamp of the record that we're
                 * currently on.
                 */
                session->txn.read_timestamp = hs_stop_tmp.timestamp;

                /*
                 * Find the base update to apply the reverse deltas. If our cursor next fails to
                 * find an update here we fall back to the datastore version. If its timestamp
                 * doesn't match our timestamp then we return not found.
                 */
                if ((ret = hs_cursor->next(hs_cursor)) == WT_NOTFOUND)
                {
                    if (hs_stop_tmp.timestamp == on_disk_start->timestamp) {
                        /* Set the history value to be the full value from the data store. */
                        orig_hs_value_buf = hs_value;
                        hs_value = on_disk_buf;
                        ret = 0;
                        upd_type = WT_UPDATE_STANDARD;
                        break;
                    } else
                        WT_ERR(ret);
                }
                hs_start_tmp.timestamp = WT_TS_NONE;
                hs_start_tmp.txnid = WT_TXN_NONE;
                /*
                 * Make sure we use the temporary variants of these variables. We need to retain the
                 * timestamps of the original modify we saw.
                 *
                 * We keep looking back into history store until we find a base update to apply the
                 * reverse deltas on top of.
                 */
                WT_ERR(hs_cursor->get_key(hs_cursor, &hs_btree_id, hs_key, &hs_start_tmp.timestamp,
                          &hs_start_tmp.txnid, &hs_stop_tmp.timestamp, &hs_stop_tmp.txnid));

                WT_ERR(__wt_compare(session, NULL, hs_key, key, &cmp));

                WT_ERR(hs_cursor->get_value(
                  hs_cursor, &durable_timestamp_tmp, &prepare_state_tmp, &upd_type, hs_value));
            }

            WT_ASSERT(session, upd_type == WT_UPDATE_STANDARD);
            while (modifies.size > 0) {
                __wt_modify_vector_pop(&modifies, &mod_upd);
                WT_ERR(__wt_modify_apply_item(session, hs_value, mod_upd->data, false));
                __wt_free_update_list(session, &mod_upd);
                mod_upd = NULL;
            }
            /* After we're done looping over modifies, reset the read timestamp. */
            __hs_restore_read_timestamp(session, saved_timestamp);
            WT_STAT_CONN_INCR(session, cache_hs_read_squash);
        }

        /* Allocate an update structure for the record found. */
        WT_ERR(__wt_update_alloc(session, hs_value, &upd, &size, upd_type));
        upd->txnid = hs_start.txnid;
        upd->durable_ts = durable_timestamp;
        upd->start_ts = hs_start.timestamp;
        upd->prepare_state = prepare_state;

        /*
         * When we find a prepared update in the history store, we should add it to our update list
         * and subsequently delete the corresponding history store entry. If it gets committed, the
         * timestamp in the las key may differ so it's easier if we get rid of it now and rewrite
         * the entry on eviction/commit/rollback.
         *
         * FIXME-PM-1521: review the code in future
         */
        if (prepare_state == WT_PREPARE_INPROGRESS) {
            WT_ASSERT(session, !modify);
            switch (cbt->ref->page->type) {
            case WT_PAGE_COL_FIX:
            case WT_PAGE_COL_VAR:
                recnop = hs_key->data;
                WT_ERR(__wt_vunpack_uint(&recnop, 0, &recno));
                WT_ERR(__wt_col_modify(cbt, recno, NULL, upd, WT_UPDATE_STANDARD, false));
                break;
            case WT_PAGE_ROW_LEAF:
                WT_ERR(__wt_row_modify(cbt, hs_key, NULL, upd, WT_UPDATE_STANDARD, false));
                break;
            }

            ret = hs_cursor->remove(hs_cursor);
            if (ret != 0)
                WT_PANIC_ERR(session, ret,
                  "initialised prepared update but was unable to remove the corresponding entry "
                  "from hs");

            /* This is going in our update list so it should be accounted for in cache usage. */
            __wt_cache_page_inmem_incr(session, cbt->ref->page, size);
        } else
            /*
             * We're not keeping this in our update list as we want to get rid of it after the read
             * has been dealt with. Mark this update as external and to be discarded when not
             * needed.
             */
            F_SET(upd, WT_UPDATE_RESTORED_FROM_DISK);
        *updp = upd;

        /* We are done, we found the record we were searching for */
        break;
    }
    WT_ERR_NOTFOUND_OK(ret);

err:
    if (orig_hs_value_buf != NULL)
        __wt_scr_free(session, &orig_hs_value_buf);
    else
        __wt_scr_free(session, &hs_value);
    __wt_scr_free(session, &hs_key);

    /*
     * Restore the read timestamp if we encountered an error while processing a modify. There's no
     * harm in doing this multiple times.
     */
    __hs_restore_read_timestamp(session, saved_timestamp);
    WT_TRET(__wt_hs_cursor_close(session, session_flags));

    __wt_free_update_list(session, &mod_upd);
    while (modifies.size > 0) {
        __wt_modify_vector_pop(&modifies, &upd);
        __wt_free_update_list(session, &upd);
    }
    __wt_modify_vector_free(&modifies);

    if (ret == 0) {
        /* Couldn't find a record. */
        if (upd == NULL) {
            ret = WT_NOTFOUND;
            WT_STAT_CONN_INCR(session, cache_hs_read_miss);
        } else {
            WT_STAT_CONN_INCR(session, cache_hs_read);
            WT_STAT_DATA_INCR(session, cache_hs_read);
        }
    }

    WT_ASSERT(session, upd != NULL || ret != 0);

    return (ret);
}
