/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_CHECK_RECOVERY_FLAG_TS_TXNID(session, txnid, durablets)           \
    (durablets == WT_TS_NONE && F_ISSET(S2C(session), WT_CONN_RECOVERING) && \
      (txnid) >= S2C(session)->recovery_ckpt_snap_min)

/* Enable rollback to stable verbose messaging during recovery. */
#define WT_VERB_RECOVERY_RTS(session) \
    (F_ISSET(S2C(session), WT_CONN_RECOVERING) ? WT_VERB_RECOVERY | WT_VERB_RTS : WT_VERB_RTS)

/*
 * __rollback_abort_update --
 *     Abort updates in an update change with timestamps newer than the rollback timestamp. Also,
 *     clear the history store flag for the first stable update in the update.
 */
static void
__rollback_abort_update(WT_SESSION_IMPL *session, WT_UPDATE *first_upd,
  wt_timestamp_t rollback_timestamp, bool *stable_update_found)
{
    WT_UPDATE *upd;
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    *stable_update_found = false;
    for (upd = first_upd; upd != NULL; upd = upd->next) {
        /* Skip the updates that are aborted. */
        if (upd->txnid == WT_TXN_ABORTED) {
            if (upd == first_upd)
                first_upd = upd->next;
        } else if (rollback_timestamp < upd->durable_ts ||
          upd->prepare_state == WT_PREPARE_INPROGRESS) {
            /*
             * If any updates are aborted, all newer updates better be aborted as well.
             *
             * Timestamp ordering relies on the validations at the time of commit. Thus if the table
             * is not configured for key consistency check, the timestamps could be out of order
             * here.
             */
            WT_ASSERT(session,
              !F_ISSET(session->dhandle, WT_DHANDLE_TS_KEY_CONSISTENT) || upd == first_upd);
            first_upd = upd->next;

            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "rollback to stable update aborted with txnid: %" PRIu64
              " durable timestamp: %s and stable timestamp: %s, prepared: %s",
              upd->txnid, __wt_timestamp_to_string(upd->durable_ts, ts_string[0]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[1]),
              rollback_timestamp < upd->durable_ts ? "false" : "true");

            upd->txnid = WT_TXN_ABORTED;
            WT_STAT_CONN_INCR(session, txn_rts_upd_aborted);
            upd->durable_ts = upd->start_ts = WT_TS_NONE;
        } else {
            /* Valid update is found. */
            WT_ASSERT(session, first_upd == upd);
            break;
        }
    }

    /*
     * Clear the history store flag for the stable update to indicate that this update should not be
     * written into the history store later, when all the aborted updates are removed from the
     * history store. The next time when this update is moved into the history store, it will have a
     * different stop time point.
     */
    if (first_upd != NULL) {
        F_CLR(first_upd, WT_UPDATE_HS);
        *stable_update_found = true;
    }
}

/*
 * __rollback_abort_col_insert_list --
 *     Apply the update abort check to each entry in an insert skip list.
 */
static void
__rollback_abort_col_insert_list(WT_SESSION_IMPL *session, WT_INSERT_HEAD *head,
  wt_timestamp_t rollback_timestamp, bool *stable_update_found)
{
    WT_INSERT *ins;
    WT_SKIP_FOREACH (ins, head)
        if (ins->upd != NULL)
            __rollback_abort_update(session, ins->upd, rollback_timestamp, stable_update_found);
}

/*
 * __rollback_abort_insert_list --
 *     Apply the update abort check to each entry in an insert skip list.
 */
static void
__rollback_abort_insert_list(
  WT_SESSION_IMPL *session, WT_INSERT_HEAD *head, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT *ins;
    bool stable_update_found;

    WT_SKIP_FOREACH (ins, head)
        if (ins->upd != NULL)
            __rollback_abort_update(session, ins->upd, rollback_timestamp, &stable_update_found);
}

/*
 * __rollback_col_modify --
 *     Add the provided update to the head of the update list.
 */
static inline int
__rollback_col_modify(WT_SESSION_IMPL *session, WT_REF *ref, WT_UPDATE *upd, uint64_t recno)
{
    WT_CURSOR_BTREE cbt;
    WT_DECL_RET;

    __wt_btcur_init(session, &cbt);
    __wt_btcur_open(&cbt);

    /* Search the page. */
    WT_ERR(__wt_col_search(&cbt, recno, ref, true, NULL));

    /* Apply the modification. */
    WT_ERR(__wt_col_modify(&cbt, recno, NULL, upd, WT_UPDATE_INVALID, true));

err:
    /* Free any resources that may have been cached in the cursor. */
    WT_TRET(__wt_btcur_close(&cbt, true));

    return (ret);
}

/*
 * __rollback_row_modify --
 *     Add the provided update to the head of the update list.
 */
static inline int
__rollback_row_modify(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip, WT_UPDATE *upd)
{
    WT_DECL_RET;
    WT_PAGE_MODIFY *mod;
    WT_UPDATE *last_upd, *old_upd, **upd_entry;
    size_t upd_size;

    last_upd = NULL;
    /* If we don't yet have a modify structure, we'll need one. */
    WT_RET(__wt_page_modify_init(session, page));
    mod = page->modify;

    /* Allocate an update array as necessary. */
    WT_PAGE_ALLOC_AND_SWAP(session, page, mod->mod_row_update, upd_entry, page->entries);

    /* Set the WT_UPDATE array reference. */
    upd_entry = &mod->mod_row_update[WT_ROW_SLOT(page, rip)];
    upd_size = __wt_update_list_memsize(upd);

    /* If there are existing updates, append them after the new updates. */
    for (last_upd = upd; last_upd->next != NULL; last_upd = last_upd->next)
        ;
    last_upd->next = *upd_entry;

    /*
     * We can either put a tombstone plus an update or a single update on the update chain.
     *
     * Set the "old" entry to the second update in the list so that the serialization function
     * succeeds in swapping the first update into place.
     */
    if (upd->next != NULL)
        *upd_entry = upd->next;
    old_upd = *upd_entry;

    /*
     * Point the new WT_UPDATE item to the next element in the list. The serialization function acts
     * as our memory barrier to flush this write.
     */
    upd->next = old_upd;

    /*
     * Serialize the update. Rollback to stable doesn't need to check the visibility of the on page
     * value to detect conflict.
     */
    WT_ERR(__wt_update_serial(session, NULL, page, upd_entry, &upd, upd_size, true));

    if (0) {
err:
        if (last_upd != NULL)
            last_upd->next = NULL;
    }

    return (ret);
}

/*
 * __rollback_check_if_txnid_non_committed --
 *     Check if the transaction id is non committed.
 */
static bool
__rollback_check_if_txnid_non_committed(WT_SESSION_IMPL *session, uint64_t txnid)
{
    WT_CONNECTION_IMPL *conn;
    bool found;

    conn = S2C(session);

    /* If not recovery then assume all the data as committed. */
    if (!F_ISSET(conn, WT_CONN_RECOVERING))
        return (false);

    /*
     * Only full checkpoint writes the metadata with snapshot. If the recovered checkpoint snapshot
     * details are zero then return false i.e, updates are committed.
     */
    if (conn->recovery_ckpt_snap_min == 0 && conn->recovery_ckpt_snap_max == 0)
        return (false);

    /*
     * Snapshot data:
     *	ids < recovery_ckpt_snap_min are committed,
     *	ids > recovery_ckpt_snap_max are non committed,
     *	everything else is committed unless it is found in the recovery_ckpt_snapshot array.
     */
    if (txnid < conn->recovery_ckpt_snap_min)
        return (false);
    else if (txnid > conn->recovery_ckpt_snap_max)
        return (true);

    /*
     * Return false when the recovery snapshot count is 0, which means there is no uncommitted
     * transaction ids.
     */
    if (conn->recovery_ckpt_snapshot_count == 0)
        return (false);

    WT_BINARY_SEARCH(
      txnid, conn->recovery_ckpt_snapshot, conn->recovery_ckpt_snapshot_count, found);

    return (found);
}

/*
 * __rollback_ondisk_fixup_key --
 *     Abort updates in the history store and replace the on-disk value with an update that
 *     satisfies the given timestamp.
 */
static int
__rollback_ondisk_fixup_key(WT_SESSION_IMPL *session, WT_REF *ref, WT_PAGE *page, WT_COL *cip,
  WT_ROW *rip, wt_timestamp_t rollback_timestamp, bool replace, uint64_t recno)
{
    WT_CELL *kcell;
    WT_CELL_UNPACK_KV *unpack, _unpack;
    WT_CURSOR *hs_cursor;
    WT_DECL_ITEM(hs_key);
    WT_DECL_ITEM(hs_value);
    WT_DECL_ITEM(key);
    WT_DECL_RET;
    WT_ITEM full_value;
    WT_TIME_WINDOW *hs_tw;
    WT_UPDATE *tombstone, *upd;
    wt_timestamp_t hs_durable_ts, hs_start_ts, hs_stop_durable_ts, newer_hs_durable_ts;
    uint64_t hs_counter, type_full;
    uint32_t hs_btree_id;
    uint8_t *memp;
    uint8_t type;
    char ts_string[4][WT_TS_INT_STRING_SIZE];
    bool valid_update_found;
#ifdef HAVE_DIAGNOSTIC
    bool first_record;
#endif

    /*
     * Assert an exclusive or for rip and cip such that either only a cip for a column store or a
     * rip for a row store are passed into the function.
     */
    WT_ASSERT(session, (rip != NULL && cip == NULL) || (rip == NULL && cip != NULL));

    if (page == NULL) {
        WT_ASSERT(session, ref != NULL);
        page = ref->page;
    }
    hs_cursor = NULL;
    tombstone = upd = NULL;
    hs_durable_ts = hs_start_ts = hs_stop_durable_ts = WT_TS_NONE;
    hs_btree_id = S2BT(session)->id;
    WT_CLEAR(full_value);
    valid_update_found = false;
#ifdef HAVE_DIAGNOSTIC
    first_record = true;
#endif

    /* Allocate buffers for the data store and history store key. */
    WT_ERR(__wt_scr_alloc(session, 0, &hs_key));
    WT_ERR(__wt_scr_alloc(session, 0, &hs_value));

    if (rip != NULL) {
        /* Unpack a row cell. */
        WT_ERR(__wt_scr_alloc(session, 0, &key));
        WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));

        /* Get the full update value from the data store. */
        unpack = &_unpack;
        __wt_row_leaf_value_cell(session, page, rip, unpack);
    } else {
        /* Unpack a column cell. */
        WT_ERR(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

        /* Get the full update value from the data store. */
        unpack = &_unpack;
        kcell = WT_COL_PTR(page, cip);
        __wt_cell_unpack_kv(session, page->dsk, kcell, unpack);
        memp = key->mem;
        WT_ERR(__wt_vpack_uint(&memp, 0, recno));
        key->size = WT_PTRDIFF(memp, key->data);
    }

    WT_ERR(__wt_page_cell_data_ref(session, page, unpack, &full_value));
    WT_ERR(__wt_buf_set(session, &full_value, full_value.data, full_value.size));
    newer_hs_durable_ts = unpack->tw.durable_start_ts;

    /* Open a history store table cursor. */
    WT_ERR(__wt_curhs_open(session, NULL, &hs_cursor));
    /*
     * Rollback-to-stable operates exclusively (i.e., it is the only active operation in the system)
     * outside the constraints of transactions. Therefore, there is no need for snapshot based
     * visibility checks.
     */
    F_SET(hs_cursor, WT_CURSTD_HS_READ_COMMITTED);

    /*
     * Scan the history store for the given btree and key with maximum start timestamp to let the
     * search point to the last version of the key and start traversing backwards to find out the
     * satisfying record according the given timestamp. Any satisfying history store record is moved
     * into data store and removed from history store. If none of the history store records satisfy
     * the given timestamp, the key is removed from data store.
     */
    hs_cursor->set_key(hs_cursor, 4, hs_btree_id, key, WT_TS_MAX, UINT64_MAX);
    ret = __wt_curhs_search_near_before(session, hs_cursor);
    for (; ret == 0; ret = hs_cursor->prev(hs_cursor)) {
        WT_ERR(hs_cursor->get_key(hs_cursor, &hs_btree_id, hs_key, &hs_start_ts, &hs_counter));

        /* Get current value and convert to full update if it is a modify. */
        WT_ERR(hs_cursor->get_value(
          hs_cursor, &hs_stop_durable_ts, &hs_durable_ts, &type_full, hs_value));
        type = (uint8_t)type_full;

        /*
         * Do not include history store updates greater than on-disk data store version to construct
         * a full update to restore. Include the most recent updates than the on-disk version
         * shouldn't be problem as the on-disk version in history store is always a full update. It
         * is better to not to include those updates as it unnecessarily increases the rollback to
         * stable time.
         *
         * Comparing with timestamps here has no problem unlike in search flow where the timestamps
         * may be reset during reconciliation. RTS detects an on-disk update is unstable based on
         * the written proper timestamp, so comparing against it with history store shouldn't have
         * any problem.
         */
        if (hs_start_ts <= unpack->tw.start_ts) {
            if (type == WT_UPDATE_MODIFY)
                WT_ERR(__wt_modify_apply_item(
                  session, S2BT(session)->value_format, &full_value, hs_value->data));
            else {
                WT_ASSERT(session, type == WT_UPDATE_STANDARD);
                WT_ERR(__wt_buf_set(session, &full_value, hs_value->data, hs_value->size));
            }
        } else
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "history store update more recent than on-disk update with start timestamp: %s,"
              " durable timestamp: %s, stop timestamp: %s and type: %" PRIu8,
              __wt_timestamp_to_string(hs_start_ts, ts_string[0]),
              __wt_timestamp_to_string(hs_durable_ts, ts_string[1]),
              __wt_timestamp_to_string(hs_stop_durable_ts, ts_string[2]), type);

        /*
         * Verify the history store timestamps are in order. The start timestamp may be equal to the
         * stop timestamp if the original update's commit timestamp is out of order. We may see
         * records newer than or equal to the onpage value if eviction runs concurrently with
         * checkpoint. In that case, don't verify the first record.
         *
         * If we have fixed the out-of-order timestamps, then the newer update reinserted with an
         * older timestamp may have a durable timestamp that is smaller than the current stop
         * durable timestamp.
         */
        WT_ASSERT(session,
          hs_stop_durable_ts <= newer_hs_durable_ts || hs_start_ts == hs_stop_durable_ts ||
            hs_start_ts == newer_hs_durable_ts || first_record);

        if (hs_stop_durable_ts < newer_hs_durable_ts)
            WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_stop_older_than_newer_start);

        /*
         * Stop processing when we find the newer version value of this key is stable according to
         * the current version stop timestamp and transaction id when it is not appending the
         * selected update to the update chain. Also it confirms that history store doesn't contains
         * any newer version than the current version for the key.
         */
        /* Retrieve the time window from the history cursor. */
        __wt_hs_upd_time_window(hs_cursor, &hs_tw);
        if (!replace &&
          (hs_stop_durable_ts != WT_TS_NONE ||
            !__rollback_check_if_txnid_non_committed(session, hs_tw->stop_txn)) &&
          (hs_stop_durable_ts <= rollback_timestamp)) {
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "history store update valid with stop timestamp: %s, stable timestamp: %s, txnid: "
              "%" PRIu64 " and type: %" PRIu8,
              __wt_timestamp_to_string(hs_stop_durable_ts, ts_string[0]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[1]), hs_tw->stop_txn, type);
            break;
        }

        /*
         * Stop processing when we find a stable update according to the given timestamp and
         * transaction id.
         */
        if ((hs_durable_ts != WT_TS_NONE ||
              !__rollback_check_if_txnid_non_committed(session, hs_tw->start_txn)) &&
          (hs_durable_ts <= rollback_timestamp)) {
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "history store update valid with start timestamp: %s, durable timestamp: %s, stop "
              "timestamp: %s, stable timestamp: %s, txnid: %" PRIu64 " and type: %" PRIu8,
              __wt_timestamp_to_string(hs_start_ts, ts_string[0]),
              __wt_timestamp_to_string(hs_durable_ts, ts_string[1]),
              __wt_timestamp_to_string(hs_stop_durable_ts, ts_string[2]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[3]), hs_tw->start_txn, type);
            WT_ASSERT(session, hs_tw->start_ts <= unpack->tw.start_ts);
            valid_update_found = true;
            break;
        }

        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "history store update aborted with start timestamp: %s, durable timestamp: %s, stop "
          "timestamp: %s, stable timestamp: %s, start txnid: %" PRIu64 ", stop txnid: %" PRIu64
          " and type: %" PRIu8,
          __wt_timestamp_to_string(hs_start_ts, ts_string[0]),
          __wt_timestamp_to_string(hs_durable_ts, ts_string[1]),
          __wt_timestamp_to_string(hs_stop_durable_ts, ts_string[2]),
          __wt_timestamp_to_string(rollback_timestamp, ts_string[3]), hs_tw->start_txn,
          hs_tw->stop_txn, type);

        /*
         * Start time point of the current record may be used as stop time point of the previous
         * record. Save it to verify against the previous record and check if we need to append the
         * stop time point as a tombstone when we rollback the history store record.
         */
        newer_hs_durable_ts = hs_durable_ts;
#ifdef HAVE_DIAGNOSTIC
        first_record = false;
#endif

        WT_ERR(hs_cursor->remove(hs_cursor));
        WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);
        WT_STAT_CONN_DATA_INCR(session, cache_hs_key_truncate_rts_unstable);
    }

    if (replace) {
        /*
         * If we found a history value that satisfied the given timestamp, add it to the update
         * list. Otherwise remove the key by adding a tombstone.
         */
        if (valid_update_found) {
            /* Retrieve the time window from the history cursor. */
            __wt_hs_upd_time_window(hs_cursor, &hs_tw);
            WT_ASSERT(session,
              hs_tw->start_ts < unpack->tw.start_ts || hs_tw->start_txn < unpack->tw.start_txn);
            WT_ERR(__wt_upd_alloc(session, &full_value, WT_UPDATE_STANDARD, &upd, NULL));

            /*
             * Set the transaction id of updates to WT_TXN_NONE when called from recovery, because
             * the connections write generation will be initialized after rollback to stable and the
             * updates in the cache will be problematic. The transaction id of pages which are in
             * disk will be automatically reset as part of unpacking cell when loaded to cache.
             */
            if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
                upd->txnid = WT_TXN_NONE;
            else
                upd->txnid = hs_tw->start_txn;
            upd->durable_ts = hs_tw->durable_start_ts;
            upd->start_ts = hs_tw->start_ts;
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "update restored from history store txnid: %" PRIu64
              ", start_ts: %s and durable_ts: %s",
              upd->txnid, __wt_timestamp_to_string(upd->start_ts, ts_string[0]),
              __wt_timestamp_to_string(upd->durable_ts, ts_string[1]));

            /*
             * Set the flag to indicate that this update has been restored from history store for
             * the rollback to stable operation.
             */
            F_SET(upd, WT_UPDATE_RESTORED_FROM_HS);
            WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_restore_updates);

            /*
             * We have a tombstone on the original update chain and it is behind the stable
             * timestamp, we need to restore that as well.
             */
            if (hs_stop_durable_ts <= rollback_timestamp &&
              hs_stop_durable_ts < newer_hs_durable_ts) {
                WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
                /*
                 * Set the transaction id of updates to WT_TXN_NONE when called from recovery,
                 * because the connections write generation will be initialized after rollback to
                 * stable and the updates in the cache will be problematic. The transaction id of
                 * pages which are in disk will be automatically reset as part of unpacking cell
                 * when loaded to cache.
                 */
                if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
                    tombstone->txnid = WT_TXN_NONE;
                else
                    tombstone->txnid = hs_tw->stop_txn;
                tombstone->durable_ts = hs_tw->durable_stop_ts;
                tombstone->start_ts = hs_tw->stop_ts;
                __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
                  "tombstone restored from history store txnid: %" PRIu64
                  ", start_ts: %s, durable_ts: %s",
                  tombstone->txnid, __wt_timestamp_to_string(tombstone->start_ts, ts_string[0]),
                  __wt_timestamp_to_string(tombstone->durable_ts, ts_string[1]));

                /*
                 * Set the flag to indicate that this update has been restored from history store
                 * for the rollback to stable operation.
                 */
                F_SET(tombstone, WT_UPDATE_RESTORED_FROM_HS);

                tombstone->next = upd;
                upd = tombstone;
                WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_restore_tombstones);
            }
        } else {
            WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
            WT_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session), "%p: key removed", (void *)key);
        }

        if (rip != NULL)
            WT_ERR(__rollback_row_modify(session, page, rip, upd));
        else
            WT_ERR(__rollback_col_modify(session, ref, upd, recno));
    }

    /* Finally remove that update from history store. */
    if (valid_update_found) {
        WT_ERR(hs_cursor->remove(hs_cursor));
        WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);
        WT_STAT_CONN_DATA_INCR(session, cache_hs_key_truncate_rts);
    }

    if (0) {
err:
        WT_ASSERT(session, tombstone == NULL || upd == tombstone);
        __wt_free_update_list(session, &upd);
    }
    __wt_scr_free(session, &hs_key);
    __wt_scr_free(session, &hs_value);
    __wt_scr_free(session, &key);
    __wt_buf_free(session, &full_value);
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));
    return (ret);
}

/*
 * __rollback_abort_ondisk_kv --
 *     Fix the on-disk K/V version according to the given timestamp.
 */
static int
__rollback_abort_ondisk_kv(WT_SESSION_IMPL *session, WT_REF *ref, WT_COL *cip, WT_ROW *rip,
  wt_timestamp_t rollback_timestamp, uint64_t recno)
{
    WT_CELL *kcell;
    WT_CELL_UNPACK_KV *vpack, _vpack;
    WT_DECL_RET;
    WT_ITEM buf;
    WT_PAGE *page;
    WT_UPDATE *upd;
    char ts_string[5][WT_TS_INT_STRING_SIZE];
    bool prepared;

    page = ref->page;
    vpack = &_vpack;
    WT_CLEAR(buf);
    upd = NULL;

    /*
     * Assert an exclusive or for rip and cip such that either only a cip for a column store or a
     * rip for a row store are passed into the function.
     */
    WT_ASSERT(session, (rip != NULL && cip == NULL) || (rip == NULL && cip != NULL));

    if (rip != NULL)
        __wt_row_leaf_value_cell(session, page, rip, vpack);
    else {
        kcell = WT_COL_PTR(page, cip);
        __wt_cell_unpack_kv(session, page->dsk, kcell, vpack);
    }

    prepared = vpack->tw.prepare;
    if (WT_IS_HS(session->dhandle)) {
        /*
         * Abort the history store update with stop durable timestamp greater than the stable
         * timestamp or the updates with max stop timestamp which implies that they are associated
         * with prepared transactions.
         */
        if (vpack->tw.durable_stop_ts > rollback_timestamp || vpack->tw.stop_ts == WT_TS_MAX) {
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "hs update aborted with start durable/commit timestamp: %s, %s, stop durable/commit "
              "timestamp: %s, %s and stable timestamp: %s",
              __wt_timestamp_to_string(vpack->tw.durable_start_ts, ts_string[0]),
              __wt_timestamp_to_string(vpack->tw.start_ts, ts_string[1]),
              __wt_timestamp_to_string(vpack->tw.durable_stop_ts, ts_string[2]),
              __wt_timestamp_to_string(vpack->tw.stop_ts, ts_string[3]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[4]));
            WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
            WT_STAT_CONN_DATA_INCR(session, txn_rts_sweep_hs_keys);
        } else
            return (0);
    } else if (((vpack->tw.durable_start_ts > rollback_timestamp) ||
                 (vpack->tw.durable_start_ts == WT_TS_NONE &&
                   __rollback_check_if_txnid_non_committed(session, vpack->tw.start_txn))) ||
      (!WT_TIME_WINDOW_HAS_STOP(&vpack->tw) && prepared)) {
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "on-disk update aborted with start durable timestamp: %s, commit timestamp: %s, "
          "prepared: %s, stable timestamp: %s and txnid : %" PRIu64,
          __wt_timestamp_to_string(vpack->tw.durable_start_ts, ts_string[0]),
          __wt_timestamp_to_string(vpack->tw.start_ts, ts_string[1]), prepared ? "true" : "false",
          __wt_timestamp_to_string(rollback_timestamp, ts_string[2]), vpack->tw.start_txn);
        if (!F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
            return (__rollback_ondisk_fixup_key(
              session, ref, NULL, cip, rip, rollback_timestamp, true, recno));
        else {
            /*
             * In-memory database don't have a history store to provide a stable update, so remove
             * the key.
             */
            WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
            WT_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
        }
    } else if (WT_TIME_WINDOW_HAS_STOP(&vpack->tw) &&
      (((vpack->tw.durable_stop_ts > rollback_timestamp) ||
         (vpack->tw.durable_stop_ts == WT_TS_NONE &&
           __rollback_check_if_txnid_non_committed(session, vpack->tw.stop_txn))) ||
        prepared)) {
        /*
         * Clear the remove operation from the key by inserting the original on-disk value as a
         * standard update.
         */
        WT_RET(__wt_page_cell_data_ref(session, page, vpack, &buf));

        /*
         * For prepared transactions, it is possible that both the on-disk key start and stop time
         * windows can be the same. To abort these updates, check for any stable update from history
         * store or remove the key.
         */
        if (vpack->tw.start_ts == vpack->tw.stop_ts &&
          vpack->tw.durable_start_ts == vpack->tw.durable_stop_ts &&
          vpack->tw.start_txn == vpack->tw.stop_txn) {
            WT_ASSERT(session, prepared == true);
            if (!F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
                return (__rollback_ondisk_fixup_key(
                  session, ref, NULL, cip, rip, rollback_timestamp, true, recno));
            else {
                /*
                 * In-memory database don't have a history store to provide a stable update, so
                 * remove the key.
                 */
                WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
                WT_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
            }
        } else {
            WT_ERR(__wt_upd_alloc(session, &buf, WT_UPDATE_STANDARD, &upd, NULL));
            /*
             * Set the transaction id of updates to WT_TXN_NONE when called from recovery, because
             * the connections write generation will be initialized after rollback to stable and the
             * updates in the cache will be problematic. The transaction id of pages which are in
             * disk will be automatically reset as part of unpacking cell when loaded to cache.
             */
            if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
                upd->txnid = WT_TXN_NONE;
            else
                upd->txnid = vpack->tw.start_txn;
            upd->durable_ts = vpack->tw.durable_start_ts;
            upd->start_ts = vpack->tw.start_ts;
            F_SET(upd, WT_UPDATE_RESTORED_FROM_DS);
            WT_STAT_CONN_DATA_INCR(session, txn_rts_keys_restored);
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "key restored with commit timestamp: %s, durable timestamp: %s, stable timestamp: "
              "%s, "
              "txnid: %" PRIu64
              " and removed commit timestamp: %s, durable timestamp: %s, txnid: %" PRIu64
              ", prepared: %s",
              __wt_timestamp_to_string(upd->start_ts, ts_string[0]),
              __wt_timestamp_to_string(upd->durable_ts, ts_string[1]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[2]), upd->txnid,
              __wt_timestamp_to_string(vpack->tw.stop_ts, ts_string[3]),
              __wt_timestamp_to_string(vpack->tw.durable_stop_ts, ts_string[4]), vpack->tw.stop_txn,
              prepared ? "true" : "false");
        }
    } else
        /* Stable version according to the timestamp. */
        return (0);

    if (rip != NULL)
        WT_ERR(__rollback_row_modify(session, page, rip, upd));
    else
        WT_ERR(__rollback_col_modify(session, ref, upd, recno));
    upd = NULL;

err:
    __wt_buf_free(session, &buf);
    __wt_free(session, upd);
    return (ret);
}

/*
 * __rollback_abort_col_var --
 *     Abort updates on a variable length col leaf page with timestamps newer than the rollback
 *     timestamp.
 */
static int
__rollback_abort_col_var(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_CELL *kcell;
    WT_CELL_UNPACK_KV unpack;
    WT_COL *cip;
    WT_INSERT_HEAD *ins;
    WT_PAGE *page;
    uint64_t recno, rle;
    uint32_t i, j;
    bool stable_update_found;

    page = ref->page;
    /*
     * If a disk image exists, start from the provided recno; or else start from 0.
     */
    if (page->dsk != NULL)
        recno = page->dsk->recno;
    else
        recno = 0;

    /* Review the changes to the original on-page data items. */
    WT_COL_FOREACH (page, cip, i) {
        stable_update_found = false;

        if ((ins = WT_COL_UPDATE(page, cip)) != NULL)
            __rollback_abort_col_insert_list(
              session, ins, rollback_timestamp, &stable_update_found);

        if (!stable_update_found && page->dsk != NULL) {
            kcell = WT_COL_PTR(page, cip);
            __wt_cell_unpack_kv(session, page->dsk, kcell, &unpack);
            rle = __wt_cell_rle(&unpack);
            for (j = 0; j < rle; j++)
                WT_RET(__rollback_abort_ondisk_kv(
                  session, ref, cip, NULL, rollback_timestamp, recno + j));
            recno += rle;
        } else {
            recno++;
        }
    }

    /* Review the append list */
    if ((ins = WT_COL_APPEND(page)) != NULL)
        __rollback_abort_insert_list(session, ins, rollback_timestamp);

    /* Mark the page as dirty to reconcile the page. */
    if (page->modify)
        __wt_page_modify_set(session, page);
    return (0);
}

/*
 * __rollback_abort_col_fix --
 *     Abort updates on a fixed length col leaf page with timestamps newer than the rollback
 *     timestamp.
 */
static void
__rollback_abort_col_fix(WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT_HEAD *ins;

    /* Review the changes to the original on-page data items */
    if ((ins = WT_COL_UPDATE_SINGLE(page)) != NULL)
        __rollback_abort_insert_list(session, ins, rollback_timestamp);

    /* Review the append list */
    if ((ins = WT_COL_APPEND(page)) != NULL)
        __rollback_abort_insert_list(session, ins, rollback_timestamp);

    /* Mark the page as dirty to reconcile the page. */
    if (page->modify)
        __wt_page_modify_set(session, page);
}

/*
 * __rollback_abort_row_reconciled_page_internal --
 *     Abort updates on a history store using the in-memory build reconciled page of data store.
 */
static int
__rollback_abort_row_reconciled_page_internal(WT_SESSION_IMPL *session, const void *image,
  const uint8_t *addr, size_t addr_size, wt_timestamp_t rollback_timestamp)
{
    WT_DECL_RET;
    WT_ITEM tmp;
    WT_PAGE *mod_page;
    WT_ROW *rip;
    uint32_t i, page_flags;
    const void *image_local;

    /*
     * Don't pass an allocated buffer to the underlying block read function, force allocation of new
     * memory of the appropriate size.
     */
    WT_CLEAR(tmp);

    mod_page = NULL;
    image_local = image;

    if (image_local == NULL) {
        WT_RET(__wt_bt_read(session, &tmp, addr, addr_size));
        image_local = tmp.data;
    }

    /* Don't free the passed image later. */
    page_flags = image != NULL ? 0 : WT_PAGE_DISK_ALLOC;
    WT_ERR(__wt_page_inmem(session, NULL, image_local, page_flags, &mod_page));
    tmp.mem = NULL;
    WT_ROW_FOREACH (mod_page, rip, i)
        WT_ERR_NOTFOUND_OK(__rollback_ondisk_fixup_key(
                             session, NULL, mod_page, NULL, rip, rollback_timestamp, false, 0),
          false);

err:
    if (mod_page != NULL)
        __wt_page_out(session, &mod_page);
    __wt_buf_free(session, &tmp);

    return (ret);
}

/*
 * __rollback_abort_row_reconciled_page --
 *     Abort updates on a history store using the reconciled pages of data store.
 */
static int
__rollback_abort_row_reconciled_page(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    uint32_t multi_entry;
    char ts_string[3][WT_TS_INT_STRING_SIZE];

    if ((mod = page->modify) == NULL)
        return (0);

    if (mod->rec_result == WT_PM_REC_REPLACE &&
      (mod->mod_replace.ta.newest_start_durable_ts > rollback_timestamp ||
        mod->mod_replace.ta.newest_stop_durable_ts > rollback_timestamp ||
        mod->mod_replace.ta.prepare)) {
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "reconciled replace block page history store update removal on-disk with start durable "
          "timestamp: %s, stop durable timestamp: %s and stable timestamp: %s",
          __wt_timestamp_to_string(mod->mod_replace.ta.newest_start_durable_ts, ts_string[0]),
          __wt_timestamp_to_string(mod->mod_replace.ta.newest_stop_durable_ts, ts_string[1]),
          __wt_timestamp_to_string(rollback_timestamp, ts_string[2]));

        /* Remove the history store newer updates. */
        if (!WT_IS_HS(session->dhandle))
            WT_RET(__rollback_abort_row_reconciled_page_internal(session, mod->u1.r.disk_image,
              mod->u1.r.replace.addr, mod->u1.r.replace.size, rollback_timestamp));

        /*
         * As this page has newer aborts that are aborted, make sure to mark the page as dirty to
         * let the reconciliation happens again on the page. Otherwise, the eviction may pick the
         * already reconciled page to write to disk with newer updates.
         */
        __wt_page_modify_set(session, page);
    } else if (mod->rec_result == WT_PM_REC_MULTIBLOCK) {
        for (multi = mod->mod_multi, multi_entry = 0; multi_entry < mod->mod_multi_entries;
             ++multi, ++multi_entry)
            if (multi->addr.ta.newest_start_durable_ts > rollback_timestamp ||
              multi->addr.ta.newest_stop_durable_ts > rollback_timestamp ||
              multi->addr.ta.prepare) {
                __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
                  "reconciled multi block page history store update removal on-disk with start "
                  "durable timestamp: %s, stop durable timestamp: %s and stable timestamp: %s",
                  __wt_timestamp_to_string(multi->addr.ta.newest_start_durable_ts, ts_string[0]),
                  __wt_timestamp_to_string(multi->addr.ta.newest_stop_durable_ts, ts_string[1]),
                  __wt_timestamp_to_string(rollback_timestamp, ts_string[2]));

                /* Remove the history store newer updates. */
                if (!WT_IS_HS(session->dhandle))
                    WT_RET(__rollback_abort_row_reconciled_page_internal(session, multi->disk_image,
                      multi->addr.addr, multi->addr.size, rollback_timestamp));

                /*
                 * As this page has newer aborts that are aborted, make sure to mark the page as
                 * dirty to let the reconciliation happens again on the page. Otherwise, the
                 * eviction may pick the already reconciled page to write to disk with newer
                 * updates.
                 */
                __wt_page_modify_set(session, page);
            }
    }

    return (0);
}

/*
 * __rollback_abort_row_leaf --
 *     Abort updates on a row leaf page with timestamps newer than the rollback timestamp.
 */
static int
__rollback_abort_row_leaf(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT_HEAD *insert;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_UPDATE *upd;
    uint32_t i;
    bool stable_update_found;

    page = ref->page;

    /*
     * Review the insert list for keys before the first entry on the disk page.
     */
    if ((insert = WT_ROW_INSERT_SMALLEST(page)) != NULL)
        __rollback_abort_insert_list(session, insert, rollback_timestamp);

    /*
     * Review updates that belong to keys that are on the disk image, as well as for keys inserted
     * since the page was read from disk.
     */
    WT_ROW_FOREACH (page, rip, i) {
        stable_update_found = false;
        if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
            __rollback_abort_update(session, upd, rollback_timestamp, &stable_update_found);

        if ((insert = WT_ROW_INSERT(page, rip)) != NULL)
            __rollback_abort_insert_list(session, insert, rollback_timestamp);

        /*
         * If there is no stable update found in the update list, abort any on-disk value.
         */
        if (!stable_update_found)
            WT_RET(__rollback_abort_ondisk_kv(session, ref, NULL, rip, rollback_timestamp, 0));
    }

    /*
     * If the configuration is not in-memory, abort history store updates from the reconciled pages
     * of data store.
     */
    if (!F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
        WT_RET(__rollback_abort_row_reconciled_page(session, page, rollback_timestamp));

    /* Mark the page as dirty to reconcile the page. */
    if (page->modify)
        __wt_page_modify_set(session, page);
    return (0);
}

/*
 * __rollback_get_ref_max_durable_timestamp --
 *     Returns the ref aggregated max durable timestamp. The max durable timestamp is calculated
 *     between both start and stop durable timestamps except for history store, because most of the
 *     history store updates have stop timestamp either greater or equal to the start timestamp
 *     except for the updates written for the prepared updates on the data store. To abort the
 *     updates with no stop timestamp, we must include the newest stop timestamp also into the
 *     calculation of maximum durable timestamp of the history store.
 */
static wt_timestamp_t
__rollback_get_ref_max_durable_timestamp(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta)
{
    if (WT_IS_HS(session->dhandle))
        return WT_MAX(ta->newest_stop_durable_ts, ta->newest_stop_ts);
    else
        return WT_MAX(ta->newest_start_durable_ts, ta->newest_stop_durable_ts);
}

/*
 * __rollback_page_needs_abort --
 *     Check whether the page needs rollback. Return true if the page has modifications newer than
 *     the given timestamp Otherwise return false.
 */
static bool
__rollback_page_needs_abort(
  WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_ADDR *addr;
    WT_CELL_UNPACK_ADDR vpack;
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    wt_timestamp_t durable_ts;
    uint64_t newest_txn;
    uint32_t i;
    char ts_string[WT_TS_INT_STRING_SIZE];
    const char *tag;
    bool prepared, result;

    addr = ref->addr;
    mod = ref->page == NULL ? NULL : ref->page->modify;
    durable_ts = WT_TS_NONE;
    newest_txn = WT_TXN_NONE;
    tag = "undefined state";
    prepared = result = false;

    /*
     * The rollback operation should be performed on this page when any one of the following is
     * greater than the given timestamp or during recovery if the newest transaction id on the page
     * is greater than or equal to recovered checkpoint snapshot min:
     * 1. The reconciled replace page max durable timestamp.
     * 2. The reconciled multi page max durable timestamp.
     * 3. The on page address max durable timestamp.
     * 4. The off page address max durable timestamp.
     */
    if (mod != NULL && mod->rec_result == WT_PM_REC_REPLACE) {
        tag = "reconciled replace block";
        durable_ts = __rollback_get_ref_max_durable_timestamp(session, &mod->mod_replace.ta);
        prepared = mod->mod_replace.ta.prepare;
        result = (durable_ts > rollback_timestamp) || prepared;
    } else if (mod != NULL && mod->rec_result == WT_PM_REC_MULTIBLOCK) {
        tag = "reconciled multi block";
        /* Calculate the max durable timestamp by traversing all multi addresses. */
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
            durable_ts = WT_MAX(
              durable_ts, __rollback_get_ref_max_durable_timestamp(session, &multi->addr.ta));
            if (multi->addr.ta.prepare)
                prepared = true;
        }
        result = (durable_ts > rollback_timestamp) || prepared;
    } else if (!__wt_off_page(ref->home, addr)) {
        tag = "on page cell";
        /* Check if the page is obsolete using the page disk address. */
        __wt_cell_unpack_addr(session, ref->home->dsk, (WT_CELL *)addr, &vpack);
        durable_ts = __rollback_get_ref_max_durable_timestamp(session, &vpack.ta);
        prepared = vpack.ta.prepare;
        newest_txn = vpack.ta.newest_txn;
        result = (durable_ts > rollback_timestamp) || prepared ||
          WT_CHECK_RECOVERY_FLAG_TS_TXNID(session, newest_txn, durable_ts);
    } else if (addr != NULL) {
        tag = "address";
        durable_ts = __rollback_get_ref_max_durable_timestamp(session, &addr->ta);
        prepared = addr->ta.prepare;
        newest_txn = addr->ta.newest_txn;
        result = (durable_ts > rollback_timestamp) || prepared ||
          WT_CHECK_RECOVERY_FLAG_TS_TXNID(session, newest_txn, durable_ts);
    }

    __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
      "%p: page with %s durable timestamp: %s, newest txn: %" PRIu64 " and prepared updates: %s",
      (void *)ref, tag, __wt_timestamp_to_string(durable_ts, ts_string), newest_txn,
      prepared ? "true" : "false");

    return (result);
}

/*
 * __rollback_abort_updates --
 *     Abort updates on this page newer than the timestamp.
 */
static int
__rollback_abort_updates(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_PAGE *page;

    /* Review deleted page saved to the ref. */
    if (ref->page_del != NULL && rollback_timestamp < ref->page_del->durable_timestamp) {
        __wt_verbose(
          session, WT_VERB_RECOVERY_RTS(session), "%p: deleted page rolled back", (void *)ref);
        WT_RET(__wt_delete_page_rollback(session, ref));
    }

    /*
     * If we have a ref with clean page, find out whether the page has any modifications that are
     * newer than the given timestamp. As eviction writes the newest version to page, even a clean
     * page may also contain modifications that need rollback.
     */
    WT_ASSERT(session, ref->page != NULL);
    page = ref->page;
    if (!__wt_page_is_modified(page) &&
      !__rollback_page_needs_abort(session, ref, rollback_timestamp)) {
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session), "%p: page skipped", (void *)ref);
        return (0);
    }

    WT_STAT_CONN_INCR(session, txn_rts_pages_visited);
    __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
      "%p: page rolled back when page is modified: %s", (void *)ref,
      __wt_page_is_modified(page) ? "true" : "false");

    switch (page->type) {
    case WT_PAGE_COL_FIX:
        __rollback_abort_col_fix(session, page, rollback_timestamp);
        break;
    case WT_PAGE_COL_VAR:
        WT_RET(__rollback_abort_col_var(session, ref, rollback_timestamp));
        break;
    case WT_PAGE_COL_INT:
    case WT_PAGE_ROW_INT:
        /*
         * There is nothing to do for internal pages, since we aren't rolling back far enough to
         * potentially include reconciled changes - and thus won't need to roll back structure
         * changes on internal pages.
         */
        break;
    case WT_PAGE_ROW_LEAF:
        WT_RET(__rollback_abort_row_leaf(session, ref, rollback_timestamp));
        break;
    default:
        WT_RET(__wt_illegal_value(session, page->type));
    }

    return (0);
}

/*
 * __rollback_abort_fast_truncate --
 *     Abort fast truncate on this page newer than the timestamp.
 */
static int
__rollback_abort_fast_truncate(
  WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    /* Review deleted page saved to the ref. */
    if (ref->page_del != NULL && rollback_timestamp < ref->page_del->durable_timestamp) {
        __wt_verbose(
          session, WT_VERB_RECOVERY_RTS(session), "%p: deleted page rolled back", (void *)ref);
        WT_RET(__wt_delete_page_rollback(session, ref));
    }

    return (0);
}

/*
 * __wt_rts_page_skip --
 *     Skip if rollback to stable doesn't requires to read this page.
 */
int
__wt_rts_page_skip(WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool *skipp)
{
    wt_timestamp_t rollback_timestamp;

    rollback_timestamp = *(wt_timestamp_t *)(context);
    *skipp = false; /* Default to reading */

    /* If the page state is other than on disk, we want to look at it. */
    if (ref->state != WT_REF_DISK)
        return (0);

    /* Check whether this ref has any possible updates to be aborted. */
    if (!__rollback_page_needs_abort(session, ref, rollback_timestamp)) {
        *skipp = true;
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session), "%p: page walk skipped", (void *)ref);
        WT_STAT_CONN_INCR(session, txn_rts_tree_walk_skip_pages);
    }

    return (0);
}

/*
 * __rollback_to_stable_btree_walk --
 *     Called for each open handle - choose to either skip or wipe the commits
 */
static int
__rollback_to_stable_btree_walk(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
    WT_DECL_RET;
    WT_REF *child_ref, *ref;

    /* Set this flag to return error instead of panic if file is corrupted. */
    F_SET(session, WT_SESSION_QUIET_CORRUPT_FILE);

    /* Walk the tree, marking commits aborted where appropriate. */
    ref = NULL;
    while ((ret = __wt_tree_walk_custom_skip(session, &ref, __wt_rts_page_skip, &rollback_timestamp,
              WT_READ_NO_EVICT | WT_READ_WONT_NEED)) == 0 &&
      ref != NULL)
        if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
            WT_INTL_FOREACH_BEGIN (session, ref->page, child_ref) {
                WT_RET(__rollback_abort_fast_truncate(session, child_ref, rollback_timestamp));
            }
            WT_INTL_FOREACH_END;
        } else
            WT_RET(__rollback_abort_updates(session, ref, rollback_timestamp));

    F_CLR(session, WT_SESSION_QUIET_CORRUPT_FILE);
    return (ret);
}

/*
 * __rollback_to_stable_btree --
 *     Called for each object handle - choose to either skip or wipe the commits
 */
static int
__rollback_to_stable_btree(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    btree = S2BT(session);
    conn = S2C(session);

    __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
      "rollback to stable connection logging enabled: %s and btree logging enabled: %s",
      FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED) ? "true" : "false",
      !F_ISSET(btree, WT_BTREE_NO_LOGGING) ? "true" : "false");

    /*
     * Immediately durable files don't get their commits wiped. This case mostly exists to support
     * the semantic required for the oplog in MongoDB - updates that have been made to the oplog
     * should not be aborted. It also wouldn't be safe to roll back updates for any table that had
     * it's records logged, since those updates would be recovered after a crash making them
     * inconsistent.
     */
    if (__wt_btree_immediately_durable(session)) {
        if (btree->id >= conn->stable_rollback_maxfile)
            WT_RET_PANIC(session, EINVAL, "btree file ID %" PRIu32 " larger than max %" PRIu32,
              btree->id, conn->stable_rollback_maxfile);
        return (0);
    }

    /* There is never anything to do for checkpoint handles */
    if (session->dhandle->checkpoint != NULL)
        return (0);

    /* There is nothing to do on an empty tree. */
    if (btree->root.page == NULL)
        return (0);

    WT_WITH_PAGE_INDEX(session, ret = __rollback_to_stable_btree_walk(session, rollback_timestamp));
    return (ret);
}

/*
 * __rollback_to_stable_check --
 *     Ensure the rollback request is reasonable.
 */
static int
__rollback_to_stable_check(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    bool txn_active;

    /*
     * Help the user comply with the requirement that there are no concurrent operations. Protect
     * against spurious conflicts with the sweep server: we exclude it from running concurrent with
     * rolling back the history store contents.
     */
    ret = __wt_txn_activity_check(session, &txn_active);
#ifdef HAVE_DIAGNOSTIC
    if (txn_active)
        WT_TRET(__wt_verbose_dump_txn(session));
#endif

    if (ret == 0 && txn_active)
        WT_RET_MSG(session, EINVAL, "rollback_to_stable illegal with active transactions");

    return (ret);
}

/*
 * __rollback_to_stable_btree_hs_truncate --
 *     Wipe all history store updates for the btree (non-timestamped tables)
 */
static int
__rollback_to_stable_btree_hs_truncate(WT_SESSION_IMPL *session, uint32_t btree_id)
{
    WT_CURSOR *hs_cursor;
    WT_DECL_ITEM(hs_key);
    WT_DECL_RET;
    wt_timestamp_t hs_start_ts;
    uint64_t hs_counter;
    uint32_t hs_btree_id;
    char ts_string[WT_TS_INT_STRING_SIZE];

    hs_cursor = NULL;

    WT_RET(__wt_scr_alloc(session, 0, &hs_key));

    /* Open a history store table cursor. */
    WT_ERR(__wt_curhs_open(session, NULL, &hs_cursor));

    /* Walk the history store for the given btree. */
    hs_cursor->set_key(hs_cursor, 1, btree_id);
    ret = __wt_curhs_search_near_after(session, hs_cursor);

    for (; ret == 0; ret = hs_cursor->next(hs_cursor)) {
        WT_ERR(hs_cursor->get_key(hs_cursor, &hs_btree_id, hs_key, &hs_start_ts, &hs_counter));

        /* We shouldn't cross the btree search space. */
        WT_ASSERT(session, btree_id == hs_btree_id);

        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "rollback to stable history store cleanup of update with start timestamp: %s",
          __wt_timestamp_to_string(hs_start_ts, ts_string));

        WT_ERR(hs_cursor->remove(hs_cursor));
        WT_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);
        WT_STAT_CONN_DATA_INCR(session, cache_hs_key_truncate_rts);
    }
    WT_ERR_NOTFOUND_OK(ret, false);

err:
    __wt_scr_free(session, &hs_key);
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));

    return (ret);
}

/*
 * __rollback_to_stable_hs_final_pass --
 *     Perform rollback to stable on the history store to remove any entries newer than the stable
 *     timestamp.
 */
static int
__rollback_to_stable_hs_final_pass(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
    WT_CONFIG ckptconf;
    WT_CONFIG_ITEM cval, durableval, key;
    WT_DECL_RET;
    wt_timestamp_t max_durable_ts, newest_stop_durable_ts, newest_stop_ts;
    char *config;
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    config = NULL;

    WT_RET(__wt_metadata_search(session, WT_HS_URI, &config));

    /*
     * Find out the max durable timestamp of the history store from checkpoint. Most of the history
     * store updates have stop timestamp either greater or equal to the start timestamp except for
     * the updates written for the prepared updates on the data store. To abort the updates with no
     * stop timestamp, we must include the newest stop timestamp also into the calculation of
     * maximum timestamp of the history store.
     */
    newest_stop_durable_ts = newest_stop_ts = WT_TS_NONE;
    WT_ERR(__wt_config_getones(session, config, "checkpoint", &cval));
    __wt_config_subinit(session, &ckptconf, &cval);
    for (; __wt_config_next(&ckptconf, &key, &cval) == 0;) {
        ret = __wt_config_subgets(session, &cval, "newest_stop_durable_ts", &durableval);
        if (ret == 0)
            newest_stop_durable_ts = WT_MAX(newest_stop_durable_ts, (wt_timestamp_t)durableval.val);
        WT_ERR_NOTFOUND_OK(ret, false);
        ret = __wt_config_subgets(session, &cval, "newest_stop_ts", &durableval);
        if (ret == 0)
            newest_stop_ts = WT_MAX(newest_stop_ts, (wt_timestamp_t)durableval.val);
        WT_ERR_NOTFOUND_OK(ret, false);
    }
    max_durable_ts = WT_MAX(newest_stop_ts, newest_stop_durable_ts);
    WT_ERR(__wt_session_get_dhandle(session, WT_HS_URI, NULL, NULL, 0));

    /*
     * The rollback operation should be performed on the history store file when the checkpoint
     * durable start/stop timestamp is greater than the rollback timestamp.
     */
    if (max_durable_ts > rollback_timestamp) {
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "tree rolled back with durable timestamp: %s",
          __wt_timestamp_to_string(max_durable_ts, ts_string[0]));
        WT_TRET(__rollback_to_stable_btree(session, rollback_timestamp));
    } else
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "tree skipped with durable timestamp: %s and stable timestamp: %s",
          __wt_timestamp_to_string(max_durable_ts, ts_string[0]),
          __wt_timestamp_to_string(rollback_timestamp, ts_string[1]));

    WT_TRET(__wt_session_release_dhandle(session));

err:
    __wt_free(session, config);
    return (ret);
}

/*
 * __rollback_to_stable_btree_apply --
 *     Perform rollback to stable to all files listed in the metadata, apart from the metadata and
 *     history store files.
 */
static int
__rollback_to_stable_btree_apply(WT_SESSION_IMPL *session)
{
    WT_CONFIG ckptconf;
    WT_CONFIG_ITEM cval, value, key;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_TXN_GLOBAL *txn_global;
    wt_timestamp_t max_durable_ts, newest_start_durable_ts, newest_stop_durable_ts,
      rollback_timestamp;
    uint64_t rollback_txnid;
    uint32_t btree_id;
    size_t addr_size;
    char ts_string[2][WT_TS_INT_STRING_SIZE];
    const char *config, *uri;
    bool durable_ts_found, prepared_updates, has_txn_updates_gt_than_ckpt_snap;
    bool dhandle_allocated, perform_rts;

    txn_global = &S2C(session)->txn_global;
    rollback_txnid = 0;
    addr_size = 0;

    /*
     * Copy the stable timestamp, otherwise we'd need to lock it each time it's accessed. Even
     * though the stable timestamp isn't supposed to be updated while rolling back, accessing it
     * without a lock would violate protocol.
     */
    WT_ORDERED_READ(rollback_timestamp, txn_global->stable_timestamp);
    __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
      "performing rollback to stable with stable timestamp: %s and oldest timestamp: %s",
      __wt_timestamp_to_string(rollback_timestamp, ts_string[0]),
      __wt_timestamp_to_string(txn_global->oldest_timestamp, ts_string[1]));

    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_SCHEMA));
    WT_RET(__wt_metadata_cursor(session, &cursor));

    if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
        __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
          "recovered checkpoint snapshot min:  %" PRIu64 ", snapshot max: %" PRIu64
          ", snapshot count: %" PRIu32,
          S2C(session)->recovery_ckpt_snap_min, S2C(session)->recovery_ckpt_snap_max,
          S2C(session)->recovery_ckpt_snapshot_count);

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &uri));
        dhandle_allocated = perform_rts = false;

        /* Ignore metadata and history store files. */
        if (strcmp(uri, WT_METAFILE_URI) == 0 || strcmp(uri, WT_HS_URI) == 0)
            continue;

        if (!WT_PREFIX_MATCH(uri, "file:"))
            continue;

        WT_ERR(cursor->get_value(cursor, &config));

        /* Find out the max durable timestamp of the object from checkpoint. */
        newest_start_durable_ts = newest_stop_durable_ts = WT_TS_NONE;
        durable_ts_found = prepared_updates = has_txn_updates_gt_than_ckpt_snap = false;

        /* Get the btree ID. */
        WT_ERR(__wt_config_getones(session, config, "id", &cval));
        btree_id = (uint32_t)cval.val;

        WT_ERR(__wt_config_getones(session, config, "checkpoint", &cval));
        __wt_config_subinit(session, &ckptconf, &cval);
        for (; __wt_config_next(&ckptconf, &key, &cval) == 0;) {
            ret = __wt_config_subgets(session, &cval, "newest_start_durable_ts", &value);
            if (ret == 0) {
                newest_start_durable_ts =
                  WT_MAX(newest_start_durable_ts, (wt_timestamp_t)value.val);
                durable_ts_found = true;
            }
            WT_ERR_NOTFOUND_OK(ret, false);
            ret = __wt_config_subgets(session, &cval, "newest_stop_durable_ts", &value);
            if (ret == 0) {
                newest_stop_durable_ts = WT_MAX(newest_stop_durable_ts, (wt_timestamp_t)value.val);
                durable_ts_found = true;
            }
            WT_ERR_NOTFOUND_OK(ret, false);
            ret = __wt_config_subgets(session, &cval, "prepare", &value);
            if (ret == 0) {
                if (value.val)
                    prepared_updates = true;
            }
            WT_ERR_NOTFOUND_OK(ret, false);
            ret = __wt_config_subgets(session, &cval, "newest_txn", &value);
            if (value.len != 0)
                rollback_txnid = (uint64_t)value.val;
            WT_ERR_NOTFOUND_OK(ret, false);
            ret = __wt_config_subgets(session, &cval, "addr", &value);
            if (ret == 0)
                addr_size = value.len;
            WT_ERR_NOTFOUND_OK(ret, false);
        }
        max_durable_ts = WT_MAX(newest_start_durable_ts, newest_stop_durable_ts);
        has_txn_updates_gt_than_ckpt_snap =
          WT_CHECK_RECOVERY_FLAG_TS_TXNID(session, rollback_txnid, max_durable_ts);

        /* Increment the inconsistent checkpoint stats counter. */
        if (has_txn_updates_gt_than_ckpt_snap)
            WT_STAT_CONN_DATA_INCR(session, txn_rts_inconsistent_ckpt);

        /*
         * The rollback to stable will skip the tables during recovery and shutdown in the following
         * conditions.
         * 1. Empty table.
         * 2. Table has timestamped updates without a stable timestamp.
         */
        if ((F_ISSET(S2C(session), WT_CONN_RECOVERING) ||
              F_ISSET(S2C(session), WT_CONN_CLOSING_TIMESTAMP)) &&
          (addr_size == 0 ||
            (txn_global->stable_timestamp == WT_TS_NONE && max_durable_ts != WT_TS_NONE))) {
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "skip rollback to stable on file %s because %s", uri,
              addr_size == 0 ? "its checkpoint address length is 0" :
                               "it has timestamped updates and the stable timestamp is 0");
            continue;
        }

        /*
         * The rollback operation should be performed on this file based on the following:
         * 1. The dhandle is present in the cache and tree is modified.
         * 2. The checkpoint durable start/stop timestamp is greater than the rollback timestamp.
         * 3. There is no durable timestamp in any checkpoint.
         * 4. The checkpoint newest txn is greater than snapshot min txn id
         */
        WT_WITH_HANDLE_LIST_READ_LOCK(session, (ret = __wt_conn_dhandle_find(session, uri, NULL)));

        if (ret == 0 && S2BT(session)->modified)
            perform_rts = true;

        WT_ERR_NOTFOUND_OK(ret, false);

        if (perform_rts || max_durable_ts > rollback_timestamp || prepared_updates ||
          !durable_ts_found || has_txn_updates_gt_than_ckpt_snap) {
            /* Set this flag to return error instead of panic if file is corrupted. */
            F_SET(session, WT_SESSION_QUIET_CORRUPT_FILE);
            ret = __wt_session_get_dhandle(session, uri, NULL, NULL, 0);
            F_CLR(session, WT_SESSION_QUIET_CORRUPT_FILE);

            /*
             * Ignore performing rollback to stable on files that does not exist or the files where
             * corruption is detected.
             */
            if ((ret == ENOENT) ||
              (ret == WT_ERROR && F_ISSET(S2C(session), WT_CONN_DATA_CORRUPTION))) {
                __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
                  "ignore performing rollback to stable on %s because the file %s", uri,
                  ret == ENOENT ? "does not exist" : "is corrupted.");
                continue;
            }
            WT_ERR(ret);

            dhandle_allocated = true;
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "tree rolled back with durable timestamp: %s, or when tree is modified: %s or "
              "prepared updates: %s or when durable time is not found: %s or txnid: %" PRIu64
              " is greater than recovery checkpoint snap min: %s",
              __wt_timestamp_to_string(max_durable_ts, ts_string[0]),
              S2BT(session)->modified ? "true" : "false", prepared_updates ? "true" : "false",
              !durable_ts_found ? "true" : "false", rollback_txnid,
              has_txn_updates_gt_than_ckpt_snap ? "true" : "false");
            WT_TRET(__rollback_to_stable_btree(session, rollback_timestamp));
        } else
            __wt_verbose(session, WT_VERB_RECOVERY_RTS(session),
              "tree skipped with durable timestamp: %s and stable timestamp: %s or txnid: %" PRIu64,
              __wt_timestamp_to_string(max_durable_ts, ts_string[0]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[1]), rollback_txnid);

        /*
         * Truncate history store entries for the non-timestamped table.
         * Exceptions:
         * 1. Modified tree - Scenarios where the tree is never checkpointed lead to zero
         * durable timestamp even they are timestamped tables. Until we have a special
         * indication of letting to know the table type other than checking checkpointed durable
         * timestamp to WT_TS_NONE, We need this exception.
         * 2. In-memory database - In this scenario, there is no history store to truncate.
         */

        if ((!dhandle_allocated || !S2BT(session)->modified) && max_durable_ts == WT_TS_NONE &&
          !F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
            WT_TRET(__rollback_to_stable_btree_hs_truncate(session, btree_id));

        if (dhandle_allocated)
            WT_TRET(__wt_session_release_dhandle(session));

        /*
         * Continue when the table is corrupted and proceed to perform rollback to stable on other
         * tables.
         */
        if (ret == WT_ERROR && F_ISSET(S2C(session), WT_CONN_DATA_CORRUPTION))
            continue;

        WT_ERR(ret);
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
        WT_ERR(__rollback_to_stable_hs_final_pass(session, rollback_timestamp));

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __rollback_to_stable --
 *     Rollback all modifications with timestamps more recent than the passed in timestamp.
 */
static int
__rollback_to_stable(WT_SESSION_IMPL *session, bool no_ckpt)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_TXN_GLOBAL *txn_global;

    conn = S2C(session);
    txn_global = &conn->txn_global;

    /*
     * Rollback to stable should ignore tombstones in the history store since it needs to scan the
     * entire table sequentially.
     */
    F_SET(session, WT_SESSION_ROLLBACK_TO_STABLE);

    WT_ERR(__rollback_to_stable_check(session));

    /*
     * Allocate a non-durable btree bitstring. We increment the global value before using it, so the
     * current value is already in use, and hence we need to add one here.
     */
    conn->stable_rollback_maxfile = conn->next_file_id + 1;
    WT_ERR(__rollback_to_stable_btree_apply(session));

    /* Rollback the global durable timestamp to the stable timestamp. */
    txn_global->has_durable_timestamp = txn_global->has_stable_timestamp;
    txn_global->durable_timestamp = txn_global->stable_timestamp;

    /*
     * If the configuration is not in-memory, forcibly log a checkpoint after rollback to stable to
     * ensure that both in-memory and on-disk versions are the same unless caller requested for no
     * checkpoint.
     */
    if (!F_ISSET(conn, WT_CONN_IN_MEMORY) && !no_ckpt)
        WT_ERR(session->iface.checkpoint(&session->iface, "force=1"));

err:
    F_CLR(session, WT_SESSION_ROLLBACK_TO_STABLE);
    return (ret);
}

/*
 * __wt_rollback_to_stable --
 *     Rollback the database to the stable timestamp.
 */
int
__wt_rollback_to_stable(WT_SESSION_IMPL *session, const char *cfg[], bool no_ckpt)
{
    WT_DECL_RET;

    WT_UNUSED(cfg);

    /*
     * Don't use the connection's default session: we are working on data handles and (a) don't want
     * to cache all of them forever, plus (b) can't guarantee that no other method will be called
     * concurrently. Copy parent session no logging option to the internal session to make sure that
     * rollback to stable doesn't generate log records.
     */
    WT_RET(__wt_open_internal_session(S2C(session), "txn rollback_to_stable", true,
      F_MASK(session, WT_SESSION_NO_LOGGING), 0, &session));

    WT_STAT_CONN_SET(session, txn_rollback_to_stable_running, 1);
    WT_WITH_CHECKPOINT_LOCK(
      session, WT_WITH_SCHEMA_LOCK(session, ret = __rollback_to_stable(session, no_ckpt)));
    WT_STAT_CONN_SET(session, txn_rollback_to_stable_running, 0);

    WT_TRET(__wt_session_close_internal(session));

    return (ret);
}
