/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __rts_btree_abort_update --
 *     Abort updates in an update change with timestamps newer than the rollback timestamp. Also,
 *     clear the history store flag for the first stable update in the update.
 */
static int
__rts_btree_abort_update(WT_SESSION_IMPL *session, WT_ITEM *key, WT_UPDATE *first_upd,
  wt_timestamp_t rollback_timestamp, bool *stable_update_found)
{
    WT_UPDATE *stable_upd, *tombstone, *upd;
    char ts_string[2][WT_TS_INT_STRING_SIZE];
    bool dryrun, hs_update, txn_id_visible;

    dryrun = S2C(session)->rts->dryrun;
    hs_update = false;

    stable_upd = tombstone = NULL;
    WT_NOT_READ(txn_id_visible, false);

    if (stable_update_found != NULL)
        *stable_update_found = false;

    /* Clear flags used by dry run. */
    if (dryrun)
        for (upd = first_upd; upd != NULL; upd = upd->next)
            F_CLR(upd, WT_UPDATE_RTS_DRYRUN_ABORT);

    for (upd = first_upd; upd != NULL; upd = upd->next) {
        /* Skip the updates that are aborted. */
        if (upd->txnid == WT_TXN_ABORTED)
            continue;

        if (F_ISSET(upd, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS))
            hs_update = true;

        /*
         * An unstable update needs to be aborted if any of the following are true:
         * 1. An update is invisible based on the checkpoint snapshot during recovery.
         * 2. The update durable timestamp is greater than the stable timestamp.
         * 3. The update is a prepared update.
         *
         * Usually during recovery, there are no in memory updates present on the page. But
         * whenever an unstable fast truncate operation is written to the disk, as part
         * of the rollback to stable page read, it instantiates the tombstones on the page.
         * The transaction id validation is ignored in all scenarios except recovery.
         */
        txn_id_visible = __wti_rts_visibility_txn_visible_id(session, upd->txnid);
        if (!txn_id_visible || rollback_timestamp < upd->durable_ts ||
          upd->prepare_state == WT_PREPARE_INPROGRESS) {
            __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
              WT_RTS_VERB_TAG_UPDATE_ABORT
              "rollback to stable aborting update with txnid=%" PRIu64
              ", txnid_not_visible=%s"
              ", stable_timestamp=%s < durable_timestamp=%s: %s, prepare_state=%s, flags 0x%" PRIx8,
              upd->txnid, !txn_id_visible ? "true" : "false",
              __wt_timestamp_to_string(rollback_timestamp, ts_string[1]),
              __wt_timestamp_to_string(upd->durable_ts, ts_string[0]),
              rollback_timestamp < upd->durable_ts ? "true" : "false",
              __wt_prepare_state_str(upd->prepare_state), upd->flags);

            if (dryrun)
                F_SET(upd, WT_UPDATE_RTS_DRYRUN_ABORT);
            else
                upd->txnid = WT_TXN_ABORTED;
            WT_RTS_STAT_CONN_INCR(session, txn_rts_upd_aborted);
        } else {
            /* Valid update is found. */
            stable_upd = upd;
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_4,
              WT_RTS_VERB_TAG_STABLE_UPDATE_FOUND
              "stable update found with txnid=%" PRIu64
              ", stable_timestamp=%s,  durable_timestamp=%s, flags 0x%" PRIx8,
              upd->txnid, __wt_timestamp_to_string(rollback_timestamp, ts_string[1]),
              __wt_timestamp_to_string(upd->durable_ts, ts_string[0]), upd->flags);
            break;
        }
    }

    if (stable_upd != NULL) {
        /*
         * During recovery, there shouldn't be any updates in the update chain except when the
         * updates are from a prepared transaction or from a reinstantiated fast deleted page. Reset
         * the transaction ID of the stable update that was restored. Ignore the history store as we
         * cannot have a prepared transaction on it and a fast deleted page in the history store
         * should never be reinstantiated as it is globally visible.
         */
        if (F_ISSET_ATOMIC_32(S2C(session), WT_CONN_RECOVERING) && !WT_IS_HS(session->dhandle)) {
            WT_ASSERT(session, first_upd->type == WT_UPDATE_TOMBSTONE);
            WT_ASSERT(session,
              F_ISSET(
                first_upd, WT_UPDATE_PREPARE_RESTORED_FROM_DS | WT_UPDATE_RESTORED_FAST_TRUNCATE));
            WT_ASSERT(session, !hs_update);
            WT_ASSERT(session, stable_upd->next == NULL);
            stable_upd->txnid = WT_TXN_NONE;
        }

        /*
         * Clear the history store flags for the stable update to indicate that this update should
         * be written to the history store later. The next time when this update is moved into the
         * history store, it will have a different stop time point.
         */
        if (hs_update) {
            /*
             * If we have a stable tombstone at the end of the update chain, it may not have been
             * inserted to the history store.
             */
            WT_ASSERT(session,
              F_ISSET(stable_upd, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS) ||
                stable_upd->type == WT_UPDATE_TOMBSTONE);
            /*
             * Find the update following a stable tombstone that has been inserted to the history
             * store.
             */
            if (stable_upd->type == WT_UPDATE_TOMBSTONE &&
              F_ISSET(stable_upd, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS)) {
                tombstone = stable_upd;
                for (stable_upd = stable_upd->next; stable_upd != NULL;
                     stable_upd = stable_upd->next) {
                    if (stable_upd->txnid != WT_TXN_ABORTED) {
                        WT_ASSERT(session,
                          stable_upd->type != WT_UPDATE_TOMBSTONE &&
                            F_ISSET(stable_upd, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS));
                        break;
                    }
                }
            }

            /*
             * Delete the first stable update and any newer update from the history store. If the
             * update following the stable tombstone is removed by obsolete check, no need to remove
             * that update from the history store as it has a globally visible tombstone. In that
             * case, it is enough to delete everything up until to the tombstone timestamp.
             */
            WT_RET(__wti_rts_history_delete_hs(
              session, key, stable_upd == NULL ? tombstone->start_ts : stable_upd->start_ts));

            /*
             * Clear the history store flags for the first stable update. Otherwise, it will not be
             * moved to history store again.
             */
            if (!dryrun) {
                if (stable_upd != NULL)
                    F_CLR(stable_upd, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS);
                if (tombstone != NULL)
                    F_CLR(tombstone, WT_UPDATE_HS | WT_UPDATE_TO_DELETE_FROM_HS);
            }
        } else if (WT_IS_HS(session->dhandle) && stable_upd->type != WT_UPDATE_TOMBSTONE) {
            /*
             * History store will have a combination of both tombstone and update/modify types in
             * the update list to represent the time window of an update. When we are aborting the
             * tombstone, make sure to remove all of the remaining updates also. In most of the
             * scenarios, there will be only one update present except when the data store is a
             * prepared commit where it is possible to have more than one update. The existing
             * on-disk versions are removed while processing the on-disk entries.
             */
            for (; stable_upd != NULL; stable_upd = stable_upd->next)
                if (!dryrun)
                    stable_upd->txnid = WT_TXN_ABORTED;
        }
        if (stable_update_found != NULL)
            *stable_update_found = true;
    }

    return (0);
}

/*
 * __rts_btree_abort_insert_list --
 *     Apply the update abort check to each entry in an insert skip list. Return how many entries
 *     had stable updates.
 */
static int
__rts_btree_abort_insert_list(WT_SESSION_IMPL *session, WT_PAGE *page, WT_INSERT_HEAD *head,
  wt_timestamp_t rollback_timestamp, uint32_t *stable_updates_count)
{
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(key_string);
    WT_DECL_RET;
    WT_INSERT *ins;
    char ts_string[WT_TS_INT_STRING_SIZE];
    bool stable_update_found;

    WT_ERR(
      __wt_scr_alloc(session, page->type == WT_PAGE_ROW_LEAF ? 0 : WT_INTPACK64_MAXSIZE, &key));

    WT_ERR(__wt_scr_alloc(session, 0, &key_string));
    WT_SKIP_FOREACH (ins, head)
        if (ins->upd != NULL) {
            key->data = WT_INSERT_KEY(ins);
            key->size = WT_INSERT_KEY_SIZE(ins);
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_4,
              WT_RTS_VERB_TAG_INSERT_LIST_UPDATE_ABORT
              "attempting to abort update on the insert list with durable_timestamp=%s, key=%s",
              __wt_timestamp_to_string(ins->upd->durable_ts, ts_string),
              __wt_key_string(
                session, key->data, key->size, S2BT(session)->key_format, key_string));

            WT_ERR(__rts_btree_abort_update(
              session, key, ins->upd, rollback_timestamp, &stable_update_found));
            if (stable_update_found && stable_updates_count != NULL)
                (*stable_updates_count)++;
        }

err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &key_string);
    return (ret);
}

/*
 * __rts_btree_row_modify --
 *     Add the provided update to the head of the update list.
 */
static WT_INLINE int
__rts_btree_row_modify(WT_SESSION_IMPL *session, WT_REF *ref, WT_UPDATE **updp, WT_ITEM *key)
{
    WT_CURSOR_BTREE cbt;
    WT_DECL_RET;
    bool dryrun;

    dryrun = S2C(session)->rts->dryrun;

    __wt_btcur_init(session, &cbt);
    __wt_btcur_open(&cbt);

    /* Search the page. */
    WT_ERR(__wt_row_search(&cbt, key, true, ref, true, NULL));

    /* Apply the modification. */
    if (!dryrun)
        WT_ERR(__wt_row_modify(&cbt, key, NULL, updp, WT_UPDATE_INVALID, true, false));

err:
    /* Free any resources that may have been cached in the cursor. */
    WT_TRET(__wt_btcur_close(&cbt, true));

    return (ret);
}

/*
 * __rts_btree_ondisk_fixup_key --
 *     Abort updates in the history store and replace the on-disk value with an update that
 *     satisfies the given timestamp.
 */
static int
__rts_btree_ondisk_fixup_key(WT_SESSION_IMPL *session, WT_REF *ref, WT_ROW *rip,
  WT_ITEM *row_key, WT_CELL_UNPACK_KV *unpack, wt_timestamp_t rollback_timestamp)
{
    WT_CURSOR *hs_cursor;
    WT_DECL_ITEM(full_value);
    WT_DECL_ITEM(hs_key);
    WT_DECL_ITEM(hs_value);
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(key_string);
    WT_DECL_RET;
    WT_PAGE *page;
    WT_TIME_WINDOW *hs_tw, *tw;
    WT_UPDATE *tombstone, *upd;
    wt_timestamp_t hs_durable_ts, hs_start_ts, hs_stop_durable_ts, newer_hs_durable_ts, pinned_ts;
    size_t max_memsize;
    uint64_t hs_counter, type_full;
    uint32_t hs_btree_id;
    uint8_t type;
    char ts_string[4][WT_TS_INT_STRING_SIZE];
    char tw_string[WT_TIME_STRING_SIZE];
    bool dryrun, first_record, valid_update_found;

    dryrun = S2C(session)->rts->dryrun;

    page = ref->page;

    hs_cursor = NULL;
    tombstone = upd = NULL;
    hs_durable_ts = hs_start_ts = hs_stop_durable_ts = WT_TS_NONE;
    hs_btree_id = S2BT(session)->id;
    valid_update_found = false;
    first_record = true;

    /* Allocate buffers for the data store and history store key. */
    WT_ERR(__wt_scr_alloc(session, 0, &hs_key));
    WT_ERR(__wt_scr_alloc(session, 0, &hs_value));

    if (row_key != NULL)
        key = row_key;
    else {
        /* Unpack a row key. */
        WT_ERR(__wt_scr_alloc(session, 0, &key));
        WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
    }

    WT_ERR(__wt_scr_alloc(session, 0, &key_string));
    __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_2,
      WT_RTS_VERB_TAG_ONDISK_KEY_ROLLBACK "rolling back the on-disk key=%s",
      __wt_key_string(session, key->data, key->size, S2BT(session)->key_format, key_string));

    WT_ERR(__wt_scr_alloc(session, 0, &full_value));
    WT_ERR(__wt_page_cell_data_ref_kv(session, page, unpack, full_value));
    /*
     * We can read overflow removed value if checkpoint has run before rollback to stable. In this
     * case, we have already appended the on page value to the update chain. At this point, we have
     * visited the update chain and decided the value is not stable. In addition, checkpoint must
     * have moved this value to the history store as a full value. Therefore, we can safely ignore
     * the on page value if it is overflow removed.
     */
    if (__wt_cell_type_raw(unpack->cell) != WT_CELL_VALUE_OVFL_RM)
        WT_ERR(__wt_buf_set(session, full_value, full_value->data, full_value->size));

    /* Retrieve the time window from the unpacked value cell. */
    __wt_cell_get_tw(unpack, &tw);

    newer_hs_durable_ts = tw->durable_start_ts;

    __wt_txn_pinned_timestamp(session, &pinned_ts);

    /* Open a history store table cursor. */
    WT_ERR(__wt_curhs_open(session, NULL, &hs_cursor));
    /*
     * Rollback-to-stable operates exclusively (i.e., it is the only active operation in the system)
     * outside the constraints of transactions. Therefore, there is no need for snapshot based
     * visibility checks.
     */
    F_SET(hs_cursor, WT_CURSTD_HS_READ_ALL);

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

        /* Retrieve the time window from the history cursor. */
        __wt_hs_upd_time_window(hs_cursor, &hs_tw);

        /*
         * We have a tombstone on the history update and it is obsolete according to the timestamp
         * and txnid, so no need to restore it. These obsolete updates are written to the disk when
         * they are not obsolete at the time of reconciliation by an eviction thread and later they
         * become obsolete according to the checkpoint.
         */
        if (__wti_rts_visibility_txn_visible_id(session, hs_tw->stop_txn) &&
          hs_tw->durable_stop_ts <= pinned_ts) {
            __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
              WT_RTS_VERB_TAG_HS_STOP_OBSOLETE
              "history store stop is obsolete with time_window=%s and pinned_timestamp=%s",
              __wt_time_window_to_string(hs_tw, tw_string),
              __wt_timestamp_to_string(pinned_ts, ts_string[0]));
            if (!dryrun)
                WT_ERR(hs_cursor->remove(hs_cursor));
            WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);

            continue;
        }

        /*
         * Do not include history store updates greater than on-disk data store version to construct
         * a full update to restore except when the on-disk update is prepared. Including more
         * recent updates than the on-disk version shouldn't be problem as the on-disk version in
         * history store is always a full update. It is better to not to include those updates as it
         * unnecessarily increases the rollback to stable time.
         *
         * Comparing with timestamps here has no problem unlike in search flow where the timestamps
         * may be reset during reconciliation. RTS detects an on-disk update is unstable based on
         * the written proper timestamp, so comparing against it with history store shouldn't have
         * any problem.
         */
        if (hs_tw->start_ts <= tw->start_ts || tw->prepare) {
            if (type == WT_UPDATE_MODIFY) {
                __wt_modify_max_memsize_format(
                  hs_value->data, S2BT(session)->value_format, full_value->size, &max_memsize);
                WT_ERR(__wt_buf_set_and_grow(
                  session, full_value, full_value->data, full_value->size, max_memsize));
                WT_ERR(__wt_modify_apply_item(
                  session, S2BT(session)->value_format, full_value, hs_value->data));
            } else {
                WT_ASSERT(session, type == WT_UPDATE_STANDARD);
                WT_ERR(__wt_buf_set(session, full_value, hs_value->data, hs_value->size));
            }
        } else
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_2,
              WT_RTS_VERB_TAG_HS_GT_ONDISK
              "history store update more recent than on-disk update with time_window=%s and "
              "type=%s",
              __wt_time_window_to_string(hs_tw, tw_string), __wt_update_type_str(type));

        /*
         * Verify the history store timestamps are in order. The start timestamp may be equal to the
         * stop timestamp if the original update's commit timestamp is in order. We may see records
         * newer than or equal to the onpage value if eviction runs concurrently with checkpoint. In
         * that case, don't verify the first record.
         *
         * It is possible during a prepared transaction rollback, the history store update that have
         * its own stop timestamp doesn't get removed leads to duplicate records in history store
         * after further operations on that same key. Rollback to stable should ignore such records
         * for timestamp ordering verification.
         *
         * It is possible that there can be an update in the history store with a max stop timestamp
         * in the middle of the same key updates. This occurs when the checkpoint writes the
         * committed prepared update and further updates on that key including the history store
         * changes before the transaction fixes the history store update to have a proper stop
         * timestamp. It is a rare scenario.
         */
        WT_ASSERT_ALWAYS(session,
          hs_stop_durable_ts <= newer_hs_durable_ts || hs_start_ts == hs_stop_durable_ts ||
            hs_start_ts == newer_hs_durable_ts || newer_hs_durable_ts == hs_durable_ts ||
            first_record || hs_stop_durable_ts == WT_TS_MAX,
          "Out of order history store updates detected");

        if (hs_stop_durable_ts < newer_hs_durable_ts)
            WT_STAT_CONN_DSRC_INCR(session, txn_rts_hs_stop_older_than_newer_start);

        /*
         * Validate the timestamps in the key and the cell are same. This must be validated only
         * after verifying it's stop time window is not globally visible. The start timestamps of
         * the time window are cleared when they are globally visible and there will be no stop
         * timestamp in the history store whenever a prepared update is written to the data store.
         */
        WT_ASSERT(session,
          (hs_tw->start_ts == WT_TS_NONE || hs_tw->start_ts == hs_start_ts) &&
            (hs_tw->durable_start_ts == WT_TS_NONE || hs_tw->durable_start_ts == hs_durable_ts) &&
            ((hs_tw->durable_stop_ts == 0 && hs_stop_durable_ts == WT_TS_MAX) ||
              hs_tw->durable_stop_ts == hs_stop_durable_ts));

        /*
         * Stop processing when we find a stable update according to the given timestamp and
         * transaction id.
         */
        if (__wti_rts_visibility_txn_visible_id(session, hs_tw->start_txn) &&
          hs_tw->durable_start_ts <= rollback_timestamp) {
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_2,
              WT_RTS_VERB_TAG_HS_UPDATE_VALID
              "history store update valid with time_window=%s, type=%s and stable_timestamp=%s",
              __wt_time_window_to_string(hs_tw, tw_string), __wt_update_type_str(type),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[0]));
            WT_ASSERT(session, tw->prepare || hs_tw->start_ts <= tw->start_ts);
            valid_update_found = true;
            break;
        }

        __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
          WT_RTS_VERB_TAG_HS_UPDATE_ABORT
          "history store update aborted with time_window=%s, type=%s and stable_timestamp=%s",
          __wt_time_window_to_string(hs_tw, tw_string), __wt_update_type_str(type),
          __wt_timestamp_to_string(rollback_timestamp, ts_string[3]));

        /*
         * Start time point of the current record may be used as stop time point of the previous
         * record. Save it to verify against the previous record and check if we need to append the
         * stop time point as a tombstone when we rollback the history store record.
         */
        newer_hs_durable_ts = hs_durable_ts;
        first_record = false;

        if (!dryrun)
            WT_ERR(hs_cursor->remove(hs_cursor));
        WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);
        WT_RTS_STAT_CONN_DATA_INCR(session, cache_hs_key_truncate_rts_unstable);
    }

    /*
     * If we found a history value that satisfied the given timestamp, add it to the update list.
     * Otherwise remove the key by adding a tombstone.
     */
    if (valid_update_found) {
        /* Retrieve the time window from the history cursor. */
        __wt_hs_upd_time_window(hs_cursor, &hs_tw);
        WT_ASSERT(session, hs_tw->start_ts < tw->start_ts || hs_tw->start_txn < tw->start_txn);
        WT_ERR(__wt_upd_alloc(session, full_value, WT_UPDATE_STANDARD, &upd, NULL));

        /*
         * Set the transaction id of updates to WT_TXN_NONE when called from recovery, because the
         * connections write generation will be initialized after rollback to stable and the updates
         * in the cache will be problematic. The transaction id of pages which are in disk will be
         * automatically reset as part of unpacking cell when loaded to cache.
         */
        if (F_ISSET_ATOMIC_32(S2C(session), WT_CONN_RECOVERING))
            upd->txnid = WT_TXN_NONE;
        else
            upd->txnid = hs_tw->start_txn;
        upd->durable_ts = hs_tw->durable_start_ts;
        upd->start_ts = hs_tw->start_ts;
        __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
          WT_RTS_VERB_TAG_HS_UPDATE_RESTORED "history store update restored txnid=%" PRIu64
                                             ", start_ts=%s and durable_ts=%s",
          upd->txnid, __wt_timestamp_to_string(upd->start_ts, ts_string[0]),
          __wt_timestamp_to_string(upd->durable_ts, ts_string[1]));

        /*
         * Set the flag to indicate that this update has been restored from history store for the
         * rollback to stable operation.
         */
        F_SET(upd, WT_UPDATE_RESTORED_FROM_HS);
        WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_hs_restore_updates);

        /*
         * We have a tombstone on the original update chain and it is stable according to the
         * timestamp and txnid, we need to restore that as well.
         */
        if (__wti_rts_visibility_txn_visible_id(session, hs_tw->stop_txn) &&
          hs_tw->durable_stop_ts <= rollback_timestamp) {
            /*
             * The restoring tombstone timestamp must be zero or less than previous update start
             * timestamp.
             */
            WT_ASSERT(session,
              hs_stop_durable_ts == WT_TS_NONE || hs_stop_durable_ts < newer_hs_durable_ts ||
                tw->prepare);

            WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
            /*
             * Set the transaction id of updates to WT_TXN_NONE when called from recovery, because
             * the connections write generation will be initialized after rollback to stable and the
             * updates in the cache will be problematic. The transaction id of pages which are in
             * disk will be automatically reset as part of unpacking cell when loaded to cache.
             */
            if (F_ISSET_ATOMIC_32(S2C(session), WT_CONN_RECOVERING))
                tombstone->txnid = WT_TXN_NONE;
            else
                tombstone->txnid = hs_tw->stop_txn;
            tombstone->durable_ts = hs_tw->durable_stop_ts;
            tombstone->start_ts = hs_tw->stop_ts;
            __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
              WT_RTS_VERB_TAG_HS_RESTORE_TOMBSTONE
              "history store tombstone restored, txnid=%" PRIu64 ", start_ts=%s and durable_ts=%s",
              tombstone->txnid, __wt_timestamp_to_string(tombstone->start_ts, ts_string[0]),
              __wt_timestamp_to_string(tombstone->durable_ts, ts_string[1]));

            /*
             * Set the flag to indicate that this update has been restored from history store for
             * the rollback to stable operation.
             */
            F_SET(tombstone, WT_UPDATE_RESTORED_FROM_HS);

            tombstone->next = upd;
            upd = tombstone;
            WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_hs_restore_tombstones);
        }
    } else {
        WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
        WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
        __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_3,
          WT_RTS_VERB_TAG_KEY_REMOVED "%s", "key removed");
    }

    if (rip != NULL)
        WT_ERR(__rts_btree_row_modify(session, ref, &upd, key));

    /* Finally remove that update from history store. */
    if (valid_update_found) {
        if (!dryrun) {
            /* Avoid freeing the updates while still in use if hs_cursor->remove fails. */
            upd = tombstone = NULL;
            WT_ERR(hs_cursor->remove(hs_cursor));
        }
        WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_hs_removed);
        WT_RTS_STAT_CONN_DATA_INCR(session, cache_hs_key_truncate_rts);
    }

    if (0) {
err:
        WT_ASSERT(session, tombstone == NULL || upd == tombstone || upd == NULL);
        __wt_free_update_list(session, &upd);
    }
    __wt_scr_free(session, &full_value);
    __wt_scr_free(session, &hs_key);
    __wt_scr_free(session, &hs_value);
    if (rip == NULL || row_key == NULL)
        __wt_scr_free(session, &key);
    __wt_scr_free(session, &key_string);
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));
    if (dryrun)
        /*
         * Dry runs don't modify the database so any upd structure allocated by this function is not
         * in use and must be cleaned up.
         */
        __wt_free_update_list(session, &upd);
    return (ret);
}

/*
 * __rts_btree_abort_ondisk_kv --
 *     Fix the on-disk K/V version according to the given timestamp.
 */
static int
__rts_btree_abort_ondisk_kv(WT_SESSION_IMPL *session, WT_REF *ref, WT_ROW *rip,
    WT_ITEM *row_key, WT_CELL_UNPACK_KV *vpack, wt_timestamp_t rollback_timestamp,
  bool *is_ondisk_stable)
{
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(key_string);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_PAGE *page;
    WT_TIME_WINDOW *tw;
    WT_UPDATE *upd;
    char time_string[WT_TIME_STRING_SIZE];
    char ts_string[5][WT_TS_INT_STRING_SIZE];
    bool prepared;

    page = ref->page;
    upd = NULL;

    /* Initialize the on-disk stable version flag. */
    if (is_ondisk_stable != NULL)
        *is_ondisk_stable = false;

    /* Retrieve the time window from the unpacked value cell. */
    __wt_cell_get_tw(vpack, &tw);

    prepared = tw->prepare;
    if (WT_IS_HS(session->dhandle)) {
        /*
         * Abort the history store update with stop durable timestamp greater than the stable
         * timestamp or the updates with max stop timestamp which implies that they are associated
         * with prepared transactions.
         */
        if (tw->durable_stop_ts > rollback_timestamp || tw->stop_ts == WT_TS_MAX) {
            __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
              WT_RTS_VERB_TAG_HS_ABORT_STOP
              "history store update aborted with start_durable/commit_timestamp=%s, %s, "
              "stop_durable/commit_timestamp=%s, %s and stable_timestamp=%s",
              __wt_timestamp_to_string(tw->durable_start_ts, ts_string[0]),
              __wt_timestamp_to_string(tw->start_ts, ts_string[1]),
              __wt_timestamp_to_string(tw->durable_stop_ts, ts_string[2]),
              __wt_timestamp_to_string(tw->stop_ts, ts_string[3]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[4]));
            WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
            WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_sweep_hs_keys);
        } else
            return (0);
    } else if (tw->durable_start_ts > rollback_timestamp ||
      !__wti_rts_visibility_txn_visible_id(session, tw->start_txn) ||
      (!WT_TIME_WINDOW_HAS_STOP(tw) && prepared)) {
        __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
          WT_RTS_VERB_TAG_ONDISK_ABORT_TW
          "on-disk update aborted with time_window=%s. "
          "Start durable_timestamp > stable_timestamp: %s, or txnid_not_visible=%s, "
          "or tw_has_no_stop_and_is_prepared=%s",
          __wt_time_point_to_string(tw->start_ts, tw->durable_start_ts, tw->start_txn, time_string),
          tw->durable_start_ts > rollback_timestamp ? "true" : "false",
          !__wti_rts_visibility_txn_visible_id(session, tw->start_txn) ? "true" : "false",
          !WT_TIME_WINDOW_HAS_STOP(tw) && prepared ? "true" : "false");
        if (!F_ISSET_ATOMIC_32(S2C(session), WT_CONN_IN_MEMORY))
            return (__rts_btree_ondisk_fixup_key(
              session, ref, rip, row_key, vpack, rollback_timestamp));
        else {
            /*
             * In-memory database don't have a history store to provide a stable update, so remove
             * the key. Note that an in-memory database will have saved old values in the update
             * chain, so we should only get here for a key/value that never existed at all as of the
             * rollback timestamp; thus, deleting it is the correct response.
             */
            WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
            WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
        }
    } else if (WT_TIME_WINDOW_HAS_STOP(tw) &&
      (tw->durable_stop_ts > rollback_timestamp ||
        !__wti_rts_visibility_txn_visible_id(session, tw->stop_txn) || prepared)) {
        /*
         * For prepared transactions, it is possible that both the on-disk key start and stop time
         * windows can be the same. To abort these updates, check for any stable update from history
         * store or remove the key.
         */
        if (tw->start_ts == tw->stop_ts && tw->durable_start_ts == tw->durable_stop_ts &&
          tw->start_txn == tw->stop_txn) {
            WT_ASSERT(session, prepared == true);
            if (!F_ISSET_ATOMIC_32(S2C(session), WT_CONN_IN_MEMORY))
                return (__rts_btree_ondisk_fixup_key(
                  session, ref, rip, row_key, vpack, rollback_timestamp));
            else {
                /*
                 * In-memory database don't have a history store to provide a stable update, so
                 * remove the key.
                 */
                WT_RET(__wt_upd_alloc_tombstone(session, &upd, NULL));
                WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_keys_removed);
            }
        } else {
            /*
             * Clear the remove operation from the key by inserting the original on-disk value as a
             * standard update.
             */
            WT_RET(__wt_scr_alloc(session, 0, &tmp));
            if ((ret = __wt_page_cell_data_ref_kv(session, page, vpack, tmp)) == 0)
                ret = __wt_upd_alloc(session, tmp, WT_UPDATE_STANDARD, &upd, NULL);
            __wt_scr_free(session, &tmp);
            WT_RET(ret);

            /*
             * Set the transaction id of updates to WT_TXN_NONE when called from recovery, because
             * the connections write generation will be initialized after rollback to stable and the
             * updates in the cache will be problematic. The transaction id of pages which are in
             * disk will be automatically reset as part of unpacking cell when loaded to cache.
             */
            if (F_ISSET_ATOMIC_32(S2C(session), WT_CONN_RECOVERING))
                upd->txnid = WT_TXN_NONE;
            else
                upd->txnid = tw->start_txn;
            upd->durable_ts = tw->durable_start_ts;
            upd->start_ts = tw->start_ts;
            F_SET(upd, WT_UPDATE_RESTORED_FROM_DS);
            WT_RTS_STAT_CONN_DATA_INCR(session, txn_rts_keys_restored);
            __wt_verbose_multi(session, WT_VERB_RECOVERY_RTS(session),
              WT_RTS_VERB_TAG_KEY_CLEAR_REMOVE
              "key restored with commit_timestamp=%s, durable_timestamp=%s, stable_timestamp=%s, "
              "txnid=%" PRIu64
              " and removed commit_timestamp=%s, durable_timestamp=%s, txnid=%" PRIu64
              ", prepared=%s",
              __wt_timestamp_to_string(upd->start_ts, ts_string[0]),
              __wt_timestamp_to_string(upd->durable_ts, ts_string[1]),
              __wt_timestamp_to_string(rollback_timestamp, ts_string[2]), upd->txnid,
              __wt_timestamp_to_string(tw->stop_ts, ts_string[3]),
              __wt_timestamp_to_string(tw->durable_stop_ts, ts_string[4]), tw->stop_txn,
              prepared ? "true" : "false");
        }
    } else {
        /* Stable version according to the timestamp. */
        if (is_ondisk_stable != NULL)
            *is_ondisk_stable = true;
        return (0);
    }

    if (row_key != NULL)
        key = row_key;
    else {
        /* Unpack a row key. */
        WT_ERR(__wt_scr_alloc(session, 0, &key));
        WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
    }

    WT_ERR(__wt_scr_alloc(session, 0, &key_string));
    __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_2,
      WT_RTS_VERB_TAG_ONDISK_KV_REMOVE "removing the key, tombstone=%s, key=%s",
      upd->type == WT_UPDATE_TOMBSTONE ? "true" : "false",
      __wt_key_string(session, key->data, key->size, S2BT(session)->key_format, key_string));

    WT_ERR(__rts_btree_row_modify(session, ref, &upd, key));

    if (S2C(session)->rts->dryrun) {
err:
        __wt_free(session, upd);
    }
    if (rip == NULL || row_key == NULL)
        __wt_scr_free(session, &key);
    __wt_scr_free(session, &key_string);
    return (ret);
}

/*
 * __rts_btree_abort_row_leaf --
 *     Abort updates on a row leaf page with timestamps newer than the rollback timestamp.
 */
static int
__rts_btree_abort_row_leaf(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_CELL_UNPACK_KV *vpack, _vpack;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(key_string);
    WT_DECL_RET;
    WT_INSERT_HEAD *insert;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_UPDATE *upd;
    uint32_t i;
    char ts_string[WT_TS_INT_STRING_SIZE];
    bool have_key, stable_update_found;

    page = ref->page;

    WT_RET(__wt_scr_alloc(session, 0, &key));

    /*
     * Review the insert list for keys before the first entry on the disk page.
     */
    if ((insert = WT_ROW_INSERT_SMALLEST(page)) != NULL)
        WT_ERR(__rts_btree_abort_insert_list(session, page, insert, rollback_timestamp, NULL));

    /*
     * Review updates that belong to keys that are on the disk image, as well as for keys inserted
     * since the page was read from disk.
     */
    WT_ERR(__wt_scr_alloc(session, 0, &key_string));
    WT_ROW_FOREACH (page, rip, i) {
        stable_update_found = false;
        if ((upd = WT_ROW_UPDATE(page, rip)) != NULL) {
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_4,
              WT_RTS_VERB_TAG_UPDATE_CHAIN_VERIFY
              "aborting any unstable updates on the update chain with rollback_timestamp=%s",
              __wt_timestamp_to_string(rollback_timestamp, ts_string));
            WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
            WT_ERR(__rts_btree_abort_update(
              session, key, upd, rollback_timestamp, &stable_update_found));
            have_key = true;
        } else
            have_key = false;

        if ((insert = WT_ROW_INSERT(page, rip)) != NULL) {
            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_4,
              WT_RTS_VERB_TAG_INSERT_LIST_CHECK
              "aborting any unstable updates on the insert list with rollback_timestamp=%s",
              __wt_timestamp_to_string(rollback_timestamp, ts_string));
            WT_ERR(__rts_btree_abort_insert_list(session, page, insert, rollback_timestamp, NULL));
        }

        /*
         * If there is no stable update found in the update list, abort any on-disk value.
         */
        if (!stable_update_found) {
            vpack = &_vpack;
            __wt_row_leaf_value_cell(session, page, rip, vpack);

            __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_3,
              WT_RTS_VERB_TAG_ONDISK_ABORT_CHECK
              "no stable update in update list found. abort any unstable on-disk value with "
              "rollback_timestamp=%s, key=%s",
              __wt_timestamp_to_string(rollback_timestamp, ts_string),
              have_key ? __wt_key_string(
                           session, key->data, key->size, S2BT(session)->key_format, key_string) :
                         NULL);
            WT_ERR(__rts_btree_abort_ondisk_kv(
              session, ref, rip, have_key ? key : NULL, vpack, rollback_timestamp, NULL));
        }
    }

err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &key_string);
    return (ret);
}

/*
 * __wti_rts_btree_abort_updates --
 *     Abort updates on this page newer than the timestamp.
 */
int
__wti_rts_btree_abort_updates(
  WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_PAGE *page;
    bool dryrun, modified;

    dryrun = S2C(session)->rts->dryrun;

    /*
     * If we have a ref with clean page, find out whether the page has any modifications that are
     * newer than the given timestamp. As eviction writes the newest version to page, even a clean
     * page may also contain modifications that need rollback.
     */
    page = ref->page;
    modified = __wt_page_is_modified(page);
    if (!modified && !__wti_rts_visibility_page_needs_abort(session, ref, rollback_timestamp)) {
        __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_3,
          WT_RTS_VERB_TAG_SKIP_UNMODIFIED "ref=%p: unmodified stable page of type=%s skipped",
          (void *)ref, __wt_page_type_str(page->type));
        return (0);
    }

    WT_STAT_CONN_INCR(session, txn_rts_pages_visited);
    __wt_verbose_level_multi(session, WT_VERB_RECOVERY_RTS(session), WT_VERBOSE_DEBUG_2,
      WT_RTS_VERB_TAG_PAGE_ROLLBACK "roll back page of type= %s, addr=%p modified=%s",
      __wt_page_type_str(page->type), (void *)ref, modified ? "true" : "false");

    switch (page->type) {
    case WT_PAGE_ROW_LEAF:
        WT_RET(__rts_btree_abort_row_leaf(session, ref, rollback_timestamp));
        break;
    case WT_PAGE_ROW_INT:
        /* This function is not called for internal pages. */
        WT_ASSERT(session, false);
        /* Fall through. */
    default:
        WT_RET(__wt_illegal_value(session, page->type));
    }

    /* Mark the page as dirty to reconcile the page. */
    if (!dryrun && page->modify)
        __wt_page_modify_set(session, page);
    return (0);
}
