/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __rec_update_stable --
 *     Return whether an update is stable or not.
 */
static bool
__rec_update_stable(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_UPDATE *upd)
{
    return (F_ISSET(r, WT_REC_VISIBLE_ALL) ?
        __wt_txn_upd_visible_all(session, upd) :
        __wt_txn_upd_visible_type(session, upd) == WT_VISIBLE_TRUE &&
          __wt_txn_visible(session, upd->txnid, upd->durable_ts));
}

/*
 * __rec_update_save --
 *     Save a WT_UPDATE list for later restoration.
 */
static int
__rec_update_save(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_INSERT *ins, void *ripcip,
  WT_UPDATE *onpage_upd, size_t upd_memsize)
{
    WT_SAVE_UPD *supd;

    WT_RET(__wt_realloc_def(session, &r->supd_allocated, r->supd_next + 1, &r->supd));
    supd = &r->supd[r->supd_next];
    supd->ins = ins;
    supd->ripcip = ripcip;
    WT_CLEAR(supd->onpage_upd);
    if (onpage_upd != NULL &&
      (onpage_upd->type == WT_UPDATE_STANDARD || onpage_upd->type == WT_UPDATE_MODIFY))
        supd->onpage_upd = onpage_upd;
    ++r->supd_next;
    r->supd_memsize += upd_memsize;
    return (0);
}

/*
 * __rec_append_orig_value --
 *     Append the key's original value to its update list.
 */
static int
__rec_append_orig_value(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd, WT_CELL_UNPACK *unpack)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_UPDATE *append, *tombstone;
    size_t size, total_size;

    for (;; upd = upd->next) {
        /* Done if at least one self-contained update is globally visible. */
        if (WT_UPDATE_DATA_VALUE(upd) && __wt_txn_upd_visible_all(session, upd))
            return (0);

        /* Add the original value after birthmarks. */
        if (upd->type == WT_UPDATE_BIRTHMARK) {
            WT_ASSERT(session, unpack != NULL && unpack->type != WT_CELL_DEL);
            break;
        }

        /* On page value already on chain */
        if (unpack != NULL && unpack->start_ts == upd->start_ts && unpack->start_txn == upd->txnid)
            return (0);

        /* Leave reference at the last item in the chain. */
        if (upd->next == NULL)
            break;
    }

    /*
     * We need the original on-page value for some reader: get a copy and append it to the end of
     * the update list with a transaction ID that guarantees its visibility.
     *
     * If we don't have a value cell, it's an insert/append list key/value pair which simply doesn't
     * exist for some reader; place a deleted record at the end of the update list.
     */
    append = tombstone = NULL; /* -Wconditional-uninitialized */
    total_size = size = 0;     /* -Wconditional-uninitialized */
    if (unpack == NULL || unpack->type == WT_CELL_DEL)
        WT_RET(__wt_update_alloc(session, NULL, &append, &size, WT_UPDATE_TOMBSTONE));
    else {
        /* Timestamp should always be in descending order */
        WT_ASSERT(session, upd->start_ts >= unpack->start_ts);

        WT_RET(__wt_scr_alloc(session, 0, &tmp));
        WT_ERR(__wt_page_cell_data_ref(session, page, unpack, tmp));
        WT_ERR(__wt_update_alloc(session, tmp, &append, &size, WT_UPDATE_STANDARD));
        append->start_ts = append->durable_ts = unpack->start_ts;
        append->txnid = unpack->start_txn;
        total_size = size;

        /*
         * We need to append a TOMBSTONE before the onpage value if the onpage value has a valid
         * stop pair.
         *
         * Imagine a case we insert and delete a value respectively at timestamp 0 and 10, and later
         * insert it again at 20. We need the TOMBSTONE to tell us there is no value between 10 and
         * 20.
         */
        if (unpack->stop_ts != WT_TS_MAX || unpack->stop_txn != WT_TXN_MAX) {
            /* Timestamp should always be in descending order */
            WT_ASSERT(session, upd->start_ts >= unpack->stop_ts);

            WT_ERR(__wt_update_alloc(session, NULL, &tombstone, &size, WT_UPDATE_TOMBSTONE));
            tombstone->txnid = unpack->stop_txn;
            tombstone->start_ts = unpack->stop_ts;
            tombstone->durable_ts = unpack->stop_ts;
            tombstone->next = append;
            total_size += size;
        }
    }

    /*
     * If we're saving the original value for a birthmark, transfer over the transaction ID and
     * clear out the birthmark update. Else, set the entry's transaction information to the lowest
     * possible value (as cleared memory matches the lowest possible transaction ID and timestamp,
     * do nothing).
     */
    if (upd->type == WT_UPDATE_BIRTHMARK) {
        /* FIXME-PM-1521: temporarily disable the assert until we figured out what is wrong */
        /* WT_ASSERT(session, append->start_ts == upd->start_ts && append->txnid == upd->txnid); */
        append->next = upd->next;
    }

    if (tombstone != NULL)
        append = tombstone;

    /* Append the new entry into the update list. */
    WT_PUBLISH(upd->next, append);

    /* Replace the birthmark with an aborted transaction. */
    if (upd->type == WT_UPDATE_BIRTHMARK) {
        WT_ORDERED_WRITE(upd->txnid, WT_TXN_ABORTED);
        WT_ORDERED_WRITE(upd->type, WT_UPDATE_STANDARD);
    }

    __wt_cache_page_inmem_incr(session, page, total_size);

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __rec_need_save_upd --
 *     Return if we need to save the update chain
 */
static bool
__rec_need_save_upd(WT_SESSION_IMPL *session, WT_UPDATE *selected_upd, uint64_t max_txn,
  wt_timestamp_t max_ts, bool list_uncommitted, uint64_t flags)
{
    /* Always save updates for in-memory database. */
    if (LF_ISSET(WT_REC_IN_MEMORY))
        return true;

    if (!LF_ISSET(WT_REC_HISTORY_STORE))
        return false;

    if (LF_ISSET(WT_REC_EVICT) && list_uncommitted)
        return true;

    /* When in checkpoint, no need to save update if no onpage value is selected. */
    if (LF_ISSET(WT_REC_CHECKPOINT) && selected_upd == NULL)
        return false;

    /* No need to save updates if everything is globally visible. */
    return !__wt_txn_visible_all(session, max_txn, max_ts);
}

/*
 * __wt_rec_upd_select --
 *     Return the update in a list that should be written (or NULL if none can be written).
 */
int
__wt_rec_upd_select(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_INSERT *ins, void *ripcip,
  WT_CELL_UNPACK *vpack, WT_UPDATE_SELECT *upd_select)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_PAGE *page;
    WT_UPDATE *first_txn_upd, *first_upd, *upd, *last_upd;
    wt_timestamp_t max_ts;
    size_t size, upd_memsize;
    uint64_t max_txn, txnid;
    bool list_uncommitted;

    /*
     * The "saved updates" return value is used independently of returning an update we can write,
     * both must be initialized.
     */
    upd_select->upd = NULL;
    upd_select->upd_saved = false;

    page = r->page;
    first_txn_upd = upd = last_upd = NULL;
    upd_memsize = 0;
    max_ts = WT_TS_NONE;
    max_txn = WT_TXN_NONE;
    list_uncommitted = false;

    /*
     * If called with a WT_INSERT item, use its WT_UPDATE list (which must exist), otherwise check
     * for an on-page row-store WT_UPDATE list (which may not exist). Return immediately if the item
     * has no updates.
     */
    if (ins != NULL)
        first_upd = ins->upd;
    else if ((first_upd = WT_ROW_UPDATE(page, ripcip)) == NULL)
        return (0);

    for (upd = first_upd; upd != NULL; upd = upd->next) {
        if ((txnid = upd->txnid) == WT_TXN_ABORTED)
            continue;

        ++r->updates_seen;
        upd_memsize += WT_UPDATE_MEMSIZE(upd);

        /*
         * Track the first update in the chain that is not aborted and the maximum transaction ID.
         */
        if (first_txn_upd == NULL)
            first_txn_upd = upd;
        if (WT_TXNID_LT(max_txn, txnid))
            max_txn = txnid;

        /*
         * Check whether the update was committed before reconciliation started. The global commit
         * point can move forward during reconciliation so we use a cached copy to avoid races when
         * a concurrent transaction commits or rolls back while we are examining its updates. As
         * prepared transaction IDs are globally visible, need to check the update state as well.
         *
         * The checkpoint transaction doesn't pin the oldest txn id, therefore the r->last_running
         * can move beyond the checkpoint transaction id. Need to do a proper visibility check for
         * metadata pages. Otherwise, eviction may select uncommitted metadata updates to write to
         * disk.
         */
        if (F_ISSET(r, WT_REC_VISIBLE_ALL) && !WT_IS_METADATA(session->dhandle) ?
            WT_TXNID_LE(r->last_running, txnid) :
            !__txn_visible_id(session, txnid)) {
            list_uncommitted = true;
            continue;
        }
        if (upd->prepare_state == WT_PREPARE_LOCKED ||
          upd->prepare_state == WT_PREPARE_INPROGRESS) {
            list_uncommitted = true;
            if (upd->start_ts > max_ts)
                max_ts = upd->start_ts;

            /*
             * Track the oldest update not on the page, used to decide whether reads can use the
             * page image, hence using the start rather than the durable timestamp.
             */
            if (upd->start_ts < r->min_skipped_ts)
                r->min_skipped_ts = upd->start_ts;
            continue;
        }

        /* Track the first update with non-zero timestamp. */
        if (upd->start_ts > max_ts)
            max_ts = upd->start_ts;

        /* Always select the newest committed update to write to disk */
        if (upd_select->upd == NULL)
            upd_select->upd = upd;

        if (!__rec_update_stable(session, r, upd)) {
            if (F_ISSET(r, WT_REC_EVICT))
                ++r->updates_unstable;

            /*
             * Rare case: when applications run at low isolation levels, update/restore eviction may
             * see a stable update followed by an uncommitted update. Give up in that case: we need
             * to discard updates from the stable update and older for correctness and we can't
             * discard an uncommitted update.
             */
            if (upd_select->upd != NULL && list_uncommitted)
                return (__wt_set_return(session, EBUSY));
        } else if (!F_ISSET(r, WT_REC_EVICT))
            break;
    }

    /* Keep track of the selected update. */
    upd = upd_select->upd;

    /* Reconciliation should never see an aborted or reserved update. */
    WT_ASSERT(
      session, upd == NULL || (upd->txnid != WT_TXN_ABORTED && upd->type != WT_UPDATE_RESERVE));

    /*
     * The checkpoint transaction is special. Make sure we never write metadata updates from a
     * checkpoint in a concurrent session.
     */
    WT_ASSERT(session, !WT_IS_METADATA(session->dhandle) || upd == NULL ||
        upd->txnid == WT_TXN_NONE || upd->txnid != S2C(session)->txn_global.checkpoint_state.id ||
        WT_SESSION_IS_CHECKPOINT(session));

    /* If all of the updates were aborted, quit. */
    if (first_txn_upd == NULL) {
        WT_ASSERT(session, upd == NULL);
        return (0);
    }

    if (upd != NULL && upd->durable_ts > r->max_ondisk_ts)
        r->max_ondisk_ts = upd->durable_ts;

    /*
     * The start timestamp is determined by the commit timestamp when the key is first inserted (or
     * last updated). The end timestamp is set when a key/value pair becomes invalid, either because
     * of a remove or a modify/update operation on the same key.
     *
     * In the case of a tombstone where the previous update is the ondisk value, we'll allocate an
     * update here to represent the ondisk value. Keep a pointer to the original update (the
     * tombstone) since we do some pointer comparisons below to check whether or not all updates are
     * stable.
     */
    if (upd != NULL) {
        upd_select->durable_ts = WT_TS_NONE;
        upd_select->start_ts = WT_TS_NONE;
        upd_select->start_txn = WT_TXN_NONE;
        upd_select->stop_ts = WT_TS_MAX;
        upd_select->stop_txn = WT_TXN_MAX;
        /*
         * If the newest is a tombstone then select the update before it and set the end of the
         * visibility window to its time pair as appropriate to indicate that we should return "not
         * found" for reads after this point.
         *
         * Otherwise, leave the end of the visibility window at the maximum possible value to
         * indicate that the value is visible to any timestamp/transaction id ahead of it.
         */
        if (upd->type == WT_UPDATE_TOMBSTONE) {
            if (upd->start_ts != WT_TS_NONE)
                upd_select->stop_ts = upd->start_ts;
            if (upd->txnid != WT_TXN_NONE)
                upd_select->stop_txn = upd->txnid;
            /* Ignore all the aborted transactions. */
            while (upd->next != NULL && upd->next->txnid == WT_TXN_ABORTED)
                upd = upd->next;
            WT_ASSERT(session, upd->next == NULL || upd->next->txnid != WT_TXN_ABORTED);
            if (upd->next == NULL)
                last_upd = upd;
            upd_select->upd = upd = upd->next;
        }
        if (upd != NULL) {
            /* The beginning of the validity window is the selected update's time pair. */
            if (upd->start_ts < upd_select->stop_ts)
                upd_select->durable_ts = upd_select->start_ts = upd->start_ts;
            if (upd->txnid < upd_select->stop_txn)
                upd_select->start_txn = upd->txnid;
        } else {
            /* If we only have a tombstone in the update list, we must have an ondisk value. */
            WT_ASSERT(session, vpack != NULL);
            /*
             * It's possible to have a tombstone as the only update in the update list. If we
             * reconciled before with only a single update and then read the page back into cache,
             * we'll have an empty update list. And applying a delete on top of that will result in
             * ONLY a tombstone in the update list.
             *
             * In this case, we should leave the selected update unset to indicate that we want to
             * keep the same on-disk value but set the stop time pair to indicate that the validity
             * window ends when this tombstone started.
             *
             * FIXME-PM-1521: Any workload/test that involves reopening a connection or opening a
             * connection on an existing database will run into issues with the logic below. When
             * opening a connection, the transaction id allocations are reset and therefore, on-disk
             * values that were previously written can have later timestamps/transaction ids than
             * new updates being applied which can lead to a malformed cell (stop time pair earlier
             * than start time pair). I've added some checks below to guard against malformed cells
             * but this logic still isn't correct and should be handled properly when we begin the
             * recovery work.
             */
            if (vpack->start_ts < upd_select->stop_ts)
                upd_select->durable_ts = upd_select->start_ts = vpack->start_ts;
            if (vpack->start_txn < upd_select->stop_txn)
                upd_select->start_txn = vpack->start_txn;
            /*
             * Leaving the update unset means that we can skip reconciling. If we've set the stop
             * time pair because of a tombstone after the on-disk value, we still have work to do so
             * that is NOT ok. Let's append the on-disk value to the chain.
             *
             * FIXME-PM-1521: How are we going to remove deleted keys from disk image? We may need
             * to return a different return code to tell reconciliation it is safe to remove the
             * key. In that case, we can use __rec_append_orig_value instead of duplicating code
             * here.
             */
            WT_ERR(__wt_scr_alloc(session, 0, &tmp));
            WT_ERR(__wt_page_cell_data_ref(session, page, vpack, tmp));
            WT_ERR(__wt_update_alloc(session, tmp, &upd, &size, WT_UPDATE_STANDARD));
            upd->start_ts = upd->durable_ts = vpack->start_ts;
            upd->txnid = vpack->start_txn;
            WT_PUBLISH(last_upd->next, upd);
            upd_select->upd = upd;
        }
        WT_ASSERT(session, upd == NULL || upd->type != WT_UPDATE_TOMBSTONE);
    }

    /*
     * Track the most recent transaction in the page. We store this in the tree at the end of
     * reconciliation in the service of checkpoints, it is used to avoid discarding trees from
     * memory when they have changes required to satisfy a snapshot read.
     */
    if (WT_TXNID_LT(r->max_txn, max_txn))
        r->max_txn = max_txn;

    /* Update the maximum timestamp. */
    if (max_ts > r->max_ts)
        r->max_ts = max_ts;

    /*
     * If the update we chose was a birthmark.
     */
    if (upd != NULL && upd->type == WT_UPDATE_BIRTHMARK) {
        /*
         * Resolve the birthmark now regardless of whether the update being written to the data file
         * is the same as it was the previous reconciliation. Otherwise the history store can end up
         * with two birthmark records in the same update chain.
         */
        WT_ERR(__rec_append_orig_value(session, page, upd, vpack));
        upd_select->upd = NULL;
    }

    /* Should not see uncommitted changes in the history store */
    WT_ASSERT(session, !F_ISSET(S2BT(session), WT_BTREE_HISTORY_STORE) || !list_uncommitted);

    r->leave_dirty |= list_uncommitted;

    /*
     * The update doesn't have any further updates that need to be written to the history store,
     * skip saving the update as saving the update will cause reconciliation to think there is work
     * that needs to be done when there might not be.
     *
     * Additionally history store reconciliation is not set skip saving an update.
     */
    if (__rec_need_save_upd(
          session, upd_select->upd, max_txn, max_ts, list_uncommitted, r->flags)) {
        WT_ASSERT(session, r->max_txn != WT_TS_NONE);

        WT_ERR(__rec_update_save(session, r, ins, ripcip, upd_select->upd, upd_memsize));
        upd_select->upd_saved = true;
    }

    /*
     * Paranoia: check that we didn't choose an update that has since been rolled back.
     */
    WT_ASSERT(session, upd_select->upd == NULL || upd_select->upd->txnid != WT_TXN_ABORTED);

    /*
     * Returning an update means the original on-page value might be lost, and that's a problem if
     * there's a reader that needs it. This call makes a copy of the on-page value and if there is a
     * birthmark in the update list, replaces it. We do that any time there are saved updates and
     * during reconciliation of a backing overflow record that will be physically removed once it's
     * no longer needed
     */
    if (upd_select->upd != NULL &&
      (upd_select->upd_saved ||
          (vpack != NULL && vpack->ovfl && vpack->raw != WT_CELL_VALUE_OVFL_RM)))
        WT_ERR(__rec_append_orig_value(session, page, upd_select->upd, vpack));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}
