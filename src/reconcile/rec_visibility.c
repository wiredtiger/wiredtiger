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
__rec_append_orig_value(WT_SESSION_IMPL *session, WT_PAGE *page, WT_INSERT *ins, void *ripcip,
  WT_UPDATE *upd, WT_CELL_UNPACK *unpack)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_PAGE_MODIFY *mod;
    WT_UPDATE *append, **upd_entry;
    size_t size;

    mod = page->modify;
    upd_entry = NULL;

    if (upd != NULL) {
        /* Done if at least one self-contained update is globally visible. */
        for (;; upd = upd->next) {
            if (WT_UPDATE_DATA_VALUE(upd) && __wt_txn_upd_visible_all(session, upd))
                return (0);

            /* Add the original value after birthmarks. */
            if (upd->type == WT_UPDATE_BIRTHMARK) {
                WT_ASSERT(session, unpack != NULL && unpack->type != WT_CELL_DEL);
                break;
            }

            /* Leave reference at the last item in the chain. */
            if (upd->next == NULL)
                break;
        }
    } else {
        /* There are no updates for this key yet. Allocate an update array if necessary. */
        if (ins == NULL) {
            WT_ASSERT(session, WT_ROW_UPDATE(page, ripcip) == NULL);

            /* Allocate an update array if necessary. */
            WT_PAGE_ALLOC_AND_SWAP(session, page, mod->mod_row_update, upd_entry, page->entries);

            /* Set the WT_UPDATE array reference. */
            upd_entry = &page->modify->mod_row_update[WT_ROW_SLOT(page, ripcip)];
        } else {
            WT_ASSERT(session, ins->upd == NULL);
            upd_entry = &ins->upd;
        }
    }

    /*
     * We need the original on-page value for some reader: get a copy and append it to the end of
     * the update list with a transaction ID that guarantees its visibility.
     *
     * If we don't have a value cell, it's an insert/append list key/value pair which simply doesn't
     * exist for some reader; place a deleted record at the end of the update list.
     */
    append = NULL; /* -Wconditional-uninitialized */
    size = 0;      /* -Wconditional-uninitialized */
    if (unpack == NULL || unpack->type == WT_CELL_DEL)
        WT_RET(__wt_update_alloc(session, NULL, &append, &size, WT_UPDATE_TOMBSTONE));
    else {
        WT_RET(__wt_scr_alloc(session, 0, &tmp));
        WT_ERR(__wt_page_cell_data_ref(session, page, unpack, tmp));
        WT_ERR(__wt_update_alloc(session, tmp, &append, &size, WT_UPDATE_STANDARD));
    }

    /*
     * If we're saving the original value for a birthmark, transfer over the transaction ID and
     * clear out the birthmark update.
     *
     * Else, set the entry's transaction information to the lowest possible value. Cleared memory
     * matches the lowest possible transaction ID and timestamp, do nothing.
     */
    if (upd != NULL && upd->type == WT_UPDATE_BIRTHMARK) {
        append->txnid = upd->txnid;
        append->start_ts = upd->start_ts;
        append->durable_ts = upd->durable_ts;
        append->next = upd->next;
    }

    /* Append the new entry into the update list. */
    if (upd_entry != NULL)
        WT_PUBLISH(*upd_entry, append);
    else
        WT_PUBLISH(upd->next, append);
    __wt_cache_page_inmem_incr(session, page, size);

    if (upd->type == WT_UPDATE_BIRTHMARK) {
        upd->type = WT_UPDATE_STANDARD;
        upd->txnid = WT_TXN_ABORTED;
    }

err:
    __wt_scr_free(session, &tmp);
    return (ret);
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
    WT_UPDATE *first_stable_upd, *first_txn_upd, *first_upd, *orig_upd, *upd;
    wt_timestamp_t max_ts;
    size_t size, upd_memsize;
    uint64_t max_txn, txnid;
    bool all_stable, list_prepared, list_uncommitted;

    /*
     * The "saved updates" return value is used independently of returning an update we can write,
     * both must be initialized.
     */
    upd_select->upd = NULL;
    upd_select->upd_saved = false;

    page = r->page;
    first_stable_upd = first_txn_upd = upd = NULL;
    upd_memsize = 0;
    max_ts = WT_TS_NONE;
    max_txn = WT_TXN_NONE;
    list_prepared = list_uncommitted = false;

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
         */
        if (F_ISSET(r, WT_REC_VISIBLE_ALL) ? WT_TXNID_LE(r->last_running, txnid) :
                                             !__txn_visible_id(session, txnid)) {
            r->update_uncommitted = list_uncommitted = true;
            continue;
        }
        if (upd->prepare_state == WT_PREPARE_LOCKED ||
          upd->prepare_state == WT_PREPARE_INPROGRESS) {
            r->update_prepared = list_prepared = true;
            if (upd->start_ts > max_ts)
                max_ts = upd->start_ts;
            continue;
        }

        /* Track the first update with non-zero timestamp. */
        if (upd->durable_ts > max_ts)
            max_ts = upd->durable_ts;

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
            if (F_ISSET(r, WT_REC_UPDATE_RESTORE) && upd_select->upd != NULL &&
              (list_prepared || list_uncommitted))
                return (__wt_set_return(session, EBUSY));
        } else if (first_stable_upd == NULL) {
            /*
             * Track the first update in the chain that is stable.
             */
            first_stable_upd = upd;

            if (!F_ISSET(r, WT_REC_EVICT))
                break;
        }
    }

    /* Keep track of the selected update. */
    upd = upd_select->upd;

    /* Reconciliation should never see an aborted or reserved update. */
    WT_ASSERT(
      session, upd == NULL || (upd->txnid != WT_TXN_ABORTED && upd->type != WT_UPDATE_RESERVE));

    /*
     * The checkpoint transaction is special. Make sure we never write metadata updates from a
     * checkpoint in a concurrent session.
     *
     * FIXME-PM-1521: temporarily disable the assert until we figured out what is wrong
     */
    // WT_ASSERT(session, !WT_IS_METADATA(session->dhandle) || upd == NULL ||
    //     upd->txnid == WT_TXN_NONE || upd->txnid != S2C(session)->txn_global.checkpoint_state.id
    //     ||
    //     WT_SESSION_IS_CHECKPOINT(session));

    /* If all of the updates were aborted, quit. */
    if (first_txn_upd == NULL) {
        WT_ASSERT(session, upd == NULL);
        return (0);
    }

    /*
     * If the selected on disk value is stable, record that we're making progress.
     *
     * FIXME-PM-1521: Should remove this when we change the eviction flow
     */
    if (upd == first_stable_upd)
        r->update_used = true;

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
    orig_upd = upd;
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
             */
            if (vpack->start_ts < upd_select->stop_ts)
                upd_select->durable_ts = upd_select->start_ts = vpack->start_ts;
            if (vpack->start_txn < upd_select->stop_txn)
                upd_select->start_txn = vpack->start_txn;
            /*
             * Leaving the update unset means that we can skip reconciling. If we've set the stop
             * time pair because of a tombstone after the on-disk value, we still have work to do so
             * that is NOT ok. Let's allocate an update equivalent to the on-disk value and continue
             * on our way!
             */
            WT_ERR(__wt_scr_alloc(session, 0, &tmp));
            WT_ERR(__wt_page_cell_data_ref(session, page, vpack, tmp));
            WT_ERR(__wt_update_alloc(session, tmp, &upd, &size, WT_UPDATE_STANDARD));
            upd->ext = 1;
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
         * is the same as it was the previous reconciliation. Otherwise lookaside can end up with
         * two birthmark records in the same update chain.
         */
        WT_ERR(__rec_append_orig_value(session, page, ins, ripcip, first_upd, vpack));
        upd_select->upd = NULL;
    }

    /*
     * Check if all updates on the page are visible, if not, it must stay dirty.
     *
     * Updates can be out of transaction ID order (but not out of timestamp order), so we track the
     * maximum transaction ID and the newest update with a timestamp (if any).
     *
     * FIXME-PM-1521: In durable history, page should be clean after reconciliation if there is no
     * uncommitted and prepared updates. However, we cannot change it here as we need to first
     * implement inserting older versions to history store for update restore.
     */
    all_stable = orig_upd == first_stable_upd && !list_prepared && !list_uncommitted &&
      __wt_txn_visible_all(session, max_txn, max_ts);

    if (all_stable)
        goto check_original_value;

    r->leave_dirty = true;

    if (F_ISSET(r, WT_REC_VISIBILITY_ERR))
        WT_PANIC_ERR(session, EINVAL, "reconciliation error, update not visible");

    /* If not trying to evict the page, we know what we'll write and we're done.
     * FIXME-PM-1521: We need to save updates for checkpoints as it needs to write to history store
     * as well
     */
    if (!F_ISSET(r, WT_REC_EVICT))
        goto check_original_value;

    /*
     * We are attempting eviction with changes that are not yet stable (i.e. globally visible).
     * There are two ways to continue, the save/restore eviction path or the lookaside table
     * eviction path. Both cannot be configured because the paths track different information. The
     * update/restore path can handle uncommitted changes, by evicting most of the page and then
     * creating a new, smaller page to which we re-attach those changes. Lookaside eviction writes
     * changes into the lookaside table and restores them on demand if and when the page is read
     * back into memory.
     *
     * Both paths are configured outside of reconciliation: the save/restore path is the
     * WT_REC_UPDATE_RESTORE flag, the lookaside table path is the WT_REC_LOOKASIDE flag.
     */
    if (!F_ISSET(r, WT_REC_LOOKASIDE | WT_REC_UPDATE_RESTORE))
        WT_ERR(__wt_set_return(session, EBUSY));
    if (list_uncommitted && !F_ISSET(r, WT_REC_UPDATE_RESTORE))
        WT_ERR(__wt_set_return(session, EBUSY));

    WT_ASSERT(session, r->max_txn != WT_TXN_NONE);

    WT_ERR(__rec_update_save(session, r, ins, ripcip, upd_select->upd, upd_memsize));
    upd_select->upd_saved = true;

check_original_value:
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
        WT_ERR(__rec_append_orig_value(session, page, ins, ripcip, first_upd, vpack));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}
