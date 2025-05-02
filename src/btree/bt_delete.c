/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Fast-delete (also called "fast-truncate") support.
 *
 * This file contains most of the code that allows WiredTiger to delete pages of data without
 * reading them into the cache.
 *
 * The way session truncate works is it explicitly reads the first and last pages of the truncate
 * range, then walks the tree with a flag so the tree walk code skips reading eligible pages within
 * the range and instead just marks them as deleted, by changing their WT_REF state to
 * WT_REF_DELETED. Pages ineligible for this fast path ("fast-truncate" or "fast-delete") include
 * pages that are already in the cache and can not be evicted, records in the pages that are not
 * visible to the transaction, pages containing overflow items, pages containing prepared values, or
 * pages that belong to FLCS trees. Ineligible pages are read and have their rows updated/deleted
 * individually ("slow-truncate"). The transaction for the delete operation is stored in memory
 * referenced by the WT_REF.page_del field.
 *
 * Future cursor walks of the tree will skip the deleted page based on the transaction stored for
 * the delete, but it gets more complicated if a read is done using a random key, or a cursor walk
 * is done with a transaction where the delete is not visible, or if an update is applied. In those
 * cases, we read the original contents of the page. The page-read code notices a deleted page is
 * being read, and as part of the read instantiates the contents of the page, creating tombstone
 * WT_UPDATE records, in the same transaction that deleted the page. In other words, the read
 * process makes it appear as if the page was read and each individual row deleted, exactly as would
 * have happened if the page had been in the cache all along.
 *
 * There's an additional complication to support transaction rollback of the page delete. When the
 * page was marked deleted, a pointer to the WT_REF was saved in the deleting session's transaction
 * list and the delete is unrolled by resetting the WT_REF_DELETED state back to WT_REF_DISK.
 * However, if the page has been instantiated by some reading thread, that's not enough; each
 * individual row on the page must have the delete operation reset. If the page split, the WT_UPDATE
 * lists might have been saved/restored during reconciliation and appear on multiple pages, and the
 * WT_REF stored in the deleting session's transaction list is no longer useful. For this reason,
 * when the page is instantiated by a read, a list of the WT_UPDATE structures on the page is stored
 * in the WT_PAGE_MODIFY.inst_updates field. That way the session resolving the delete can find all
 * WT_UPDATE structures that require update.
 *
 * There are two other ways pages can be marked deleted: if they reconcile empty, or if they are
 * found to be eligible for deletion and contain only obsolete items. (The latter is known as
 * "checkpoint cleanup" and happens in bt_sync.c.) There are also two cases in which deleted pages
 * are manufactured out of thin air: in VLCS, if a key-space gap exists between the start recno of
 * an internal page and the start recno of its first child, a deleted page is created to cover this
 * space; and, when new trees are created they are created with a single deleted leaf page. In these
 * cases, the WT_REF state will be set to WT_REF_DELETED but there will not be any associated
 * WT_REF.page_del field since the page contains no data. These pages are always skipped during
 * cursor traversal, and if read is forced to instantiate such a page, it creates an empty page from
 * scratch.
 */

/*
 * __wti_delete_page --
 *     If deleting a range, try to delete the page without instantiating it.
 */
int
__wti_delete_page(WT_SESSION_IMPL *session, WT_REF *ref, bool *skipp)
{
    WT_ADDR_COPY addr;
    WT_DECL_RET;
    WT_REF_STATE previous_state;

    *skipp = false;

    /* If we have a clean page in memory, attempt to evict it. */
    previous_state = WT_REF_GET_STATE(ref);
    if (previous_state == WT_REF_MEM &&
      WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED)) {
        if (__wt_page_is_modified(ref->page)) {
            WT_REF_SET_STATE(ref, previous_state);
            return (0);
        }

        WT_RET(__wt_curhs_cache(session));
        (void)__wt_atomic_addv32(&S2BT(session)->evict_busy, 1);
        ret = __wt_evict(session, ref, previous_state, 0);
        (void)__wt_atomic_subv32(&S2BT(session)->evict_busy, 1);
        WT_RET_BUSY_OK(ret);
        ret = 0;
    }

    /*
     * Fast check to see if it's worth locking, then atomically switch the page's state to lock it.
     */
    previous_state = WT_REF_GET_STATE(ref);
    if (previous_state != WT_REF_DISK)
        return (0);

    if (!WT_REF_CAS_STATE(session, ref, WT_REF_DISK, WT_REF_LOCKED))
        return (0);

    /*
     * There should be no previous page-delete information: if the page was previously deleted and
     * remains deleted, it'll be in WT_REF_DELETED state and we won't get here to do another delete.
     * If the page was previously deleted and instantiated, we can only get here if it was written
     * out again or we successfully just evicted it; in that case, the reconciliation will have
     * cleared the final traces of the previous deletion and instantiation. Furthermore, any prior
     * deletion must have committed or another attempt would have failed with an update conflict.
     */
    WT_ASSERT(session, ref->page_del == NULL);

    /*
     * We cannot truncate pages that have overflow key/value items as the overflow blocks have to be
     * discarded. The way we figure that out is to check the page's cell type, cells for leaf pages
     * without overflow items are special.
     *
     * Additionally, if the page has prepared updates or the aggregated start time point on the page
     * is not visible to us then we cannot truncate the page.
     *
     * Note that we indicate this by succeeding without setting the skip flag, not via EBUSY.
     */
    if (!__wt_ref_addr_copy(session, ref, &addr))
        goto err;
    if (addr.type != WT_ADDR_LEAF_NO)
        goto err;
    if (addr.ta.prepare)
        goto err;

    /*
     * When performing a truncate operation with no associated timestamp, limit fast-truncate to
     * pages where all its data is globally visible. This is done to prevent data in the history
     * store (that should have been cleared) from appearing again. Technically we don't need to
     * check the newest stop durable timestamp, but for consistency, we check for the maximum of
     * both the start and stop timestamps.
     */
    if (F_ISSET(session->txn, WT_TXN_TS_NOT_SET) &&
      !__wt_txn_visible_all(session, addr.ta.newest_txn,
        WT_MAX(addr.ta.newest_start_durable_ts, addr.ta.newest_stop_durable_ts)))
        goto err;

    /*
     * History store data are always visible. No need to check visibility. Other than history store,
     * use the max durable timestamp that is available in the page aggregation for the visibility
     * checks as we do not track the aggregated commit timestamp.
     */
    if (!WT_IS_HS(session->dhandle) &&
      !__wt_txn_snap_min_visible(session, addr.ta.newest_txn,
        WT_MAX(addr.ta.newest_start_durable_ts, addr.ta.newest_stop_durable_ts),
        WT_MAX(addr.ta.newest_start_durable_ts, addr.ta.newest_stop_durable_ts)))
        goto err;

    /*
     * This action dirties the parent page: mark it dirty now, there's no future reconciliation of
     * the child leaf page that will dirty it as we write the tree.
     */
    WT_ERR(__wt_page_parent_modify_set(session, ref, false));

    /*
     * Allocate and initialize `page_del` for pages, excluding those in the history store.
     *
     * A `null` `page_del` means that a fast-truncated page is globally visible. Since truncation in
     * the history store is non-transactional and applies only to globally visible pages, ensure
     * that `page_del` remains null for history store pages.
     *
     * An exception is selective backup, which can truncate non-globally visible history store
     * pages. However, since this data is intended for permanent discard, it can also be treated as
     * globally visible without causing issues.
     */
    if (!WT_IS_HS(session->dhandle)) {
        WT_ERR(__wt_calloc_one(session, &ref->page_del));
        WT_ERR(__wt_txn_modify_page_delete(session, ref));
    }

    *skipp = true;
    WT_STAT_CONN_DSRC_INCR(session, rec_page_delete_fast);

    /* Set the page to its new state. */
    WT_REF_SET_STATE(ref, WT_REF_DELETED);
    return (0);

err:
    __wt_free(session, ref->page_del);

    /* Return the page to its previous state. */
    WT_REF_SET_STATE(ref, WT_REF_DISK);
    return (ret);
}

/*
 * __wt_delete_page_rollback --
 *     Transaction rollback for a fast-truncate operation.
 */
int
__wt_delete_page_rollback(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_REF_STATE current_state;
    WT_UPDATE **updp;
    uint64_t sleep_usecs, yield_count;
    bool locked;

    /* Lock the reference. We cannot access ref->page_del except when locked. */
    for (locked = false, sleep_usecs = yield_count = 0;;) {
        switch (current_state = WT_REF_GET_STATE(ref)) {
        case WT_REF_LOCKED:
            break;
        case WT_REF_DELETED:
        case WT_REF_MEM:
        case WT_REF_SPLIT:
            if (WT_REF_CAS_STATE(session, ref, current_state, WT_REF_LOCKED))
                locked = true;
            break;
        case WT_REF_DISK:
        default:
            return (__wt_illegal_value(session, current_state));
        }

        if (locked)
            break;

        /*
         * We wait for the change in page state, yield before retrying, and if we've yielded enough
         * times, start sleeping so we don't burn CPU to no purpose.
         */
        __wt_spin_backoff(&yield_count, &sleep_usecs);
        WT_STAT_CONN_INCRV(session, page_del_rollback_blocked, sleep_usecs);
    }

    /*
     * There are two possible cases:
     *
     * 1. The state is WT_REF_DELETED. In this case page_del cannot be null, because the
     * operation cannot reach global visibility while its transaction remains uncommitted. The page
     * itself is as we left it, so we can just reset the state.
     *
     * 2. The state is WT_REF_MEM. We check mod->inst_updates for a list of updates to abort. Allow
     * the update list to be null to be conservative.
     */
    if (current_state == WT_REF_DELETED) {
        /*
         * When fast truncate succeeds it moves a WT_REF from WT_REF_DISK to WT_REF_DELETED. Thus
         * the reverse operation is to return the state to WT_REF_DISK.
         */
        current_state = WT_REF_DISK;
        /*
         * Don't set the WT_PAGE_DELETED transaction ID to aborted; instead, just discard the
         * structure. This avoids having to check for an aborted delete in other situations.
         */
        __wt_free(session, ref->page_del);
    } else {
        WT_ASSERT(session, ref->page != NULL && ref->page->modify != NULL);
        if ((updp = ref->page->modify->inst_updates) != NULL) {
            /*
             * Walk any list of update structures and abort them. We can't use the normal read path
             * to get the pages with updates (the original page may have split, so there may be more
             * than one page), because the session may have closed the cursor, and we no longer have
             * the reference to the tree required for a hazard pointer. We're safe since pages with
             * unresolved transactions aren't going anywhere.
             */
            for (; *updp != NULL; ++updp)
                (*updp)->txnid = WT_TXN_ABORTED;
            /* Now discard the updates. */
            __wt_free(session, ref->page->modify->inst_updates);
        }
        /*
         * Drop any page_deleted information remaining in the ref. Note that while this must have
         * been an instantiated page, the information (and flag) is only kept until the page is
         * reconciled for the first time after instantiation, so it might not be set now.
         */
        if (ref->page->modify->instantiated) {
            ref->page->modify->instantiated = false;
            __wt_free(session, ref->page_del);
        }
    }

    WT_REF_SET_STATE(ref, current_state);
    return (0);
}

/*
 * __delete_redo_window_cleanup_internal --
 *     Process one internal page for __wt_delete_redo_window_cleanup. This fixes up the transaction
 *     IDs in the delete info. Since we're called at the end of recovery there's no need to lock the
 *     ref or worry about races.
 */
static void
__delete_redo_window_cleanup_internal(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_REF *child;

    WT_ASSERT(session, F_ISSET(ref, WT_REF_FLAG_INTERNAL));
    if (ref->page != NULL) {
        WT_INTL_FOREACH_BEGIN (session, ref->page, child) {
            if (WT_REF_GET_STATE(child) == WT_REF_DELETED && child->page_del != NULL)
                __cell_redo_page_del_cleanup(session, ref->page->dsk, child->page_del);
        }
        WT_INTL_FOREACH_END;
    }
}

/*
 * __delete_redo_window_cleanup_skip --
 *     Tree-walk skip function for __wt_delete_redo_window_cleanup. This skips all leaf pages; we'll
 *     visit all in-memory internal pages via the flag settings on the tree-walk call. Note that we
 *     won't be called (even here) for deleted leaf pages themselves, because they're skipped by
 *     default.
 */
static int
__delete_redo_window_cleanup_skip(
  WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool visible_all, bool *skipp)
{
    WT_UNUSED(ref);
    WT_UNUSED(session);
    WT_UNUSED(context);
    WT_UNUSED(visible_all);

    *skipp = F_ISSET(ref, WT_REF_FLAG_LEAF);
    return (0);
}

/*
 * __wt_delete_redo_window_cleanup --
 *     Clear old transaction IDs from already-loaded page_del structures to make them look like we
 *     just unpacked the information. Called after the tree write generation is bumped during
 *     recovery so that old transaction IDs don't come back to life. Note that this can only fail if
 *     something goes wrong in the tree walk; it doesn't itself ever fail.
 */
int
__wt_delete_redo_window_cleanup(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    WT_REF *ref;

    /*
     * Walk the tree and look for internal pages holding fast-truncate information. Note: we pass
     * WT_READ_VISIBLE_ALL because we have no snapshot, but we aren't actually doing any visibility
     * checks.
     */
    ref = NULL;
    while ((ret = __wt_tree_walk_custom_skip(session, &ref, __delete_redo_window_cleanup_skip, NULL,
              WT_READ_CACHE | WT_READ_VISIBLE_ALL)) == 0 &&
      ref != NULL)
        WT_WITH_PAGE_INDEX(session, __delete_redo_window_cleanup_internal(session, ref));

    return (ret);
}

/*
 * __wti_delete_page_skip --
 *     If iterating a cursor, skip deleted pages that are either visible to us or globally visible.
 */
bool
__wti_delete_page_skip(WT_SESSION_IMPL *session, WT_REF *ref, bool visible_all)
{
    bool discard, skip;

    /*
     * Deleted pages come from several possible sources (as described at the top of this file).
     *
     * In all cases, the WT_REF state will be WT_REF_DELETED. If there is a WT_PAGE_DELETED
     * structure describing a transaction, the deletion is visible (so the page is *not* visible) if
     * the transaction is visible. If there is no WT_PAGE_DELETED structure, the deletion is
     * globally visible. This happens either because the structure described a transaction that had
     * become globally visible and was previously removed, or because the page was deleted by a
     * non-transactional mechanism. (In the latter case, the deletion is inherently globally
     * visible; pages only become empty if nothing in them remains visible to anyone, and newly
     * minted empty pages cannot have anything in them to see.)
     *
     * We're here because we found a WT_REF state set to WT_REF_DELETED. It is possible the page is
     * being read into memory right now, though, and the page could switch to an in-memory state at
     * any time. Lock down the structure, just to be safe.
     */
    if (!WT_REF_CAS_STATE(session, ref, WT_REF_DELETED, WT_REF_LOCKED))
        return (false);

    /*
     * Check visibility.
     *
     * Use the option to hide prepared transactions in all checks; we can't skip a page if the
     * deletion is only prepared (we need to visit it to generate a prepare conflict), and we can't
     * discard the page_del info either, as doing so leads to dropping the on-disk page and if the
     * prepared transaction rolls back we'd then be in trouble.
     */
    if (visible_all)
        skip = discard = __wt_page_del_visible_all(session, ref->page_del, true);
    else {
        skip = __wt_page_del_visible(session, ref->page_del, true);
        discard = skip ? __wt_page_del_visible_all(session, ref->page_del, true) : false;
    }

    /*
     * The fast-truncate structure can be freed as soon as the delete is globally visible: it is
     * only read when the ref state is locked. It is worth checking every time we come through
     * because once this is freed, we no longer need synchronization to check the ref.
     */
    if (discard && ref->page_del != NULL)
        __wt_overwrite_and_free(session, ref->page_del);

    WT_REF_SET_STATE(ref, WT_REF_DELETED);
    return (skip);
}

/*
 * __tombstone_update_alloc --
 *     Allocate and initialize a page-deleted tombstone update structure.
 */
static int
__tombstone_update_alloc(
  WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del, WT_UPDATE **updp, size_t *sizep)
{
    WT_UPDATE *upd;

    WT_RET(__wt_upd_alloc_tombstone(session, &upd, sizep));
    F_SET(upd, WT_UPDATE_RESTORED_FAST_TRUNCATE);

    /*
     * Cleared memory matches the lowest possible transaction ID and timestamp; do nothing if the
     * page_del pointer is null.
     */
    if (page_del != NULL) {
        upd->txnid = page_del->txnid;
        upd->durable_ts = page_del->durable_timestamp;
        upd->start_ts = page_del->timestamp;
        upd->prepare_state = page_del->prepare_state;
    }
    *updp = upd;
    return (0);
}

/*
 * __instantiate_tombstone --
 *     Instantiate a single tombstone on a page.
 */
static WT_INLINE int
__instantiate_tombstone(WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del,
  WT_UPDATE **update_list, uint32_t *countp, const WT_TIME_WINDOW *tw, WT_UPDATE **updp,
  size_t *sizep)
{
    /*
     * If we find an existing stop time point we don't need to append a tombstone. Such rows would
     * not have been visible to the original truncate operation and were, logically, skipped over
     * rather than re-deleted. (If the row _was_ visible to the truncate in spite of having been
     * subsequently removed, the stop time not being visible would have forced its page to be slow-
     * truncated rather than fast-truncated.)
     */
    if (WT_TIME_WINDOW_HAS_STOP(tw))
        *updp = NULL;
    else {
        WT_RET(__tombstone_update_alloc(session, page_del, updp, sizep));

        if (update_list != NULL)
            update_list[(*countp)++] = *updp;
    }

    return (0);
}

/*
 * __instantiate_row --
 *     Iterate over a row-store page and instantiate tombstones.
 */
static int
__instantiate_row(WT_SESSION_IMPL *session, WT_REF *ref, WT_PAGE_DELETED *page_del,
  WT_UPDATE **update_list, uint32_t *countp)
{
    WT_DECL_RET;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_TIME_WINDOW tw;
    WT_UPDATE *upd, **upd_array;
    size_t size, total_size;
    uint32_t i;

    page = ref->page;
    upd = NULL;
    total_size = 0;

    /* Allocate the per-page update array if one doesn't already exist. */
    if (page->entries != 0 && page->modify->mod_row_update == NULL)
        WT_PAGE_ALLOC_AND_SWAP(
          session, page, page->modify->mod_row_update, upd_array, page->entries);

    upd_array = page->modify->mod_row_update;

    /* We just read the page and it's still locked. The insert lists should be empty. */
    WT_ASSERT(session, WT_ROW_INSERT_SMALLEST(page) == NULL);

    /* Walk the page entries, giving each one a tombstone. */
    WT_ROW_FOREACH (page, rip, i) {
        /* Retrieve the stop time point from the page's row. */
        __wti_read_row_time_window(session, page, rip, &tw);

        WT_RET(__instantiate_tombstone(session, page_del, update_list, countp, &tw, &upd, &size));
        if (upd != NULL) {
            upd->next = upd_array[WT_ROW_SLOT(page, rip)];
            upd_array[WT_ROW_SLOT(page, rip)] = upd;
            total_size += size;
        }

        /* We just read the page and it's still locked. The insert lists should be empty. */
        WT_ASSERT(session, WT_ROW_INSERT(page, rip) == NULL);
    }

    __wt_cache_page_inmem_incr(session, page, total_size, false);

    /*
     * Note that the label is required by the alloc-and-swap macro. There isn't anything we need to
     * clean up; both the row update structure that allocates and the tombstones are attached to the
     * page and will get flushed along with it further up the call chain.
     */
err:
    return (ret);
}

/*
 * __wti_delete_page_instantiate --
 *     Instantiate an entirely deleted leaf page. Note that FLCS is not supported.
 */
int
__wti_delete_page_instantiate(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_DELETED *page_del;
    WT_ROW *rip;
    WT_UPDATE **update_list;
    uint32_t count, i;

    /*
     * An operation is accessing a "deleted" page, and we're building an in-memory version of the
     * page, making it look like all entries in the page were individually updated by a remove
     * operation. We end up here if a transaction used a truncate call to delete the page without
     * reading it, and something else that can't yet see the truncation decided to read the page.
     * (We also end up here if someone who _can_ see the truncation writes new data into the same
     * namespace before the deleted pages are discarded.)
     *
     * This can happen after the truncate transaction resolves, but it can also happen before. In
     * the latter case, we need to keep track of the updates we populate the page with, so they can
     * be found when the transaction resolves. The page we're loading might split, in which case
     * finding the updates any other way would become a problem.
     */

    page = ref->page;
    page_del = ref->page_del;
    update_list = NULL;

    /* Fast-truncate only happens to leaf pages. */
    WT_ASSERT(session, page->type == WT_PAGE_ROW_LEAF);

    /* Empty pages should get skipped before reaching this point. */
    WT_ASSERT(session, page->entries > 0);

    WT_STAT_CONN_DSRC_INCR(session, cache_read_deleted);

    /* Track the prepared, fast-truncate pages we've had to instantiate. */
    if (page_del != NULL && page_del->prepare_state != WT_PREPARE_INIT)
        WT_STAT_CONN_DSRC_INCR(session, cache_read_deleted_prepared);

    /*
     * Give the page a modify structure. We need it to remember that the page has been instantiated.
     * We do not need to mark the page dirty here. (It used to be necessary because evicting a clean
     * instantiated page would lose the delete information; but that is no longer the case.) Note
     * though that because VLCS instantiation goes through col_modify it will mark the page dirty
     * regardless, except in read-only trees where attempts to mark things dirty are ignored. (Row-
     * store instantiation adds the tombstones by hand and so does not need to mark the page dirty.)
     *
     * Note that partially visible truncates that may need instantiation can appear in read-only
     * trees (whether a read-only open of the live database or via a checkpoint cursor) if they were
     * not yet globally visible when the tree was checkpointed.
     */
    WT_RET(__wt_page_modify_init(session, page));

    /*
     * If the truncate operation is not yet resolved, count how many updates we're going to need and
     * allocate an array for them. This allows linking them in the page-deleted structure so they
     * can be found when the transaction is resolved, even if they have moved to other pages. If the
     * page-deleted structure is NULL, that means the truncate is globally visible, and therefore
     * committed. Use an extra slot to mark the end with NULL so we don't need to also store the
     * length.
     */
    if (page_del != NULL && !page_del->committed) {
        count = 0;

        /* One tombstone for each row on the page. */
        WT_ROW_FOREACH (page, rip, i)
            ++count;
        WT_RET(__wt_calloc_def(session, count + 1, &update_list));
    }

    /*
     * Copy the page-deleted structure's timestamp information into an update for each row on the
     * page.
     */

    count = 0;

    WT_ERR(__instantiate_row(session, ref, page_del, update_list, &count));
    page->modify->instantiated = true;
    page->modify->inst_updates = update_list;

    /*
     * We will leave the WT_PAGE_DELETED structure in the ref; all of its information has been
     * copied to the list of WT_UPDATE structures (if any), but we may still need it for internal
     * page reconciliation until the instantiated page is itself successfully reconciled.
     */

    return (0);

err:
    __wt_free(session, update_list);
    return (ret);
}
