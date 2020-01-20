/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * A list of WT_REF's.
 */
typedef struct {
    WT_REF **list;
    size_t entry;     /* next entry available in list */
    size_t max_entry; /* how many allocated in list */
} WT_REF_LIST;

/*
 * __sync_checkpoint_can_skip --
 *     There are limited conditions under which we can skip writing a dirty page during checkpoint.
 */
static inline bool
__sync_checkpoint_can_skip(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    WT_TXN *txn;
    u_int i;

    mod = page->modify;
    txn = &session->txn;

    /*
     * We can skip some dirty pages during a checkpoint. The requirements:
     *
     * 1. they must be leaf pages,
     * 2. there is a snapshot transaction active (which is the case in
     *    ordinary application checkpoints but not all internal cases),
     * 3. the first dirty update on the page is sufficiently recent the
     *    checkpoint transaction would skip them,
     * 4. there's already an address for every disk block involved.
     */
    if (WT_PAGE_IS_INTERNAL(page))
        return (false);
    if (!F_ISSET(txn, WT_TXN_HAS_SNAPSHOT))
        return (false);
    if (!WT_TXNID_LT(txn->snap_max, mod->first_dirty_txn))
        return (false);

    /*
     * The problematic case is when a page was evicted but when there were unresolved updates and
     * not every block associated with the page has a disk address. We can't skip such pages because
     * we need a checkpoint write with valid addresses.
     *
     * The page's modification information can change underfoot if the page is being reconciled, so
     * we'd normally serialize with reconciliation before reviewing page-modification information.
     * However, checkpoint is the only valid writer of dirty leaf pages at this point, we skip the
     * lock.
     */
    if (mod->rec_result == WT_PM_REC_MULTIBLOCK)
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i)
            if (multi->addr.addr == NULL)
                return (false);

    return (true);
}

/*
 * __sync_dup_hazard_pointer --
 *     Get a duplicate hazard pointer.
 */
static inline int
__sync_dup_hazard_pointer(WT_SESSION_IMPL *session, WT_REF *walk, WT_REF **dupp)
{
    bool busy;

    /* Get a duplicate hazard pointer. */
    for (;;) {
#ifdef HAVE_DIAGNOSTIC
        WT_RET(__wt_hazard_set(session, walk, &busy, __func__, __LINE__));
#else
        WT_RET(__wt_hazard_set(session, walk, &busy));
#endif
        /*
         * We already have a hazard pointer, we should generally be able to get another one. We can
         * get spurious busy errors (e.g., if eviction is attempting to lock the page. Keep trying:
         * we have one hazard pointer so we should be able to get another one.
         */
        if (!busy)
            break;
        __wt_yield();
    }

    *dupp = walk;
    return (0);
}

/*
 * __sync_dup_walk --
 *     Duplicate a tree walk point.
 */
static inline int
__sync_dup_walk(WT_SESSION_IMPL *session, WT_REF *walk, uint32_t flags, WT_REF **dupp)
{
    WT_REF *old;

    if ((old = *dupp) != NULL) {
        *dupp = NULL;
        WT_RET(__wt_page_release(session, old, flags));
    }

    /* It is okay to duplicate a walk before it starts. */
    if (walk == NULL || __wt_ref_is_root(walk)) {
        *dupp = walk;
        return (0);
    }

    return (__sync_dup_hazard_pointer(session, walk, dupp));
}

/*
 * __sync_ref_list_add --
 *     Add an obsolete history store ref to the list.
 */
static int
__sync_ref_list_add(WT_SESSION_IMPL *session, WT_REF_LIST *rlp, WT_REF *ref)
{
    WT_RET(__wt_realloc_def(session, &rlp->max_entry, rlp->entry + 1, &rlp->list));
    rlp->list[rlp->entry++] = ref;
    return (0);
}

/*
 * __sync_ref_list_pop --
 *     Add the stored ref to urgent eviction queue and free the list.
 */
static void
__sync_ref_list_pop(WT_SESSION_IMPL *session, WT_REF_LIST *rlp, uint32_t flags)
{
    WT_DECL_RET;
    size_t i;

    for (i = 0; i < rlp->entry; i++) {
        WT_IGNORE_RET_BOOL(__wt_page_evict_urgent(session, rlp->list[i]));
        WT_ERR(__wt_page_release(session, rlp->list[i], flags));
        WT_STAT_CONN_INCR(session, hs_gc_pages_evict);
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: is an in-memory obsolete page, added to urgent eviction queue.",
          (void *)rlp->list[i]);
    }

err:
    __wt_free(session, rlp->list);
    rlp->entry = 0;
    rlp->max_entry = 0;
}

/*
 * __sync_ref_obsolete_check --
 *     Check whether the ref is obsolete according to the newest stop time pair and handle the
 *     obsolete page.
 */
static int
__sync_ref_obsolete_check(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_LIST *rlp)
{
    WT_ADDR *addr;
    WT_CELL_UNPACK vpack;
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    WT_REF *dup;
    wt_timestamp_t multi_newest_stop_ts;
    uint64_t multi_newest_stop_txn;
    uint32_t i, previous_state;
    bool obsolete;

    /* Ignore root pages as they can never be deleted. */
    if (__wt_ref_is_root(ref)) {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC, "%p: skipping root page", (void *)ref);
        return (0);
    }

    /* Ignore deleted pages. */
    if (ref->state == WT_REF_DELETED) {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC, "%p: skipping deleted page", (void *)ref);
        return (0);
    }

    /* Lock the ref to avoid any change before it is checked for obsolete. */
    previous_state = ref->state;
    if (!WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
        return (0);

    /* Ignore internal pages, these are taken care of during reconciliation. */
    if (ref->addr != NULL && !__wt_ref_is_leaf(session, ref)) {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC, "%p: skipping internal page with parent: %p",
          (void *)ref, (void *)ref->home);
        return (0);
    }

    WT_STAT_CONN_INCR(session, hs_gc_pages_visited);
    multi_newest_stop_ts = WT_TS_NONE;
    multi_newest_stop_txn = WT_TXN_NONE;
    addr = ref->addr;
    mod = ref->page == NULL ? NULL : ref->page->modify;

    /* Check for the page obsolete, if the page is modified and reconciled. */
    if (mod != NULL && mod->rec_result == WT_PM_REC_REPLACE) {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: page obsolete check with reconciled replace block stop time pair txn "
          "and timestamp: %" PRIu64 ", %" PRIu64,
          (void *)ref, mod->mod_replace.newest_stop_txn, mod->mod_replace.newest_stop_ts);
        obsolete = __wt_txn_visible_all(
          session, mod->mod_replace.newest_stop_txn, mod->mod_replace.newest_stop_ts);
    } else if (mod != NULL && mod->rec_result == WT_PM_REC_MULTIBLOCK) {
        /* Calculate the max stop time pair by traversing all multi addresses. */
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
            multi_newest_stop_ts = WT_MAX(multi_newest_stop_ts, multi->addr.newest_stop_ts);
            multi_newest_stop_txn = WT_MAX(multi_newest_stop_txn, multi->addr.newest_stop_txn);
        }
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: page obsolete check with reconciled multi block stop time pair txn "
          "and timestamp: %" PRIu64 ", %" PRIu64,
          (void *)ref, multi_newest_stop_txn, multi_newest_stop_ts);
        obsolete = __wt_txn_visible_all(session, multi_newest_stop_txn, multi_newest_stop_ts);
    } else if (!__wt_off_page(ref->home, addr)) {
        /* Check if the page is obsolete using the page disk address. */
        __wt_cell_unpack(session, ref->home, (WT_CELL *)addr, &vpack);
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: page obsolete check with unpacked address stop time pair txn "
          "and timestamp: %" PRIu64 ", %" PRIu64,
          (void *)ref, vpack.newest_stop_txn, vpack.newest_stop_ts);
        obsolete = __wt_txn_visible_all(session, vpack.newest_stop_txn, vpack.newest_stop_ts);
    } else {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: page obsolete check with off page address stop time pair txn "
          "and timestamp: %" PRIu64 ", %" PRIu64,
          (void *)ref, addr->newest_stop_txn, addr->newest_stop_ts);
        obsolete = __wt_txn_visible_all(session, addr->newest_stop_txn, addr->newest_stop_ts);
    }

    if (obsolete) {
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC, "%p: page is found as obsolete", (void *)ref);

        /*
         * Mark the page as deleted and also set the parent page as dirty. This is to ensure the
         * parent page must be written during checkpoint and the child page discarded.
         */
        if (previous_state == WT_REF_DISK) {
            WT_REF_SET_STATE(ref, WT_REF_DELETED);
            WT_STAT_CONN_INCR(session, hs_gc_pages_removed);
            __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
              "%p: page is marked for deletion with parent page: %p", (void *)ref,
              (void *)ref->home);
            return (__wt_page_parent_modify_set(session, ref, true));
        }

        /* Add the in-memory obsolete history store page into the list of pages to be evicted. */
        WT_REF_SET_STATE(ref, previous_state);
        WT_RET(__sync_dup_hazard_pointer(session, ref, &dup));
        WT_RET(__sync_ref_list_add(session, rlp, dup));
        __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
          "%p: is an in-memory obsolete page, stored for eviction.", (void *)dup);
        return (0);
    }

    WT_REF_SET_STATE(ref, previous_state);
    return (0);
}

/*
 * __sync_ref_int_obsolete_cleanup --
 *     Traverse the internal page and identify the leaf pages that are obsolete and mark them as
 *     deleted.
 */
static int
__sync_ref_int_obsolete_cleanup(WT_SESSION_IMPL *session, WT_REF *parent, WT_REF_LIST *rlp)
{
    WT_PAGE_INDEX *pindex;
    WT_REF *ref;
    uint32_t slot;

    WT_INTL_INDEX_GET(session, parent->page, pindex);
    __wt_verbose(session, WT_VERB_CHECKPOINT_GC,
      "%p: traversing the internal page %p for obsolete child pages", (void *)parent,
      (void *)parent->page);

    for (slot = 0; slot < pindex->entries; slot++) {
        ref = pindex->index[slot];

        WT_RET(__sync_ref_obsolete_check(session, ref, rlp));
    }

    return (0);
}

/*
 * __wt_sync_file --
 *     Flush pages for a specific file.
 */
int
__wt_sync_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_MODIFY *mod;
    WT_REF *prev, *walk;
    WT_REF_LIST ref_list;
    WT_TXN *txn;
    uint64_t internal_bytes, internal_pages, leaf_bytes, leaf_pages;
    uint64_t oldest_id, saved_pinned_id, time_start, time_stop;
    uint32_t flags, rec_flags;
    bool is_hs, timer, tried_eviction;

    conn = S2C(session);
    btree = S2BT(session);
    prev = walk = NULL;
    txn = &session->txn;
    tried_eviction = false;
    time_start = time_stop = 0;
    is_hs = false;

    /* Only visit pages in cache and don't bump page read generations. */
    flags = WT_READ_CACHE | WT_READ_NO_GEN;

    /*
     * Skip all deleted pages. For a page to be marked deleted, it must have been evicted from cache
     * and marked clean. Checkpoint should never instantiate deleted pages: if a truncate is not
     * visible to the checkpoint, the on-disk version is correct. If the truncate is visible, we
     * skip over the child page when writing its parent. We check whether a truncate is visible in
     * the checkpoint as part of reconciling internal pages (specifically in __rec_child_modify).
     */
    LF_SET(WT_READ_DELETED_SKIP);

    internal_bytes = leaf_bytes = 0;
    internal_pages = leaf_pages = 0;
    saved_pinned_id = WT_SESSION_TXN_STATE(session)->pinned_id;
    timer = WT_VERBOSE_ISSET(session, WT_VERB_CHECKPOINT);
    if (timer)
        time_start = __wt_clock(session);

    switch (syncop) {
    case WT_SYNC_WRITE_LEAVES:
        /*
         * Write all immediately available, dirty in-cache leaf pages.
         *
         * Writing the leaf pages is done without acquiring a high-level lock, serialize so multiple
         * threads don't walk the tree at the same time.
         */
        if (!btree->modified)
            return (0);
        __wt_spin_lock(session, &btree->flush_lock);
        if (!btree->modified) {
            __wt_spin_unlock(session, &btree->flush_lock);
            return (0);
        }

        /*
         * Save the oldest transaction ID we need to keep around. Otherwise, in a busy system, we
         * could be updating pages so fast that write leaves never catches up. We deliberately have
         * no transaction running at this point that would keep the oldest ID from moving forwards
         * as we walk the tree.
         */
        oldest_id = __wt_txn_oldest_id(session);

        LF_SET(WT_READ_NO_WAIT | WT_READ_SKIP_INTL);
        for (;;) {
            WT_ERR(__wt_tree_walk(session, &walk, flags));
            if (walk == NULL)
                break;

            /*
             * Write dirty pages if nobody beat us to it. Don't try to write hot pages (defined as
             * pages that have been updated since the write phase leaves started): checkpoint will
             * have to visit them anyway.
             */
            page = walk->page;
            if (__wt_page_is_modified(page) && WT_TXNID_LT(page->modify->update_txn, oldest_id)) {
                if (txn->isolation == WT_ISO_READ_COMMITTED)
                    __wt_txn_get_snapshot(session);
                leaf_bytes += page->memory_footprint;
                ++leaf_pages;
                WT_ERR(__wt_reconcile(session, walk, NULL, WT_REC_CHECKPOINT));
            }
        }
        break;
    case WT_SYNC_CHECKPOINT:
        /*
         * If we are flushing a file at read-committed isolation, which is of particular interest
         * for flushing the metadata to make a schema-changing operation durable, get a
         * transactional snapshot now.
         *
         * All changes committed up to this point should be included. We don't update the snapshot
         * in between pages because the metadata shouldn't have many pages. Instead, read-committed
         * isolation ensures that all metadata updates completed before the checkpoint are included.
         */
        if (txn->isolation == WT_ISO_READ_COMMITTED)
            __wt_txn_get_snapshot(session);

        /*
         * We cannot check the tree modified flag in the case of a checkpoint, the checkpoint code
         * has already cleared it.
         *
         * Writing the leaf pages is done without acquiring a high-level lock, serialize so multiple
         * threads don't walk the tree at the same time. We're holding the schema lock, but need the
         * lower-level lock as well.
         */
        __wt_spin_lock(session, &btree->flush_lock);

        /*
         * In the final checkpoint pass, child pages cannot be evicted from underneath internal
         * pages nor can underlying blocks be freed until the checkpoint's block lists are stable.
         * Also, we cannot split child pages into parents unless we know the final pass will write a
         * consistent view of that namespace. Set the checkpointing flag to block such actions and
         * wait for any problematic eviction or page splits to complete.
         */
        WT_ASSERT(session, btree->syncing == WT_BTREE_SYNC_OFF && btree->sync_session == NULL);

        btree->sync_session = session;
        btree->syncing = WT_BTREE_SYNC_WAIT;
        __wt_gen_next_drain(session, WT_GEN_EVICT);
        btree->syncing = WT_BTREE_SYNC_RUNNING;
        is_hs = WT_IS_HS(btree);

        /*
         * Add in history store reconciliation for standard files.
         *
         * FIXME-PM-1521: Remove the history store check, and assert that no updates from the
         * history store are copied to the history store recursively.
         */
        rec_flags = WT_REC_CHECKPOINT;
        if (!is_hs && !WT_IS_METADATA(btree->dhandle))
            rec_flags |= WT_REC_HS;

        /* Write all dirty in-cache pages. */
        LF_SET(WT_READ_NO_EVICT);

        /* Read pages with history store entries and evict them asap. */
        LF_SET(WT_READ_WONT_NEED);

        /* Read internal pages if it is history store */
        if (is_hs) {
            LF_CLR(WT_READ_CACHE);
            LF_SET(WT_READ_CACHE_LEAF);
            memset(&ref_list, 0, sizeof(ref_list));
        }

        for (;;) {
            WT_ERR(__sync_dup_walk(session, walk, flags, &prev));
            WT_ERR(__wt_tree_walk(session, &walk, flags));

            if (walk == NULL) {
                if (is_hs)
                    __sync_ref_list_pop(session, &ref_list, flags);
                break;
            }

            /* Traverse through the internal page for obsolete child pages. */
            if (is_hs && WT_PAGE_IS_INTERNAL(walk->page)) {
                WT_WITH_PAGE_INDEX(
                  session, ret = __sync_ref_int_obsolete_cleanup(session, walk, &ref_list));
                WT_ERR(ret);
            }

            /*
             * Take a local reference to the page modify structure now that we know the page is
             * dirty. It needs to be done in this order otherwise the page modify structure could
             * have been created between taking the reference and checking modified.
             */
            page = walk->page;

            /*
             * Skip clean pages, but need to make sure maximum transaction ID is always updated.
             */
            if (!__wt_page_is_modified(page)) {
                if (((mod = page->modify) != NULL) && mod->rec_max_txn > btree->rec_max_txn)
                    btree->rec_max_txn = mod->rec_max_txn;
                if (mod != NULL && btree->rec_max_timestamp < mod->rec_max_timestamp)
                    btree->rec_max_timestamp = mod->rec_max_timestamp;

                continue;
            }

            /*
             * Write dirty pages, if we can't skip them. If we skip a page, mark the tree dirty. The
             * checkpoint marked it clean and we can't skip future checkpoints until this page is
             * written.
             */
            if (__sync_checkpoint_can_skip(session, page)) {
                __wt_tree_modify_set(session);
                continue;
            }

            if (WT_PAGE_IS_INTERNAL(page)) {
                internal_bytes += page->memory_footprint;
                ++internal_pages;
                /* Slow down checkpoints. */
                if (F_ISSET(conn, WT_CONN_DEBUG_SLOW_CKPT))
                    __wt_sleep(0, 10000);
            } else {
                leaf_bytes += page->memory_footprint;
                ++leaf_pages;
            }

            /*
             * If the page was pulled into cache by our read, try to evict it now.
             *
             * For eviction to have a chance, we first need to move the walk point to the next page
             * checkpoint will visit. We want to avoid this code being too special purpose, so try
             * to reuse the ordinary eviction path.
             *
             * Regardless of whether eviction succeeds or fails, the walk continues from the
             * previous location. We remember whether we tried eviction, and don't try again. Even
             * if eviction fails (the page may stay in cache clean but with history that cannot be
             * discarded), that is not wasted effort because checkpoint doesn't need to write the
             * page again.
             *
             * Once the transaction has given up it's snapshot it is no longer safe to reconcile
             * pages. That happens prior to the final metadata checkpoint.
             */
            if (!WT_PAGE_IS_INTERNAL(page) && page->read_gen == WT_READGEN_WONT_NEED &&
              !tried_eviction && F_ISSET(&session->txn, WT_TXN_HAS_SNAPSHOT)) {
                ret = __wt_page_release_evict(session, walk, 0);
                walk = NULL;
                WT_ERR_BUSY_OK(ret);

                walk = prev;
                prev = NULL;
                tried_eviction = true;
                continue;
            }
            tried_eviction = false;

            WT_ERR(__wt_reconcile(session, walk, NULL, rec_flags));

            /*
             * Update checkpoint IO tracking data if configured to log verbose progress messages.
             */
            if (conn->ckpt_timer_start.tv_sec > 0) {
                conn->ckpt_write_bytes += page->memory_footprint;
                ++conn->ckpt_write_pages;

                /* Periodically log checkpoint progress. */
                if (conn->ckpt_write_pages % 5000 == 0)
                    __wt_checkpoint_progress(session, false);
            }
        }
        break;
    case WT_SYNC_CLOSE:
    case WT_SYNC_DISCARD:
        WT_ERR(__wt_illegal_value(session, syncop));
        break;
    }

    if (timer) {
        time_stop = __wt_clock(session);
        __wt_verbose(session, WT_VERB_CHECKPOINT,
          "__sync_file WT_SYNC_%s wrote: %" PRIu64 " leaf pages (%" PRIu64 "B), %" PRIu64
          " internal pages (%" PRIu64 "B), and took %" PRIu64 "ms",
          syncop == WT_SYNC_WRITE_LEAVES ? "WRITE_LEAVES" : "CHECKPOINT", leaf_pages, leaf_bytes,
          internal_pages, internal_bytes, WT_CLOCKDIFF_MS(time_stop, time_start));
    }

err:
    /* On error, clear any left-over tree walk. */
    WT_TRET(__wt_page_release(session, walk, flags));
    WT_TRET(__wt_page_release(session, prev, flags));

    /* On error, Process the ref that are saved and free the list. */
    if (is_hs)
        __sync_ref_list_pop(session, &ref_list, flags);

    /*
     * If we got a snapshot in order to write pages, and there was no snapshot active when we
     * started, release it.
     */
    if (txn->isolation == WT_ISO_READ_COMMITTED && saved_pinned_id == WT_TXN_NONE)
        __wt_txn_release_snapshot(session);

    /* Clear the checkpoint flag. */
    btree->syncing = WT_BTREE_SYNC_OFF;
    btree->sync_session = NULL;

    __wt_spin_unlock(session, &btree->flush_lock);

    /*
     * Leaves are written before a checkpoint (or as part of a file close, before checkpointing the
     * file). Start a flush to stable storage, but don't wait for it.
     */
    if (ret == 0 && syncop == WT_SYNC_WRITE_LEAVES && F_ISSET(conn, WT_CONN_CKPT_SYNC))
        WT_RET(btree->bm->sync(btree->bm, session, false));

    return (ret);
}
