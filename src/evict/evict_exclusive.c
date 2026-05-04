/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __evict_clear_walk_and_saved_tree_if_current_locked(WT_SESSION_IMPL *);

/*
 * __wti_evict_lock_handle_list --
 *     Try to get the handle list lock, with yield and sleep back off. Keep timing statistics
 *     overall.
 */
int
__wti_evict_lock_handle_list(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_RWLOCK *dh_lock;
    u_int spins;

    conn = S2C(session);
    evict = conn->evict;
    dh_lock = &conn->dhandle_lock;

    /*
     * Use a custom lock acquisition back off loop so the eviction server notices any interrupt
     * quickly.
     */
    for (spins = 0; (ret = __wt_try_readlock(session, dh_lock)) == EBUSY &&
         __wt_atomic_load_uint32_v_relaxed(&evict->pass_intr) == 0;
         spins++) {
        if (spins < WT_THOUSAND)
            __wt_yield();
        else
            __wt_sleep(0, WT_THOUSAND);
    }
    return (ret);
}

/*
 * __wti_evict_set_saved_walk_tree --
 *     Set saved walk tree maintaining use count. Call it with NULL to clear the saved walk tree.
 */
void
__wti_evict_set_saved_walk_tree(WT_SESSION_IMPL *session, WT_DATA_HANDLE *new_dhandle)
{
    WT_DATA_HANDLE *old_dhandle;
    WT_EVICT *evict;

    evict = S2C(session)->evict;
    old_dhandle = evict->walk_tree;

    if (old_dhandle == new_dhandle)
        return;

    if (new_dhandle != NULL)
        (void)__wt_atomic_add_int32(&new_dhandle->session_inuse, 1);

    evict->walk_tree = new_dhandle;

    if (old_dhandle != NULL) {
        WT_ASSERT(session, __wt_atomic_load_int32_relaxed(&old_dhandle->session_inuse) > 0);
        (void)__wt_atomic_sub_int32(&old_dhandle->session_inuse, 1);
    }
}

/*
 * __wti_evict_clear_walk --
 *     Clear a single walk point and remember its position as a soft pointer if clear_pos is unset.
 */
int
__wti_evict_clear_walk(WT_SESSION_IMPL *session, bool clear_pos)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_REF *ref;
#define PATH_STR_MAX 1024
    char path_str[PATH_STR_MAX];
    const char *where;
    size_t path_str_offset;
    double pos;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_PASS));

    if ((ref = __wt_atomic_load_ptr_relaxed(&btree->evict_ref)) == NULL)
        return (0);

    if (!evict->use_npos_in_pass || clear_pos)
        WT_STAT_CONN_INCR(session, eviction_walks_abandoned);

    /*
     * Clear evict_ref before releasing it in case that forces eviction (we assert that we never try
     * to evict the current eviction walk point).
     */
    __wt_atomic_store_ptr_relaxed(&btree->evict_ref, NULL);

    if (evict->use_npos_in_pass) {
        /* If soft pointers are in use, remember the page's position unless clear_pos is set. */
        if (clear_pos)
            __wt_evict_clear_npos(btree);
        else {
            /*
             * Remember the last position before clearing it so that we can restart from about the
             * same point later. evict_saved_ref_check is used as an opaque page id to compare with
             * it upon restoration for the purpose of stats.
             */
            btree->evict_saved_ref_check = (uint64_t)ref;

            if (F_ISSET(ref, WT_REF_FLAG_LEAF)) {
                /* If we're at a leaf page, use the middle of the page. */
                pos = WT_NPOS_MID;
                where = "MIDDLE";
            } else {
                /*
                 * If we're at an internal page, then we've just finished all its leafs, so get the
                 * position of the very beginning or the very end of it depending on the direction
                 * of walk.
                 */
                if (btree->evict_start_type == WT_EVICT_WALK_NEXT ||
                  btree->evict_start_type == WT_EVICT_WALK_RAND_NEXT) {
                    pos = WT_NPOS_RIGHT;
                    where = "RIGHT";
                } else {
                    pos = WT_NPOS_LEFT;
                    where = "LEFT";
                }
            }
            if (!WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1))
                btree->evict_pos = __wt_page_npos(session, ref, pos, NULL, NULL, 0);
            else {
                btree->evict_pos =
                  __wt_page_npos(session, ref, pos, path_str, &path_str_offset, PATH_STR_MAX);
                __wt_verbose_debug1(session, WT_VERB_EVICTION,
                  "Evict walk point memorized at position %lf %s of %s page %s ref %p",
                  btree->evict_pos, where, F_ISSET(ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF",
                  path_str, (void *)ref);
            }
        }
    }

    WT_WITH_DHANDLE(evict->walk_session, session->dhandle,
      (ret = __wt_page_release(evict->walk_session, ref, WT_READ_NO_EVICT)));
    return (ret);
#undef PATH_STR_MAX
}

/*
 * __wti_evict_clear_all_walks_and_saved_tree --
 *     Clear the eviction walk points for all files a session is waiting on.
 */
int
__wti_evict_clear_all_walks_and_saved_tree(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    conn = S2C(session);

    TAILQ_FOREACH (dhandle, &conn->dhqh, q)
        if (WT_DHANDLE_BTREE(dhandle))
            WT_WITH_DHANDLE(session, dhandle, WT_TRET(__wti_evict_clear_walk(session, true)));
    __wti_evict_set_saved_walk_tree(session, NULL);
    return (ret);
}

/*
 * __evict_clear_walk_and_saved_tree_if_current_locked --
 *     Clear single walk points and clear the walk tree if it's the current session's dhandle.
 */
static int
__evict_clear_walk_and_saved_tree_if_current_locked(WT_SESSION_IMPL *session)
{
    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->evict->evict_pass_lock);
    if (session->dhandle == S2C(session)->evict->walk_tree)
        __wti_evict_set_saved_walk_tree(session, NULL);
    return (__wti_evict_clear_walk(session, false));
}

/* !!!
 * __wt_evict_file_exclusive_on --
 *     Acquire exclusive access to a file/tree making it possible to evict the entire file using
 *     `__wt_evict_file`. It does this by incrementing the `evict_disabled` counter for a
 *     tree, which disables all other means of eviction (except file eviction).
 *
 *     For the incremented `evict_disabled` value, the eviction server skips walking this tree for
 *     eviction candidates, and force-evicting or queuing pages from this tree is not allowed.
 *
 *     It is called from multiple places in the code base, such as when initiating file eviction
 *     `__wt_evict_file` or when opening or closing trees.
 *
 *     Return an error code if unable to acquire necessary locks or clear the eviction queues.
 */
int
__wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *evict_entry;
    u_int elem, i, q;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    /* Hold the walk lock to turn off eviction. */
    __wt_spin_lock(session, &evict->evict_walk_lock);
    if (++btree->evict_disabled > 1) {
        __wt_spin_unlock(session, &evict->evict_walk_lock);
        return (0);
    }

    __wt_verbose_debug1(session, WT_VERB_EVICTION, "obtained exclusive eviction lock on btree %s",
      btree->dhandle->name);

    /*
     * Special operations don't enable eviction, however the underlying command (e.g. verify) may
     * choose to turn on eviction. This falls outside of the typical eviction flow, and here
     * eviction may forcibly remove pages from the cache. Consequently, we may end up evicting
     * internal pages which still have child pages present on the pre-fetch queue. Remove any refs
     * still present on the pre-fetch queue so that they are not accidentally accessed in an invalid
     * way later on.
     */
    WT_ERR(__wt_conn_prefetch_clear_tree(session, false));

    /*
     * Ensure no new pages from the file will be queued for eviction after this point, then clear
     * any existing LRU eviction walk for the file.
     */
    (void)__wt_atomic_add_uint32_v(&evict->pass_intr, 1);
    WTI_WITH_PASS_LOCK(session, ret = __evict_clear_walk_and_saved_tree_if_current_locked(session));
    (void)__wt_atomic_sub_uint32_v(&evict->pass_intr, 1);
    WT_ERR(ret);

    /*
     * The eviction candidate list might reference pages from the file, clear it. Hold the evict
     * lock to remove queued pages from a file.
     */
    __wt_spin_lock(session, &evict->evict_queue_lock);

    for (q = 0; q < WTI_EVICT_QUEUE_MAX; q++) {
        __wt_spin_lock(session, &evict->evict_queues[q].evict_lock);
        elem = evict->evict_queues[q].evict_max;
        for (i = 0, evict_entry = evict->evict_queues[q].evict_queue; i < elem; i++, evict_entry++)
            if (evict_entry->btree == btree)
                __evict_list_clear(session, evict_entry);
        __wt_spin_unlock(session, &evict->evict_queues[q].evict_lock);
    }

    __wt_spin_unlock(session, &evict->evict_queue_lock);

    /*
     * We have disabled further eviction: wait for concurrent LRU eviction activity to drain.
     */
    while (__wt_tsan_suppress_load_uint32_v(&btree->evict_busy) > 0)
        __wt_yield();

    if (0) {
err:
        --btree->evict_disabled;
    }
    __wt_spin_unlock(session, &evict->evict_walk_lock);
    return (ret);
}

/* !!!
 * __wt_evict_file_exclusive_off --
 *     Release exclusive access to a file/tree by decrementing the `evict_disabled` count
 *     back to zero, allowing eviction to proceed for the tree.
 *
 *     It is called from multiple places in the code where exclusive eviction access is no longer
 *     needed.
 */
void
__wt_evict_file_exclusive_off(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;

    btree = S2BT(session);

    /*
     * We have seen subtle bugs with multiple threads racing to turn eviction on/off. Make races
     * more likely in diagnostic builds.
     */
    WT_DIAGNOSTIC_YIELD;

/*
 * Atomically decrement the evict-disabled count, without acquiring the eviction walk-lock. We can't
 * acquire that lock here because there's a potential deadlock. When acquiring exclusive eviction
 * access, we acquire the eviction walk-lock and then the eviction's pass-intr lock. The eviction
 * server can hold the pass-intr lock and call into this function, which might deadlock with another
 * thread trying to get exclusive eviction access.
 */
#if defined(HAVE_DIAGNOSTIC)
    {
        int32_t v;

        WT_ASSERT(session, __wt_atomic_load_ptr_relaxed(&btree->evict_ref) == NULL);
        v = __wt_atomic_sub_int32(&btree->evict_disabled, 1);
        WT_ASSERT(session, v >= 0);
    }
#else
    (void)__wt_atomic_sub_int32(&btree->evict_disabled, 1);
#endif
    __wt_verbose_debug1(session, WT_VERB_EVICTION, "released exclusive eviction lock on btree %s",
      btree->dhandle->name);
}
