/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __txn_rollback_to_stable_hs_fixup --
 *     Remove any updates that need to be rolled back from the history store file.
 */
static int
__txn_rollback_to_stable_hs_fixup(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM hs_key, hs_value;
    WT_TXN_GLOBAL *txn_global;
    wt_timestamp_t durable_timestamp, hs_timestamp, rollback_timestamp;
    uint64_t hs_txnid;
    uint32_t hs_btree_id, session_flags;
    uint8_t prepare_state, upd_type;

    conn = S2C(session);
    cursor = NULL;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */

    /*
     * Copy the stable timestamp, otherwise we'd need to lock it each time it's accessed. Even
     * though the stable timestamp isn't supposed to be updated while rolling back, accessing it
     * without a lock would violate protocol.
     */
    txn_global = &conn->txn_global;
    WT_ORDERED_READ(rollback_timestamp, txn_global->stable_timestamp);

    __wt_hs_cursor(session, &cursor, &session_flags);

    /* Discard pages we read as soon as we're done with them. */
    F_SET(session, WT_SESSION_READ_WONT_NEED);

    /* Walk the file. */
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &hs_btree_id, &hs_key, &hs_timestamp, &hs_txnid));

        /* Check the file ID so we can skip durable tables */
        if (hs_btree_id >= conn->stable_rollback_maxfile)
            WT_PANIC_RET(session, EINVAL,
              "file ID %" PRIu32 " in the history store table larger than max %" PRIu32,
              hs_btree_id, conn->stable_rollback_maxfile);
        if (__bit_test(conn->stable_rollback_bitstring, hs_btree_id))
            continue;

        WT_ERR(cursor->get_value(cursor, &durable_timestamp, &prepare_state, &upd_type, &hs_value));

        /*
         * Entries with no timestamp will have a timestamp of zero, which will fail the following
         * check and cause them to never be removed.
         */
        if (rollback_timestamp < durable_timestamp) {
            WT_ERR(cursor->remove(cursor));
            WT_STAT_CONN_INCR(session, txn_rollback_hs_removed);
        }
    }
    WT_ERR_NOTFOUND_OK(ret);
err:
    WT_TRET(__wt_hs_cursor_close(session, &cursor, session_flags));

    F_CLR(session, WT_SESSION_READ_WONT_NEED);

    return (ret);
}

/*
 * __txn_abort_newer_update --
 *     Abort updates in an update change with timestamps newer than the rollback timestamp.
 */
static void
__txn_abort_newer_update(
  WT_SESSION_IMPL *session, WT_UPDATE *first_upd, wt_timestamp_t rollback_timestamp)
{
    WT_UPDATE *upd;

    for (upd = first_upd; upd != NULL; upd = upd->next) {
        /*
         * Updates with no timestamp will have a timestamp of zero and will never be rolled back. If
         * the table is configured for strict timestamp checking, assert that all more recent
         * updates were also rolled back.
         */
        if (upd->txnid == WT_TXN_ABORTED || upd->start_ts == WT_TS_NONE) {
            if (upd == first_upd)
                first_upd = upd->next;
        } else if (rollback_timestamp < upd->durable_ts) {
            /*
             * If any updates are aborted, all newer updates better be aborted as well.
             *
             * Timestamp ordering relies on the validations at the time of commit. Thus if the table
             * is not configured for key consistency check, the timestamps could be out of order
             * here.
             */
            WT_ASSERT(session, !FLD_ISSET(S2BT(session)->assert_flags, WT_ASSERT_COMMIT_TS_KEYS) ||
                upd == first_upd);
            first_upd = upd->next;

            upd->txnid = WT_TXN_ABORTED;
            WT_STAT_CONN_INCR(session, txn_rollback_upd_aborted);
            upd->durable_ts = upd->start_ts = WT_TS_NONE;
        }
    }
}

/*
 * __txn_abort_newer_insert --
 *     Apply the update abort check to each entry in an insert skip list
 */
static void
__txn_abort_newer_insert(
  WT_SESSION_IMPL *session, WT_INSERT_HEAD *head, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT *ins;

    WT_SKIP_FOREACH (ins, head)
        __txn_abort_newer_update(session, ins->upd, rollback_timestamp);
}

/*
 * __txn_abort_newer_col_var --
 *     Abort updates on a variable length col leaf page with timestamps newer than the rollback
 *     timestamp.
 */
static void
__txn_abort_newer_col_var(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
    WT_COL *cip;
    WT_INSERT_HEAD *ins;
    uint32_t i;

    /* Review the changes to the original on-page data items */
    WT_COL_FOREACH (page, cip, i)
        if ((ins = WT_COL_UPDATE(page, cip)) != NULL)
            __txn_abort_newer_insert(session, ins, rollback_timestamp);

    /* Review the append list */
    if ((ins = WT_COL_APPEND(page)) != NULL)
        __txn_abort_newer_insert(session, ins, rollback_timestamp);
}

/*
 * __txn_abort_newer_col_fix --
 *     Abort updates on a fixed length col leaf page with timestamps newer than the rollback
 *     timestamp.
 */
static void
__txn_abort_newer_col_fix(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT_HEAD *ins;

    /* Review the changes to the original on-page data items */
    if ((ins = WT_COL_UPDATE_SINGLE(page)) != NULL)
        __txn_abort_newer_insert(session, ins, rollback_timestamp);

    /* Review the append list */
    if ((ins = WT_COL_APPEND(page)) != NULL)
        __txn_abort_newer_insert(session, ins, rollback_timestamp);
}

/*
 * __txn_abort_newer_row_leaf --
 *     Abort updates on a row leaf page with timestamps newer than the rollback timestamp.
 */
static void
__txn_abort_newer_row_leaf(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
    WT_INSERT_HEAD *insert;
    WT_ROW *rip;
    WT_UPDATE *upd;
    uint32_t i;

    /*
     * Review the insert list for keys before the first entry on the disk page.
     */
    if ((insert = WT_ROW_INSERT_SMALLEST(page)) != NULL)
        __txn_abort_newer_insert(session, insert, rollback_timestamp);

    /*
     * Review updates that belong to keys that are on the disk image, as well as for keys inserted
     * since the page was read from disk.
     */
    WT_ROW_FOREACH (page, rip, i) {
        if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
            __txn_abort_newer_update(session, upd, rollback_timestamp);

        if ((insert = WT_ROW_INSERT(page, rip)) != NULL)
            __txn_abort_newer_insert(session, insert, rollback_timestamp);
    }
}

/*
 * __txn_page_needs_rollback --
 *     Check whether the page needs rollback. Return true if the page has modifications newer than
 *     the given timestamp Otherwise return false.
 */
static bool
__txn_page_needs_rollback(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_ADDR *addr;
    WT_CELL_UNPACK vpack;
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    wt_timestamp_t multi_newest_durable_ts;
    uint32_t i;

    addr = ref->addr;
    mod = ref->page == NULL ? NULL : ref->page->modify;

    /*
     * The rollback operation should be performed on this page when any one of the following is
     * greater than the given timestamp:
     * 1. The reconciled replace page max durable timestamp.
     * 2. The reconciled multi page max durable timestamp.
     * 3. The on page address max durable timestamp.
     * 4. The off page address max durable timestamp.
     */
    if (mod != NULL && mod->rec_result == WT_PM_REC_REPLACE)
        return (mod->mod_replace.newest_durable_ts > rollback_timestamp);
    else if (mod != NULL && mod->rec_result == WT_PM_REC_MULTIBLOCK) {
        multi_newest_durable_ts = WT_TS_NONE;
        /* Calculate the max durable timestamp by traversing all multi addresses. */
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i)
            multi_newest_durable_ts =
              WT_MAX(multi_newest_durable_ts, multi->addr.newest_durable_ts);
        return (multi_newest_durable_ts > rollback_timestamp);
    } else if (!__wt_off_page(ref->home, addr)) {
        /* Check if the page is obsolete using the page disk address. */
        __wt_cell_unpack(session, ref->home, (WT_CELL *)addr, &vpack);
        return (vpack.newest_durable_ts > rollback_timestamp);
    } else if (addr != NULL)
        return (addr->newest_durable_ts > rollback_timestamp);

    return (false);
}

/*
 * __txn_abort_newer_updates --
 *     Abort updates on this page newer than the timestamp.
 */
static int
__txn_abort_newer_updates(WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
    WT_PAGE *page;

    /* Review deleted page saved to the ref. */
    if (ref->page_del != NULL && rollback_timestamp < ref->page_del->durable_timestamp)
        WT_RET(__wt_delete_page_rollback(session, ref));

    /*
     * If we have a ref with no page, or the page is clean, find out whether the page has any
     * modifications that are newer than the given timestamp. As eviction writes the newest version
     * to page, even a clean page may also contain modifications that need rollback. Such pages are
     * read back into memory and processed like other modified pages.
     */
    if ((page = ref->page) == NULL || !__wt_page_is_modified(page)) {
        if (!__txn_page_needs_rollback(session, ref, rollback_timestamp))
            return (0);

        /* Page needs rollback, read it into cache. */
        if (page == NULL)
            WT_RET(__wt_page_in(session, ref, 0));
    }

    switch (page->type) {
    case WT_PAGE_COL_FIX:
        __txn_abort_newer_col_fix(session, page, rollback_timestamp);
        break;
    case WT_PAGE_COL_VAR:
        __txn_abort_newer_col_var(session, page, rollback_timestamp);
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
        __txn_abort_newer_row_leaf(session, page, rollback_timestamp);
        break;
    default:
        WT_RET(__wt_illegal_value(session, page->type));
    }

    return (0);
}

/*
 * __txn_rollback_to_stable_btree_walk --
 *     Called for each open handle - choose to either skip or wipe the commits
 */
static int
__txn_rollback_to_stable_btree_walk(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
    WT_DECL_RET;
    WT_REF *child_ref, *ref;

    /* Walk the tree, marking commits aborted where appropriate. */
    ref = NULL;
    while ((ret = __wt_tree_walk(
              session, &ref, WT_READ_CACHE_LEAF | WT_READ_NO_EVICT | WT_READ_WONT_NEED)) == 0 &&
      ref != NULL) {
        if (WT_PAGE_IS_INTERNAL(ref->page)) {
            WT_INTL_FOREACH_BEGIN (session, ref->page, child_ref) {
                WT_RET(__txn_abort_newer_updates(session, child_ref, rollback_timestamp));
            }
            WT_INTL_FOREACH_END;
        } else
            WT_RET(__txn_abort_newer_updates(session, ref, rollback_timestamp));
    }
    return (ret);
}

/*
 * __txn_rollback_eviction_drain --
 *     Wait for eviction to drain from a tree.
 */
static int
__txn_rollback_eviction_drain(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_UNUSED(cfg);

    WT_RET(__wt_evict_file_exclusive_on(session));
    __wt_evict_file_exclusive_off(session);
    return (0);
}

/*
 * __txn_rollback_to_stable_btree --
 *     Called for each object handle - choose to either skip or wipe the commits
 */
static int
__txn_rollback_to_stable_btree(WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    btree = S2BT(session);
    conn = S2C(session);

    /*
     * Immediately durable files don't get their commits wiped. This case mostly exists to support
     * the semantic required for the oplog in MongoDB - updates that have been made to the oplog
     * should not be aborted. It also wouldn't be safe to roll back updates for any table that had
     * it's records logged, since those updates would be recovered after a crash making them
     * inconsistent.
     */
    if (__wt_btree_immediately_durable(session)) {
        /*
         * Add the btree ID to the bitstring, so we can exclude any history store entries for this
         * btree.
         */
        if (btree->id >= conn->stable_rollback_maxfile)
            WT_PANIC_RET(session, EINVAL, "btree file ID %" PRIu32 " larger than max %" PRIu32,
              btree->id, conn->stable_rollback_maxfile);
        __bit_set(conn->stable_rollback_bitstring, btree->id);
        return (0);
    }

    /* There is never anything to do for checkpoint handles */
    if (session->dhandle->checkpoint != NULL)
        return (0);

    /* There is nothing to do on an empty tree. */
    if (btree->root.page == NULL)
        return (0);
    /*
     * Ensure the eviction server is out of the file - we don't want it messing with us. This step
     * shouldn't be required, but it simplifies some of the reasoning about what state trees can be
     * in.
     */
    WT_RET(__wt_evict_file_exclusive_on(session));
    WT_WITH_PAGE_INDEX(
      session, ret = __txn_rollback_to_stable_btree_walk(session, rollback_timestamp));
    __wt_evict_file_exclusive_off(session);

    return (ret);
}

/*
 * __txn_rollback_to_stable_check --
 *     Ensure the rollback request is reasonable.
 */
static int
__txn_rollback_to_stable_check(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_TXN_GLOBAL *txn_global;
    bool txn_active;

    conn = S2C(session);
    txn_global = &conn->txn_global;

    if (!txn_global->has_stable_timestamp)
        WT_RET_MSG(session, EINVAL, "rollback_to_stable requires a stable timestamp");

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
 * __txn_rollback_to_stable_btree_apply --
 *     Perform rollback to stable to all files listed in the metadata, apart from the metadata and
 *     history store files.
 */
static int
__txn_rollback_to_stable_btree_apply(WT_SESSION_IMPL *session)
{
    WT_CONFIG ckptconf;
    WT_CONFIG_ITEM cval, durableval, key;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_TXN_GLOBAL *txn_global;
    wt_timestamp_t newest_durable_ts, rollback_timestamp;
    const char *config, *uri;
    bool durable_ts_found;

    txn_global = &S2C(session)->txn_global;

    /*
     * Copy the stable timestamp, otherwise we'd need to lock it each time it's accessed. Even
     * though the stable timestamp isn't supposed to be updated while rolling back, accessing it
     * without a lock would violate protocol.
     */
    WT_ORDERED_READ(rollback_timestamp, txn_global->stable_timestamp);

    WT_ASSERT(session, F_ISSET(session, WT_SESSION_LOCKED_SCHEMA));
    WT_RET(__wt_metadata_cursor(session, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &uri));

        /* Ignore metadata and history store files. */
        if (strcmp(uri, WT_METAFILE_URI) == 0 || strcmp(uri, WT_HS_URI) == 0)
            continue;

        if (!WT_PREFIX_MATCH(uri, "file:"))
            continue;

        WT_ERR(cursor->get_value(cursor, &config));

        /* Find out the max durable timestamp of the object from checkpoint. */
        newest_durable_ts = WT_TS_NONE;
        durable_ts_found = false;
        WT_ERR(__wt_config_getones(session, config, "checkpoint", &cval));
        __wt_config_subinit(session, &ckptconf, &cval);
        for (; __wt_config_next(&ckptconf, &key, &cval) == 0;) {
            ret = __wt_config_subgets(session, &cval, "newest_durable_ts", &durableval);
            if (ret == 0) {
                newest_durable_ts = WT_MAX(newest_durable_ts, (wt_timestamp_t)durableval.val);
                durable_ts_found = true;
            }
            WT_ERR_NOTFOUND_OK(ret);
        }

        WT_ERR(__wt_session_get_dhandle(session, uri, NULL, NULL, 0));
        /*
         * The rollback operation should be performed on this file based on the following:
         * 1. The tree is modified.
         * 2. The checkpoint durable timestamp is greater than the rollback timestamp.
         * 3. There is no durable timestamp in any checkpoint.
         */
        if (S2BT(session)->modified || newest_durable_ts > rollback_timestamp || !durable_ts_found)
            WT_TRET(__txn_rollback_to_stable_btree(session, rollback_timestamp));
        WT_TRET(__wt_session_release_dhandle(session));
        WT_ERR(ret);
    }
    WT_ERR_NOTFOUND_OK(ret);

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __txn_rollback_to_stable --
 *     Rollback all modifications with timestamps more recent than the passed in timestamp.
 */
static int
__txn_rollback_to_stable(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    /*
     * Mark that a rollback operation is in progress and wait for eviction to drain. This is
     * necessary because history store eviction uses transactions and causes the check for a
     * quiescent system to fail.
     *
     * Configuring history store eviction off isn't atomic, safe because the flag is only otherwise
     * set when closing down the database. Assert to avoid confusion in the future.
     */
    WT_ASSERT(session, !F_ISSET(conn, WT_CONN_EVICTION_NO_HS));
    F_SET(conn, WT_CONN_EVICTION_NO_HS);

    WT_ERR(__wt_conn_btree_apply(session, NULL, __txn_rollback_eviction_drain, NULL, cfg));

    WT_ERR(__txn_rollback_to_stable_check(session));

    F_CLR(conn, WT_CONN_EVICTION_NO_HS);

    /*
     * Allocate a non-durable btree bitstring. We increment the global value before using it, so the
     * current value is already in use, and hence we need to add one here.
     */
    conn->stable_rollback_maxfile = conn->next_file_id + 1;
    WT_ERR(__bit_alloc(session, conn->stable_rollback_maxfile, &conn->stable_rollback_bitstring));
    WT_WITH_SCHEMA_LOCK(session, ret = __txn_rollback_to_stable_btree_apply(session));

    /*
     * Clear any offending content from the history store file. This must be done after the
     * in-memory application, since the process of walking trees in cache populates a list that is
     * used to check which history store records should be removed.
     */
    if (!F_ISSET(conn, WT_CONN_IN_MEMORY))
        WT_ERR(__txn_rollback_to_stable_hs_fixup(session));

err:
    F_CLR(conn, WT_CONN_EVICTION_NO_HS);
    __wt_free(session, conn->stable_rollback_bitstring);
    return (ret);
}

/*
 * __wt_txn_rollback_to_stable --
 *     Rollback all modifications with timestamps more recent than the passed in timestamp.
 */
int
__wt_txn_rollback_to_stable(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_DECL_RET;

    /*
     * Don't use the connection's default session: we are working on data handles and (a) don't want
     * to cache all of them forever, plus (b) can't guarantee that no other method will be called
     * concurrently.
     */
    WT_RET(__wt_open_internal_session(S2C(session), "txn rollback_to_stable", true, 0, &session));
    ret = __txn_rollback_to_stable(session, cfg);
    WT_TRET(session->iface.close(&session->iface, NULL));

    return (ret);
}
