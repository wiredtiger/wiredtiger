/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __layered_last_checkpoint_order(
  WT_SESSION_IMPL *session, const char *shared_uri, int64_t *ckpt_order);

/*
 * __layered_assert_stable_btree_state --
 *     Assert stable btree invariants before applying ingest updates for a key: (1) no unresolved
 *     preserved prepared update exists; and (2) if the ingest chain ends with a tombstone, a
 *     corresponding value exists to delete.
 */
static WT_INLINE void
__layered_assert_stable_btree_state(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *last_upd)
{
    bool has_value;

    if (cbt->compare != 0) {
        if (last_upd->type != WT_UPDATE_TOMBSTONE)
            return;
        /* No on-page value to check; rely solely on visibility. */
        has_value = false;
    } else if (cbt->ins != NULL) {
        /*
         * The key was found via the insert list rather than the on-page binary-search array. This
         * is legitimate when the stable btree page was reconciled during the leader's last
         * checkpoint but not yet evicted: in-memory WT_INSERT nodes survive reconciliation until
         * the page is evicted. Derive has_value by scanning the insert's update chain for any
         * committed non-tombstone update.
         */
        WT_UPDATE *ins_upd;
        has_value = false;
        for (ins_upd = cbt->ins->upd; ins_upd != NULL; ins_upd = ins_upd->next)
            if (ins_upd->txnid != WT_TXN_ABORTED && ins_upd->type != WT_UPDATE_TOMBSTONE) {
                has_value = true;
                break;
            }
    } else {
        WT_UPDATE *upd = NULL;
        if (cbt->ref->page->modify != NULL && cbt->ref->page->modify->mod_row_update != NULL)
            upd = cbt->ref->page->modify->mod_row_update[cbt->slot];
        else
            upd = NULL;

        /*
         * Walk the chain: assert no unresolved preserved prepared update exists, and advance past
         * any rolled-back preserved prepared updates to find the first visible update.
         */
        for (; upd != NULL; upd = upd->next) {
            if (upd->txnid == WT_TXN_ABORTED) {
                WT_ASSERT_ALWAYS(session, upd->prepare_state == WT_PREPARE_INPROGRESS,
                  "During ingest drain, aborted updates on the stable btree must be "
                  "rolled-back preserved prepared transactions");
                continue;
            }

            WT_ASSERT_ALWAYS(session, upd->prepare_state != WT_PREPARE_INPROGRESS,
              "During ingest drain, found an unresolved prepared update on the stable btree; "
              "prepared transactions must be resolved before step-up");
            break;
        }

        if (last_upd->type != WT_UPDATE_TOMBSTONE)
            return;

        if (upd != NULL)
            has_value = upd->type != WT_UPDATE_TOMBSTONE;
        else {
            WT_TIME_WINDOW tw;
            bool tw_found = __wt_read_cell_time_window(cbt, &tw);
            has_value =
              tw_found && !WT_TIME_WINDOW_HAS_PREPARE(&tw) && !WT_TIME_WINDOW_HAS_STOP(&tw);
        }
    }

    /*
     * If a globally visible tombstone is observed at the end, the update it deletes may have been
     * removed during the obsolete check.
     */
    WT_ASSERT_ALWAYS(session, has_value || __wt_txn_upd_visible_all(session, last_upd),
      "No corresponding value exists on the stable table to delete");
}

/*
 * __layered_move_updates --
 *     Move the updates of a key to the stable table. Any unresolved prepared update on the stable
 *     table should now have been resolved.
 */
static int
__layered_move_updates(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *key,
  WT_UPDATE *upds, WT_UPDATE *last_upd)
{
    WT_DECL_RET;

    /*
     * Disable bulk load if the btree is empty. Otherwise, checkpoint may skip this btree if it has
     * never been checkpointed.
     */
    __wt_btree_disable_bulk(session);

    /*
     * Re-search and retry on WT_RESTART: concurrent drain workers inserting into the same stable
     * btree page can cause __wt_insert_serial to return WT_RESTART. The standard WiredTiger pattern
     * is to re-search before every retry. Reset the cursor first to release the hazard pointer
     * acquired by the previous __wt_row_search, then search again.
     */
retry:
    WT_WITH_PAGE_INDEX(session, ret = __wt_row_search(cbt, key, true, NULL, false, NULL));
    WT_ERR(ret);

    __layered_assert_stable_btree_state(session, cbt, last_upd);

    /*
     * If the oldest update being moved is an aborted prepared update and the stable btree has no
     * existing value for this key, append a globally visible tombstone after the chain. Any newer
     * updates may themselves be non-stable while the update's rollback timestamp has already become
     * stable; without a fallback below, reconciliation has nothing to write in place of the aborted
     * prepared update, leaving an orphaned prepared value on the disk image. The tombstone keeps
     * the post-rollback state well-defined (the key never existed).
     */
    if (cbt->compare != 0 && last_upd->txnid == WT_TXN_ABORTED && last_upd->next == NULL) {
        /*
         * Don't reallocate on a WT_RESTART-driven retry: cbt->compare and last_upd->txnid are
         * stable across retries for the same key, so without the next-NULL guard each retry would
         * allocate a fresh tombstone and orphan the previous one.
         */
        WT_ASSERT(session, last_upd->prepared_id != WT_PREPARED_ID_NONE);
        WT_UPDATE *tombstone;
        WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
        last_upd->next = tombstone;
    }

    ret = __wt_row_modify(cbt, key, NULL, &upds, WT_UPDATE_INVALID, false, false);
    if (ret == WT_RESTART) {
        /* Release the hazard pointer before searching again. */
        WT_ERR(__wt_btcur_reset(cbt));
        goto retry;
    }
    WT_ERR(ret);

err:
    WT_TRET(__wt_btcur_reset(cbt));
    return (ret);
}

/*
 * __layered_clear_ingest_table --
 *     After ingest content has been drained to the stable table, clear out the ingest table.
 */
static int
__layered_clear_ingest_table(WT_SESSION_IMPL *session, const char *uri)
{
    WT_ASSERT(session, WT_URI_IS_INGEST(uri));

    /*
     * Truncate needs a running txn. We should probably do something more like the history store and
     * make this non-transactional -- this happens during step-up, so we know there are no other
     * transactions running, so it's safe.
     */
    WT_RET(__wt_txn_begin(session, NULL));

    /*
     * No other transactions are running, we're only doing this truncate, and it should become
     * immediately visible. So this transaction doesn't have to care about timestamps.
     */
    F_SET(session->txn, WT_TXN_TS_NOT_SET);

    WT_RET(session->iface.truncate(&session->iface, uri, NULL, NULL, NULL));

    WT_RET(__wt_txn_commit(session, NULL));

    return (0);
}

/*
 * __layered_reset_ingest_table_prune_timestamp --
 *     Reset the prune timestamp for the ingest table.
 *
 * This is used when connection steps up from follower to leader. Resetting the prune timestamp to
 *     WT_TS_NONE will allow immediate eviction of dirty ingest pages. These dirty pages are not
 *     needed any more since the new leader just drained all the ingest content to the stable table.
 */
static int
__layered_reset_ingest_table_prune_timestamp(WT_SESSION_IMPL *session, const char *ingest_uri)
{
    WT_BTREE *btree = NULL;
    WT_DECL_RET;
    wt_timestamp_t btree_prune_timestamp;

    WT_RET_ERROR_OK(ret = __wt_session_get_dhandle(session, ingest_uri, NULL, NULL, 0), ENOENT);
    if (ret == ENOENT) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "Handle not found for ingest table uri: %s", ingest_uri);
        return (0);
    }

    btree = (WT_BTREE *)session->dhandle->handle;
    btree_prune_timestamp = __wt_atomic_load_uint64_relaxed(&btree->prune_timestamp);

    __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
      "Reset prune timestamp from %" PRIu64 " to WT_TS_NONE(%d)", btree_prune_timestamp,
      WT_TS_NONE);

    __wt_atomic_store_uint64_relaxed(&btree->prune_timestamp, WT_TS_NONE);

    WT_RET(__wt_session_release_dhandle(session));

    return (ret);
}

/*
 * __layered_derive_stable_uri --
 *     Derive the stable constituent URI corresponding to an ingest constituent URI. The result is
 *     written into the caller's scratch buffer, which must already be allocated.
 */
static int
__layered_derive_stable_uri(WT_SESSION_IMPL *session, const char *ingest_uri, WT_ITEM *buf)
{
    static const char ingest_suffix[] = ".wt_ingest";
    size_t prefix_len, uri_len;

    uri_len = strlen(ingest_uri);
    WT_ASSERT_ALWAYS(session, uri_len > sizeof(ingest_suffix) - 1,
      "Ingest URI is too short to contain an ingest suffix");
    prefix_len = uri_len - (sizeof(ingest_suffix) - 1);
    WT_ASSERT_ALWAYS(session, strcmp(ingest_uri + prefix_len, ingest_suffix) == 0,
      "Ingest URI does not end in the expected ingest suffix");
    return (__wt_buf_fmt(session, buf, "%.*s.wt_stable", (int)prefix_len, ingest_uri));
}

/*
 * __layered_derive_layered_uri --
 *     Derive the parent layered URI from a constituent ingest URI.
 */
static int
__layered_derive_layered_uri(WT_SESSION_IMPL *session, const char *ingest_uri, WT_ITEM *buf)
{
    static const char file_prefix[] = "file:";
    static const char ingest_suffix[] = ".wt_ingest";
    size_t uri_len = strlen(ingest_uri);
    size_t prefix_len = strlen(file_prefix);
    size_t suffix_len = strlen(ingest_suffix);

    if (!WT_PREFIX_MATCH(ingest_uri, file_prefix) || !WT_URI_IS_INGEST(ingest_uri))
        WT_RET_MSG(session, EINVAL,
          "Ingest URI \"%s\" does not match expected file:<name>.wt_ingest shape", ingest_uri);
    WT_ASSERT(session, uri_len > prefix_len + suffix_len);
    size_t name_len = uri_len - prefix_len - suffix_len;
    return (__wt_buf_fmt(session, buf, "layered:%.*s", (int)name_len, ingest_uri + prefix_len));
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __layered_assert_ingest_table_empty --
 *     Verify that the ingest table has no records. Called after truncation as a post-condition
 *     check.
 */
static int
__layered_assert_ingest_table_empty(WT_SESSION_IMPL *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *cursor_config[] = {
      WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "readonly", NULL, NULL};

    WT_RET(__wt_open_cursor(session, uri, NULL, cursor_config, &cursor));
    ret = cursor->next(cursor);
    WT_ASSERT(session, ret == WT_NOTFOUND);
    WT_TRET(cursor->close(cursor));

    return (ret == WT_NOTFOUND ? 0 : ret);
}
#endif

/*
 * __layered_fix_prepared_transaction_callback --
 *     Callback for session walk to fix prepared transactions that may be active during the ingest
 *     btree drain.
 */
static int
__layered_fix_prepared_transaction_callback(
  WT_SESSION_IMPL *session, WT_SESSION_IMPL *array_session, bool *exit_walkp, void *cookiep)
{
    WT_FIX_PREPARED_COOKIE *cookie;
    WT_TXN *txn;
    bool patched;

    cookie = (WT_FIX_PREPARED_COOKIE *)cookiep;
    txn = array_session->txn;
    *exit_walkp = false;
    patched = false;

    if (!F_ISSET(txn, WT_TXN_PREPARE))
        return (0);

    /*
     * Prefer matching by transaction id: a live in-flight prepared transaction that survived
     * step-up shares its session's transaction id with the on-disk start record. Only fall back to
     * the prepared id when the transaction id does not match, which covers sessions that reclaimed
     * the prepared transaction from a checkpoint at startup recovery -- those sessions have no
     * transaction id assigned but do carry a prepared id.
     */
    if (!F_ISSET(&txn->time_point, WT_TXN_TIME_POINT_HAS_ID)) {
        WT_ASSERT(session, F_ISSET(&txn->time_point, WT_TXN_TIME_POINT_HAS_PREPARED_ID));
        if (txn->time_point.prepared_id != cookie->prepared_id)
            return (0);
    } else if (txn->time_point.id != cookie->txnid)
        return (0);
    else
        WT_ASSERT(session,
          !F_ISSET(&txn->time_point, WT_TXN_TIME_POINT_HAS_PREPARED_ID) ||
            txn->time_point.prepared_id == cookie->prepared_id);

    for (size_t i = 0; i < txn->mod_count; i++) {
        WT_TXN_OP *op = &txn->mod[i];

        if (op->type == WT_TXN_OP_NONE)
            continue;

        if (op->btree != cookie->ingest_btree)
            continue;

        int cmp;
        WT_RET(__wt_compare(session, op->btree->collator, &op->u.op_row.key, cookie->key, &cmp));

        if (cmp < 0)
            continue;

        /*
         * The operation keys in a prepared transaction are sorted. We have passed the key we're
         * looking for.
         */
        if (cmp > 0)
            break;

        /*
         * Mark the original update on the ingest btree as aborted. Otherwise, we may get a
         * WT_ROLLBACK error when we try to truncate the ingest btree.
         *
         * Use the synchronized store: a concurrent drain worker's version cursor may be reading
         * this update's txnid (to skip aborted updates) while we abort it. This matches how WT
         * aborts an update's txnid elsewhere and pairs with the atomic reads in the visibility
         * path.
         */
        __wt_tsan_suppress_store_uint64_v(&op->u.op_upd->txnid, WT_TXN_ABORTED);
        /* Point the operation to the stable btree. */
        op->btree = cookie->stable_btree;

        /*
         * Transfer the session_inuse reference from the ingest btree to the stable btree. The
         * ingest btree's session_inuse was incremented when this operation was recorded in the
         * transaction, and op->btree's (now the stable btree) session_inuse will be decremented
         * when the operation is freed. Adjust both counts to keep them balanced.
         */
        (void)__wt_atomic_sub_int32(&cookie->ingest_btree->dhandle->session_inuse, 1);
        (void)__wt_atomic_add_int32(&cookie->stable_btree->dhandle->session_inuse, 1);
        patched = true;
    }

    /*
     * Only stop the walk when this session actually owned the key. In a split-prepared scenario two
     * sessions can share the same prepared_id: one session reclaimed the id from a checkpoint (no
     * transaction id assigned) and holds mods for some tables, while a second live session holds
     * mods for different tables with the same id. If the first session matched by prepared_id but
     * had no mods for this ingest btree, the walk must continue so the second session can be found.
     */
    *exit_walkp = patched;
    return (0);
}

/*
 * __layered_fix_prepared_transaction --
 *     During ingest drain, a key that was prepared on the ingest btree is being moved to the stable
 *     btree. If the owning transaction is still in-flight (not yet committed or rolled back), its
 *     WT_TXN_OP entries still reference the ingest btree and the in-memory update on it. This
 *     function patches those entries so that commit/rollback will operate on the stable btree
 *     instead. For each matching operation it: (1) aborts the original in-memory update on the
 *     ingest btree so that a subsequent truncate of the ingest table does not trip over a live
 *     prepared update, (2) redirects op->btree to the stable btree, and (3) transfers the
 *     session_inuse reference from the ingest dhandle to the stable dhandle to keep reference
 *     counts balanced.
 *
 * The owning session is identified by either the on-disk transaction id (set on a session whose
 *     prepared transaction remained in-flight across step-up) or the on-disk prepared id (set on a
 *     session that reclaimed the prepared transaction from a checkpoint at startup recovery, where
 *     no transaction id is assigned).
 *
 * This is a temporary solution. It assumes no concurrent commit/rollback of the prepared
 *     transaction and no prepared fast-truncate operations.
 */
static int
__layered_fix_prepared_transaction(WT_SESSION_IMPL *session, WT_ITEM *key, WT_BTREE *ingest_btree,
  WT_BTREE *stable_btree, uint64_t txnid, uint64_t prepared_id)
{
    WT_DECL_RET;
    WT_FIX_PREPARED_COOKIE cookie;

    cookie.key = key;
    cookie.ingest_btree = ingest_btree;
    cookie.stable_btree = stable_btree;
    cookie.txnid = txnid;
    cookie.prepared_id = prepared_id;

    /*
     * Serialize across parallel drain workers. Each worker can independently encounter a key
     * belonging to a still-in-flight prepared transaction and call into here -- the callback then
     * walks every session in the connection and writes op->btree / op->u.op_upd->txnid for the
     * matching op. Workers fix different ops (one per key) but they iterate the same txn->mod[]
     * array, so an unguarded concurrent walk produces an unsynchronized read+write on each
     * op->btree as workers scan past the other worker's op. The lock is held only across the walk;
     * rare path, no measurable contention.
     */
    __wt_spin_lock(session, &S2C(session)->layered_drain_data.fix_prepared_lock);
    ret =
      __wt_session_array_walk(session, __layered_fix_prepared_transaction_callback, true, &cookie);
    __wt_spin_unlock(session, &S2C(session)->layered_drain_data.fix_prepared_lock);
    return (ret);
}

/* Buffer large enough for 255 bytes of key as hex plus NUL. */
#define WT_LAYERED_KEY_HEX_BUFSIZE 512

/*
 * __layered_key_hex --
 *     Write the full hex encoding of a key bound into buf for logging. A zero-size item (unbounded
 *     range end) is rendered as "(none)".
 */
static void
__layered_key_hex(const WT_ITEM *key, char *buf, size_t bufsize)
{
    static const char hex[] = "0123456789abcdef";
    const uint8_t *data;
    size_t i, pos;

    if (key->size == 0) {
        WT_IGNORE_RET(__wt_snprintf(buf, bufsize, "(none)"));
        return;
    }

    data = (const uint8_t *)key->data;
    for (i = 0, pos = 0; i < key->size && pos + 2 < bufsize; i++) {
        buf[pos++] = hex[(data[i] >> 4) & 0xf];
        buf[pos++] = hex[data[i] & 0xf];
    }
    buf[pos] = '\0';
}

/*
 * __layered_copy_ingest_table --
 *     Copy all data from a single ingest table (or a key-range sub-section) to the corresponding
 *     stable table. key_start and key_stop are optional inclusive-lower / exclusive-upper bounds;
 *     NULL means unbounded at that end.
 */
static int
__layered_copy_ingest_table(WT_SESSION_IMPL *session, const char *ingest_uri,
  const WT_ITEM *key_start, const WT_ITEM *key_stop, wt_timestamp_t from_ts, wt_timestamp_t to_ts,
  uint64_t *nkeysp)
{
    WT_BTREE *ingest_btree, *stable_btree;
    WT_CURSOR *ingest_btree_cursor, *ingest_version_cursor, *prepare_cursor, *stable_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_ITEM(first_key);
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(stable_uri_buf);
    WT_DECL_ITEM(tmp_key);
    WT_DECL_ITEM(value);
    WT_DECL_RET;
    WT_UPDATE *last_upd, *prev_upd, *upd, *upds;
    wt_timestamp_t cursor_start_ts, last_checkpoint_timestamp;
    wt_timestamp_t durable_start_ts, durable_stop_ts, start_prepare_ts, start_ts, stop_prepare_ts,
      stop_ts;
    uint64_t start_prepared_id, start_txn, stop_prepared_id, stop_txn;
    uint8_t flags, location, prepare, type;
    int cmp;
    char buf[256], buf2[64];
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL, NULL, NULL};
    const char *open_cfg[] = {
      WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL, NULL};
    char hex1[WT_LAYERED_KEY_HEX_BUFSIZE], hex2[WT_LAYERED_KEY_HEX_BUFSIZE];
    bool first_key_set, in_ts_range, is_prepare_rollback, preserve_prepared, prepare_resolved,
      prepare_txn_fixed, skip_first_next;

    ingest_version_cursor = prepare_cursor = stable_cursor = NULL;
    last_upd = prev_upd = upd = upds = NULL;
    first_key_set = prepare_resolved = prepare_txn_fixed = skip_first_next = false;
    preserve_prepared = F_ISSET(S2C(session), WT_CONN_PRESERVE_PREPARED);
    *nkeysp = 0;

    WT_RET(__wt_scr_alloc(session, 0, &stable_uri_buf));
    WT_ERR(__layered_derive_stable_uri(session, ingest_uri, stable_uri_buf));

    last_checkpoint_timestamp = __wt_atomic_load_uint64_acquire(
      &S2C(session)->disaggregated_storage.last_checkpoint_timestamp);
    WT_ERR(__wt_open_cursor(session, stable_uri_buf->data, NULL, open_cfg, &stable_cursor));
    cbt = (WT_CURSOR_BTREE *)stable_cursor;
    stable_btree = CUR2BT(cbt);

    /*
     * The version cursor skips updates at or below cursor_start_ts to avoid re-draining data
     * already covered by a previous pass or a checkpoint.
     */
    cursor_start_ts = (from_ts > last_checkpoint_timestamp) ? from_ts : last_checkpoint_timestamp;
    if (cursor_start_ts != WT_TS_NONE)
        WT_ERR(__wt_snprintf(buf2, sizeof(buf2), "start_timestamp=%" PRIx64 "", cursor_start_ts));
    else
        buf2[0] = '\0';
    WT_ERR(__wt_snprintf(buf, sizeof(buf),
      "debug=(dump_version=(enabled=true,raw_key_value=true,timestamp_order=true,cross_key=true,"
      "show_prepared_rollback=%s,%s))",
      preserve_prepared ? "true" : "false", buf2));
    cfg[1] = buf;
    WT_ERR(__wt_open_cursor(session, ingest_uri, NULL, cfg, &ingest_version_cursor));
    ingest_btree_cursor = ((WT_CURSOR_VERSION *)ingest_version_cursor)->file_cursor;
    ingest_btree = CUR2BT(ingest_btree_cursor);

    WT_ERR(__wt_scr_alloc(session, 0, &first_key));
    WT_ERR(__wt_scr_alloc(session, 0, &key));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp_key));
    WT_ERR(__wt_scr_alloc(session, 0, &value));

    /*
     * If a start key is supplied, position the version cursor at the first key at or after
     * key_start that has a version visible in this slice. A plain exact search is not enough under
     * timestamp-sliced drain: a split key may have no version in a given slice's timestamp range,
     * so we search_near and skip forward to the first qualifying key.
     *
     * search_near can also return WT_NOTFOUND when the only keys at or after key_start are visible
     * solely as prepared (uncommitted) updates: its anchor walk skips a key whose only version is
     * filtered out by the slice's start timestamp, even though a plain next() walk would surface
     * that prepared version. In that case fall back to an unbounded scan from the start and skip
     * keys below key_start in the main loop, so a range whose lower keys are prepared-only is still
     * drained (and its prepared transactions still fixed). A genuinely empty range drains nothing
     * either way -- the from-start scan simply finds no key in [key_start, key_stop).
     */
    if (key_start != NULL) {
        int start_exact;
        ingest_version_cursor->set_key(ingest_version_cursor, key_start);
        WT_ERR_NOTFOUND_OK(
          ingest_version_cursor->search_near(ingest_version_cursor, &start_exact), true);
        if (ret == WT_NOTFOUND)
            WT_ERR(ingest_version_cursor->reset(ingest_version_cursor));
        else {
            WT_ERR(ret);
            /* Positioned strictly below key_start: advance into the range on the first next(). */
            skip_first_next = start_exact >= 0;
        }
    }

    for (;;) {
        upd = NULL;
        if (skip_first_next)
            skip_first_next = false;
        else
            WT_ERR_NOTFOUND_OK(ingest_version_cursor->next(ingest_version_cursor), true);
        if (ret == WT_NOTFOUND) {
            if (key->size > 0 && upds != NULL) {
                WT_WITH_DHANDLE(session, cbt->dhandle,
                  ret = __layered_move_updates(session, cbt, key, upds, last_upd));
                WT_ERR(ret);
                ++(*nkeysp);
                upds = NULL;
            } else
                ret = 0;
            break;
        }

        WT_ERR(ingest_version_cursor->get_key(ingest_version_cursor, tmp_key));

        /*
         * Skip keys strictly below key_start. The cursor is normally already positioned at or after
         * key_start, so this never fires; it matters only on the from-start fallback taken when
         * search_near could not anchor on a prepared-only boundary key. No pending updates can have
         * accumulated yet (the skipped keys precede any in-range key), so there is nothing to
         * flush.
         */
        if (key_start != NULL) {
            WT_ERR(__wt_compare(session, stable_btree->collator, tmp_key, key_start, &cmp));
            if (cmp < 0)
                continue;
        }

        /*
         * If a stop key is set for this range, check whether the current key has reached or passed
         * it. Flush any pending updates accumulated for the previous key, then exit the range
         * without processing the current key (it belongs to the next range worker).
         */
        if (key_stop != NULL) {
            WT_ERR(__wt_compare(session, stable_btree->collator, tmp_key, key_stop, &cmp));
            if (cmp >= 0) {
                __layered_key_hex(tmp_key, hex1, sizeof(hex1));
                __layered_key_hex(key_stop, hex2, sizeof(hex2));
                __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1,
                  "Drain range stop_boundary: table=%s at=%s stop=%s", ingest_uri, hex1, hex2);
                if (upds != NULL) {
                    WT_WITH_DHANDLE(session, cbt->dhandle,
                      ret = __layered_move_updates(session, cbt, key, upds, last_upd));
                    WT_ERR(ret);
                    ++(*nkeysp);
                    upds = NULL;
                }
                goto err;
            }
        }

        WT_ERR(__wt_compare(session, stable_btree->collator, key, tmp_key, &cmp));
        if (cmp != 0) {
            /*
             * Ensure keys returned are in correctly sorted order. Only perform this check when key
             * has been initialized.
             */
            WT_ASSERT(session, key->size == 0 || cmp <= 0);

            if (!first_key_set) {
                WT_ERR(__wt_buf_set(session, first_key, tmp_key->data, tmp_key->size));
                first_key_set = true;
            }

            if (upds != NULL) {
                WT_WITH_DHANDLE(session, cbt->dhandle,
                  ret = __layered_move_updates(session, cbt, key, upds, last_upd));
                WT_ERR(ret);
                ++(*nkeysp);
            }

            upds = NULL;
            prev_upd = NULL;
            prepare_txn_fixed = false;
            prepare_resolved = false;
            WT_ERR(__wt_buf_set(session, key, tmp_key->data, tmp_key->size));
        }

        WT_ERR(ingest_version_cursor->get_value(ingest_version_cursor, &start_txn, &start_ts,
          &durable_start_ts, &start_prepare_ts, &start_prepared_id, &stop_txn, &stop_ts,
          &durable_stop_ts, &stop_prepare_ts, &stop_prepared_id, &type, &prepare, &flags, &location,
          value));

        is_prepare_rollback = start_txn == WT_TXN_ABORTED;
        /*
         * Only process updates whose durable timestamp falls in (from_ts, to_ts]. Prepared updates
         * are included only in the final pass since their commit timestamp is not yet resolved.
         * This is what lets truncate replays interleave at their timestamp: a key's updates reach
         * the stable table oldest-first across slices, so each prepend is genuinely the newest.
         */
        in_ts_range = prepare ? (to_ts == WT_TS_MAX) :
                                (durable_start_ts > from_ts && durable_start_ts <= to_ts);
        if (in_ts_range) {
            /*
             * If the "preserve prepared" option is enabled and the ingest btree contains a resolved
             * prepared update for this key whose prepared timestamp is less than or equal to the
             * last checkpoint timestamp, the stable btree must still contain an unresolved prepared
             * cell from a previous checkpoint. To ensure data consistency, resolve the unresolved
             * prepared cell before applying the ingest updates.
             */
            if (preserve_prepared && start_prepared_id != WT_PREPARED_ID_NONE &&
              start_prepare_ts <= last_checkpoint_timestamp) {
                if (prepare) {
                    if (!prepare_txn_fixed) {
                        WT_ASSERT(session, upds == NULL);
                        WT_ERR(__layered_fix_prepared_transaction(
                          session, key, ingest_btree, stable_btree, start_txn, start_prepared_id));
                        prepare_txn_fixed = true;
                    }
                } else if (!prepare_resolved) {
                    /* Only resolve the updates from the same prepared transaction once. */
                    if (is_prepare_rollback) {
                        /*
                         * The original transaction id is stored in start timestamp and the rollback
                         * timestamp is stored in durable timestamp.
                         */
                        WT_TXN_TIME_POINT txn_time_point;
                        WT_ASSERT(session, start_prepared_id != WT_PREPARED_ID_NONE);
                        WT_ASSERT(session, start_prepare_ts != WT_TS_NONE);
                        WT_ASSERT(session, durable_start_ts != WT_TS_NONE);
                        WT_CLEAR(txn_time_point);
                        txn_time_point.id = start_ts;
                        txn_time_point.prepared_id = start_prepared_id;
                        txn_time_point.prepare_timestamp = start_prepare_ts;
                        txn_time_point.rollback_timestamp = durable_start_ts;
                        F_SET(&txn_time_point,
                          WT_TXN_TIME_POINT_HAS_PREPARED_ID | WT_TXN_TIME_POINT_HAS_TS_PREPARE |
                            WT_TXN_TIME_POINT_HAS_TS_ROLLBACK);
                        /* Sessions that claimed by prepared id alone carry no transaction id. */
                        if (start_ts != WT_TXN_NONE)
                            F_SET(&txn_time_point, WT_TXN_TIME_POINT_HAS_ID);
                        WT_ERR(__wt_txn_resolve_prepared_op(session, stable_btree, &txn_time_point,
                          key, WT_RECNO_OOB, false, &prepare_cursor));
                    } else {
                        WT_TXN_TIME_POINT txn_time_point;
                        WT_ASSERT(session, start_prepared_id != WT_PREPARED_ID_NONE);
                        WT_ASSERT(session, start_prepare_ts != WT_TS_NONE);
                        WT_ASSERT(session, start_ts != WT_TS_NONE);
                        WT_ASSERT(session, durable_start_ts != WT_TS_NONE);
                        WT_CLEAR(txn_time_point);
                        txn_time_point.id = start_txn;
                        txn_time_point.prepared_id = start_prepared_id;
                        txn_time_point.prepare_timestamp = start_prepare_ts;
                        txn_time_point.commit_timestamp = start_ts;
                        txn_time_point.durable_timestamp = durable_start_ts;
                        F_SET(&txn_time_point,
                          WT_TXN_TIME_POINT_HAS_PREPARED_ID | WT_TXN_TIME_POINT_HAS_TS_PREPARE |
                            WT_TXN_TIME_POINT_HAS_TS_COMMIT | WT_TXN_TIME_POINT_HAS_TS_DURABLE);
                        /* Sessions that claimed by prepared id alone carry no transaction id. */
                        if (start_txn != WT_TXN_NONE)
                            F_SET(&txn_time_point, WT_TXN_TIME_POINT_HAS_ID);
                        WT_ERR(__wt_txn_resolve_prepared_op(session, stable_btree, &txn_time_point,
                          key, WT_RECNO_OOB, true, &prepare_cursor));
                    }
                    prepare_resolved = true;
                }
            } else {
                /*
                 * If the update is not a prepared update or a resolved prepared update that has
                 * never been written to the checkpoint as a prepared update, move it to the stable
                 * table directly.
                 */
                /*
                 * FIXME-WT-14732: this is an ugly layering violation. But I can't think of a better
                 * way now.
                 */
                if (__wt_clayered_deleted(value))
                    WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
                else
                    WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
                /*
                 * If the prepared update is aborted, move the aborted update to the stable table
                 * because we may write a prepared update to the disk in a future reconciliation.
                 */
                if (is_prepare_rollback) {
                    /* Prepared transactions must have a prepared id in disagg. */
                    WT_ASSERT(session,
                      !prepare && preserve_prepared && start_prepared_id != WT_PREPARED_ID_NONE);
                    /*
                     * The original transaction id is stored in start timestamp and the rollback
                     * timestamp is stored in durable timestamp.
                     */
                    upd->txnid = WT_TXN_ABORTED;
                    upd->prepare_state = WT_PREPARE_INPROGRESS;
                    upd->prepare_ts = start_prepare_ts;
                    upd->prepared_id = start_prepared_id;
                    upd->upd_saved_txnid = start_ts;
                    upd->upd_rollback_ts = durable_start_ts;
                } else {
                    WT_ASSERT(session, !prepare || durable_start_ts == WT_TS_NONE);
                    upd->txnid = start_txn;
                    if (prepare)
                        upd->prepare_state = WT_PREPARE_INPROGRESS;
                    else if (start_prepared_id != WT_PREPARED_ID_NONE)
                        upd->prepare_state = WT_PREPARE_RESOLVED;
                    upd->prepare_ts = start_prepare_ts;
                    upd->prepared_id = start_prepared_id;
                    upd->upd_start_ts = start_ts;
                    upd->upd_durable_ts = durable_start_ts;
                }
                /* This is for debugging purpose and it is not checked in the code. */
                F_SET(upd, WT_UPDATE_RESTORED_FROM_INGEST);
                last_upd = upd;

                if (prepare && !prepare_txn_fixed) {
                    WT_ASSERT(session, upds == NULL);
                    WT_ERR(__layered_fix_prepared_transaction(
                      session, key, ingest_btree, stable_btree, start_txn, start_prepared_id));
                    prepare_txn_fixed = true;
                }
            }
        }

        if (upd != NULL) {
            /* If a prepared update is resolved, it must be the final update to be drained. */
            WT_ASSERT(session, !prepare_resolved);
            if (prev_upd != NULL)
                prev_upd->next = upd;
            else
                upds = upd;

            prev_upd = upd;
        }
    }

err:
    if (*nkeysp > 0) {
        __layered_key_hex(first_key, hex1, sizeof(hex1));
        __layered_key_hex(key, hex2, sizeof(hex2));
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1,
          "Drain range extent: table=%s keys=%" PRIu64 " first=%s last=%s", ingest_uri, *nkeysp,
          hex1, hex2);
    }
    __wt_scr_free(session, &first_key);
    if (upd != NULL)
        __wt_free(session, upd);
    if (upds != NULL)
        __wt_free_update_list(session, &upds);
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &stable_uri_buf);
    __wt_scr_free(session, &tmp_key);
    __wt_scr_free(session, &value);
    if (ingest_version_cursor != NULL)
        WT_TRET(ingest_version_cursor->close(ingest_version_cursor));
    if (prepare_cursor != NULL)
        WT_TRET(prepare_cursor->close(prepare_cursor));
    if (stable_cursor != NULL)
        WT_TRET(stable_cursor->close(stable_cursor));
    return (ret);
}

/*
 * __layered_ingest_table_is_empty --
 *     Return true if the ingest table has no records.
 */
static int
__layered_ingest_table_is_empty(WT_SESSION_IMPL *session, const char *ingest_uri, bool *emptyp)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;

    *emptyp = false;
    cursor = NULL;

    if (F_ISSET(S2C(session), WT_CONN_PRESERVE_PREPARED)) {
        /*
         * With preserve_prepared, a rolled-back prepared insert on the ingest btree leaves the key
         * with an [aborted-prepared-upd -> globally-visible-tombstone] chain. A regular read-
         * uncommitted cursor walks past the aborted update and sees the tombstone, so it reports
         * the key as deleted and the ingest btree as empty -- but the corresponding claim-prepared
         * cell on the stable btree is still unresolved, and only the drain main loop (which
         * iterates a version cursor with show_prepared_rollback=true) will resolve it. We can't
         * cheaply distinguish "ingest btree truly empty" from "ingest btree contains only rolled-
         * back prepares" without running roughly the same scan the drain itself would do, so just
         * treat the table as non-empty whenever preserve_prepared is enabled. The cost is running
         * the drain main loop once over a genuinely empty btree -- a single version-cursor next()
         * returning WT_NOTFOUND.
         */
        *emptyp = false;
    } else {
        WT_RET(__wt_open_cursor(session, ingest_uri, NULL, NULL, &cursor));
        /* Set WT_TXN_IGNORE_PREPARE so prepared updates don't cause WT_PREPARE_CONFLICT. */
        F_SET(session->txn, WT_TXN_IGNORE_PREPARE);
        WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED, ret = cursor->next(cursor));
        F_CLR(session->txn, WT_TXN_IGNORE_PREPARE);
        /*
         * WT_ROLLBACK here means the cursor hit a modify update that read-uncommitted cannot
         * reconstruct (WT_MODIFY_READ_UNCOMMITTED). A record exists; the table is not empty.
         */
        if (ret == WT_ROLLBACK)
            *emptyp = false;
        else if (ret != 0 && ret != WT_NOTFOUND)
            WT_ERR(ret);
        else
            *emptyp = (ret == WT_NOTFOUND);
    }
    ret = 0;

err:
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));
    return (ret);
}

/*
 * __layered_apply_truncate_to_stable --
 *     Replay a single committed follower truncate against the stable btree over its full key range.
 *     Installs the txn/ts context and issues a range truncate via the INGEST_REPLAY path so that
 *     tombstones carry the original timestamps.
 */
static int
__layered_apply_truncate_to_stable(WT_SESSION_IMPL *session, WT_TRUNCATE *t)
{
    WT_CURSOR *trunc_start, *trunc_stop;
    WT_DECL_RET;
    const char *open_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "raw=true", NULL};

    WT_ASSERT(session, t->start_key.size > 0 && t->stop_key.size > 0);
    WT_ASSERT(session, t->start_ts > WT_TS_NONE);
    WT_ASSERT(session, t->durable_ts >= t->start_ts);

    trunc_start = trunc_stop = NULL;
    WT_ERR(__wt_open_cursor(session, t->layered_table->stable_uri, NULL, open_cfg, &trunc_start));
    WT_ERR(__wt_open_cursor(session, t->layered_table->stable_uri, NULL, open_cfg, &trunc_stop));

    trunc_start->set_key(trunc_start, &t->start_key);
    trunc_stop->set_key(trunc_stop, &t->stop_key);

    session->replay_trunc_ctx.txn_id = t->txn_id;
    session->replay_trunc_ctx.commit_ts = t->start_ts;
    session->replay_trunc_ctx.durable_ts = t->durable_ts;

    F_SET(session, WT_SESSION_INGEST_REPLAY);
    ret = __wt_session_range_truncate(session, NULL, trunc_start, trunc_stop);
    F_CLR(session, WT_SESSION_INGEST_REPLAY);

err:
    if (trunc_start != NULL)
        WT_TRET(trunc_start->close(trunc_start));
    if (trunc_stop != NULL)
        WT_TRET(trunc_stop->close(trunc_stop));
    return (ret);
}

/*
 * __layered_apply_and_clear_truncates --
 *     After all ingest ranges for a table have been drained, apply every committed follower
 *     truncate from the truncate list to the stable btree (covering keys that only exist in stable
 *     and were never in the ingest btree), then clear the list. Must be called once per table after
 *     all parallel drain workers for that table have finished.
 */
static int
__layered_apply_and_clear_truncates(WT_SESSION_IMPL *session, const char *layered_uri)
{
    WT_DATA_HANDLE *layered_dhandle;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *t;

    layered_dhandle = NULL;

    WT_RET_ERROR_OK(ret = __wt_session_get_dhandle(session, layered_uri, NULL, NULL, 0), ENOENT);
    if (ret == ENOENT)
        return (0);
    layered_dhandle = session->dhandle;
    layered_table = (WT_LAYERED_TABLE *)layered_dhandle;

    __wt_readlock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (t, &layered_table->truncateqh, q) {
        /* Match the release-store at __wt_txn_truncate_commit. */
        if (!__wt_atomic_load_bool_acquire(&t->committed))
            continue;
        __wt_readunlock(session, &layered_table->truncate_lock);
        WT_TRET(__layered_apply_truncate_to_stable(session, t));
        __wt_readlock(session, &layered_table->truncate_lock);
    }
    __wt_readunlock(session, &layered_table->truncate_lock);

    __wt_layered_table_truncate_clear(session, layered_table);

    WT_WITH_DHANDLE(session, layered_dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    return (ret);
}

/*
 * __truncate_cmp_by_start_ts --
 *     qsort comparator: ascending order by truncate start timestamp and txn id.
 */
static int
__truncate_cmp_by_start_ts(const void *a, const void *b)
{
    const WT_TRUNCATE *ta = *(const WT_TRUNCATE *const *)a;
    const WT_TRUNCATE *tb = *(const WT_TRUNCATE *const *)b;

    if (ta->start_ts < tb->start_ts)
        return (-1);
    if (ta->start_ts > tb->start_ts)
        return (1);
    if (ta->txn_id < tb->txn_id)
        return (-1);
    if (ta->txn_id > tb->txn_id)
        return (1);
    return (0);
}

/*
 * __layered_build_sorted_truncates --
 *     Create a timestamp-sorted array of committed truncates from the table's truncate list.
 */
static int
__layered_build_sorted_truncates(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table,
  WT_TRUNCATE ***sortedp, size_t *ntruncatesp)
{
    WT_DECL_RET;
    WT_TRUNCATE *t, **sorted;
    size_t i, ntruncates;

    *sortedp = NULL;
    *ntruncatesp = 0;
    sorted = NULL;
    i = ntruncates = 0;

    __wt_readlock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (t, &layered_table->truncateqh, q)
        if (__wt_atomic_load_bool_acquire(&t->committed))
            ++ntruncates;
    if (ntruncates == 0)
        goto err;

    WT_ERR(__wt_calloc(session, ntruncates, sizeof(WT_TRUNCATE *), &sorted));
    TAILQ_FOREACH (t, &layered_table->truncateqh, q)
        if (__wt_atomic_load_bool_acquire(&t->committed))
            sorted[i++] = t;

    __wt_qsort(sorted, ntruncates, sizeof(WT_TRUNCATE *), __truncate_cmp_by_start_ts);
    *sortedp = sorted;
    *ntruncatesp = ntruncates;

err:
    __wt_readunlock(session, &layered_table->truncate_lock);
    if (ret != 0)
        __wt_free(session, sorted);
    return (ret);
}

/*
 * WT_LAYERED_INTERLEAVE_COPY_ARG --
 *     Per-range argument for a parallel slice copy in the interleaved drain. Each spawned thread
 *     copies one key range of one timestamp slice and records its result code.
 */
typedef struct {
    WT_SESSION_IMPL *session; /* Worker's own session; never the calling thread's. */
    const char *ingest_uri;
    const WT_ITEM *key_start; /* Inclusive lower bound, NULL = unbounded. */
    const WT_ITEM *key_stop;  /* Exclusive upper bound, NULL = unbounded. */
    wt_timestamp_t from_ts;   /* Slice is (from_ts, to_ts]. */
    wt_timestamp_t to_ts;
    uint64_t nkeys;
    int result;
} WT_LAYERED_INTERLEAVE_COPY_ARG;

/*
 * __layered_drain_interleave_copy_thread --
 *     Thread callback for the interleaved drain: copy one key range of one timestamp slice. Errors
 *     are stored in the argument, not returned, so the spawning thread can aggregate them after the
 *     join. The slice ordering delivers a key's updates oldest-first, so the single-threaded
 *     truncate replay between slices is always newest.
 */
static WT_THREAD_RET
__layered_drain_interleave_copy_thread(void *arg)
{
    WT_LAYERED_INTERLEAVE_COPY_ARG *a;

    a = (WT_LAYERED_INTERLEAVE_COPY_ARG *)arg;
    a->result = __layered_copy_ingest_table(
      a->session, a->ingest_uri, a->key_start, a->key_stop, a->from_ts, a->to_ts, &a->nkeys);
    return (WT_THREAD_RET_VALUE);
}

/*
 * __layered_drain_table_interleaved --
 *     Drain one ingest table to its stable table for the interleaved-drain proto path. The COPY of
 *     each timestamp slice is parallelized across K disjoint key ranges; the truncate replays
 *     between slices run single-threaded. Serializing the truncate-applies is the whole point: the
 *     concurrent per-worker replay of Structure A conflicted with the ingest fast-truncate clear.
 *     Here every slice's copies fully finish (their version-cursor snapshots released at the join)
 *     before the single replay runs, and the fast-truncate clear runs only after every slice with
 *     no concurrent transaction in flight.
 */
static int
__layered_drain_table_interleaved(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE_MANAGER_ENTRY *entry, uint64_t *nkeysp)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *layered_dhandle;
    WT_DECL_ITEM(layered_uri_buf);
    WT_DECL_RET;
    WT_ITEM *split_keys;
    WT_LAYERED_INTERLEAVE_COPY_ARG *args;
    WT_LAYERED_TABLE *layered_table;
    WT_SESSION_IMPL **worker_sessions;
    WT_TRUNCATE **sorted_truncates;
    wt_thread_t *tids;
    wt_timestamp_t prev_ts, to_ts;
    size_t s, ntruncates;
    uint32_t actual_splits, j, k, nranges;
    const char *ingest_uri;
    bool empty;

    conn = S2C(session);
    ingest_uri = entry->ingest_uri;
    layered_dhandle = NULL;
    layered_table = NULL;
    split_keys = NULL;
    sorted_truncates = NULL;
    args = NULL;
    tids = NULL;
    worker_sessions = NULL;
    ntruncates = 0;
    actual_splits = 0;
    nranges = 0;

    /* Empty tables: leave their truncates to the post-loop apply-and-clear pass. */
    WT_RET(__layered_ingest_table_is_empty(session, ingest_uri, &empty));
    if (empty)
        return (0);

    /*
     * Single range per table for now; key-range sampling is added in a follow-up. With no split
     * keys, actual_splits stays 0 and every slice copies its whole keyspace as one range.
     */
    nranges = actual_splits + 1;

    WT_ERR(__wt_scr_alloc(session, 0, &layered_uri_buf));
    WT_ERR(__layered_derive_layered_uri(session, ingest_uri, layered_uri_buf));
    WT_ERR(__wt_session_get_dhandle(session, layered_uri_buf->data, NULL, NULL, 0));
    layered_dhandle = session->dhandle;
    layered_table = (WT_LAYERED_TABLE *)layered_dhandle;

    WT_ERR(
      __layered_build_sorted_truncates(session, layered_table, &sorted_truncates, &ntruncates));

    WT_ERR(__wt_calloc_def(session, nranges, &args));
    /*
     * The calling thread copies range 0 inline; only ranges 1..nranges-1 need their own thread and
     * session. Allocate the full arrays for index symmetry but populate only the spawned slots.
     */
    WT_ERR(__wt_calloc_def(session, nranges, &tids));
    WT_ERR(__wt_calloc_def(session, nranges, &worker_sessions));
    for (k = 1; k < nranges; k++)
        WT_ERR(__wt_open_internal_session(conn, "disagg-drain-interleave", false,
          WT_SESSION_CAN_WAIT, session->lock_flags, &worker_sessions[k]));

    /*
     * Walk slices oldest-first. Slice s covers (prev_ts, to_ts]; the tail slice (s == ntruncates)
     * runs to WT_TS_MAX and is where prepared updates drain. After each non-tail slice's copies
     * complete, apply that truncate single-threaded.
     */
    prev_ts = WT_TS_NONE;
    for (s = 0; s <= ntruncates; s++) {
        uint32_t spawned;

        to_ts = (s < ntruncates) ? sorted_truncates[s]->start_ts : WT_TS_MAX;

        for (k = 0; k < nranges; k++) {
            args[k].session = (k == 0) ? session : worker_sessions[k];
            args[k].ingest_uri = ingest_uri;
            args[k].key_start = (k == 0) ? NULL : &split_keys[k - 1];
            args[k].key_stop = (k == nranges - 1) ? NULL : &split_keys[k];
            args[k].from_ts = prev_ts;
            args[k].to_ts = to_ts;
            args[k].nkeys = 0;
            args[k].result = 0;
        }

        /*
         * Spawn ranges 1..nranges-1; the join below is the barrier for this slice. Threads are
         * created in order and we stop at the first failure, so the spawned threads are always the
         * contiguous prefix tids[1..spawned].
         */
        spawned = 1;
        for (k = 1; k < nranges; k++) {
            ret = __wt_thread_create(
              session, &tids[k], __layered_drain_interleave_copy_thread, &args[k]);
            if (ret != 0)
                break;
            spawned = k + 1;
        }

        /* Copy range 0 inline only if no spawn failed; otherwise go straight to the join. */
        if (ret == 0)
            args[0].result = __layered_copy_ingest_table(session, ingest_uri, args[0].key_start,
              args[0].key_stop, prev_ts, to_ts, &args[0].nkeys);

        /* Join every thread we actually created, even on a mid-slice error, to avoid a leak. */
        for (k = 1; k < spawned; k++)
            WT_TRET(__wt_thread_join(session, &tids[k]));

        /* First non-zero result wins: thread-create errors, then per-range copy errors. */
        for (k = 0; ret == 0 && k < nranges; k++)
            ret = args[k].result;
        WT_ERR(ret);

        for (k = 0; k < nranges; k++)
            *nkeysp += args[k].nkeys;

        /* Single-threaded full-range replay between slices, now that all snapshots are released. */
        if (s < ntruncates)
            WT_ERR(__layered_apply_truncate_to_stable(session, sorted_truncates[s]));
        prev_ts = to_ts;
    }

    /*
     * The workers applied no truncates (we did, single-threaded), so just clear the list. The fast-
     * truncate clear of the ingest table is safe here: no drain transaction is in flight.
     */
    __wt_layered_table_truncate_clear(session, layered_table);
    WT_WITH_DHANDLE(session, layered_dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    layered_dhandle = NULL;

    WT_ERR(__layered_clear_ingest_table(session, ingest_uri));
#ifdef HAVE_DIAGNOSTIC
    WT_ERR(__layered_assert_ingest_table_empty(session, ingest_uri));
#endif
    WT_ERR(__layered_reset_ingest_table_prune_timestamp(session, ingest_uri));

err:
    if (worker_sessions != NULL) {
        for (k = 1; k < nranges; k++)
            if (worker_sessions[k] != NULL)
                WT_TRET(__wt_session_close_internal(worker_sessions[k]));
        __wt_free(session, worker_sessions);
    }
    __wt_free(session, tids);
    __wt_free(session, args);
    __wt_free(session, sorted_truncates);
    if (layered_dhandle != NULL)
        WT_WITH_DHANDLE(session, layered_dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    if (split_keys != NULL) {
        for (j = 0; j < actual_splits; j++)
            __wt_buf_free(session, &split_keys[j]);
        __wt_free(session, split_keys);
    }
    __wt_scr_free(session, &layered_uri_buf);
    return (ret);
}

/*
 * __wti_layered_drain_ingest_tables --
 *     Move all data from ingest tables to stable tables. Tables are processed sequentially; within
 *     each non-empty table the per-timestamp-slice copy is parallelized across key ranges while the
 *     truncate replays between slices run single-threaded (see __layered_drain_table_interleaved).
 */
int
__wti_layered_drain_ingest_tables(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY **entries;
    size_t i, table_count;
    uint64_t nkeys, t_start;
    int64_t bytes_after, bytes_before;
    bool lock_initialized;

    conn = S2C(session);
    manager = &conn->layered_table_manager;
    entries = NULL;
    bytes_before = 0;
    t_start = 0;
    lock_initialized = false;

    /*
     * Snapshot the table manager's entry array under the lock so we don't race with entries being
     * added, removed, or the array being reallocated (fixes FIXME-WT-14734). Capture the calloc
     * return value and check it only after unlocking so we never take the error path with the lock
     * held.
     */
    __wt_spin_lock(session, &manager->layered_table_lock);
    table_count = manager->open_layered_table_count;
    if (table_count > 0) {
        ret = __wt_calloc_def(session, table_count, &entries);
        if (ret == 0)
            for (i = 0; i < table_count; i++)
                entries[i] = manager->entries[i];
    }
    __wt_spin_unlock(session, &manager->layered_table_lock);
    WT_ERR(ret);

    bytes_before = WT_STAT_CONN_READ(conn->stats, block_byte_read);
    __wt_atomic_store_uint64(&conn->layered_drain_data.total_keys_drained, 0);
    t_start = __wt_clock(session);

    /*
     * The per-slice copies of a table are run on internal worker sessions that call into
     * __layered_fix_prepared_transaction; serialize those calls with the fix-prepared lock.
     */
    WT_ERR(__wt_spin_init(
      session, &conn->layered_drain_data.fix_prepared_lock, "layered drain fix-prepared lock"));
    lock_initialized = true;

    /*
     * Drain each non-empty table with the timestamp-interleaved algorithm, which clears that
     * table's truncate list itself. Empty/skipped tables are left for the post-loop apply-and-clear
     * pass.
     */
    for (i = 0; i < table_count; i++) {
        if (entries[i] == NULL)
            continue;
        nkeys = 0;
        WT_ERR(__layered_drain_table_interleaved(session, entries[i], &nkeys));
        (void)__wt_atomic_add_uint64(&conn->layered_drain_data.total_keys_drained, nkeys);
    }

err:
    /*
     * Apply committed follower truncates to the stable btree and clear the truncate list for every
     * table whose ingest btree was empty (and therefore skipped above). Keys that only exist in
     * stable never written to the ingest btree are not covered by ingest tombstones; the explicit
     * range truncate replay is the only path that stamps them deleted. For drained tables the list
     * was already cleared, so this is a no-op (apply nothing, clear nothing).
     */
    if (entries != NULL) {
        for (i = 0; i < table_count; i++) {
            WT_LAYERED_TABLE_MANAGER_ENTRY *e = entries[i];
            if (e == NULL)
                continue;
            WT_TRET(__layered_apply_and_clear_truncates(session, e->layered_uri));
        }
    }

    bytes_after = WT_STAT_CONN_READ(conn->stats, block_byte_read);
    if (t_start > 0)
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_INFO,
          "Drain complete: %" WT_SIZET_FMT " table(s) drained_keys=%" PRIu64
          " block_bytes_read=%" PRId64 " total_ms=%" PRIu64,
          table_count, __wt_atomic_load_uint64(&conn->layered_drain_data.total_keys_drained),
          bytes_after - bytes_before, WT_CLOCKDIFF_MS(__wt_clock(session), t_start));

    if (lock_initialized)
        __wt_spin_destroy(session, &conn->layered_drain_data.fix_prepared_lock);
    __wt_free(session, entries);
    return (ret);
}

/*
 * __layered_update_ingest_table_prune_timestamp --
 *     Update the prune timestamp of the specified ingest table.
 *
 * We want to see what is the oldest checkpoint on the provided table that is in use by any open
 *     cursor. Even if there are no open cursors on it, the most recent checkpoint on the table is
 *     always considered in use. The basic plan is to start with the last checkpoint in use that we
 *     knew about, and check it again. If it's no longer in use, we go to the next one, etc. This
 *     gives us a list (possibly zero length), of checkpoints that are no longer in use by cursors
 *     on this table. Thus, the timestamp associated with the newest such checkpoint can be used for
 *     garbage collection pruning. Any item in the ingest table older than that timestamp must be
 *     including in one of the checkpoints we're saving, and thus can be removed.
 *
 * The `uri_at_checkpoint_buf` argument is used only to avoid extra allocations between consecutive
 *     calls.
 */
static int
__layered_update_ingest_table_prune_timestamp(WT_SESSION_IMPL *session, const char *layered_uri,
  wt_timestamp_t checkpoint_timestamp, WT_ITEM *uri_at_checkpoint_buf)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    wt_timestamp_t btree_prune_timestamp, prune_timestamp;
    int64_t ckpt_inuse, last_ckpt;
    int32_t layered_dhandle_inuse, stable_dhandle_inuse;

    layered_table = NULL;
    prune_timestamp = WT_TS_NONE;
    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     */
    WT_RET_ERROR_OK(ret = __wt_session_get_dhandle(session, layered_uri, NULL, NULL, 0), ENOENT);
    if (ret == ENOENT) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "GC %s: Layered table was not found.", layered_uri);
        return (0);
    }
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    /*
     * Get the last existing checkpoint. If we've never seen a checkpoint, then there's nothing in
     * the ingest table we can remove. Move on.
     */
    WT_ERR_NOTFOUND_OK(
      __layered_last_checkpoint_order(session, layered_table->stable_uri, &last_ckpt), true);
    if (ret == WT_NOTFOUND) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "GC %s: Layered table checkpoint does not exist: %s", layered_table->iface.name,
          layered_table->stable_uri);
        ret = 0;
        goto err;
    }

    /*
     * If we are setting a prune timestamp the first time, the previous checkpoint could still be in
     * use, so start from it.
     */
    ckpt_inuse = layered_table->last_ckpt_inuse;
    if (ckpt_inuse == 0)
        ckpt_inuse = (last_ckpt > 1) ? last_ckpt - 1 : last_ckpt;

    /* Find the last checkpoint which is still in use. */
    while (ckpt_inuse < last_ckpt) {
        stable_dhandle_inuse = 0;
        WT_ERR(__wt_buf_fmt(session, uri_at_checkpoint_buf, "%s/%s.%" PRId64,
          layered_table->stable_uri, WT_CHECKPOINT, ckpt_inuse));

        /* If it's in use, then it must be in the connection cache. */
        WT_WITH_HANDLE_LIST_READ_LOCK(session,
          if ((ret = __wt_conn_dhandle_find(session, uri_at_checkpoint_buf->data, NULL)) == 0)
            WT_DHANDLE_ACQUIRE(session->dhandle));

        /* If one exists, read all the required info, then release. */
        if (ret == 0) {
            stable_dhandle_inuse = __wt_atomic_load_int32_acquire(&session->dhandle->session_inuse);
            WT_ASSERT(session, prune_timestamp <= S2BT(session)->checkpoint_timestamp);
            prune_timestamp = S2BT(session)->checkpoint_timestamp;
            WT_DHANDLE_RELEASE(session->dhandle);
        }

        WT_ERR_NOTFOUND_OK(ret, false);

        /* If it's in use by any session, then we're done. */
        if (stable_dhandle_inuse > 0)
            break;

        ++ckpt_inuse;
    }

    layered_dhandle_inuse =
      __wt_atomic_load_int32_acquire(&((WT_DATA_HANDLE *)layered_table)->session_inuse);
    if (ckpt_inuse == last_ckpt && (last_ckpt != 1 || layered_dhandle_inuse == 0))
        prune_timestamp = checkpoint_timestamp;

    if (ckpt_inuse == layered_table->last_ckpt_inuse) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "GC %s: Nothing to update - the last checkpoint is still in use %" PRId64,
          layered_table->iface.name, ckpt_inuse);
        ret = 0;
        goto err;
    }

    if (prune_timestamp == WT_TS_NONE) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "GC %s: No checkpoint is eligible for pruning. The last checkpoint in use is %" PRId64,
          layered_table->iface.name, ckpt_inuse);
        ret = 0;
        goto err;
    }

    /*
     * Set the prune timestamp in the btree if it is open, typically it is. However, it's possible
     * that it hasn't been opened yet. In that case, we need to skip updating its timestamp for
     * pruning, and we'll get another chance to update the prune timestamp at the next checkpoint.
     */
    WT_ERR_ERROR_OK(
      __wt_session_get_dhandle(session, layered_table->ingest_uri, NULL, NULL, 0), ENOENT, true);
    if (ret == ENOENT) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "GC %s: Handle not found for ingest table uri: %s", layered_table->iface.name,
          layered_table->ingest_uri);
        ret = 0;
        goto err;
    }

    btree = (WT_BTREE *)session->dhandle->handle;

    btree_prune_timestamp = __wt_atomic_load_uint64_relaxed(&btree->prune_timestamp);
    WT_ASSERT(session, prune_timestamp >= btree_prune_timestamp);

    __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
      "GC %s: update prune timestamp from %" PRIu64 " to %" PRIu64
      " and checkpoint in use from %" PRId64 " to %" PRId64,
      layered_table->iface.name, btree_prune_timestamp, prune_timestamp,
      layered_table->last_ckpt_inuse, ckpt_inuse);

    /*
     * The prune timestamp should be monotonically increasing. It is fine for the user to read the
     * obsolete value. Therefore, no synchronization is required.
     */
    __wt_atomic_store_uint64_relaxed(&btree->prune_timestamp, prune_timestamp);
    layered_table->last_ckpt_inuse = ckpt_inuse;

    WT_ERR(__wt_session_release_dhandle(session));

err:
    WT_ASSERT(session, layered_table != NULL);
    session->dhandle = (WT_DATA_HANDLE *)layered_table;
    WT_TRET(__wt_session_release_dhandle(session));

    return (ret);
}

/*
 * __wti_layered_iterate_ingest_tables_for_gc_pruning --
 *     Iterate over all ingest tables and check whether their prune timestamps could be updated.
 */
int
__wti_layered_iterate_ingest_tables_for_gc_pruning(
  WT_SESSION_IMPL *session, wt_timestamp_t checkpoint_timestamp)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(layered_table_uri_buf);
    WT_DECL_ITEM(uri_at_checkpoint_buf);
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    size_t i;

    conn = S2C(session);
    manager = &conn->layered_table_manager;
    WT_RET(__wt_scr_alloc(session, 0, &layered_table_uri_buf));
    WT_RET(__wt_scr_alloc(session, 0, &uri_at_checkpoint_buf));

    WT_ASSERT(session, manager->init);

    __wt_spin_lock(session, &manager->layered_table_lock);
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if ((entry = manager->entries[i]) == NULL)
            continue;
        ret = __wt_buf_setstr(session, layered_table_uri_buf, entry->layered_uri);

        /*
         * Unlock the mutex while handling a table since while updating the prune timestamp we get a
         * dhandle lock which could cause a deadlock.
         *
         * Releasing the mutex may allow the table to grow, shrink or be modified during this
         * operation. It's okay to prune an element twice in a loop (the second pruning will
         * probably do nothing), or miss an element to prune (it will be visited next time).
         */
        __wt_spin_unlock(session, &manager->layered_table_lock);

        /* Check the buffer-copy result here to avoid returning with the mutex held. */
        WT_ERR(ret);

        WT_ERR(__layered_update_ingest_table_prune_timestamp(
          session, layered_table_uri_buf->data, checkpoint_timestamp, uri_at_checkpoint_buf));

        __wt_spin_lock(session, &manager->layered_table_lock);
    }
    __wt_spin_unlock(session, &manager->layered_table_lock);

err:
    if (ret != 0)
        __wt_verbose_level(
          session, WT_VERB_LAYERED, WT_VERBOSE_ERROR, "GC ingest tables prune failed by: %d", ret);

    __wt_scr_free(session, &layered_table_uri_buf);
    __wt_scr_free(session, &uri_at_checkpoint_buf);
    return (ret);
}

/*
 * __layered_last_checkpoint_order --
 *     For a URI, get the order number for the most recent checkpoint.
 */
static int
__layered_last_checkpoint_order(
  WT_SESSION_IMPL *session, const char *shared_uri, int64_t *ckpt_order)
{
    int scanf_ret;

    const char *checkpoint_name;
    int64_t order_from_name;

    *ckpt_order = 0;

    /* Pull up the last checkpoint for this URI. It could return WT_NOTFOUND. */
    WT_RET(__wt_meta_checkpoint_last_name(session, shared_uri, &checkpoint_name, ckpt_order, NULL));

    /* Sanity check: we make sure that the name returned matches the order number. */
    scanf_ret = sscanf(checkpoint_name, WT_CHECKPOINT ".%" PRId64, &order_from_name);
    __wt_free(session, checkpoint_name);

    if (scanf_ret != 1)
        WT_RET_MSG(session, EINVAL,
          "shared metadata checkpoint unknown format: %s, scan returns %d", checkpoint_name,
          scanf_ret);

    /* These should always be the same. */
    WT_ASSERT(session, *ckpt_order == order_from_name);

    return (0);
}

#ifdef HAVE_UNITTEST

/*
 * __ut_layered_derive_layered_uri --
 *     Unit test wrapper for __layered_derive_layered_uri.
 */
int
__ut_layered_derive_layered_uri(WT_SESSION_IMPL *session, const char *ingest_uri, WT_ITEM *buf)
{
    return (__layered_derive_layered_uri(session, ingest_uri, buf));
}

#endif
