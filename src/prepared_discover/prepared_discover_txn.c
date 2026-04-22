/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_DEFAULT_PENDING_PREPARED_DISCOVER_HASHSIZE 256

/*
 * __wt_prepared_discover_find_item --
 *     Find a pending prepared item by its ID in the pending prepared items hash map.
 */
int
__wt_prepared_discover_find_item(
  WT_SESSION_IMPL *session, uint64_t prepared_id, WT_PENDING_PREPARED_ITEM **prepared_item)
{
    WT_CONNECTION_IMPL *conn;
    WT_PENDING_PREPARED_ITEM *item;
    WT_PENDING_PREPARED_MAP *pending_prepare_items;
    WT_TXN_GLOBAL *txn_global;
    uint64_t bucket;
    conn = S2C(session);
    txn_global = &conn->txn_global;
    pending_prepare_items = &txn_global->pending_prepare_items;
    if (pending_prepare_items->hash != NULL) {
        bucket = prepared_id & (pending_prepare_items->hash_size - 1);
        TAILQ_FOREACH (item, &pending_prepare_items->hash[bucket], hashq) {
            if (item->prepared_id == prepared_id) {
                *prepared_item = item;
                return (0);
            }
        }
    }
    return (WT_NOTFOUND);
}

/*
 * __prepare_discover_alloc_upd --
 *     Create the actual update for a pending prepared value.
 */
static int
__prepare_discover_alloc_upd(WT_SESSION_IMPL *session, WT_ITEM *value, WT_CELL_UNPACK_KV *unpack,
  WT_UPDATE **updp, size_t *sizep)
{
    WT_UPDATE *upd;

    *sizep = 0;
    upd = NULL;
    if (WT_TIME_WINDOW_HAS_STOP_PREPARE(&(unpack->tw))) {
        /*
         * Usually we would allocate a tombstone update when seeing a stop timestamp. However in
         * this code flow, we're restoring the update into ingest table with no tombstone allowed,
         * create a standard update with a special tombstone value instead of a tombstone. In the
         * case where the update has both start and stop prepared, no need to restore the start
         * prepared.
         */
        WT_RET(__wt_upd_alloc(session, &__wt_tombstone, WT_UPDATE_STANDARD, &upd, sizep));
        upd->txnid = unpack->tw.stop_txn;
        upd->prepared_id = unpack->tw.stop_prepared_id;
        upd->prepare_ts = unpack->tw.stop_prepare_ts;
        upd->upd_durable_ts = WT_TS_NONE;
        upd->upd_start_ts = unpack->tw.stop_prepare_ts;
        upd->prepare_state = WT_PREPARE_INPROGRESS;
    } else {
        WT_ASSERT(session, WT_TIME_WINDOW_HAS_START_PREPARE(&(unpack->tw)));
        WT_RET(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, sizep));
        upd->txnid = unpack->tw.start_txn;
        upd->prepared_id = unpack->tw.start_prepared_id;
        upd->prepare_ts = unpack->tw.start_prepare_ts;
        upd->upd_durable_ts = WT_TS_NONE;
        upd->upd_start_ts = unpack->tw.start_prepare_ts;
        upd->prepare_state = WT_PREPARE_INPROGRESS;
    }
    *updp = upd;
    return (0);
}

/*
 * __pending_prepare_items_init --
 *     Initialize pending prepared txn hash map.
 */
static int
__pending_prepare_items_init(
  WT_SESSION_IMPL *session, WT_PENDING_PREPARED_MAP *pending_prepare_items, u_int hash_size)
{
    /* Hash size must be a power of 2 for efficient bucket calculation. */
    WT_ASSERT(session, (hash_size & (hash_size - 1)) == 0);

    pending_prepare_items->hash_size = hash_size;
    WT_RET(
      __wt_calloc_def(session, pending_prepare_items->hash_size, &pending_prepare_items->hash));
    for (uint64_t i = 0; i < pending_prepare_items->hash_size; i++) {
        TAILQ_INIT(&pending_prepare_items->hash[i]); /* hash lists */
    }
    return (0);
}

/*
 * __prepared_discover_find_or_create_item --
 *     We have learned that a prepared transaction with a particular ID exists. If this is the first
 *     time it's been noticed, create an item corresponding to it. Otherwise return the matching
 *     item.
 */
static int
__prepared_discover_find_or_create_item(WT_SESSION_IMPL *session, uint64_t prepared_id,
  wt_timestamp_t prepare_timestamp, WT_PENDING_PREPARED_ITEM **prepared_item)
{
    WT_CONNECTION_IMPL *conn;
    WT_PENDING_PREPARED_ITEM *item;
    WT_PENDING_PREPARED_MAP *pending_prepare_items;
    WT_TXN_GLOBAL *txn_global;
    uint64_t bucket;

    if (__wt_prepared_discover_find_item(session, prepared_id, prepared_item) == 0)
        return (0);

    conn = S2C(session);
    txn_global = &conn->txn_global;
    pending_prepare_items = &txn_global->pending_prepare_items;
    if (pending_prepare_items->hash == NULL) {
        WT_RET(__pending_prepare_items_init(session, pending_prepare_items,
          /* hash size*/ WT_DEFAULT_PENDING_PREPARED_DISCOVER_HASHSIZE));
    }

    WT_RET(__wt_calloc_one(session, &item));
    item->prepared_id = prepared_id;
    item->prepare_timestamp = prepare_timestamp;
    bucket = prepared_id & (pending_prepare_items->hash_size - 1);
    TAILQ_INSERT_HEAD(&pending_prepare_items->hash[bucket], item, hashq);
    *prepared_item = item;
    return (0);
}

/*
 * __wt_prepared_discover_remove_item --
 *     Find and remove a pending prepared item by its ID in the pending prepared items hash map.
 */
int
__wt_prepared_discover_remove_item(WT_SESSION_IMPL *session, uint64_t prepared_id)
{
    WT_CONNECTION_IMPL *conn;
    WT_PENDING_PREPARED_ITEM *item;
    WT_PENDING_PREPARED_MAP *pending_prepare_items;
    WT_TXN_GLOBAL *txn_global;
    uint64_t bucket;
    conn = S2C(session);
    txn_global = &conn->txn_global;
    pending_prepare_items = &txn_global->pending_prepare_items;

    if (pending_prepare_items->hash != NULL) {
        bucket = prepared_id & (pending_prepare_items->hash_size - 1);
        TAILQ_FOREACH (item, &pending_prepare_items->hash[bucket], hashq) {
            if (item->prepared_id == prepared_id) {
                TAILQ_REMOVE(&pending_prepare_items->hash[bucket], item, hashq);
                /* Clean up memory of unclaimed mod array */
                WT_ASSERT_ALWAYS(
                  session, item->mod_count == 0, "Removing an unclaimed prepared item.");
                __wt_free(session, item->mod);
                __wt_free(session, item);
                return (0);
            }
        }
    }
    return (WT_NOTFOUND);
}

/*
 * __wti_prepared_discover_add_artifact_upd --
 *     Add an artifact to a pending prepared transaction.
 */
int
__wti_prepared_discover_add_artifact_upd(WT_SESSION_IMPL *session, WT_UPDATE *upd, WT_ITEM *key)
{
    WT_PENDING_PREPARED_ITEM *prepared_item;
    WT_TXN_OP *op;
    WT_RET(__prepared_discover_find_or_create_item(
      session, upd->prepared_id, upd->prepare_ts, &prepared_item));
    /*
     * We need the key and btree information to help with the search of the update when resolving
     * txn.
     */
    WT_RET(__wt_pending_prepared_next_op(session, &op, prepared_item, key));
    WT_RET(__wt_op_modify(session, upd, op));

    WT_ASSERT(session, op->type == WT_TXN_OP_BASIC_ROW || op->type == WT_TXN_OP_INMEM_ROW);

#ifdef HAVE_DIAGNOSTIC
    ++prepared_item->prepare_count;
#endif
    return (0);
}

/*
 * __wti_prepared_discover_restore_and_add_artifact_upd --
 *     In disaggregated storage, in follower mode, stable table cannot be modified, therefore a
 *     prepared update needs to be restored onto ingest table so that the follower node can then
 *     commit the prepared transaction. This function opens the ingest table and inserts the update
 *     restored from disk onto the ingest table.
 */
int
__wti_prepared_discover_restore_and_add_artifact_upd(WT_SESSION_IMPL *session,
  const char *stable_uri, WT_ITEM *key, WT_ITEM *value, WT_CELL_UNPACK_KV *unpack)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    WT_UPDATE *upd;
    size_t size;
    char *ingest_uri;
    bool dhandle_acquired, hold_chunk_lock;

    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL, NULL};

    cursor = NULL;
    ingest_uri = NULL;
    dhandle_acquired = false;
    hold_chunk_lock = false;
    conn = S2C(session);
    layered = NULL;

    /*
     * Find the matching layered table by walking the connection handle list and comparing the
     * stable URI from the layered handle. Acquire a reference to the layered dhandle so that its
     * ingest_uris/ingest_chunk_lock remain valid once we drop the handle list lock.
     *
     * Note: the caller's stable_uri is the dhandle name from the prepared-discover walk, which on
     * a follower is the stable table's URI with a trailing "/<checkpoint_name>" suffix (see
     * __wt_prepared_discover_filter_apply_handles). layered->stable_uri stores the bare URI, so
     * we match by prefix and require the next character (if any) to be a '/' checkpoint
     * separator.
     */
    WT_WITH_HANDLE_LIST_READ_LOCK(session, {
        for (dhandle = NULL;;) {
            WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
            if (dhandle == NULL)
                break;
            if (dhandle->type != WT_DHANDLE_TYPE_LAYERED || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
                continue;

            layered = (WT_LAYERED_TABLE *)dhandle;
            if (layered->stable_uri != NULL) {
                size_t sul = strlen(layered->stable_uri);
                if (strncmp(stable_uri, layered->stable_uri, sul) == 0 &&
                  (stable_uri[sul] == '\0' || stable_uri[sul] == '/')) {
                    WT_DHANDLE_ACQUIRE(dhandle);
                    dhandle_acquired = true;
                    break;
                }
            }
        }
    });

    if (!dhandle_acquired)
        WT_RET_MSG(session, WT_NOTFOUND,
          "unable to find matching layered table for stable URI \"%s\" while restoring a "
          "prepared update",
          stable_uri);

    layered = (WT_LAYERED_TABLE *)dhandle;

    /*
     * Target the newest (primary) ingest chunk for the restored update.
     *
     * This function runs on the follower during prepared-update discovery, which scans the stable
     * table's checkpoint image for prepared artifacts and reinstates them on the ingest side so
     * the follower can later resolve them. In that window the follower has not started handling
     * writes, so rollover has not yet produced secondary ingest chunks and the primary is the
     * only ingest chunk. Still, the code below is written to be correct if that ever changes:
     *
     *   - Layered readers iterate every ingest chunk, so the restored update is visible regardless
     *     of which chunk physically holds it.
     *   - __wt_row_modify below runs under the btree opened via __wt_open_cursor(ingest_uri),
     *     which registers the resulting WT_TXN_OP against that specific ingest btree and
     *     increments its ingest_gc_pending_ops. The counter stays non-zero until the prepared
     *     transaction resolves, so the ingest chunk server's fast obsolete-for-drop path cannot
     *     retire the chunk while the prepared update is outstanding.
     *
     * Rollover (__clayered_rollover_ingest) and drop-oldest (__layered_ingest_chunk_drop_oldest)
     * both replace layered->ingest_uris[] under ingest_chunk_lock. Read the URI while holding
     * that lock so that n_ingest_uris and ingest_uris[] stay consistent; the strdup'd copy then
     * outlives the lock.
     */
    __wt_spin_lock(session, &layered->ingest_chunk_lock);
    hold_chunk_lock = true;
    WT_ASSERT_ALWAYS(session, layered->n_ingest_uris > 0,
      "layered table \"%s\" has no ingest URIs when restoring a prepared update",
      layered->iface.name);
    WT_ERR(__wt_strdup(session, WT_LAYERED_PRIMARY_INGEST_URI(layered), &ingest_uri));
    __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    hold_chunk_lock = false;

    /*
     * Release the layered dhandle: we have a standalone copy of the URI and __wt_open_cursor will
     * acquire its own references on the ingest btree below.
     */
    WT_DHANDLE_RELEASE(dhandle);
    dhandle_acquired = false;
    layered = NULL;

    WT_ERR(__wt_open_cursor(session, ingest_uri, NULL, cfg, &cursor));

    cbt = (WT_CURSOR_BTREE *)cursor;
    WT_ERR(__prepare_discover_alloc_upd(session, value, unpack, &upd, &size));

    WT_WITH_PAGE_INDEX(session, ret = __wt_row_search(cbt, key, true, NULL, false, NULL));
    WT_ERR(ret);
    WT_ERR(__wt_row_modify(cbt, key, NULL, &upd, WT_UPDATE_INVALID, true, true));
    WT_ERR(__wti_prepared_discover_add_artifact_upd(session, upd, key));

err:
    if (hold_chunk_lock)
        __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));
    if (dhandle_acquired)
        WT_DHANDLE_RELEASE(dhandle);
    __wt_free(session, ingest_uri);
    return (ret);
}
