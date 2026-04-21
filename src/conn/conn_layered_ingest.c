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
 * __layered_assert_tombstone_has_value_on_stable_btree --
 *     Assert that a value exists on the stable btree before moving a tombstone intended to delete
 *     it.
 */
static WT_INLINE void
__layered_assert_tombstone_has_value_on_stable_btree(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *last_upd)
{
    bool has_value;

    if (last_upd->type != WT_UPDATE_TOMBSTONE)
        return;

    /*
     * If the last update is a tombstone, ensure that there is a corresponding value on the stable
     * table that it deletes.
     */
    if (cbt->compare != 0)
        /* No on-page value to check; rely solely on visibility. */
        has_value = false;
    else {
        WT_ASSERT_ALWAYS(session, cbt->ins == NULL,
          "The stable btree should not contain inserts prior to draining");
        WT_UPDATE *upd = NULL;
        if (cbt->ref->page->modify != NULL && cbt->ref->page->modify->mod_row_update != NULL)
            upd = cbt->ref->page->modify->mod_row_update[cbt->slot];

        if (upd != NULL) {
            WT_ASSERT_ALWAYS(session, upd->txnid != WT_TXN_ABORTED,
              "The stable btree should not contain aborted updates prior to draining");
            has_value = upd->type != WT_UPDATE_TOMBSTONE;
        } else {
            WT_TIME_WINDOW tw;
            bool tw_found = __wt_read_cell_time_window(cbt, &tw);
            has_value = tw_found && !WT_TIME_WINDOW_HAS_STOP(&tw);
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
 *     Move the updates of a key to the stable table
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

    /* Search the page. */
    WT_WITH_PAGE_INDEX(session, ret = __wt_row_search(cbt, key, true, NULL, false, NULL));
    WT_ERR(ret);

    __layered_assert_tombstone_has_value_on_stable_btree(session, cbt, last_upd);

    /* Apply the modification. */
    WT_ERR(__wt_row_modify(cbt, key, NULL, &upds, WT_UPDATE_INVALID, false, false));

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
    WT_ASSERT(session, WT_SUFFIX_MATCH(uri, ".wt_ingest"));

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
 * __layered_copy_ingest_table --
 *     Moving all the data from a single ingest table to the corresponding stable table
 */
static int
__layered_copy_ingest_table(WT_SESSION_IMPL *session, WT_LAYERED_URI_DESC *entry)
{
    WT_CURSOR *stable_cursor, *version_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(tmp_key);
    WT_DECL_ITEM(value);
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    WT_UPDATE *last_upd, *prev_upd, *tombstone, *upd, *upds;
    wt_timestamp_t last_checkpoint_timestamp;
    wt_timestamp_t durable_start_ts, durable_stop_ts, start_prepare_ts, start_ts, stop_prepare_ts,
      stop_ts;
    uint64_t start_prepared_id, start_txn, stop_prepared_id, stop_txn;
    uint8_t flags, location, prepare, type;
    int cmp;
    char buf[256], buf2[64];
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL, NULL, NULL};
    bool has_stop, is_prepare_rollback;

    stable_cursor = version_cursor = NULL;
    last_upd = prev_upd = tombstone = upd = upds = NULL;
    layered = NULL;

    last_checkpoint_timestamp = __wt_atomic_load_uint64_acquire(
      &S2C(session)->disaggregated_storage.last_checkpoint_timestamp);

    WT_ASSERT(session, entry->pinned_dhandle != NULL);
    WT_WITH_DHANDLE(
      session, entry->pinned_dhandle, do {
          const char *ingest_uri;
          const char *stable_uri;
          const char *stable_cfg[4];

          layered = (WT_LAYERED_TABLE *)session->dhandle;
          WT_ASSERT(session, layered->stable_uri != NULL && layered->n_ingest_uris > 0);
          ingest_uri = WT_LAYERED_PRIMARY_INGEST_URI(layered);
          stable_uri = layered->stable_uri;

          stable_cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
          stable_cfg[1] = "overwrite";
          stable_cfg[2] = NULL;
          stable_cfg[3] = NULL;

          WT_ERR(__wt_open_cursor(session, stable_uri, NULL, stable_cfg, &stable_cursor));
          cbt = (WT_CURSOR_BTREE *)stable_cursor;

          if (last_checkpoint_timestamp != WT_TS_NONE)
              WT_ERR(__wt_snprintf(
                buf2, sizeof(buf2), "start_timestamp=%" PRIx64 "", last_checkpoint_timestamp));
          else
              buf2[0] = '\0';

          WT_ERR(__wt_snprintf(buf, sizeof(buf),
            "debug=(dump_version=(enabled=true,raw_key_value=true,visible_only=true,"
            "timestamp_order=true,cross_key=true,%s))",
            buf2));
          cfg[1] = buf;

          WT_ERR(__wt_open_cursor(session, ingest_uri, NULL, cfg, &version_cursor));
      } while (0));

    WT_ERR(__wt_scr_alloc(session, 0, &key));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp_key));
    WT_ERR(__wt_scr_alloc(session, 0, &value));

    for (;;) {
        tombstone = upd = NULL;
        WT_ERR_NOTFOUND_OK(version_cursor->next(version_cursor), true);
        if (ret == WT_NOTFOUND) {
            if (key->size > 0 && upds != NULL) {
                WT_WITH_DHANDLE(session, cbt->dhandle,
                  ret = __layered_move_updates(session, cbt, key, upds, last_upd));
                WT_ERR(ret);
                upds = NULL;
            } else
                ret = 0;
            break;
        }

        WT_ERR(version_cursor->get_key(version_cursor, tmp_key));
        WT_ERR(__wt_compare(session, CUR2BT(cbt)->collator, key, tmp_key, &cmp));
        if (cmp != 0) {
            /*
             * Ensure keys returned are in correctly sorted order. Only perform this check when key
             * has been initialized.
             */
            WT_ASSERT(session, key->size == 0 || cmp <= 0);

            if (upds != NULL) {
                WT_WITH_DHANDLE(session, cbt->dhandle,
                  ret = __layered_move_updates(session, cbt, key, upds, last_upd));
                WT_ERR(ret);
            }

            upds = NULL;
            prev_upd = NULL;
            WT_ERR(__wt_buf_set(session, key, tmp_key->data, tmp_key->size));
        }

        WT_ERR(version_cursor->get_value(version_cursor, &start_txn, &start_ts, &durable_start_ts,
          &start_prepare_ts, &start_prepared_id, &stop_txn, &stop_ts, &durable_stop_ts,
          &stop_prepare_ts, &stop_prepared_id, &type, &prepare, &flags, &location, value));

        has_stop = stop_txn != WT_TXN_MAX;
        is_prepare_rollback = start_txn == WT_TXN_ABORTED;
        /* FIXME-WT-16744 Remove this assertion when prepared update on the stable table are
         * resolved during draining. */
        WT_ASSERT(session, !is_prepare_rollback);
        /* We assume the updates returned will be in timestamp order. */
        if (prev_upd != NULL) {
            WT_ASSERT(session,
              stop_txn <= prev_upd->txnid && stop_ts <= prev_upd->upd_start_ts &&
                durable_stop_ts <= prev_upd->upd_durable_ts);
            WT_ASSERT(session,
              start_txn <= prev_upd->txnid && start_ts <= prev_upd->upd_start_ts &&
                durable_start_ts <= prev_upd->upd_durable_ts);
            if (stop_txn != prev_upd->txnid || stop_ts != prev_upd->upd_start_ts ||
              durable_stop_ts != prev_upd->upd_durable_ts)
                WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
        } else if (has_stop)
            WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));

        /*
         * It is possible to see a full value that is smaller than or equal to the last checkpoint
         * timestamp with a tombstone that is larger than the last checkpoint timestamp. Ignore the
         * update in this case.
         */
        if (durable_start_ts > last_checkpoint_timestamp) {
            /*
             * FIXME-WT-14732: this is an ugly layering violation. But I can't think of a better way
             * now.
             */
            if (__wt_clayered_deleted(value)) {
                /*
                 * If we use tombstone value, we should never see a real tombstone on the ingest
                 * table.
                 */
                WT_ASSERT(session, tombstone == NULL);
                WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
            } else
                WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
            upd->prepare_ts = start_prepare_ts;
            upd->prepared_id = start_prepared_id;
            if (is_prepare_rollback) {
                /*
                 * WT_UPDATE stores these in a union, so they share the same underlying slots as
                 * durable/start timestamps. We assign via rollback names here for readability.
                 */
                upd->txnid = WT_TXN_ABORTED;
                upd->upd_rollback_ts = durable_start_ts;
                upd->upd_saved_txnid = start_ts;
            } else {
                upd->txnid = start_txn;
                upd->upd_start_ts = start_ts;
                upd->upd_durable_ts = durable_start_ts;
            }
            /* This is for debugging purpose and it is not checked in the code. */
            F_SET(upd, WT_UPDATE_RESTORED_FROM_INGEST);
            last_upd = upd;
        } else {
            WT_ASSERT(session, tombstone != NULL);
            last_upd = tombstone;
        }

        /*
         * FIXME-WT-14732: we can simplify the algorithm if we don't use real tombstones on the
         * ingest table.
         */
        if (tombstone != NULL) {
            tombstone->txnid = stop_txn;
            tombstone->upd_start_ts = stop_ts;
            tombstone->upd_durable_ts = durable_stop_ts;
            tombstone->prepare_ts = stop_prepare_ts;
            tombstone->prepared_id = stop_prepared_id;
            tombstone->next = upd;
            /* This is for debugging purpose and it is not checked in the code. */
            F_SET(tombstone, WT_UPDATE_RESTORED_FROM_INGEST);

            WT_ASSERT(session, tombstone->upd_durable_ts > last_checkpoint_timestamp);

            if (prev_upd != NULL)
                prev_upd->next = tombstone;
            else
                upds = tombstone;

            prev_upd = upd;
            tombstone = NULL;
            upd = NULL;
        } else {
            if (prev_upd != NULL)
                prev_upd->next = upd;
            else
                upds = upd;

            prev_upd = upd;
            upd = NULL;
        }
    }

err:
    if (tombstone != NULL)
        __wt_free(session, tombstone);
    if (upd != NULL)
        __wt_free(session, upd);
    if (upds != NULL)
        __wt_free_update_list(session, &upds);
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &tmp_key);
    __wt_scr_free(session, &value);
    if (version_cursor != NULL)
        WT_TRET(version_cursor->close(version_cursor));
    if (stable_cursor != NULL)
        WT_TRET(stable_cursor->close(stable_cursor));
    return (ret);
}

/*
 * __layered_drain_worker_run --
 *     Run function for drain workers.
 */
static int
__layered_drain_worker_run(WT_SESSION_IMPL *session, WT_THREAD *ctx)
{
    WT_DECL_RET;
    WT_CONNECTION_IMPL *conn = S2C(session);
    WT_UNUSED(ctx);
    __wt_spin_lock(session, &conn->layered_drain_data.queue_lock);
    /* If the queue is empty we are done. */
    if (TAILQ_EMPTY(&conn->layered_drain_data.work_queue)) {
        __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
        return (0);
    }

    WT_LAYERED_DRAIN_ENTRY *work_item = TAILQ_FIRST(&conn->layered_drain_data.work_queue);
    WT_ASSERT(session, work_item != NULL);
    TAILQ_REMOVE(&conn->layered_drain_data.work_queue, work_item, q);
    __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
    WT_ERR_MSG_CHK(session, __layered_copy_ingest_table(session, work_item->entryp),
      "Failed to copy ingest table \"%s\" to stable table \"%s\"", work_item->entryp->ingest_uri,
      work_item->entryp->stable_uri);
    WT_ERR_MSG_CHK(session, __layered_clear_ingest_table(session, work_item->entryp->ingest_uri),
      "Failed to clear ingest table \"%s\"", work_item->entryp->ingest_uri);

#ifdef HAVE_DIAGNOSTIC
    WT_ERR(__layered_assert_ingest_table_empty(session, work_item->entryp->ingest_uri));
#endif

    WT_ASSERT(session, work_item->entryp->pinned_dhandle != NULL);
    WT_WITH_DHANDLE(session, work_item->entryp->pinned_dhandle, {
        work_item->entryp->pinned_dhandle = NULL;
        __wt_cursor_dhandle_decr_use(session);
    });

err:
    __wt_free(session, work_item->layered_uri_alloc);
    __wt_free(session, work_item->ingest_uri_alloc);
    __wt_free(session, work_item->stable_uri_alloc);
    __wt_free(session, work_item);
    return (ret);
}

/*
 * __layered_drain_worker_check --
 *     Check function for drain workers.
 */
static bool
__layered_drain_worker_check(WT_SESSION_IMPL *session)
{
    return (__wt_atomic_load_bool_relaxed(&S2C(session)->layered_drain_data.running));
}

/*
 * __layered_drain_clear_work_queue --
 *     Clear the work queue for ingest table drain.
 */
static void
__layered_drain_clear_work_queue(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn = S2C(session);
    __wt_spin_lock(session, &conn->layered_drain_data.queue_lock);
    if (!TAILQ_EMPTY(&conn->layered_drain_data.work_queue)) {
        WT_LAYERED_DRAIN_ENTRY *work_item = NULL, *work_item_tmp = NULL;
        TAILQ_FOREACH_SAFE(work_item, &conn->layered_drain_data.work_queue, q, work_item_tmp)
        {
            TAILQ_REMOVE(&conn->layered_drain_data.work_queue, work_item, q);
            __wt_free(session, work_item->layered_uri_alloc);
            __wt_free(session, work_item->ingest_uri_alloc);
            __wt_free(session, work_item->stable_uri_alloc);
            __wt_free(session, work_item);
        }
    }
    WT_ASSERT_ALWAYS(session, TAILQ_EMPTY(&conn->layered_drain_data.work_queue),
      "Layered drain work queue failed to drain");
    __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
    __wt_spin_destroy(session, &conn->layered_drain_data.queue_lock);
}

/*
 * __wti_layered_drain_ingest_tables --
 *     Moving all the data from the ingest tables to the stable tables
 */
int
__wti_layered_drain_ingest_tables(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    size_t i, layered_uris_alloc, layered_uris_count;
    char **layered_uris;
    bool empty, group_created;

    conn = S2C(session);
    group_created = false;
    layered_uris = NULL;
    layered_uris_alloc = layered_uris_count = 0;

    /* Initialize the work queue. */
    TAILQ_INIT(&conn->layered_drain_data.work_queue);
    WT_RET(__wt_spin_init(
      session, &conn->layered_drain_data.queue_lock, "layered drain work queue lock"));

    __wt_atomic_store_bool(&conn->layered_drain_data.running, true);

    bool multithreaded = conn->layered_drain_data.thread_count > 1;

    /*
     * Create the thread group. The application thread is also a drain thread so the configured
     * thread count needs to be greater than 1 for this to be meaningful. We still lock and queue
     * work for single threaded mode, as such single threaded is only recommended for testing.
     */
    if (multithreaded) {
        WT_ERR(__wt_thread_group_create(session, &conn->layered_drain_data.threads, "disagg-drain",
          conn->layered_drain_data.thread_count - 1, conn->layered_drain_data.thread_count - 1,
          WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL, __layered_drain_worker_check,
          __layered_drain_worker_run, NULL));
        group_created = true;
    }

    /* Collect layered URIs from the connection handle list. */
    WT_WITH_HANDLE_LIST_READ_LOCK(session, {
        for (dhandle = NULL;;) {
            WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
            if (dhandle == NULL)
                break;
            if (dhandle->type != WT_DHANDLE_TYPE_LAYERED || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
                continue;

            if (layered_uris_count == layered_uris_alloc) {
                size_t new_alloc = layered_uris_alloc == 0 ? 8 : layered_uris_alloc * 2;
                WT_ERR(__wt_realloc_def(session, &layered_uris_alloc, new_alloc, &layered_uris));
                layered_uris_alloc = new_alloc;
            }
            WT_ERR(__wt_strdup(session, dhandle->name, &layered_uris[layered_uris_count++]));
        }
    });

    /* FIXME-WT-14735: skip empty ingest tables. */
    for (i = 0; i < layered_uris_count; i++) {
        WT_LAYERED_DRAIN_ENTRY *work_item;
        WT_ERR(__wt_calloc_one(session, &work_item));

        work_item->entryp = &work_item->entry;
        work_item->layered_uri_alloc = layered_uris[i];
        layered_uris[i] = NULL;

        WT_CLEAR(work_item->entry);
        work_item->entry.layered_uri = work_item->layered_uri_alloc;

        /*
         * Mark the layered table in use, we don't want it to be closed between now and when the
         * drain takes place.
         */
        WT_ERR(__wt_cursor_uri_incr_use(
          session, work_item->entry.layered_uri, &work_item->entry.pinned_dhandle));

        /* Populate URIs from the pinned layered handle. */
        {
            const char *ingest_uri;
            const char *stable_uri;
            uint32_t ingest_id;

            WT_WITH_DHANDLE(session, work_item->entry.pinned_dhandle, {
                layered = (WT_LAYERED_TABLE *)session->dhandle;
                WT_ASSERT(session, layered->n_ingest_uris > 0 && layered->stable_uri != NULL);
                ingest_id = WT_LAYERED_PRIMARY_INGEST_BTREE_ID(layered);
                ingest_uri = WT_LAYERED_PRIMARY_INGEST_URI(layered);
                stable_uri = layered->stable_uri;
            });
            WT_ERR(__wt_strdup(session, ingest_uri, &work_item->ingest_uri_alloc));
            WT_ERR(__wt_strdup(session, stable_uri, &work_item->stable_uri_alloc));
            work_item->entry.ingest_id = ingest_id;
        }
        work_item->entry.ingest_uri = work_item->ingest_uri_alloc;
        work_item->entry.stable_uri = work_item->stable_uri_alloc;

        __wt_spin_lock(session, &conn->layered_drain_data.queue_lock);
        TAILQ_INSERT_HEAD(&conn->layered_drain_data.work_queue, work_item, q);
        __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
    }

    /*
     * We can be lazy here and use the current thread as a worker thread. Then once this loop exits
     * we can kill our thread group.
     */
    while (true) {
        __wt_spin_lock(session, &conn->layered_drain_data.queue_lock);
        empty = TAILQ_EMPTY(&conn->layered_drain_data.work_queue);
        __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
        if (empty) {
            /*
             * Notify the other threads to exit. Relaxed is okay here as the worker threads will
             * observe this change eventually.
             */
            __wt_atomic_store_bool_relaxed(&conn->layered_drain_data.running, false);
            break;
        }
        WT_ERR(__layered_drain_worker_run(session, NULL));
    }

err:
    if (layered_uris != NULL) {
        for (i = 0; i < layered_uris_count; i++)
            __wt_free(session, layered_uris[i]);
        __wt_free(session, layered_uris);
    }
    /* Let any running threads finish up. */
    if (group_created) {
        __wt_cond_signal(session, conn->layered_drain_data.threads.wait_cond);
        __wt_writelock(session, &conn->layered_drain_data.threads.lock);
        WT_TRET(__wt_thread_group_destroy(session, &conn->layered_drain_data.threads));
    }
    /* Cleanup and release resources. */
    __layered_drain_clear_work_queue(session);
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
    wt_timestamp_t prune_timestamp;
    int64_t ckpt_inuse, last_ckpt;
    uint32_t i;
    int32_t stable_dhandle_inuse;

    layered_table = NULL;
    prune_timestamp = WT_TS_NONE;

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     */
    WT_RET_NOTFOUND_OK(__wt_session_get_dhandle(session, layered_uri, NULL, NULL, 0));
    if (ret == WT_NOTFOUND) {
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

    /*
     * If we reached the newest stable checkpoint without finding any stable checkpoint currently in
     * use, we can safely advance the ingest prune timestamp to the current checkpoint timestamp.
     *
     * Note: The layered table handle itself may be in use (writes continuing on newer ingest
     * chunks), but that should not prevent pruning older ingest chunks that are fully covered by
     * durable stable checkpoints.
     */
    if (ckpt_inuse == last_ckpt)
        prune_timestamp = checkpoint_timestamp;

    if (ckpt_inuse == layered_table->last_ckpt_inuse) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC prune: \"%s\" cannot advance prune_timestamp yet: stable checkpoint %" PRId64
          " still matches last_ckpt_inuse (a session likely holds that checkpoint open); "
          "checkpoint_timestamp=%" PRIu64 " last_ckpt=%" PRId64,
          layered_table->iface.name, ckpt_inuse, checkpoint_timestamp, last_ckpt);
        ret = 0;
        goto err;
    }

    /*
     * Set the prune timestamp in the btree if it is open, typically it is. However, it's possible
     * that it hasn't been opened yet. In that case, we need to skip updating its timestamp for
     * pruning, and we'll get another chance to update the prune timestamp at the next checkpoint.
     */
    for (i = 0; i < layered_table->n_ingest_uris; i++) {
        WT_ERR_NOTFOUND_OK(
          __wt_session_get_dhandle(session, layered_table->ingest_uris[i], NULL, NULL, 0), true);
        if (ret == WT_NOTFOUND) {
            __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
              "GC %s: Handle not found for ingest table uri: %s", layered_table->iface.name,
              layered_table->ingest_uris[i]);
            ret = 0;
            continue;
        }

        btree = (WT_BTREE *)session->dhandle->handle;

        if (prune_timestamp != WT_TS_NONE) {
            uint64_t btree_prune_timestamp =
              __wt_atomic_load_uint64_relaxed(&btree->prune_timestamp);
            WT_ASSERT(session, prune_timestamp >= btree_prune_timestamp);

            __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
              "GC %s: update prune timestamp from %" PRIu64 " to %" PRIu64
              " and checkpoint in use from %" PRId64 " to %" PRId64,
              layered_table->iface.name, btree_prune_timestamp, prune_timestamp,
              layered_table->last_ckpt_inuse, ckpt_inuse);

            /*
             * The prune timestamp should be monotonically increasing. It is fine for the user to
             * read the obsolete value. Therefore, no synchronization is required.
             */
            __wt_atomic_store_uint64_relaxed(&btree->prune_timestamp, prune_timestamp);
        }

        WT_ERR(__wt_session_release_dhandle(session));
    }

    if (prune_timestamp != WT_TS_NONE)
        layered_table->last_ckpt_inuse = ckpt_inuse;

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
    WT_DATA_HANDLE *dhandle;
    WT_DECL_ITEM(layered_table_uri_buf);
    WT_DECL_ITEM(uri_at_checkpoint_buf);
    WT_DECL_RET;
    size_t i, layered_uris_alloc, layered_uris_count;
    char **layered_uris;

    conn = S2C(session);
    WT_RET(__wt_scr_alloc(session, 0, &layered_table_uri_buf));
    WT_RET(__wt_scr_alloc(session, 0, &uri_at_checkpoint_buf));
    layered_uris = NULL;
    layered_uris_alloc = layered_uris_count = 0;

    /* Collect layered URIs from the connection handle list. */
    WT_WITH_HANDLE_LIST_READ_LOCK(session, {
        for (dhandle = NULL;;) {
            WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
            if (dhandle == NULL)
                break;
            if (dhandle->type != WT_DHANDLE_TYPE_LAYERED || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
                continue;

            if (layered_uris_count == layered_uris_alloc) {
                size_t new_alloc = layered_uris_alloc == 0 ? 8 : layered_uris_alloc * 2;
                WT_ERR(__wt_realloc_def(session, &layered_uris_alloc, new_alloc, &layered_uris));
                layered_uris_alloc = new_alloc;
            }
            WT_ERR(__wt_strdup(session, dhandle->name, &layered_uris[layered_uris_count++]));
        }
    });

    for (i = 0; i < layered_uris_count; i++) {
        WT_ERR(__wt_buf_setstr(session, layered_table_uri_buf, layered_uris[i]));
        WT_ERR(__layered_update_ingest_table_prune_timestamp(
          session, layered_table_uri_buf->data, checkpoint_timestamp, uri_at_checkpoint_buf));
    }

err:
    if (ret != 0)
        __wt_verbose_level(
          session, WT_VERB_LAYERED, WT_VERBOSE_ERROR, "GC ingest tables prune failed by: %d", ret);

    if (layered_uris != NULL) {
        for (i = 0; i < layered_uris_count; i++)
            __wt_free(session, layered_uris[i]);
        __wt_free(session, layered_uris);
    }
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
    WT_DECL_RET;
    int64_t order_from_name;
    int scanf_ret;
    const char *checkpoint_name;

    *ckpt_order = 0;
    checkpoint_name = NULL;

    /* Pull up the last checkpoint for this URI. It could return WT_NOTFOUND. */
    WT_RET(__wt_meta_checkpoint_last_name(session, shared_uri, &checkpoint_name, ckpt_order, NULL));

    /* Sanity check: we make sure that the name returned matches the order number. */
    scanf_ret = sscanf(checkpoint_name, WT_CHECKPOINT ".%" PRId64, &order_from_name);
    if (scanf_ret != 1)
        WT_ERR_MSG(session, EINVAL,
          "shared metadata checkpoint unknown format: %s, scan returns %d", checkpoint_name,
          scanf_ret);

    /* These should always be the same. */
    WT_ASSERT(session, *ckpt_order == order_from_name);

err:
    __wt_free(session, checkpoint_name);
    return (ret);
}

/*
 * __layered_ingest_chunk_server_run_chk --
 *     Predicate for the layered ingest chunk server condition wait.
 */
static bool
__layered_ingest_chunk_server_run_chk(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    return (__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running));
}

/*
 * __layered_ingest_upd_chain_scan --
 *     Walk an update chain: track the largest start/prepare timestamp and whether every
 *     non-aborted update is globally visible.
 */
static void
__layered_ingest_upd_chain_scan(
  WT_SESSION_IMPL *session, WT_UPDATE *upd, wt_timestamp_t *max_tsp, bool *all_visible_allp)
{
    wt_timestamp_t max_ts;
    uint64_t txnid;

    max_ts = *max_tsp;
    for (; upd != NULL; upd = upd->next) {
        txnid = __wt_atomic_load_uint64_v_acquire(&upd->txnid);
        if (txnid == WT_TXN_ABORTED)
            continue;

        if (!__wt_txn_upd_visible_all(session, upd))
            *all_visible_allp = false;

        switch (upd->prepare_state) {
        case WT_PREPARE_INPROGRESS:
        case WT_PREPARE_LOCKED:
            if (upd->prepare_ts > max_ts)
                max_ts = upd->prepare_ts;
            break;
        default:
            if (upd->upd_start_ts != WT_TS_NONE && upd->upd_start_ts > max_ts)
                max_ts = upd->upd_start_ts;
            break;
        }
    }
    *max_tsp = max_ts;
}

/*
 * __layered_ingest_row_leaf_scan_upd --
 *     Scan row-store leaf update / insert chains for prune/GC decisions.
 */
static void
__layered_ingest_row_leaf_scan_upd(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t *max_tsp, bool *all_visible_allp)
{
    WT_INSERT *ins;
    WT_INSERT_HEAD *head;
    WT_ROW *rip;
    WT_UPDATE *upd;
    uint32_t i;
    wt_timestamp_t m;

    m = *max_tsp;
    if ((head = WT_ROW_INSERT_SMALLEST(page)) != NULL)
        WT_SKIP_FOREACH (ins, head)
            if (ins->upd != NULL)
                __layered_ingest_upd_chain_scan(session, ins->upd, &m, all_visible_allp);
    WT_ROW_FOREACH (page, rip, i)
    {
        if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
            __layered_ingest_upd_chain_scan(session, upd, &m, all_visible_allp);
        if ((head = WT_ROW_INSERT(page, rip)) != NULL)
            WT_SKIP_FOREACH (ins, head)
                if (ins->upd != NULL)
                    __layered_ingest_upd_chain_scan(session, ins->upd, &m, all_visible_allp);
    }
    *max_tsp = m;
}

/*
 * __layered_ingest_col_var_leaf_scan_upd --
 *     Scan column-store variable-length leaf update / append chains for prune/GC decisions.
 */
static void
__layered_ingest_col_var_leaf_scan_upd(
  WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t *max_tsp, bool *all_visible_allp)
{
    WT_COL *cip;
    WT_INSERT *ins;
    WT_INSERT_HEAD *head;
    uint32_t i;
    wt_timestamp_t m;

    m = *max_tsp;
    WT_COL_FOREACH (page, cip, i)
    {
        if ((head = WT_COL_UPDATE(page, cip)) != NULL)
            WT_SKIP_FOREACH (ins, head)
                if (ins->upd != NULL)
                    __layered_ingest_upd_chain_scan(session, ins->upd, &m, all_visible_allp);
    }
    if ((head = WT_COL_APPEND(page)) != NULL)
        WT_SKIP_FOREACH (ins, head)
            if (ins->upd != NULL)
                __layered_ingest_upd_chain_scan(session, ins->upd, &m, all_visible_allp);
    *max_tsp = m;
}

/*
 * __layered_ingest_btree_obsolete_for_drop --
 *     Return whether every modified leaf in the ingest btree has no commit timestamp newer than the
 *     btree prune timestamp (all content is at or before the prune cutoff).
 */
static int
__layered_ingest_btree_obsolete_for_drop(WT_SESSION_IMPL *session, WT_BTREE *btree, bool *obsolete)
{
    WT_DECL_RET;
    WT_PAGE_MODIFY *mod;
    WT_REF *ref;
    wt_timestamp_t prune_ts;
    WT_CONNECTION_IMPL *conn;
    bool ingest_gc_block_logged;

    *obsolete = false;

    conn = S2C(session);
    prune_ts = __wt_atomic_load_uint64_acquire(&btree->prune_timestamp);
    if (prune_ts != WT_TS_NONE)
        conn->layered_ingest_chunk_server.ingest_gc_diag_suppress = 0;
    if (prune_ts == WT_TS_NONE) {
        uint32_t n;

        /*
         * This is expected until checkpoint pickup installs prune timestamps on ingest btrees;
         * throttle to avoid flooding logs when verbose=[layered:2] is enabled.
         */
        n = ++conn->layered_ingest_chunk_server.ingest_gc_diag_suppress;
        if (n == 1 || n % 25 == 0)
            __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
              "ingest GC: \"%s\" cannot mark oldest chunk obsolete: btree prune_timestamp is unset "
              "(follower ingest prune runs after checkpoint metadata pickup advances stable "
              "checkpoint handles)",
              btree->dhandle->name);
        return (0);
    }

    /* Root not instantiated yet; do not treat as obsolete. */
    if (btree->root.page == NULL) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC: \"%s\" cannot mark oldest chunk obsolete: btree root page not instantiated yet",
          btree->dhandle->name);
        return (0);
    }

    /*
     * Cheap obsolete check: prepare/commit publish max timestamps per btree and pair them with a
     * pending op count. If no ops are in flight and the aggregate is at or before the prune cutoff,
     * the chunk cannot contain newer committed timestamps than prune_ts.
     */
    if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT)) {
        int32_t pending1, pending2;
        uint64_t tracked;
        bool never_modified;

        pending1 = __wt_atomic_load_int32_relaxed(&btree->ingest_gc_pending_ops);
        tracked = __wt_atomic_load_uint64_acquire((uint64_t *)&btree->ingest_gc_max_timestamp);
        pending2 = __wt_atomic_load_int32_relaxed(&btree->ingest_gc_pending_ops);
        never_modified = !btree->modified;
        if (pending1 == pending2) {
            if (pending1 == 0 && tracked != WT_TS_NONE && tracked <= prune_ts) {
                *obsolete = true;
                return (0);
            }
            if (pending1 == 0 && tracked > prune_ts) {
                *obsolete = false;
                return (0);
            }
            if (pending1 == 0 && tracked == WT_TS_NONE && never_modified) {
                *obsolete = true;
                return (0);
            }
        }
    }

    ingest_gc_block_logged = false;
    ref = NULL;
    while (
      (ret = __wt_tree_walk(session, &ref,
         WT_READ_CACHE | WT_READ_INTERNAL_OP | WT_READ_VISIBLE_ALL | WT_READ_WONT_NEED)) == 0 &&
      ref != NULL) {
        /*
         * Shutdown wants this background thread to exit promptly. If we are asked to stop while
         * walking the tree, bail out early (treat as "not obsolete" so we don't drop).
         */
        if (!__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running)) {
            __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
              "ingest GC: \"%s\" obsolete walk interrupted: ingest chunk server shutting down",
              btree->dhandle->name);
            WT_ERR(__wt_page_release(session, ref, 0));
            ref = NULL;
            return (0);
        }
        if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
            WT_ERR(__wt_page_release(session, ref, 0));
            ref = NULL;
            continue;
        }
        if (__wt_page_is_modified(ref->page)) {
            wt_timestamp_t chain_max, effective;
            bool all_visible_all;

            mod = __wt_tsan_suppress_load_wt_page_modify_ptr(&ref->page->modify);
            /*
             * Do not compare mod->newest_commit_timestamp to the prune timestamp: that field is an
             * approximate cache for eviction (see __wt_page_modify_update_timestamp) and tracks the
             * global newest_seen timestamp, not the newest commit timestamp on this page. Using it
             * here can stall ingest chunk GC indefinitely while still being conservative for
             * eviction. Use the max timestamp from update chains plus reconciliation bookkeeping.
             */
            all_visible_all = true;
            chain_max = WT_TS_NONE;
            if (ref->page->type == WT_PAGE_ROW_LEAF)
                __layered_ingest_row_leaf_scan_upd(session, ref->page, &chain_max, &all_visible_all);
            else if (ref->page->type == WT_PAGE_COL_VAR)
                __layered_ingest_col_var_leaf_scan_upd(
                  session, ref->page, &chain_max, &all_visible_all);
            else if (mod != NULL) {
                /*
                 * Rare page types: keep the timestamp heuristic and approximate liveness from
                 * eviction (update_txn vs last_running).
                 */
                chain_max = mod->newest_commit_timestamp;
                if (__wt_atomic_load_uint64_relaxed(&mod->update_txn) >=
                  __wt_atomic_load_uint64_v_relaxed(&conn->txn_global.last_running))
                    all_visible_all = false;
            }

            effective = chain_max;
            if (mod != NULL && mod->rec_max_timestamp > effective)
                effective = mod->rec_max_timestamp;

            if (!all_visible_all || effective > prune_ts) {
                if (!ingest_gc_block_logged) {
                    const char *reason;

                    ingest_gc_block_logged = true;
                    if (!all_visible_all && effective > prune_ts)
                        reason = "invisible or uncommitted updates and effective timestamp above prune";
                    else if (!all_visible_all)
                        reason = "invisible or uncommitted updates on dirty leaf";
                    else
                        reason = "effective timestamp (max of on-page chain and rec_max) above prune";
                    __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
                      "ingest GC: \"%s\" oldest chunk not obsolete: blocking %s dirty leaf "
                      "(prune_ts=%" PRIu64 " effective_ts=%" PRIu64 " chain_max_ts=%" PRIu64
                      " rec_max_ts=%" PRIu64 " all_updates_visible_all=%s; reason=%s)",
                      btree->dhandle->name, __wt_page_type_string(ref->page->type), prune_ts,
                      effective, chain_max,
                      mod != NULL ? mod->rec_max_timestamp : WT_TS_NONE,
                      all_visible_all ? "true" : "false", reason);
                }
                WT_ERR(__wt_page_release(session, ref, 0));
                ref = NULL;
                return (0);
            }
        }
        WT_ERR(__wt_page_release(session, ref, 0));
        ref = NULL;
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    *obsolete = true;

err:
    if (ref != NULL)
        WT_TRET(__wt_page_release(session, ref, 0));
    return (ret);
}

/*
 * __layered_ingest_chunk_drop_oldest --
 *     Remove the oldest ingest chunk from a layered table (follower only). Caller holds
 *     ingest_chunk_lock and must ensure at least two ingest URIs exist.
 */
static int
__layered_ingest_chunk_drop_oldest(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered)
{
    WT_DECL_ITEM(ingest_list);
    WT_DECL_ITEM(layered_update);
    WT_DECL_RET;
    WT_SESSION_IMPL *int_session;
    uint32_t *ids_new;
    uint32_t i, new_n;
    char *drop_uri, *layered_meta, *merged, **uris_new;
    const char *cfg[4];
    const char *drop_cfg[4];
    const char *layered_uri;

    int_session = NULL;
    ingest_list = layered_update = NULL;
    layered_meta = merged = NULL;
    uris_new = NULL;
    ids_new = NULL;
    drop_uri = NULL;

    layered_uri = layered->iface.name;
    WT_ASSERT(session, layered->n_ingest_uris >= 2);
    WT_ASSERT(session, WT_PREFIX_MATCH(layered_uri, "layered:"));

    WT_ERR(__wt_strdup(session, layered->ingest_uris[0], &drop_uri));

    /*
     * Dedicated internal session: the caller holds session->dhandle (the layered table). Reusing
     * the caller for schema/metadata would replace that handle while it is still logically held.
     */
    WT_ERR(__wt_open_internal_session(S2C(session), "ingest-chunk-drop", true,
      WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE, 0, &int_session));

    WT_ERR(__wt_metadata_search(int_session, layered_uri, &layered_meta));
    WT_ERR(__wt_scr_alloc(int_session, 0, &ingest_list));
    WT_ERR(__wt_scr_alloc(int_session, 0, &layered_update));

    new_n = layered->n_ingest_uris - 1;
    if (new_n == 1)
        WT_ERR(__wt_buf_fmt(int_session, layered_update, "ingest=\"%s\"", layered->ingest_uris[1]));
    else {
        WT_ERR(__wt_buf_fmt(int_session, ingest_list, "("));
        for (i = 1; i < layered->n_ingest_uris; i++)
            WT_ERR(__wt_buf_catfmt(
              int_session, ingest_list, "%s%s", i == 1 ? "" : ",", layered->ingest_uris[i]));
        WT_ERR(__wt_buf_catfmt(int_session, ingest_list, ")"));
        WT_ERR(__wt_buf_fmt(int_session, layered_update, "ingest=\"%.*s\"", (int)ingest_list->size,
          (const char *)ingest_list->data));
    }

    cfg[0] = layered_meta;
    cfg[1] = layered_update->data;
    cfg[2] = NULL;
    cfg[3] = NULL;
    WT_ERR(__wt_config_collapse(int_session, cfg, &merged));
    WT_WITH_SCHEMA_LOCK(int_session, ret = __wt_metadata_insert(int_session, layered_uri, merged));
    WT_ERR(ret);
    __wt_free(int_session, merged);
    merged = NULL;

    WT_WITH_TABLE_WRITE_LOCK(int_session, {
        ret = __wt_calloc(session, (size_t)new_n, sizeof(char *), &uris_new);
        if (ret == 0)
            ret = __wt_calloc(session, (size_t)new_n, sizeof(uint32_t), &ids_new);
        if (ret == 0) {
            for (i = 1; i < layered->n_ingest_uris; i++) {
                uris_new[i - 1] = layered->ingest_uris[i];
                ids_new[i - 1] = layered->ingest_btree_ids[i];
            }
            __wt_free(session, layered->ingest_uris);
            __wt_free(session, layered->ingest_btree_ids);
            layered->ingest_uris = uris_new;
            layered->ingest_btree_ids = ids_new;
            layered->n_ingest_uris = new_n;
            uris_new = NULL;
            ids_new = NULL;
        }
    });
    WT_ERR(ret);

    drop_cfg[0] = WT_CONFIG_BASE(int_session, WT_SESSION_drop);
    drop_cfg[1] = "force=true";
    drop_cfg[2] = NULL;
    drop_cfg[3] = NULL;
    WT_WITH_SCHEMA_LOCK(int_session,
      ret = __wt_schema_drop(int_session, drop_uri, drop_cfg, false));
    WT_ERR(ret);

    WT_WITHOUT_DHANDLE(int_session, ret = __wti_conn_dhandle_outdated(int_session, drop_uri));
    WT_ERR(ret);

    __wt_verbose_info(session, WT_VERB_LAYERED,
      "layered follower ingest GC: dropped oldest ingest chunk \"%s\" from \"%s\" (remaining=%u)",
      drop_uri, layered_uri, new_n);

err:
    __wt_free(session, drop_uri);
    if (int_session != NULL) {
        __wt_free(int_session, layered_meta);
        __wt_free(int_session, merged);
        __wt_free(session, uris_new);
        __wt_free(session, ids_new);
        __wt_scr_free(int_session, &ingest_list);
        __wt_scr_free(int_session, &layered_update);
        WT_TRET(__wt_schema_close_internal_session(session, int_session));
    } else {
        __wt_free(session, uris_new);
        __wt_free(session, ids_new);
    }
    return (ret);
}

/*
 * __layered_ingest_chunk_try_drop_obsolete_oldest --
 *     If the oldest ingest chunk for a layered table is obsolete, drop it.
 */
static int
__layered_ingest_chunk_try_drop_obsolete_oldest(WT_SESSION_IMPL *session, const char *layered_uri)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    char *oldest;
    bool obsolete;
    WT_CONNECTION_IMPL *conn;

    oldest = NULL;
    conn = S2C(session);

    /* If we are shutting down, don't start new work. */
    if (!__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running))
        return (0);

    ret = __wt_session_get_dhandle(session, layered_uri, NULL, NULL, 0);
    if (ret == WT_NOTFOUND)
        return (0);
    WT_ERR(ret);
    layered = (WT_LAYERED_TABLE *)session->dhandle;
    __wt_spin_lock(session, &layered->ingest_chunk_lock);
    if (layered->n_ingest_uris < 2) {
        __wt_spin_unlock(session, &layered->ingest_chunk_lock);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC: \"%s\" skip drop: fewer than two ingest URIs (nothing to retire as oldest)",
          layered_uri);
        WT_ERR(__wt_session_release_dhandle(session));
        return (0);
    }
    WT_ERR(__wt_strdup(session, layered->ingest_uris[0], &oldest));
    __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    WT_ERR(__wt_session_release_dhandle(session));

    if (!__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running)) {
        __wt_free(session, oldest);
        return (0);
    }

    ret = __wt_session_get_dhandle(session, oldest, NULL, NULL, 0);
    if (ret == WT_NOTFOUND) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC: \"%s\" skip drop: oldest ingest URI \"%s\" not found (metadata/list race?)",
          layered_uri, oldest);
        __wt_free(session, oldest);
        return (0);
    }
    WT_ERR(ret);
    btree = (WT_BTREE *)session->dhandle->handle;
    if (!F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT)) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC: \"%s\" skip drop: oldest ingest btree \"%s\" is not garbage-collect mode "
          "(expected only on follower ingest chunks)",
          layered_uri, oldest);
        WT_ERR(__wt_session_release_dhandle(session));
        __wt_free(session, oldest);
        return (0);
    }
    obsolete = false;
    WT_ERR(__layered_ingest_btree_obsolete_for_drop(session, btree, &obsolete));
    WT_ERR(__wt_session_release_dhandle(session));
    if (!obsolete) {
        __wt_free(session, oldest);
        return (0);
    }

    ret = __wt_session_get_dhandle(session, layered_uri, NULL, NULL, 0);
    if (ret == WT_NOTFOUND) {
        __wt_free(session, oldest);
        return (0);
    }
    WT_ERR(ret);
    layered = (WT_LAYERED_TABLE *)session->dhandle;
    __wt_spin_lock(session, &layered->ingest_chunk_lock);
    if (layered->n_ingest_uris < 2 || strcmp(layered->ingest_uris[0], oldest) != 0) {
        __wt_spin_unlock(session, &layered->ingest_chunk_lock);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
          "ingest GC: \"%s\" skip drop: ingest list changed under lock (expected \"%s\", "
          "n_ingest_uris=%u); retry on next server pass",
          layered_uri, oldest, layered->n_ingest_uris);
        WT_ERR(__wt_session_release_dhandle(session));
        __wt_free(session, oldest);
        return (0);
    }
    ret = __layered_ingest_chunk_drop_oldest(session, layered);
    __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    WT_ERR(__wt_session_release_dhandle(session));
    __wt_free(session, oldest);
    return (ret);

err:
    __wt_free(session, oldest);
    if (session->dhandle != NULL)
        WT_TRET(__wt_session_release_dhandle(session));
    return (ret);
}

/*
 * __layered_ingest_chunk_server_pass --
 *     One pass of the ingest chunk server: try to drop obsolete oldest ingest chunks.
 */
static int
__layered_ingest_chunk_server_pass(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    char **layered_uris;
    size_t alloc, i, layered_uris_count;

    conn = S2C(session);
    layered_uris = NULL;
    layered_uris_count = alloc = 0;

    if (conn->disagg_layered_leader)
        return (0);

    WT_WITH_HANDLE_LIST_READ_LOCK(session, {
        for (dhandle = NULL;;) {
            WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
            if (dhandle == NULL)
                break;
            if (dhandle->type != WT_DHANDLE_TYPE_LAYERED || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
                continue;
            layered = (WT_LAYERED_TABLE *)dhandle;
            if (layered->n_ingest_uris < 2)
                continue;
            if (layered_uris_count == alloc) {
                size_t new_alloc = alloc == 0 ? 8 : alloc * 2;

                ret = __wt_realloc_def(session, &alloc, new_alloc, &layered_uris);
                if (ret != 0)
                    break;
                alloc = new_alloc;
            }
            ret = __wt_strdup(session, layered->iface.name, &layered_uris[layered_uris_count++]);
            if (ret != 0)
                break;
        }
    });
    WT_ERR(ret);

    conn->layered_ingest_chunk_server.ingest_gc_last_layered_count =
      (uint32_t)WT_MIN(layered_uris_count, (size_t)UINT32_MAX);

    for (i = 0; i < layered_uris_count; i++) {
        if (!__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running))
            break;
        WT_ERR(__layered_ingest_chunk_try_drop_obsolete_oldest(session, layered_uris[i]));
    }

err:
    if (layered_uris != NULL) {
        for (i = 0; i < layered_uris_count; i++)
            __wt_free(session, layered_uris[i]);
        __wt_free(session, layered_uris);
    }
    return (ret);
}

/*
 * __layered_ingest_chunk_server_thread --
 *     Background thread for dropping obsolete layered ingest chunk files.
 */
static WT_THREAD_RET
__layered_ingest_chunk_server_thread(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = arg;
    conn = S2C(session);

    for (;;) {
        __wt_cond_wait(session, conn->layered_ingest_chunk_server.cond, 100 * WT_THOUSAND,
          __layered_ingest_chunk_server_run_chk);
        if (!__wt_atomic_load_bool_relaxed(&conn->layered_ingest_chunk_server.running))
            break;
        if (conn->disagg_layered_leader)
            continue;
        {
            uint64_t hb_now, pass_duration_sec;

            __wt_seconds(session, &conn->layered_ingest_chunk_server.ingest_gc_pass_start_sec);
            WT_ERR(__layered_ingest_chunk_server_pass(session));
            ++conn->layered_ingest_chunk_server.ingest_gc_completed_passes;
            __wt_seconds(session, &hb_now);
            pass_duration_sec =
              hb_now - conn->layered_ingest_chunk_server.ingest_gc_pass_start_sec;
            if (conn->layered_ingest_chunk_server.ingest_gc_last_hb_sec == 0 ||
              hb_now - conn->layered_ingest_chunk_server.ingest_gc_last_hb_sec >= 5) {
                __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_2,
                  "ingest GC: chunk server heartbeat: completed %" PRIu64 " pass(es); "
                  "last_pass_layered_tables=%" PRIu32 " last_pass_duration_sec=%" PRIu64 " "
                  "(expect about every 5s while active; a long silence means a pass is still in "
                  "progress or the thread exited)",
                  conn->layered_ingest_chunk_server.ingest_gc_completed_passes,
                  conn->layered_ingest_chunk_server.ingest_gc_last_layered_count,
                  pass_duration_sec);
                conn->layered_ingest_chunk_server.ingest_gc_last_hb_sec = hb_now;
            }
        }
    }

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "layered ingest chunk server error"));
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wti_layered_ingest_chunk_server_create --
 *     Start the layered ingest chunk server thread (follower disaggregated connections).
 */
int
__wti_layered_ingest_chunk_server_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    if (!__wt_conn_is_disagg(session) || F_ISSET(conn, WT_CONN_READONLY))
        return (0);
    if (conn->layered_ingest_chunk_server.tid_set)
        return (0);

    WT_RET(__wt_cond_alloc(session, "layered-ingest-chunk-server", &conn->layered_ingest_chunk_server.cond));

    WT_RET(__wt_open_internal_session(conn, "layered-ingest-chunk-server", true,
      WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE, 0,
      &conn->layered_ingest_chunk_server.session));
    session = conn->layered_ingest_chunk_server.session;

    conn->layered_ingest_chunk_server.ingest_gc_diag_suppress = 0;
    conn->layered_ingest_chunk_server.ingest_gc_completed_passes = 0;
    conn->layered_ingest_chunk_server.ingest_gc_last_layered_count = 0;
    conn->layered_ingest_chunk_server.ingest_gc_last_hb_sec = 0;
    conn->layered_ingest_chunk_server.ingest_gc_pass_start_sec = 0;
    __wt_atomic_store_bool_relaxed(&conn->layered_ingest_chunk_server.running, true);
    WT_ERR(__wt_thread_create(session, &conn->layered_ingest_chunk_server.tid,
      __layered_ingest_chunk_server_thread, session));
    conn->layered_ingest_chunk_server.tid_set = true;

    return (0);

err:
    __wt_atomic_store_bool_relaxed(&conn->layered_ingest_chunk_server.running, false);
    if (conn->layered_ingest_chunk_server.session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->layered_ingest_chunk_server.session));
        conn->layered_ingest_chunk_server.session = NULL;
    }
    if (conn->layered_ingest_chunk_server.cond != NULL) {
        __wt_cond_destroy(session, &conn->layered_ingest_chunk_server.cond);
        conn->layered_ingest_chunk_server.cond = NULL;
    }
    return (ret);
}

/*
 * __wti_layered_ingest_chunk_server_destroy --
 *     Shut down the layered ingest chunk server thread.
 */
int
__wti_layered_ingest_chunk_server_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    if (!conn->layered_ingest_chunk_server.tid_set)
        return (0);

    __wt_atomic_store_bool_relaxed(&conn->layered_ingest_chunk_server.running, false);
    __wt_cond_signal(session, conn->layered_ingest_chunk_server.cond);
    WT_TRET(__wt_thread_join(session, &conn->layered_ingest_chunk_server.tid));
    conn->layered_ingest_chunk_server.tid_set = false;

    __wt_cond_destroy(session, &conn->layered_ingest_chunk_server.cond);
    conn->layered_ingest_chunk_server.cond = NULL;

    if (conn->layered_ingest_chunk_server.session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->layered_ingest_chunk_server.session));
        conn->layered_ingest_chunk_server.session = NULL;
    }

    return (ret);
}
