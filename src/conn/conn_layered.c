/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __layered_create_missing_ingest_table --
 *     Create a missing ingest table from an existing layered table configuration.
 */
static int
__layered_create_missing_ingest_table(
  WT_SESSION_IMPL *session, const char *uri, const char *layered_cfg)
{
    WT_CONFIG_ITEM key_format, value_format;
    WT_DECL_ITEM(ingest_config);
    WT_DECL_RET;

    WT_ERR(__wt_config_getones(session, layered_cfg, "key_format", &key_format));
    WT_ERR(__wt_config_getones(session, layered_cfg, "value_format", &value_format));

    /* TODO Refactor this with __create_layered? */
    WT_ERR(__wt_scr_alloc(session, 0, &ingest_config));
    WT_ERR(__wt_buf_fmt(session, ingest_config,
      "key_format=\"%.*s\",value_format=\"%.*s\","
      "layered_table_log=(enabled=true,layered_constituent=true),in_memory=true,"
      "disaggregated=(page_log=none,storage_source=none)",
      (int)key_format.len, key_format.str, (int)value_format.len, value_format.str));

    WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_create(session, uri, ingest_config->data));

err:
    __wt_scr_free(session, &ingest_config);
    return (ret);
}

/*
 * __disagg_pick_up_checkpoint --
 *     Pick up a new checkpoint.
 */
static int
__disagg_pick_up_checkpoint(WT_SESSION_IMPL *session, uint64_t checkpoint_id)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor, *md_cursor;
    WT_DECL_ITEM(item);
    WT_DECL_RET;
    WT_SESSION_IMPL *internal_session, *shared_metadata_session;
    size_t len, metadata_value_cfg_len;
    uint64_t global_checkpoint_id;
    char *buf, *cfg_ret, *metadata_value_cfg, *layered_ingest_uri;
    const char *cfg[3], *current_value, *metadata_key, *metadata_value;

    conn = S2C(session);

    buf = NULL;
    cursor = NULL;
    internal_session = NULL;
    md_cursor = NULL;
    metadata_key = NULL;
    metadata_value = NULL;
    metadata_value_cfg = NULL;
    layered_ingest_uri = NULL;
    shared_metadata_session = NULL;

    WT_ERR(__wt_scr_alloc(session, 16 * WT_KILOBYTE, &item));

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    WT_ACQUIRE_READ(global_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);

    if (checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
        return (EINVAL);

    /* Check the checkpoint ID to ensure that we are not going backwards. */
    if (checkpoint_id + 1 < global_checkpoint_id)
        WT_ERR_MSG(session, EINVAL, "Global checkpoint ID went backwards: %" PRIu64 " -> %" PRIu64,
          global_checkpoint_id - 1, checkpoint_id);

    /*
     * Part 1: Get the metadata of the shared metadata table and insert it into our metadata table.
     */

    /* Read the checkpoint metadata of the shared metadata table from the special metadata page. */
    WT_ERR(__wt_disagg_get_meta(session, WT_DISAGG_METADATA_MAIN_PAGE_ID, checkpoint_id, item));

    /* Add the terminating zero byte to the end of the buffer. */
    len = item->size + 1;
    WT_ERR(__wt_calloc_def(session, len, &buf));
    memcpy(buf, item->data, item->size);
    buf[item->size] = '\0';

    metadata_key = WT_DISAGG_METADATA_URI;
    metadata_value = buf;

    /* We need an internal session when modifying metadata. */
    WT_ERR(__wt_open_internal_session(conn, "checkpoint-pick-up", false, 0, 0, &internal_session));

    /* Open up a metadata cursor pointing at our table */
    WT_ERR(__wt_metadata_cursor(internal_session, &md_cursor));
    md_cursor->set_key(md_cursor, metadata_key);
    WT_ERR(md_cursor->search(md_cursor));

    /* Pull the value out. */
    WT_ERR(md_cursor->get_value(md_cursor, &current_value));
    len = strlen("checkpoint=") + strlen(metadata_value) + 1 /* for NUL */;

    /* Allocate/create a new config we're going to insert */
    metadata_value_cfg_len = len;
    WT_ERR(__wt_calloc_def(session, metadata_value_cfg_len, &metadata_value_cfg));
    WT_ERR(__wt_snprintf(metadata_value_cfg, len, "checkpoint=%s", metadata_value));
    cfg[0] = current_value;
    cfg[1] = metadata_value_cfg;
    cfg[2] = NULL;
    WT_ERR(__wt_config_collapse(session, cfg, &cfg_ret));

    /* Put our new config in */
    WT_ERR(__wt_metadata_insert(internal_session, metadata_key, cfg_ret));

    /*
     * Part 2: Get the metadata for other tables from the shared metadata table.
     */

    /* We need a separate internal session to pick up the new checkpoint. */
    WT_ERR(__wt_open_internal_session(
      conn, "checkpoint-pick-up-shared", false, 0, 0, &shared_metadata_session));

    /*
     * Scan the metadata table. The cursor config below ensures that we open the latest checkpoint,
     * which we just received from the primary. We do this by creating a checkpoint cursor on
     * WT_CHECKPOINT (we can't specify the exact checkpoint order anyways, as it is prohibited by
     * the API). This is nonetheless guaranteed to be the latest checkpoint because:
     *   - The checkpoint ID can only advance, so the received checkpoint is guaranteed to be more
     *     recent than any checkpoint that we have received previously.
     *   - We can't create or receive another checkpoint in the meantime, because we currently hold
     *     the checkpoint lock.
     */
    cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
    cfg[1] = "checkpoint=" WT_CHECKPOINT ",checkpoint_use_history=false";
    cfg[2] = NULL;
    WT_ERR(__wt_open_cursor(shared_metadata_session, WT_DISAGG_METADATA_URI, NULL, cfg, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &metadata_key));
        WT_ERR(cursor->get_value(cursor, &metadata_value));

        md_cursor->set_key(md_cursor, metadata_key);
        WT_ERR_NOTFOUND_OK(md_cursor->search(md_cursor), true);

        if (ret == 0 && WT_PREFIX_MATCH(metadata_key, "file:")) {
            /* Existing table: Just apply the new metadata. */
            WT_ERR(__wt_config_getones(session, metadata_value, "checkpoint", &cval));
            len = strlen("checkpoint=") + strlen(metadata_value) + 1 /* for NUL */;
            if (len > metadata_value_cfg_len) {
                metadata_value_cfg_len = len;
                WT_ERR(
                  __wt_realloc_noclear(session, NULL, metadata_value_cfg_len, &metadata_value_cfg));
            }
            WT_ERR(
              __wt_snprintf(metadata_value_cfg, len, "checkpoint=%.*s", (int)cval.len, cval.str));

            /* Merge the new checkpoint metadata into the current table metadata. */
            WT_ERR(md_cursor->get_value(md_cursor, &current_value));
            cfg[0] = current_value;
            cfg[1] = metadata_value_cfg;
            cfg[2] = NULL;
            WT_ERR(__wt_config_collapse(session, cfg, &cfg_ret));

            /* TODO: Possibly check that the other parts of the metadata are identical. */

            /* Put our new config in */
            md_cursor->set_value(md_cursor, cfg_ret);
            WT_ERR(md_cursor->insert(md_cursor));
        } else if (ret == WT_NOTFOUND) {
            /* New table: Insert new metadata. */
            /* TODO: Verify that there is no btree ID conflict. */

            /* Create the corresponding ingest table, if it does not exist. */
            if (WT_PREFIX_MATCH(metadata_key, "layered:")) {
                WT_ERR(__wt_config_getones(session, metadata_value, "ingest", &cval));
                if (cval.len > 0) {
                    WT_ERR(__wt_calloc_def(session, cval.len + 1, &layered_ingest_uri));
                    memcpy(layered_ingest_uri, cval.str, cval.len);
                    layered_ingest_uri[cval.len] = '\0';
                    md_cursor->set_key(md_cursor, layered_ingest_uri);
                    WT_ERR_NOTFOUND_OK(md_cursor->search(md_cursor), true);
                    if (ret == WT_NOTFOUND)
                        WT_ERR(__layered_create_missing_ingest_table(
                          internal_session, layered_ingest_uri, metadata_value));
                }
            }

            /* Insert the actual metadata. */
            md_cursor->set_key(md_cursor, metadata_key);
            md_cursor->set_value(md_cursor, metadata_value);
            WT_ERR(md_cursor->insert(md_cursor));
        }
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    /*
     * Part 3: Do the bookkeeping.
     */

    /*
     * WiredTiger will reload the dir store's checkpoint when opening a cursor: Opening a file
     * cursor triggers __wt_btree_open (even if the file has been opened before).
     */
    WT_STAT_CONN_DSRC_INCR(session, layered_table_manager_checkpoints_refreshed);

    /*
     * Update the checkpoint ID. This doesn't require further synchronization, because the updates
     * are protected by the checkpoint lock.
     */
    WT_RELEASE_WRITE(conn->disaggregated_storage.global_checkpoint_id, checkpoint_id + 1);

err:
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));
    if (md_cursor != NULL)
        WT_TRET(__wt_metadata_cursor_release(internal_session, &md_cursor));

    if (internal_session != NULL)
        WT_TRET(__wt_session_close_internal(internal_session));
    if (shared_metadata_session != NULL)
        WT_TRET(__wt_session_close_internal(shared_metadata_session));

    __wt_free(session, buf);
    __wt_free(session, metadata_value_cfg);
    __wt_free(session, layered_ingest_uri);

    __wt_scr_free(session, &item);
    return (ret);
}

/*
 * __wt_layered_table_manager_start --
 *     Start the layered table manager thread
 */
int
__wt_layered_table_manager_start(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    uint32_t session_flags;

    conn = S2C(session);
    manager = &conn->layered_table_manager;

    /* It's possible to race - only start the manager if we are the winner */
    if (!__wt_atomic_cas32(
          &manager->state, WT_LAYERED_TABLE_MANAGER_OFF, WT_LAYERED_TABLE_MANAGER_STARTING)) {
        /* This isn't optimal, but it'll do. It's uncommon for multiple threads to be trying to
         * start the layered table manager at the same time.
         * It's probably fine for any "loser" to proceed without waiting, but be conservative
         * and have a semantic where a return from this function indicates a running layered table
         * manager->
         */
        while (__wt_atomic_load32(&manager->state) != WT_LAYERED_TABLE_MANAGER_RUNNING)
            __wt_sleep(0, 1000);
        return (0);
    }

    WT_RET(__wt_spin_init(session, &manager->layered_table_lock, "layered table manager"));

    /*
     * TODO Be lazy for now, allow for up to 1000 files to be allocated. In the future this should
     * be able to grow dynamically and a more conservative number used here. Until then layered
     * table application will crash in a system with more than 1000 files.
     */
    manager->open_layered_table_count = conn->next_file_id + 1000;
    WT_ERR(__wt_calloc(session, sizeof(WT_LAYERED_TABLE_MANAGER_ENTRY *),
      manager->open_layered_table_count, &manager->entries));

    session_flags = WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL;
    WT_ERR(__wt_thread_group_create(session, &manager->threads, "layered-table-manager",
      WT_LAYERED_TABLE_THREAD_COUNT, WT_LAYERED_TABLE_THREAD_COUNT, session_flags,
      __wt_layered_table_manager_thread_chk, __wt_layered_table_manager_thread_run, NULL));

    WT_MAX_LSN(&manager->max_replay_lsn);

    WT_STAT_CONN_SET(session, layered_table_manager_running, 1);
    __wt_verbose_level(
      session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5, "%s", "__wt_layered_table_manager_start");
    FLD_SET(conn->server_flags, WT_CONN_SERVER_LAYERED);

    /* Now that everything is setup, allow the manager to be used. */
    __wt_atomic_store32(&manager->state, WT_LAYERED_TABLE_MANAGER_RUNNING);
    return (0);

err:
    /* Quit the layered table server. */
    WT_TRET(__wt_layered_table_manager_destroy(session, false));
    return (ret);
}

/*
 * __wt_layered_table_manager_thread_chk --
 *     Check to decide if the layered table manager thread should continue running
 */
bool
__wt_layered_table_manager_thread_chk(WT_SESSION_IMPL *session)
{
    if (!S2C(session)->layered_table_manager.leader)
        return (false);
    return (__wt_atomic_load32(&S2C(session)->layered_table_manager.state) ==
      WT_LAYERED_TABLE_MANAGER_RUNNING);
}

/*
 * __wt_layered_table_manager_add_table --
 *     Add a table to the layered table manager when it's opened
 */
int
__wt_layered_table_manager_add_table(
  WT_SESSION_IMPL *session, uint32_t ingest_id, uint32_t stable_id)
{
    WT_CONNECTION_IMPL *conn;
    WT_LAYERED_TABLE *layered;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;

    conn = S2C(session);
    manager = &conn->layered_table_manager;
    fprintf(stderr, "adding %u to layered table manager\n", ingest_id);

    WT_ASSERT_ALWAYS(session, session->dhandle->type == WT_DHANDLE_TYPE_LAYERED,
      "Adding a layered tree to tracking without the right dhandle context.");
    layered = (WT_LAYERED_TABLE *)session->dhandle;

    WT_ASSERT_ALWAYS(session,
      __wt_atomic_load32(&manager->state) == WT_LAYERED_TABLE_MANAGER_RUNNING,
      "Adding a layered table, but the manager isn't running");
    __wt_spin_lock(session, &manager->layered_table_lock);
    /* Diagnostic sanity check - don't keep adding the same table */
    if (manager->entries[ingest_id] != NULL)
        WT_IGNORE_RET(__wt_panic(session, WT_PANIC,
          "Internal server error: opening the same layered table multiple times"));
    WT_RET(__wt_calloc_one(session, &entry));
    entry->ingest_id = ingest_id;
    entry->stable_id = stable_id;
    entry->stable_cursor = NULL;
    entry->layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    /*
     * There is a bootstrapping problem. Use the global oldest ID as a starting point. Nothing can
     * have been written into the ingest table, so it will be a conservative choice.
     */
    entry->checkpoint_txn_id = __wt_atomic_loadv64(&conn->txn_global.oldest_id);
    WT_ACQUIRE_READ(entry->read_checkpoint, conn->disaggregated_storage.global_checkpoint_id);

    /*
     * It's safe to just reference the same string. The lifecycle of the layered tree is longer than
     * it will live in the tracker here.
     */
    entry->stable_uri = layered->stable_uri;
    WT_STAT_CONN_INCR(session, layered_table_manager_tables);
    __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
      "__wt_layered_table_manager_add_table uri=%s ingest=%" PRIu32 " stable=%" PRIu32 " name=%s",
      entry->stable_uri, ingest_id, stable_id, session->dhandle->name);
    manager->entries[ingest_id] = entry;

    __wt_spin_unlock(session, &manager->layered_table_lock);
    return (0);
}

/*
 * __layered_table_manager_remove_table_inlock --
 *     Internal table remove implementation.
 */
static void
__layered_table_manager_remove_table_inlock(
  WT_SESSION_IMPL *session, uint32_t ingest_id, bool from_shutdown)
{
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;

    manager = &S2C(session)->layered_table_manager;

    if ((entry = manager->entries[ingest_id]) != NULL) {
        WT_STAT_CONN_DECR(session, layered_table_manager_tables);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "__wt_layered_table_manager_remove_table %s", entry->stable_uri);

        /* Cursors get automatically closed via the session handle in shutdown. */
        if (!from_shutdown && entry->stable_cursor != NULL)
            entry->stable_cursor->close(entry->stable_cursor);
        __wt_free(session, entry);
        fprintf(stderr, "layered table mgr clearing %u\n", ingest_id);
        manager->entries[ingest_id] = NULL;
    }
}

/*
 * __wt_layered_table_manager_remove_table --
 *     Remove a table to the layered table manager when it's opened. Note that it is always safe to
 *     remove a table from tracking immediately here. It will only be removed when the handle is
 *     closed and a handle is only closed after a checkpoint has completed that included all writes
 *     to the table. By that time the processor would have finished with any records from the
 *     layered table.
 */
void
__wt_layered_table_manager_remove_table(WT_SESSION_IMPL *session, uint32_t ingest_id)
{
    WT_LAYERED_TABLE_MANAGER *manager;
    uint32_t manager_state;

    manager = &S2C(session)->layered_table_manager;

    manager_state = __wt_atomic_load32(&manager->state);

    /* Shutdown calls this redundantly - ignore cases when the manager is already closed. */
    if (manager_state == WT_LAYERED_TABLE_MANAGER_OFF)
        return;

    WT_ASSERT_ALWAYS(session,
      manager_state == WT_LAYERED_TABLE_MANAGER_RUNNING ||
        manager_state == WT_LAYERED_TABLE_MANAGER_STOPPING,
      "Adding a layered table, but the manager isn't running");
    __wt_spin_lock(session, &manager->layered_table_lock);
    __layered_table_manager_remove_table_inlock(session, ingest_id, false);

    __wt_spin_unlock(session, &manager->layered_table_lock);
}

/*
 * __layered_table_get_constituent_cursor --
 *     Retrieve or open a constituent cursor for a layered tree.
 */
static int
__layered_table_get_constituent_cursor(
  WT_SESSION_IMPL *session, uint32_t ingest_id, WT_CURSOR **cursorp)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *stable_cursor;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    uint64_t global_ckpt_id;

    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL, NULL};

    conn = S2C(session);
    entry = conn->layered_table_manager.entries[ingest_id];

    *cursorp = NULL;

    if (entry == NULL)
        return (0);

    if (entry->stable_cursor != NULL) {
        *cursorp = entry->stable_cursor;
        return (0);
    }

    WT_ACQUIRE_READ(global_ckpt_id, conn->disaggregated_storage.global_checkpoint_id);
    if (global_ckpt_id > entry->read_checkpoint)
        cfg[2] = "force=true";

    /* Open the cursor and keep a reference in the manager entry and our caller */
    WT_RET(__wt_open_cursor(session, entry->stable_uri, NULL, cfg, &stable_cursor));
    entry->stable_cursor = stable_cursor;
    entry->read_checkpoint = global_ckpt_id;
    *cursorp = stable_cursor;

    return (0);
}

/*
 * __layered_table_manager_checkpoint_locked --
 *     Trigger a checkpoint of the handle - will acquire necessary locks
 */
static int
__layered_table_manager_checkpoint_locked(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    WT_STAT_CONN_DSRC_INCR(session, layered_table_manager_checkpoints);
    WT_WITH_CHECKPOINT_LOCK(
      session, WT_WITH_SCHEMA_LOCK(session, ret = __wt_checkpoint(session, 0)));
    return (ret);
}

/*
 * __layered_table_manager_checkpoint_one --
 *     Review the layered tables and checkpoint one if it has enough accumulated content. For now
 *     this just checkpoints the first table that meets the threshold. In the future it should be
 *     more fair in selecting a table.
 */
static int
__layered_table_manager_checkpoint_one(WT_SESSION_IMPL *session)
{
    WT_BTREE *ingest_btree;
    WT_CURSOR *stable_cursor;
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    WT_TXN_ISOLATION saved_isolation;
    uint64_t satisfied_txn_id;
    uint32_t i;

    manager = &S2C(session)->layered_table_manager;

    /* The table count never shrinks, so this is safe. It probably needs the layered table lock */
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL &&
          entry->accumulated_write_bytes > WT_LAYERED_TABLE_CHECKPOINT_THRESHOLD) {
            /*
             * Retrieve the current tranasaction ID - ensure it actually gets read from the shared
             * variable here, it would lead to data loss if it was read later and included
             * transaction IDs that aren't included in the checkpoint. It's OK for it to miss IDs -
             * this requires an "at least as much" guarantee, not an exact match guarantee.
             */
            WT_READ_ONCE(satisfied_txn_id, manager->max_applied_txnid);
            __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
              "layered table %s being checkpointed, satisfied txnid=%" PRIu64, entry->stable_uri,
              satisfied_txn_id);

            WT_RET(
              __layered_table_get_constituent_cursor(session, entry->ingest_id, &stable_cursor));
            /*
             * Clear out the byte count before checkpointing - otherwise any writes done during the
             * checkpoint won't count towards the next threshold.
             */
            entry->accumulated_write_bytes = 0;

            /*
             * We know all content in the table is visible - use the cheapest check we can during
             * reconciliation.
             */
            saved_isolation = session->txn->isolation;
            session->txn->isolation = WT_ISO_READ_UNCOMMITTED;

            /*
             * Turn on metadata tracking to ensure the checkpoint gets the necessary handle locks.
             */
            WT_RET(__wt_meta_track_on(session));
            WT_WITH_DHANDLE(session, ((WT_CURSOR_BTREE *)stable_cursor)->dhandle,
              ret = __layered_table_manager_checkpoint_locked(session));
            WT_TRET(__wt_meta_track_off(session, false, ret != 0));
            session->txn->isolation = saved_isolation;
            if (ret == 0) {
                entry->checkpoint_txn_id = satisfied_txn_id;
                ingest_btree = (WT_BTREE *)entry->layered_table->ingest->handle;
                WT_ASSERT_ALWAYS(session, F_ISSET(ingest_btree, WT_BTREE_GARBAGE_COLLECT),
                  "Ingest table not setup for garbage collection");
                ingest_btree->oldest_live_txnid = satisfied_txn_id;
            }

            /* We've done (or tried to do) a checkpoint - that's it. */
            return (ret);
        } else if (entry != NULL) {
            if (entry->accumulated_write_bytes > 0)
                __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
                  "not checkpointing table %s bytes=%" PRIu64, entry->stable_uri,
                  entry->accumulated_write_bytes);
        }
    }

    WT_STAT_CONN_SET(session, layered_table_manager_checkpoint_candidates, i);
    return (0);
}

/*
 * __layered_table_log_replay_op_apply --
 *     Apply a transactional operation during recovery.
 */
static int
__layered_table_log_replay_op_apply(
  WT_SESSION_IMPL *session, WT_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
    WT_CURSOR *stable_cursor;
    WT_DECL_RET;
    WT_ITEM key, start_key, stop_key, value;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    wt_timestamp_t commit, durable, first_commit, prepare, read;
    uint64_t recno, start_recno, stop_recno, t_nsec, t_sec;
    uint32_t fileid, mode, opsize, optype;
    bool applied;

    manager = &S2C(session)->layered_table_manager;
    stable_cursor = NULL;
    applied = false;
    fileid = 0;
    memset(&value, 0, sizeof(WT_ITEM));

    /* Peek at the size and the type. */
    WT_ERR(__wt_layered_table_logop_read(session, pp, end, &optype, &opsize));
    end = *pp + opsize;

    /*
     * If it is an operation type that should be ignored, we're done. Note that file ids within
     * known operations also use the same macros to indicate that operation should be ignored.
     */
    if (WT_LOGOP_IS_IGNORED(optype)) {
        *pp += opsize;
        goto done;
    }

    switch (optype) {
    case WT_LOGOP_COL_MODIFY:
        WT_ERR(
          __wt_layered_table_logop_col_modify_unpack(session, pp, end, &fileid, &recno, &value));
        break;
    case WT_LOGOP_COL_PUT:
        WT_ERR(__wt_layered_table_logop_col_put_unpack(session, pp, end, &fileid, &recno, &value));
        break;
    case WT_LOGOP_COL_REMOVE:
        WT_ERR(__wt_layered_table_logop_col_remove_unpack(session, pp, end, &fileid, &recno));
        break;
    case WT_LOGOP_COL_TRUNCATE:
        WT_ERR(__wt_layered_table_logop_col_truncate_unpack(
          session, pp, end, &fileid, &start_recno, &stop_recno));
        break;
    case WT_LOGOP_ROW_MODIFY:
        WT_ERR(__wt_layered_table_logop_row_modify_unpack(session, pp, end, &fileid, &key, &value));
        if ((entry = manager->entries[fileid]) != NULL) {
            WT_ERR(__layered_table_get_constituent_cursor(session, fileid, &stable_cursor));
            __wt_cursor_set_raw_key(stable_cursor, &key);
            if ((ret = stable_cursor->search(stable_cursor)) != 0)
                WT_ERR_NOTFOUND_OK(ret, false);
            else {
                /*
                 * Build/insert a complete value during recovery rather than using cursor modify to
                 * create a partial update (for no particular reason than simplicity).
                 */
                WT_ERR(__wt_modify_apply_item(CUR2S(stable_cursor), stable_cursor->value_format,
                  &stable_cursor->value, value.data));
                WT_ERR(stable_cursor->insert(stable_cursor));
                entry->accumulated_write_bytes += (key.size + stable_cursor->value.size);
                applied = true;
            }
        }
        break;

    case WT_LOGOP_ROW_PUT:
        WT_ERR(__wt_layered_table_logop_row_put_unpack(session, pp, end, &fileid, &key, &value));
        if ((entry = manager->entries[fileid]) != NULL) {
            /*
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1,
          "layered table log application row store put applying to stable table %s",
        entry->ingest_uri);
          */
            WT_ERR(__layered_table_get_constituent_cursor(session, fileid, &stable_cursor));
            __wt_cursor_set_raw_key(stable_cursor, &key);
            __wt_cursor_set_raw_value(stable_cursor, &value);
            WT_ERR(stable_cursor->insert(stable_cursor));

            entry->accumulated_write_bytes += (key.size + value.size);
            applied = true;
        }
        break;

    case WT_LOGOP_ROW_REMOVE:
        /*
         * TODO: There should not be any remove operations logged - we turn them into tombstone
         * writes.
         */
        WT_ERR(__wt_layered_table_logop_row_remove_unpack(session, pp, end, &fileid, &key));
        if ((entry = manager->entries[fileid]) != NULL) {
            WT_ERR(__layered_table_get_constituent_cursor(session, fileid, &stable_cursor));
            __wt_cursor_set_raw_key(stable_cursor, &key);
            /*
             * WT_NOTFOUND is an expected error because the checkpoint snapshot we're rolling
             * forward may race with a remove, resulting in the key not being in the tree, but
             * recovery still processing the log record of the remove.
             */
            WT_ERR_NOTFOUND_OK(stable_cursor->remove(stable_cursor), false);
            entry->accumulated_write_bytes += (key.size + value.size);
            applied = true;
        }
        break;

    case WT_LOGOP_ROW_TRUNCATE:
        WT_ERR(__wt_layered_table_logop_row_truncate_unpack(
          session, pp, end, &fileid, &start_key, &stop_key, &mode));
        break;
    case WT_LOGOP_TXN_TIMESTAMP:
        /*
         * Timestamp records are informational only. We have to unpack it to properly move forward
         * in the log record to the next operation, but otherwise ignore.
         */
        WT_ERR(__wt_layered_table_logop_txn_timestamp_unpack(
          session, pp, end, &t_sec, &t_nsec, &commit, &durable, &first_commit, &prepare, &read));
        break;
    default:
        WT_ERR(__wt_illegal_value(session, optype));
    }

    /*
     * The zero file ID means either the metadata table, or no file ID was retrieved from the log
     * record - it is safe to skip either case.
     */
    if (fileid != 0 && !applied && manager->entries[fileid] != NULL) {
        WT_STAT_CONN_DSRC_INCR(session, layered_table_manager_logops_skipped);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1,
          "layered table log application skipped a record associated with layered tree. Record "
          "type: "
          "%" PRIu32,
          optype);
    } else {
        if (applied)
            WT_STAT_CONN_DSRC_INCR(session, layered_table_manager_logops_applied);
    }

done:
    /* Reset the cursor so it doesn't block eviction. */
    if (stable_cursor != NULL)
        WT_ERR(stable_cursor->reset(stable_cursor));
    return (0);

err:
    __wt_err(session, ret,
      "operation apply failed during recovery: operation type %" PRIu32 " at LSN %" PRIu32
      "/%" PRIu32,
      optype, lsnp->l.file, __wt_layered_table_lsn_offset(lsnp));
    return (ret);
}

/*
 * __layered_table_log_replay_commit_apply --
 *     Apply a commit record during layered table log replay.
 */
static int
__layered_table_log_replay_commit_apply(
  WT_SESSION_IMPL *session, WT_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
    /*
     * __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1, "%s",
     *   "layered table log application commit applying");
     */

    /* The logging subsystem zero-pads records. */
    while (*pp < end && **pp)
        WT_RET(__layered_table_log_replay_op_apply(session, lsnp, pp, end));

    return (0);
}

/*
 * __layered_table_log_replay --
 *     Review a log record and replay it against a layered stable constituent if relevant. This
 *     could be done in a number of ways, including: * Generalize the code in
 *     txn_recover::__txn_op_apply and its callers to work for this runtime case and apply
 *     operations to a different file identifier. * Create a simplified duplicate of the recovery
 *     code. * Use the log cursor implementation as-is, and see how far we get. * Implement a new
 *     log cursor, or extend existing to be more closely aligned with this need. I chose the
 *     simplified duplicate approach for now - it was most expedient, debuggable and performant.
 *     Long term we might want to do something different.
 */
static int
__layered_table_log_replay(WT_SESSION_IMPL *session, WT_ITEM *logrec, WT_LSN *lsnp,
  WT_LSN *next_lsnp, void *cookie, int firstrecord)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    uint64_t txnid;
    uint32_t rectype;
    const uint8_t *end, *p;

    manager = &S2C(session)->layered_table_manager;
    p = WT_LOG_SKIP_HEADER(logrec->data);
    end = (const uint8_t *)logrec->data + logrec->size;
    /* If this becomes multi-threaded we might move the context from manager here */
    WT_UNUSED(cookie);
    WT_UNUSED(firstrecord);

    if (!manager->leader)
        return (0);

    /* First, peek at the log record type. */
    WT_RET(__wt_layered_table_logrec_read(session, &p, end, &rectype));

    /* We are only ever interested in commit records */
    if (rectype != WT_LOGREC_COMMIT)
        return (0);

    if (!WT_IS_MAX_LSN(&manager->max_replay_lsn) &&
      __wt_layered_table_log_cmp(lsnp, &manager->max_replay_lsn) < 0) {
        WT_STAT_CONN_DSRC_INCR(session, layered_table_manager_skip_lsn);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_1,
          "Layered table skipping previously applied LSN: [%" PRIu32 "][%" PRIu32 "]", lsnp->l.file,
          lsnp->l.offset);
        return (0);
    }

    if ((ret = __wt_vunpack_uint(&p, WT_PTRDIFF(end, p), &txnid)) != 0)
        WT_RET_MSG(session, ret, "layered_table_log_replay: unpack failure");
    WT_RET(__layered_table_log_replay_commit_apply(session, lsnp, &p, end));

    /* Record the highest LSN we've processed so future scans can start from there. */
    WT_ASSIGN_LSN(&manager->max_replay_lsn, next_lsnp);
    /* This will need to be made thread-safe if log application becomes multi-threaded */
    manager->max_applied_txnid = txnid;

    return (0);
}

/*
 * __wt_layered_table_manager_thread_run --
 *     Entry function for a layered table manager thread. This is called repeatedly from the thread
 *     group code so it does not need to loop itself.
 */
int
__wt_layered_table_manager_thread_run(WT_SESSION_IMPL *session_shared, WT_THREAD *thread)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_SESSION_IMPL *session;

    WT_UNUSED(session_shared);
    session = thread->session;
    WT_ASSERT(session, session->id != 0);
    manager = &S2C(session)->layered_table_manager;

    WT_STAT_CONN_SET(session, layered_table_manager_active, 1);

    /*
     * There are two threads: let one do log replay and the other checkpoints. For now use just the
     * first thread in the group for log application, otherwise the way cursors are saved in the
     * manager queue gets confused (since they are associated with sessions).
     */
    /* __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5, "%s",
     * "__wt_layered_table_manager_thread_run"); */
    if (thread->id == 0 && __wt_atomic_load32(&manager->log_applying) == 0 &&
      __wt_atomic_cas32(&manager->log_applying, 0, 1)) {
        if (WT_IS_MAX_LSN(&manager->max_replay_lsn))
            ret = __wt_layered_table_log_scan(
              session, NULL, NULL, WT_LOGSCAN_FIRST, __layered_table_log_replay, NULL);
        else
            ret = __wt_layered_table_log_scan(
              session, &manager->max_replay_lsn, NULL, 0, __layered_table_log_replay, NULL);

        /* Ignore errors at startup or attempting to read more log record when no additional content
         * has been generated */
        if (ret == ENOENT || ret == WT_NOTFOUND)
            ret = 0;
        /*
         * The log scan interface returns a generic error if the LSN is past the end of the log
         * file. In that case bump the LSN to be the first record in the next file.
         */
        if (ret == WT_ERROR) {
            manager->max_replay_lsn.l.file++;
            manager->max_replay_lsn.l.offset = 0;
            ret = 0;
        }
        __wt_atomic_store32(&manager->log_applying, 0);
    } else if (thread->id == 1)
        WT_RET(__layered_table_manager_checkpoint_one(session));

    WT_STAT_CONN_SET(session, layered_table_manager_active, 0);

    /* Sometimes the logging subsystem is still getting started and ENOENT is expected */
    if (ret == ENOENT)
        ret = 0;
    return (ret);
}

/*
 * __wt_layered_table_manager_get_pinned_id --
 *     Retrieve the oldest checkpoint ID that's relevant to garbage collection
 */
void
__wt_layered_table_manager_get_pinned_id(WT_SESSION_IMPL *session, uint64_t *pinnedp)
{
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    uint64_t pinned;
    uint32_t i;

    manager = &S2C(session)->layered_table_manager;

    /* If no tables are being managed, then don't pin anything */
    pinned = WT_TXN_MAX;
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL && WT_TXNID_LT(entry->checkpoint_txn_id, pinned))
            pinned = entry->checkpoint_txn_id;
    }

    *pinnedp = pinned;

    WT_STAT_CONN_SET(session, layered_table_manager_pinned_id_tables_searched, i);
}

/*
 * __wt_layered_table_manager_destroy --
 *     Destroy the layered table manager thread(s)
 */
int
__wt_layered_table_manager_destroy(WT_SESSION_IMPL *session, bool from_shutdown)
{
    WT_CONNECTION_IMPL *conn;
    WT_LAYERED_TABLE_MANAGER *manager;
    uint32_t i;

    conn = S2C(session);
    manager = &conn->layered_table_manager;

    __wt_verbose_level(
      session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5, "%s", "__wt_layered_table_manager_destroy");

    if (__wt_atomic_load32(&manager->state) == WT_LAYERED_TABLE_MANAGER_OFF)
        return (0);

    /*
     * Spin until exclusive access is gained. If we got here from the startup path seeing an error,
     * the state might still be "starting" rather than "running".
     */
    while (!__wt_atomic_cas32(&manager->state, WT_LAYERED_TABLE_MANAGER_RUNNING,
             WT_LAYERED_TABLE_MANAGER_STOPPING) &&
      !__wt_atomic_cas32(
        &manager->state, WT_LAYERED_TABLE_MANAGER_STARTING, WT_LAYERED_TABLE_MANAGER_STOPPING)) {
        /* If someone beat us to it, we are done */
        if (__wt_atomic_load32(&manager->state) == WT_LAYERED_TABLE_MANAGER_OFF)
            return (0);
        __wt_sleep(0, 1000);
    }

    /* Ensure other things that engage with the layered table server know it's gone. */
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_LAYERED);

    __wt_spin_lock(session, &manager->layered_table_lock);

    /* Let any running threads finish up. */
    __wt_cond_signal(session, manager->threads.wait_cond);
    __wt_writelock(session, &manager->threads.lock);

    WT_RET(__wt_thread_group_destroy(session, &manager->threads));

    /* Close any cursors and free any related memory */
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if (manager->entries[i] != NULL)
            __layered_table_manager_remove_table_inlock(session, i, from_shutdown);
    }
    __wt_free(session, manager->entries);
    manager->open_layered_table_count = 0;
    WT_MAX_LSN(&manager->max_replay_lsn);

    __wt_atomic_store32(&manager->state, WT_LAYERED_TABLE_MANAGER_OFF);
    WT_STAT_CONN_SET(session, layered_table_manager_running, 0);

    return (0);
}

/*
 * __disagg_metadata_table_init --
 *     Initialize the shared metadata table.
 */
static int
__disagg_metadata_table_init(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SESSION_IMPL *internal_session;

    conn = S2C(session);

    WT_ERR(__wt_open_internal_session(conn, "disagg-init", false, 0, 0, &internal_session));
    WT_ERR(__wt_session_create(internal_session, WT_DISAGG_METADATA_URI,
      "key_format=S,value_format=S,log=(enabled=false),layered_table_log=(enabled=false)"));

err:
    if (internal_session != NULL)
        WT_TRET(__wt_session_close_internal(internal_session));
    return (ret);
}

/*
 * __wti_disagg_conn_config --
 *     Parse and setup the disaggregated server options for the connection.
 */
int
__wti_disagg_conn_config(WT_SESSION_IMPL *session, const char **cfg, bool reconfig)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_NAMED_PAGE_LOG *npage_log;
    uint64_t checkpoint_id, next_checkpoint_id;
    bool leader, was_leader;

    conn = S2C(session);
    leader = was_leader = conn->layered_table_manager.leader;
    npage_log = NULL;

    /* Reconfig-only settings. */
    if (reconfig) {

        /* Pick up a new checkpoint (followers only). */
        WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_id", &cval));
        if (cval.len > 0 && cval.val >= 0) {
            if (leader)
                WT_ERR(EINVAL); /* Leaders can't pick up new checkpoints. */
            else {
                WT_WITH_CHECKPOINT_LOCK(
                  session, ret = __disagg_pick_up_checkpoint(session, (uint64_t)cval.val));
                WT_ERR(ret);
            }
        }
    }

    /* Common settings between initial connection config and reconfig. */

    /* Get the next checkpoint ID. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.next_checkpoint_id", &cval));
    if (cval.len > 0 && cval.val >= 0)
        next_checkpoint_id = (uint64_t)cval.val;
    else
        next_checkpoint_id = WT_DISAGG_CHECKPOINT_ID_NONE;

    /* Set the role. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.role", &cval));
    if (cval.len == 0)
        conn->layered_table_manager.leader = leader = false;
    else {
        if (WT_CONFIG_LIT_MATCH("follower", cval))
            conn->layered_table_manager.leader = leader = false;
        else if (WT_CONFIG_LIT_MATCH("leader", cval))
            conn->layered_table_manager.leader = leader = true;
        else
            WT_ERR_MSG(session, EINVAL, "Invalid node role");

        /* Follower step-up. */
        if (reconfig && !was_leader && leader) {
            /*
             * Note that we should have picked up a new checkpoint ID above. Now that we are the new
             * leader, we need to begin the next checkpoint.
             */
            if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
                WT_ACQUIRE_READ(
                  next_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
            if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
                next_checkpoint_id = WT_DISAGG_CHECKPOINT_ID_FIRST;
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __wt_disagg_begin_checkpoint(session, next_checkpoint_id));
            WT_ERR(ret);
        }
    }

    /* Connection init settings only. */

    if (reconfig)
        return (0);

    /* Remember the configuration. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.page_log", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &conn->disaggregated_storage.page_log));
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.stable_prefix", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &conn->disaggregated_storage.stable_prefix));

    /* Setup any configured page log. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.page_log", &cval));
    WT_ERR(__wt_schema_open_page_log(session, &cval, &npage_log));
    conn->disaggregated_storage.npage_log = npage_log;

    /* Set up a handle for accessing shared metadata. */
    if (npage_log != NULL) {
        WT_ERR(npage_log->page_log->pl_open_handle(npage_log->page_log, &session->iface,
          WT_DISAGG_METADATA_TABLE_ID, &conn->disaggregated_storage.page_log_meta));
    }

    if (__wt_conn_is_disagg(session)) {

        /* Initialize the shared metadata table. */
        WT_ERR(__disagg_metadata_table_init(session));

        /* Pick up the selected checkpoint. */
        WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_id", &cval));
        if (cval.len > 0 && cval.val >= 0) {
            checkpoint_id = (uint64_t)cval.val;
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __disagg_pick_up_checkpoint(session, checkpoint_id));
            WT_ERR(ret);
        } else
            /*
             * TODO: If we are starting with local files, get the checkpoint ID from them?
             * Alternatively, maybe we should just fail if the checkpoint ID is not specified?
             */
            checkpoint_id = WT_DISAGG_CHECKPOINT_ID_NONE;

        /* If we are starting as primary (e.g., for internal testing), begin the checkpoint. */
        if (leader) {
            if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
                next_checkpoint_id = checkpoint_id + 1;
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __wt_disagg_begin_checkpoint(session, next_checkpoint_id));
            WT_ERR(ret);
        }
    }

err:
    return (ret);
}

/*
 * __wt_conn_is_disagg --
 *     Check whether the connection uses disaggregated storage.
 */
bool
__wt_conn_is_disagg(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    return (disagg->page_log_meta != NULL);
}

/*
 * __wti_disagg_destroy --
 *     Shut down disaggregated storage.
 */
int
__wti_disagg_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_DISAGGREGATED_STORAGE *disagg;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    /* Close the metadata handles. */
    if (disagg->page_log_meta != NULL) {
        WT_TRET(disagg->page_log_meta->plh_close(disagg->page_log_meta, &session->iface));
        disagg->page_log_meta = NULL;
    }

    __wt_free(session, disagg->page_log);
    __wt_free(session, disagg->stable_prefix);
    __wt_free(session, disagg->storage_source);
    return (ret);
}

/*
 * __wt_disagg_get_meta --
 *     Read metadata from disaggregated storage.
 */
int
__wt_disagg_get_meta(
  WT_SESSION_IMPL *session, uint64_t page_id, uint64_t checkpoint_id, WT_ITEM *item)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    WT_PAGE_LOG_GET_ARGS get_args;
    u_int count, retry;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    WT_CLEAR(get_args);

    if (disagg->page_log_meta != NULL) {
        retry = 0;
        for (;;) {
            count = 1;
            WT_RET(disagg->page_log_meta->plh_get(disagg->page_log_meta, &session->iface, page_id,
              checkpoint_id, &get_args, item, &count));
            WT_ASSERT(session, count <= 1 && get_args.delta_count == 0); /* TODO: corrupt data */

            /* Found the data. */
            if (count == 1)
                break;

            /* Otherwise retry up to 100 times to account for page materialization delay. */
            if (retry > 100)
                return (WT_NOTFOUND);
            __wt_verbose_notice(session, WT_VERB_READ,
              "retry #%" PRIu32 " for metadata page_id %" PRIu64 ", checkpoint_id %" PRIu64, retry,
              page_id, checkpoint_id);
            __wt_sleep(0, 10000 + retry * 5000);
            ++retry;
        }

        return (0);
    }

    return (ENOTSUP);
}

/*
 * __wt_disagg_put_meta --
 *     Write metadata to disaggregated storage.
 */
int
__wt_disagg_put_meta(
  WT_SESSION_IMPL *session, uint64_t page_id, uint64_t checkpoint_id, WT_ITEM *item)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    WT_PAGE_LOG_PUT_ARGS put_args;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    WT_CLEAR(put_args);
    if (disagg->page_log_meta != NULL) {
        WT_RET(disagg->page_log_meta->plh_put(
          disagg->page_log_meta, &session->iface, page_id, checkpoint_id, &put_args, item));
        __wt_atomic_addv64(&disagg->num_meta_put, 1);
        return (0);
    }
    return (ENOTSUP);
}

/*
 * __wt_disagg_begin_checkpoint --
 *     Begin the next checkpoint.
 */
int
__wt_disagg_begin_checkpoint(WT_SESSION_IMPL *session, uint64_t next_checkpoint_id)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    uint64_t cur_checkpoint_id;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    /* Only the leader can begin a global checkpoint. */
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0);

    if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
        return (EINVAL);

    WT_ACQUIRE_READ(cur_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
    if (next_checkpoint_id < cur_checkpoint_id)
        WT_RET_MSG(session, EINVAL, "The checkpoint ID did not advance");

    WT_RET(disagg->npage_log->page_log->pl_begin_checkpoint(
      disagg->npage_log->page_log, &session->iface, next_checkpoint_id));

    /* Store is sufficient because updates are protected by the checkpoint lock. */
    WT_RELEASE_WRITE(disagg->global_checkpoint_id, next_checkpoint_id);
    disagg->num_meta_put_at_ckpt_begin = disagg->num_meta_put;
    return (0);
}

/*
 * __wt_disagg_advance_checkpoint --
 *     Advance to the next checkpoint. If the current checkpoint is 0, just start the next one.
 */
int
__wt_disagg_advance_checkpoint(WT_SESSION_IMPL *session, bool ckpt_success)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    uint64_t checkpoint_id;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    /* Only the leader can advance the global checkpoint ID. */
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0);

    WT_ACQUIRE_READ(checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
    WT_ASSERT(session, checkpoint_id >= WT_DISAGG_CHECKPOINT_ID_FIRST);

    if (ckpt_success)
        WT_RET(disagg->npage_log->page_log->pl_complete_checkpoint(
          disagg->npage_log->page_log, &session->iface, checkpoint_id));

    WT_RET(__wt_disagg_begin_checkpoint(session, checkpoint_id + 1));

    return (0);
}

/*
 * __layered_move_updates --
 *     Move the updates of a key to the stable table
 */
static int
__layered_move_updates(WT_CURSOR_BTREE *cbt, WT_ITEM *key, WT_UPDATE *upds)
{
    /* Search the page. */
    WT_RET(__wt_row_search(cbt, key, true, NULL, false, NULL));

    /* Apply the modification. */
    WT_RET(__wt_row_modify(cbt, key, NULL, &upds, WT_UPDATE_INVALID, false, false));

    return (0);
}

/*
 * __layered_drain_ingest_table --
 *     Moving all the data from a single ingest table to the corresponding stable table
 */
static int
__layered_drain_ingest_table(WT_SESSION_IMPL *session, WT_LAYERED_TABLE_MANAGER_ENTRY *entry)
{
    WT_CURSOR *stable_cursor, *version_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(tmp_key);
    WT_DECL_ITEM(value);
    WT_DECL_RET;
    WT_TIME_WINDOW tw;
    WT_UPDATE *prev_upd, *tombstone, *upd, *upds;
    uint8_t flags, location, prepare, type;
    int cmp;
    char buf[256];
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL, NULL, NULL};

    WT_RET(__layered_table_get_constituent_cursor(session, entry->ingest_id, &stable_cursor));
    cbt = (WT_CURSOR_BTREE *)stable_cursor;
    WT_RET(__wt_snprintf(buf, sizeof(buf),
      "debug=(dump_version=(enabled=true,visible_only=true,start_timestamp=%" PRIx64 "))",
      S2C(session)->txn_global.stable_timestamp));
    cfg[1] = buf;
    WT_RET(__wt_open_cursor(session, entry->ingest_uri, NULL, cfg, &version_cursor));
    /* We only care about binary data. */
    F_SET(version_cursor, WT_CURSTD_RAW);

    prev_upd = tombstone = upd = upds = NULL;

    WT_ERR(__wt_scr_alloc(session, 0, &key));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp_key));
    WT_ERR(__wt_scr_alloc(session, 0, &value));

    for (;;) {
        tombstone = upd = NULL;
        WT_ERR_NOTFOUND_OK(ret = version_cursor->next(version_cursor), true);
        if (ret == WT_NOTFOUND) {
            if (key->size > 0 && upds != NULL)
                WT_ERR(__layered_move_updates(cbt, key, upds));
            break;
        }

        WT_ERR(version_cursor->get_key(version_cursor, tmp_key));
        WT_ERR(__wt_compare(session, CUR2BT(cbt)->collator, key, tmp_key, &cmp));
        if (cmp != 0) {
            WT_ASSERT(session, cmp <= 0);

            if (upds != NULL)
                WT_ERR(__layered_move_updates(cbt, key, upds));

            upds = NULL;
            prev_upd = NULL;
            WT_ERR(__wt_buf_set(session, key, tmp_key->data, tmp_key->size));
        }

        WT_ERR(version_cursor->get_value(version_cursor, &tw.start_txn, &tw.start_ts,
          &tw.durable_start_ts, &tw.stop_txn, &tw.stop_ts, &tw.durable_stop_ts, &type, &prepare,
          &flags, &location, value));
        /* We shouldn't seen any prepared updates. */
        WT_ASSERT(session, prepare == 0);

        /*
         * We assume the updates returned will be in timestamp order. TODO: ensure the version
         * cursor don't return duplicate values to honor the timestamp ordering.
         */
        if (prev_upd != NULL) {
            WT_ASSERT(session,
              tw.stop_txn <= prev_upd->txnid && tw.stop_ts <= prev_upd->start_ts &&
                tw.durable_stop_ts <= prev_upd->durable_ts);
            WT_ASSERT(session,
              tw.start_txn <= prev_upd->txnid && tw.start_ts <= prev_upd->start_ts &&
                tw.durable_start_ts <= prev_upd->durable_ts);
            if (tw.stop_txn != prev_upd->txnid || tw.stop_txn != prev_upd->start_ts ||
              tw.durable_stop_ts != prev_upd->durable_ts)
                WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
        } else if (WT_TIME_WINDOW_HAS_STOP(&tw))
            WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));

        WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
        upd->txnid = tw.start_txn;
        upd->start_ts = tw.start_ts;
        upd->durable_ts = tw.durable_start_ts;

        if (tombstone != NULL) {
            tombstone->txnid = tw.stop_txn;
            tombstone->start_ts = tw.start_ts;
            tombstone->durable_ts = tw.durable_start_ts;
            tombstone->next = upd;

            if (prev_upd != NULL)
                prev_upd->next = tombstone;
            else {
                prev_upd = upd;
                upds = tombstone;
            }
            tombstone = NULL;
            upd = NULL;
        } else {
            if (prev_upd != NULL)
                prev_upd->next = upd;
            else {
                prev_upd = upd;
                upds = upd;
            }
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
    return (ret);
}

/*
 * __layered_drain_ingest_tables --
 *     Moving all the data from the ingest tables to the stable tables
 */
static int
__layered_drain_ingest_tables(WT_SESSION_IMPL *session)
{
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    size_t i;

    manager = &S2C(session)->layered_table_manager;

    /* The table count never shrinks, so this is safe. It probably needs the layered table lock */
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL && entry->accumulated_write_bytes > 0)
            WT_RET(__layered_drain_ingest_table(session, entry));
    }

    return (0);
}
