/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

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
    char buf[4096], *cfg_ret,
      *metadata_value_cfg; /* TODO the 4096 puts an upper bound on metadata entry length */
    const char *cfg[3], *current_value, *metadata_key, *metadata_value;
    size_t len, metadata_value_cfg_len, sep;
    uint64_t global_checkpoint_id;

    conn = S2C(session);

    cursor = NULL;
    internal_session = NULL;
    md_cursor = NULL;
    metadata_key = NULL;
    metadata_value = NULL;
    metadata_value_cfg = NULL;
    shared_metadata_session = NULL;

    WT_ERR(__wt_scr_alloc(session, 4096, &item));

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    WT_ACQUIRE_READ(global_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);

    /* Check the checkpoint ID to ensure that we are not going backwards. */
    if (checkpoint_id + 1 < global_checkpoint_id)
        WT_ERR_MSG(session, EINVAL, "Global checkpoint ID went backwards: %" PRIu64 " -> %" PRIu64,
          global_checkpoint_id - 1, checkpoint_id);

    /*
     * Part 1: Get the metadata of the shared metadata table and insert it into our metadata table.
     */

    /* Read the checkpoint metadata of the shared metadata table from the special metadata page. */
    WT_ERR(__wt_disagg_get_meta(session, WT_DISAGG_METADATA_MAIN_PAGE_ID, checkpoint_id, item));

    if (item->size >= sizeof(buf))
        WT_ERR(ENOMEM);
    memcpy(buf, item->data, item->size);
    buf[item->size] = '\0';
    if (item->size > 0 && buf[item->size - 1] == '\n')
        buf[item->size - 1] = '\0';

    /* Parse out the key and the new checkpoint config value. */
    metadata_key = buf;
    for (sep = 0; sep < item->size; sep++)
        if (buf[sep] == '\n') {
            buf[sep] = '\0';
            metadata_value = buf + sep + 1;
            break;
        }
    if (metadata_value == NULL)
        WT_ERR(EINVAL);

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

    /* Scan the metadata table. */
    cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
    cfg[1] = "checkpoint=" WT_CHECKPOINT ",checkpoint_use_history=false";
    cfg[2] = NULL;
    WT_ERR(__wt_open_cursor(shared_metadata_session, WT_DISAGG_METADATA_URI, NULL, cfg, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &metadata_key));
        WT_ERR(cursor->get_value(cursor, &metadata_value));

        md_cursor->set_key(md_cursor, metadata_key);
        WT_ERR_NOTFOUND_OK(md_cursor->search(md_cursor), true);

        if (ret == 0) {
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

            /* Put our new config in */
            md_cursor->set_value(md_cursor, cfg_ret);
            WT_ERR(md_cursor->insert(md_cursor));
        } else {
            /* New table: Insert new metadata. */
            /* TODO: Verify that there is no btree ID conflict. */
            md_cursor->set_value(md_cursor, metadata_value);
            WT_ERR(md_cursor->insert(md_cursor));
            /* TODO: Create the corresponding oligarch table metadata. */
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
    WT_STAT_CONN_DSRC_INCR(session, oligarch_manager_checkpoints_refreshed);

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
    __wt_free(session, metadata_value_cfg);
    __wt_scr_free(session, &item);
    return (ret);
}

/*
 * __wt_oligarch_manager_start --
 *     Start the oligarch manager thread
 */
int
__wt_oligarch_manager_start(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_OLIGARCH_MANAGER *manager;
    uint32_t session_flags;

    conn = S2C(session);
    manager = &conn->oligarch_manager;

    /* It's possible to race - only start the manager if we are the winner */
    if (!__wt_atomic_cas32(
          &manager->state, WT_OLIGARCH_MANAGER_OFF, WT_OLIGARCH_MANAGER_STARTING)) {
        /* This isn't optimal, but it'll do. It's uncommon for multiple threads to be trying to
         * start the oligarch manager at the same time.
         * It's probably fine for any "loser" to proceed without waiting, but be conservative
         * and have a semantic where a return from this function indicates a running oligarch
         * manager->
         */
        while (__wt_atomic_load32(&manager->state) != WT_OLIGARCH_MANAGER_RUNNING)
            __wt_sleep(0, 1000);
        return (0);
    }

    WT_RET(__wt_spin_init(session, &manager->oligarch_lock, "oligarch manager"));

    /*
     * Be lazy for now, allow for up to 1000 files to be allocated. In the future this should be
     * able to grow dynamically and a more conservative number used here. Until then oligarch table
     * application will crash in a system with more than 1000 files.
     */
    manager->open_oligarch_table_count = conn->next_file_id + 1000;
    WT_ERR(__wt_calloc(session, sizeof(WT_OLIGARCH_MANAGER_ENTRY *),
      manager->open_oligarch_table_count, &manager->entries));

    session_flags = WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL;
    WT_ERR(__wt_thread_group_create(session, &manager->threads, "oligarch-manager",
      WT_OLIGARCH_THREAD_COUNT, WT_OLIGARCH_THREAD_COUNT, session_flags,
      __wt_oligarch_manager_thread_chk, __wt_oligarch_manager_thread_run, NULL));

    WT_MAX_LSN(&manager->max_replay_lsn);

    WT_STAT_CONN_SET(session, oligarch_manager_running, 1);
    __wt_verbose_level(
      session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5, "%s", "__wt_oligarch_manager_start");
    FLD_SET(conn->server_flags, WT_CONN_SERVER_OLIGARCH);

    /* Now that everything is setup, allow the manager to be used. */
    __wt_atomic_store32(&manager->state, WT_OLIGARCH_MANAGER_RUNNING);
    return (0);

err:
    /* Quit the oligarch server. */
    WT_TRET(__wt_oligarch_manager_destroy(session, false));
    return (ret);
}

/*
 * __wt_oligarch_manager_thread_chk --
 *     Check to decide if the oligarch manager thread should continue running
 */
bool
__wt_oligarch_manager_thread_chk(WT_SESSION_IMPL *session)
{
    if (!S2C(session)->oligarch_manager.leader)
        return (false);
    return (
      __wt_atomic_load32(&S2C(session)->oligarch_manager.state) == WT_OLIGARCH_MANAGER_RUNNING);
}

/*
 * __wt_oligarch_manager_add_table --
 *     Add a table to the oligarch manager when it's opened
 */
int
__wt_oligarch_manager_add_table(WT_SESSION_IMPL *session, uint32_t ingest_id, uint32_t stable_id)
{
    WT_CONNECTION_IMPL *conn;
    WT_OLIGARCH *oligarch;
    WT_OLIGARCH_MANAGER *manager;
    WT_OLIGARCH_MANAGER_ENTRY *entry;

    conn = S2C(session);
    manager = &conn->oligarch_manager;
    fprintf(stderr, "adding %u to oligarch manager\n", ingest_id);

    WT_ASSERT_ALWAYS(session, session->dhandle->type == WT_DHANDLE_TYPE_OLIGARCH,
      "Adding an oligarch tree to tracking without the right dhandle context.");
    oligarch = (WT_OLIGARCH *)session->dhandle;

    WT_ASSERT_ALWAYS(session, __wt_atomic_load32(&manager->state) == WT_OLIGARCH_MANAGER_RUNNING,
      "Adding an oligarch table, but the manager isn't running");
    __wt_spin_lock(session, &manager->oligarch_lock);
    /* Diagnostic sanity check - don't keep adding the same table */
    if (manager->entries[ingest_id] != NULL)
        WT_IGNORE_RET(__wt_panic(session, WT_PANIC,
          "Internal server error: opening the same oligarch table multiple times"));
    WT_RET(__wt_calloc_one(session, &entry));
    entry->ingest_id = ingest_id;
    entry->stable_id = stable_id;
    entry->stable_cursor = NULL;
    entry->oligarch_table = (WT_OLIGARCH *)session->dhandle;

    /*
     * There is a bootstrapping problem. Use the global oldest ID as a starting point. Nothing can
     * have been written into the ingest table, so it will be a conservative choice.
     */
    entry->checkpoint_txn_id = __wt_atomic_loadv64(&conn->txn_global.oldest_id);
    WT_ACQUIRE_READ(entry->read_checkpoint, conn->disaggregated_storage.global_checkpoint_id);

    /*
     * It's safe to just reference the same string. The lifecycle of the oligarch tree is longer
     * than it will live in the tracker here.
     */
    entry->stable_uri = oligarch->stable_uri;
    WT_STAT_CONN_INCR(session, oligarch_manager_tables);
    __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5,
      "__wt_oligarch_manager_add_table uri=%s ingest=%" PRIu32 " stable=%" PRIu32 " name=%s",
      entry->stable_uri, ingest_id, stable_id, session->dhandle->name);
    manager->entries[ingest_id] = entry;

    __wt_spin_unlock(session, &manager->oligarch_lock);
    return (0);
}

/*
 * __oligarch_manager_remove_table_inlock --
 *     Internal table remove implementation.
 */
static void
__oligarch_manager_remove_table_inlock(
  WT_SESSION_IMPL *session, uint32_t ingest_id, bool from_shutdown)
{
    WT_OLIGARCH_MANAGER *manager;
    WT_OLIGARCH_MANAGER_ENTRY *entry;

    manager = &S2C(session)->oligarch_manager;

    if ((entry = manager->entries[ingest_id]) != NULL) {
        WT_STAT_CONN_DECR(session, oligarch_manager_tables);
        __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5,
          "__wt_oligarch_manager_remove_table %s", entry->stable_uri);

        /* Cursors get automatically closed via the session handle in shutdown. */
        if (!from_shutdown && entry->stable_cursor != NULL)
            entry->stable_cursor->close(entry->stable_cursor);
        __wt_free(session, entry);
        fprintf(stderr, "oligarch mgr clearing %u\n", ingest_id);
        manager->entries[ingest_id] = NULL;
    }
}

/*
 * __wt_oligarch_manager_remove_table --
 *     Remove a table to the oligarch manager when it's opened. Note that it is always safe to
 *     remove a table from tracking immediately here. It will only be removed when the handle is
 *     closed and a handle is only closed after a checkpoint has completed that included all writes
 *     to the table. By that time the processor would have finished with any records from the
 *     oligarch table.
 */
void
__wt_oligarch_manager_remove_table(WT_SESSION_IMPL *session, uint32_t ingest_id)
{
    WT_OLIGARCH_MANAGER *manager;
    uint32_t manager_state;

    manager = &S2C(session)->oligarch_manager;

    manager_state = __wt_atomic_load32(&manager->state);

    /* Shutdown calls this redundantly - ignore cases when the manager is already closed. */
    if (manager_state == WT_OLIGARCH_MANAGER_OFF)
        return;

    WT_ASSERT_ALWAYS(session,
      manager_state == WT_OLIGARCH_MANAGER_RUNNING || manager_state == WT_OLIGARCH_MANAGER_STOPPING,
      "Adding an oligarch table, but the manager isn't running");
    __wt_spin_lock(session, &manager->oligarch_lock);
    __oligarch_manager_remove_table_inlock(session, ingest_id, false);

    __wt_spin_unlock(session, &manager->oligarch_lock);
}

/*
 * __oligarch_get_constituent_cursor --
 *     Retrieve or open a constituent cursor for an oligarch tree.
 */
static int
__oligarch_get_constituent_cursor(WT_SESSION_IMPL *session, uint32_t ingest_id, WT_CURSOR **cursorp)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *stable_cursor;
    WT_OLIGARCH_MANAGER_ENTRY *entry;
    uint64_t global_ckpt_id;

    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL, NULL};

    conn = S2C(session);
    entry = conn->oligarch_manager.entries[ingest_id];

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
 * __oligarch_manager_checkpoint_locked --
 *     Trigger a checkpoint of the handle - will acquire necessary locks
 */
static int
__oligarch_manager_checkpoint_locked(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    WT_STAT_CONN_DSRC_INCR(session, oligarch_manager_checkpoints);
    WT_WITH_CHECKPOINT_LOCK(
      session, WT_WITH_SCHEMA_LOCK(session, ret = __wt_checkpoint(session, 0)));
    return (ret);
}

/*
 * __oligarch_manager_checkpoint_one --
 *     Review the oligarch tables and checkpoint one if it has enough accumulated content. For now
 *     this just checkpoints the first table that meets the threshold. In the future it should be
 *     more fair in selecting a table.
 */
static int
__oligarch_manager_checkpoint_one(WT_SESSION_IMPL *session)
{
    WT_BTREE *ingest_btree;
    WT_CURSOR *stable_cursor;
    WT_DECL_RET;
    WT_OLIGARCH_MANAGER *manager;
    WT_OLIGARCH_MANAGER_ENTRY *entry;
    WT_TXN_ISOLATION saved_isolation;
    uint64_t satisfied_txn_id;
    uint32_t i;

    manager = &S2C(session)->oligarch_manager;

    /* The table count never shrinks, so this is safe. It probably needs the oligarch lock */
    for (i = 0; i < manager->open_oligarch_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL &&
          entry->accumulated_write_bytes > WT_OLIGARCH_TABLE_CHECKPOINT_THRESHOLD) {
            /*
             * Retrieve the current tranasaction ID - ensure it actually gets read from the shared
             * variable here, it would lead to data loss if it was read later and included
             * transaction IDs that aren't included in the checkpoint. It's OK for it to miss IDs -
             * this requires an "at least as much" guarantee, not an exact match guarantee.
             */
            WT_READ_ONCE(satisfied_txn_id, manager->max_applied_txnid);
            __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5,
              "oligarch table %s being checkpointed, satisfied txnid=%" PRIu64, entry->stable_uri,
              satisfied_txn_id);

            WT_RET(__oligarch_get_constituent_cursor(session, entry->ingest_id, &stable_cursor));
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
              ret = __oligarch_manager_checkpoint_locked(session));
            WT_TRET(__wt_meta_track_off(session, false, ret != 0));
            session->txn->isolation = saved_isolation;
            if (ret == 0) {
                entry->checkpoint_txn_id = satisfied_txn_id;
                ingest_btree = (WT_BTREE *)entry->oligarch_table->ingest->handle;
                WT_ASSERT_ALWAYS(session, F_ISSET(ingest_btree, WT_BTREE_GARBAGE_COLLECT),
                  "Ingest table not setup for garbage collection");
                ingest_btree->oldest_live_txnid = satisfied_txn_id;
            }

            /* We've done (or tried to do) a checkpoint - that's it. */
            return (ret);
        } else if (entry != NULL) {
            __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5,
              "not checkpointing table %s bytes=%" PRIu64, entry->stable_uri,
              entry->accumulated_write_bytes);
        }
    }

    WT_STAT_CONN_SET(session, oligarch_manager_checkpoint_candidates, i);
    return (0);
}

/*
 * __oligarch_log_replay_op_apply --
 *     Apply a transactional operation during recovery.
 */
static int
__oligarch_log_replay_op_apply(
  WT_SESSION_IMPL *session, WT_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
    WT_CURSOR *stable_cursor;
    WT_DECL_RET;
    WT_ITEM key, start_key, stop_key, value;
    WT_OLIGARCH_MANAGER *manager;
    WT_OLIGARCH_MANAGER_ENTRY *entry;
    wt_timestamp_t commit, durable, first_commit, prepare, read;
    uint64_t recno, start_recno, stop_recno, t_nsec, t_sec;
    uint32_t fileid, mode, opsize, optype;
    bool applied;

    manager = &S2C(session)->oligarch_manager;
    stable_cursor = NULL;
    applied = false;
    fileid = 0;
    memset(&value, 0, sizeof(WT_ITEM));

    /* Peek at the size and the type. */
    WT_ERR(__wt_oligarch_logop_read(session, pp, end, &optype, &opsize));
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
        WT_ERR(__wt_oligarch_logop_col_modify_unpack(session, pp, end, &fileid, &recno, &value));
        break;
    case WT_LOGOP_COL_PUT:
        WT_ERR(__wt_oligarch_logop_col_put_unpack(session, pp, end, &fileid, &recno, &value));
        break;
    case WT_LOGOP_COL_REMOVE:
        WT_ERR(__wt_oligarch_logop_col_remove_unpack(session, pp, end, &fileid, &recno));
        break;
    case WT_LOGOP_COL_TRUNCATE:
        WT_ERR(__wt_oligarch_logop_col_truncate_unpack(
          session, pp, end, &fileid, &start_recno, &stop_recno));
        break;
    case WT_LOGOP_ROW_MODIFY:
        WT_ERR(__wt_oligarch_logop_row_modify_unpack(session, pp, end, &fileid, &key, &value));
        if ((entry = manager->entries[fileid]) != NULL) {
            WT_ERR(__oligarch_get_constituent_cursor(session, fileid, &stable_cursor));
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
        WT_ERR(__wt_oligarch_logop_row_put_unpack(session, pp, end, &fileid, &key, &value));
        if ((entry = manager->entries[fileid]) != NULL) {
            /*
        __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_1,
          "oligarch log application row store put applying to stable table %s", entry->ingest_uri);
          */
            WT_ERR(__oligarch_get_constituent_cursor(session, fileid, &stable_cursor));
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
        WT_ERR(__wt_oligarch_logop_row_remove_unpack(session, pp, end, &fileid, &key));
        if ((entry = manager->entries[fileid]) != NULL) {
            WT_ERR(__oligarch_get_constituent_cursor(session, fileid, &stable_cursor));
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
        WT_ERR(__wt_oligarch_logop_row_truncate_unpack(
          session, pp, end, &fileid, &start_key, &stop_key, &mode));
        break;
    case WT_LOGOP_TXN_TIMESTAMP:
        /*
         * Timestamp records are informational only. We have to unpack it to properly move forward
         * in the log record to the next operation, but otherwise ignore.
         */
        WT_ERR(__wt_oligarch_logop_txn_timestamp_unpack(
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
        WT_STAT_CONN_DSRC_INCR(session, oligarch_manager_logops_skipped);
        __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_1,
          "oligarch log application skipped a record associated with oligarch tree. Record type: "
          "%" PRIu32,
          optype);
    } else {
        if (applied)
            WT_STAT_CONN_DSRC_INCR(session, oligarch_manager_logops_applied);
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
      optype, lsnp->l.file, __wt_oligarch_lsn_offset(lsnp));
    return (ret);
}

/*
 * __oligarch_log_replay_commit_apply --
 *     Apply a commit record during oligarch log replay.
 */
static int
__oligarch_log_replay_commit_apply(
  WT_SESSION_IMPL *session, WT_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
    /*
     * __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_1, "%s",
     *   "oligarch log application commit applying");
     */

    /* The logging subsystem zero-pads records. */
    while (*pp < end && **pp)
        WT_RET(__oligarch_log_replay_op_apply(session, lsnp, pp, end));

    return (0);
}

/*
 * __oligarch_log_replay --
 *     Review a log record and replay it against an oligarch stable constituent if relevant. This
 *     could be done in a number of ways, including: * Generalize the code in
 *     txn_recover::__txn_op_apply and its callers to work for this runtime case and apply
 *     operations to a different file identifier. * Create a simplified duplicate of the recovery
 *     code. * Use the log cursor implementation as-is, and see how far we get. * Implement a new
 *     log cursor, or extend existing to be more closely aligned with this need. I chose the
 *     simplified duplicate approach for now - it was most expedient, debuggable and performant.
 *     Long term we might want to do something different.
 */
static int
__oligarch_log_replay(WT_SESSION_IMPL *session, WT_ITEM *logrec, WT_LSN *lsnp, WT_LSN *next_lsnp,
  void *cookie, int firstrecord)
{
    WT_DECL_RET;
    WT_OLIGARCH_MANAGER *manager;
    uint64_t txnid;
    uint32_t rectype;
    const uint8_t *end, *p;

    manager = &S2C(session)->oligarch_manager;
    p = WT_LOG_SKIP_HEADER(logrec->data);
    end = (const uint8_t *)logrec->data + logrec->size;
    /* If this becomes multi-threaded we might move the context from manager here */
    WT_UNUSED(cookie);
    WT_UNUSED(firstrecord);

    if (!manager->leader)
        return (0);

    /* First, peek at the log record type. */
    WT_RET(__wt_oligarch_logrec_read(session, &p, end, &rectype));

    /* We are only ever interested in commit records */
    if (rectype != WT_LOGREC_COMMIT)
        return (0);

    if (!WT_IS_MAX_LSN(&manager->max_replay_lsn) &&
      __wt_oligarch_log_cmp(lsnp, &manager->max_replay_lsn) < 0) {
        WT_STAT_CONN_DSRC_INCR(session, oligarch_manager_skip_lsn);
        __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_1,
          "Oligarch skipping previously applied LSN: [%" PRIu32 "][%" PRIu32 "]", lsnp->l.file,
          lsnp->l.offset);
        return (0);
    }

    if ((ret = __wt_vunpack_uint(&p, WT_PTRDIFF(end, p), &txnid)) != 0)
        WT_RET_MSG(session, ret, "oligarch_log_replay: unpack failure");
    WT_RET(__oligarch_log_replay_commit_apply(session, lsnp, &p, end));

    /* Record the highest LSN we've processed so future scans can start from there. */
    WT_ASSIGN_LSN(&manager->max_replay_lsn, next_lsnp);
    /* This will need to be made thread-safe if log application becomes multi-threaded */
    manager->max_applied_txnid = txnid;

    return (0);
}

/*
 * __wt_oligarch_manager_thread_run --
 *     Entry function for an oligarch manager thread. This is called repeatedly from the thread
 *     group code so it does not need to loop itself.
 */
int
__wt_oligarch_manager_thread_run(WT_SESSION_IMPL *session_shared, WT_THREAD *thread)
{
    WT_DECL_RET;
    WT_OLIGARCH_MANAGER *manager;
    WT_SESSION_IMPL *session;

    WT_UNUSED(session_shared);
    session = thread->session;
    WT_ASSERT(session, session->id != 0);
    manager = &S2C(session)->oligarch_manager;

    WT_STAT_CONN_SET(session, oligarch_manager_active, 1);

    /*
     * There are two threads: let one do log replay and the other checkpoints. For now use just the
     * first thread in the group for log application, otherwise the way cursors are saved in the
     * manager queue gets confused (since they are associated with sessions).
     */
    /* __wt_verbose_level(session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5, "%s",
     * "__wt_oligarch_manager_thread_run"); */
    if (thread->id == 0 && __wt_atomic_load32(&manager->log_applying) == 0 &&
      __wt_atomic_cas32(&manager->log_applying, 0, 1)) {
        if (WT_IS_MAX_LSN(&manager->max_replay_lsn))
            ret = __wt_oligarch_log_scan(
              session, NULL, NULL, WT_LOGSCAN_FIRST, __oligarch_log_replay, NULL);
        else
            ret = __wt_oligarch_log_scan(
              session, &manager->max_replay_lsn, NULL, 0, __oligarch_log_replay, NULL);

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
        WT_RET(__oligarch_manager_checkpoint_one(session));

    WT_STAT_CONN_SET(session, oligarch_manager_active, 0);

    /* Sometimes the logging subsystem is still getting started and ENOENT is expected */
    if (ret == ENOENT)
        ret = 0;
    return (ret);
}

/*
 * __wt_oligarch_manager_get_pinned_id --
 *     Retrieve the oldest checkpoint ID that's relevant to garbage collection
 */
void
__wt_oligarch_manager_get_pinned_id(WT_SESSION_IMPL *session, uint64_t *pinnedp)
{
    WT_OLIGARCH_MANAGER *manager;
    WT_OLIGARCH_MANAGER_ENTRY *entry;
    uint64_t pinned;
    uint32_t i;

    manager = &S2C(session)->oligarch_manager;

    /* If no tables are being managed, then don't pin anything */
    pinned = WT_TXN_MAX;
    for (i = 0; i < manager->open_oligarch_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL && WT_TXNID_LT(entry->checkpoint_txn_id, pinned))
            pinned = entry->checkpoint_txn_id;
    }

    *pinnedp = pinned;

    WT_STAT_CONN_SET(session, oligarch_manager_pinned_id_tables_searched, i);
}

/*
 * __wt_oligarch_manager_destroy --
 *     Destroy the oligarch manager thread(s)
 */
int
__wt_oligarch_manager_destroy(WT_SESSION_IMPL *session, bool from_shutdown)
{
    WT_CONNECTION_IMPL *conn;
    WT_OLIGARCH_MANAGER *manager;
    uint32_t i;

    conn = S2C(session);
    manager = &conn->oligarch_manager;

    __wt_verbose_level(
      session, WT_VERB_OLIGARCH, WT_VERBOSE_DEBUG_5, "%s", "__wt_oligarch_manager_destroy");

    if (__wt_atomic_load32(&manager->state) == WT_OLIGARCH_MANAGER_OFF)
        return (0);

    /*
     * Spin until exclusive access is gained. If we got here from the startup path seeing an error,
     * the state might still be "starting" rather than "running".
     */
    while (!__wt_atomic_cas32(
             &manager->state, WT_OLIGARCH_MANAGER_RUNNING, WT_OLIGARCH_MANAGER_STOPPING) &&
      !__wt_atomic_cas32(
        &manager->state, WT_OLIGARCH_MANAGER_STARTING, WT_OLIGARCH_MANAGER_STOPPING)) {
        /* If someone beat us to it, we are done */
        if (__wt_atomic_load32(&manager->state) == WT_OLIGARCH_MANAGER_OFF)
            return (0);
        __wt_sleep(0, 1000);
    }

    /* Ensure other things that engage with the oligarch server know it's gone. */
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_OLIGARCH);

    __wt_spin_lock(session, &manager->oligarch_lock);

    /* Let any running threads finish up. */
    __wt_cond_signal(session, manager->threads.wait_cond);
    __wt_writelock(session, &manager->threads.lock);

    WT_RET(__wt_thread_group_destroy(session, &manager->threads));

    /* Close any cursors and free any related memory */
    for (i = 0; i < manager->open_oligarch_table_count; i++) {
        if (manager->entries[i] != NULL)
            __oligarch_manager_remove_table_inlock(session, i, from_shutdown);
    }
    __wt_free(session, manager->entries);
    manager->open_oligarch_table_count = 0;
    WT_MAX_LSN(&manager->max_replay_lsn);

    __wt_atomic_store32(&manager->state, WT_OLIGARCH_MANAGER_OFF);
    WT_STAT_CONN_SET(session, oligarch_manager_running, 0);

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
    WT_ERR(
      __wt_session_create(internal_session, WT_DISAGG_METADATA_URI, "key_format=S,value_format=S"));

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
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_NAMED_PAGE_LOG *npage_log;
    WT_NAMED_STORAGE_SOURCE *nstorage;
    WT_STORAGE_SOURCE *storage;
    uint64_t checkpoint_id, next_checkpoint_id;
    bool leader, was_leader;

    conn = S2C(session);
    leader = was_leader = conn->oligarch_manager.leader;
    npage_log = NULL;
    bstorage = NULL;
    nstorage = NULL;

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
        next_checkpoint_id = 0;

    /* Set the role. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.role", &cval));
    if (cval.len == 0)
        conn->oligarch_manager.leader = leader = false;
    else {
        if (WT_CONFIG_LIT_MATCH("follower", cval))
            conn->oligarch_manager.leader = leader = false;
        else if (WT_CONFIG_LIT_MATCH("leader", cval))
            conn->oligarch_manager.leader = leader = true;
        else
            WT_ERR_MSG(session, EINVAL, "Invalid node role");

        /* Follower step-up. */
        if (reconfig && !was_leader && leader) {
            /*
             * Note that we should have picked up a new checkpoint ID above. Now that we are the new
             * leader, we need to begin the next checkpoint.
             */
            if (next_checkpoint_id == 0)
                WT_ACQUIRE_READ(
                  next_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
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
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.storage_source", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &conn->disaggregated_storage.storage_source));

    /* Setup any configured page log. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.page_log", &cval));
    WT_ERR(__wt_schema_open_page_log(session, &cval, &npage_log));
    conn->disaggregated_storage.npage_log = npage_log;

    /* Setup any configured storage source on the data handle */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.storage_source", &cval));
    WT_ERR(__wt_schema_open_storage_source(session, &cval, &nstorage));

    /* TODO: Deduplicate this with __btree_setup_storage_source */
    if (nstorage != NULL) {
        WT_ERR(__wt_calloc_one(session, &bstorage));
        bstorage->auth_token = NULL;
        bstorage->cache_directory = NULL;
        WT_ERR(__wt_strndup(session, "foo", 3, &bstorage->bucket));
        WT_ERR(__wt_strndup(session, "bar", 3, &bstorage->bucket_prefix));

        storage = nstorage->storage_source;
        WT_ERR(storage->ss_customize_file_system(
          storage, &session->iface, bstorage->bucket, "", NULL, &bstorage->file_system));
        bstorage->storage_source = storage;

        F_SET(bstorage, WT_BUCKET_FREE);
        conn->disaggregated_storage.bstorage = bstorage;
        conn->disaggregated_storage.nstorage = nstorage;
    }

    /* Set up a handle for accessing shared metadata. */
    if (npage_log != NULL) {
        WT_ERR(npage_log->page_log->pl_open_handle(npage_log->page_log, &session->iface,
          WT_DISAGG_METADATA_TABLE_ID, &conn->disaggregated_storage.page_log_meta));
    }
    if (bstorage != NULL) {
        WT_WITH_BUCKET_STORAGE(bstorage, session, {
            WT_FILE_SYSTEM *fs = bstorage->file_system;
            ret =
              fs->fs_open_file(fs, &session->iface, "metadata.meta", WT_FS_OPEN_FILE_TYPE_REGULAR,
                WT_FS_OPEN_CREATE, &conn->disaggregated_storage.bstorage_meta);
        });
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
            checkpoint_id = 0;

        /* If we are starting as primary (e.g., for internal testing), begin the checkpoint. */
        if (leader) {
            if (next_checkpoint_id == 0)
                next_checkpoint_id = checkpoint_id + 1;
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __wt_disagg_begin_checkpoint(session, next_checkpoint_id));
            WT_ERR(ret);
        }
    }

    if (0) {
err:
        if (bstorage != NULL && conn->disaggregated_storage.bstorage == NULL) {
            __wt_free(session, bstorage->bucket);
            __wt_free(session, bstorage->bucket_prefix);
            __wt_free(session, bstorage);
        }
    }

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

    return (disagg->page_log_meta != NULL || disagg->bstorage_meta != NULL);
}

/*
 * __wti_disagg_destroy_conn_config --
 *     Free the disaggregated storage state.
 */
int
__wti_disagg_destroy_conn_config(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_DISAGGREGATED_STORAGE *disagg;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    __wt_free(session, disagg->page_log);
    __wt_free(session, disagg->stable_prefix);
    __wt_free(session, disagg->storage_source);

    /* Close the metadata handles. */
    if (disagg->page_log_meta != NULL) {
        WT_TRET(disagg->page_log_meta->plh_close(disagg->page_log_meta, &session->iface));
        disagg->page_log_meta = NULL;
    }
    if (disagg->bstorage_meta != NULL) {
        WT_TRET(disagg->bstorage_meta->close(disagg->bstorage_meta, &session->iface));
        disagg->bstorage_meta = NULL;
    }

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
    WT_ITEM result;
    WT_PAGE_LOG_GET_ARGS get_args;
    u_int count;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    WT_CLEAR(result);
    WT_CLEAR(get_args);

    if (disagg->page_log_meta != NULL) {
        WT_ASSERT(session, disagg->bstorage_meta == NULL);
        count = 1;
        WT_RET(disagg->page_log_meta->plh_get(disagg->page_log_meta, &session->iface, page_id,
          checkpoint_id, &get_args, &result, &count));
        /* TODO: Add retries if the metadata is not found - maybe it was not yet materialized. */
        WT_ASSERT(session, count == 1 && get_args.delta_count == 0); /* TODO: corrupt data */
        *item = result;
        return (0);
    }

    if (disagg->bstorage_meta != NULL) {
        WT_RET(
          disagg->bstorage_meta->fh_obj_checkpoint_load(disagg->bstorage_meta, &session->iface));
        return (
          disagg->bstorage_meta->fh_obj_get(disagg->bstorage_meta, &session->iface, page_id, item));
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
        WT_ASSERT(session, disagg->bstorage_meta == NULL);
        WT_RET(disagg->page_log_meta->plh_put(
          disagg->page_log_meta, &session->iface, page_id, checkpoint_id, &put_args, item));
        __wt_atomic_addv64(&disagg->num_meta_put, 1);
        return (0);
    }

    if (disagg->bstorage_meta != NULL) {
        WT_RET(
          disagg->bstorage_meta->fh_obj_put(disagg->bstorage_meta, &session->iface, page_id, item));
        WT_RET(disagg->bstorage_meta->fh_obj_checkpoint(disagg->bstorage_meta, &session->iface));
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
    if (disagg->npage_log == NULL || !conn->oligarch_manager.leader)
        return (0);

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
__wt_disagg_advance_checkpoint(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    uint64_t checkpoint_id;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    /* Only the leader can advance the global checkpoint ID. */
    if (disagg->npage_log == NULL || !conn->oligarch_manager.leader)
        return (0);

    WT_ACQUIRE_READ(checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
    WT_ASSERT(session, checkpoint_id > 0);

    WT_RET(disagg->npage_log->page_log->pl_complete_checkpoint(
      disagg->npage_log->page_log, &session->iface, checkpoint_id));
    WT_RET(__wt_disagg_begin_checkpoint(session, checkpoint_id + 1));

    return (0);
}
