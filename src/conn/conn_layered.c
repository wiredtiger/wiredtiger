/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __layered_drain_ingest_tables(WT_SESSION_IMPL *session);

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
      "in_memory=true,"
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
__disagg_pick_up_checkpoint(WT_SESSION_IMPL *session, uint64_t meta_lsn, uint64_t checkpoint_id)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor, *md_cursor;
    WT_DECL_ITEM(item);
    WT_DECL_RET;
    WT_SESSION_IMPL *internal_session, *shared_metadata_session;
    size_t len, metadata_value_cfg_len;
    uint64_t checkpoint_timestamp, global_checkpoint_id;
    char *buf, *cfg_ret, *checkpoint_config, *metadata_value_cfg, *layered_ingest_uri;
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
    WT_ERR(__wt_disagg_get_meta(
      session, WT_DISAGG_METADATA_MAIN_PAGE_ID, meta_lsn, checkpoint_id, item));

    /* Add the terminating zero byte to the end of the buffer. */
    len = item->size + 1;
    WT_ERR(__wt_calloc_def(session, len, &buf)); /* This already zeroes out the buffer. */
    memcpy(buf, item->data, item->size);

    /* Parse out the checkpoint config string. */
    checkpoint_config = strchr(buf, '\n');
    if (checkpoint_config == NULL)
        WT_ERR_MSG(session, EINVAL, "Invalid checkpoint metadata: No checkpoint config string");
    *checkpoint_config = '\0';
    checkpoint_config++;

    /* Parse the checkpoint config. */
    WT_ERR(__wt_config_getones(session, checkpoint_config, "timestamp", &cval));
    if (cval.len > 0 && cval.val == 0)
        checkpoint_timestamp = WT_TS_NONE;
    else
        WT_ERR(__wt_txn_parse_timestamp(session, "checkpoint", &checkpoint_timestamp, &cval));

    /* Save the metadata key-value pair. */
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
     * Scan the metadata table. Reopen the table to ensure that we are on the most recent
     * checkpoint.
     */
    cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
    cfg[1] = "checkpoint_use_history=false,force=true";
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

    /* Update the checkpoint timestamp. */
    WT_RELEASE_WRITE(conn->disaggregated_storage.last_checkpoint_timestamp, checkpoint_timestamp);

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
 * __disagg_pick_up_checkpoint_meta --
 *     Pick up a new checkpoint from metadata config.
 */
static int
__disagg_pick_up_checkpoint_meta(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *meta_item, uint64_t *idp)
{
    WT_CONFIG_ITEM cval;
    uint64_t checkpoint_id, metadata_lsn;

    /* Extract the arguments. */
    WT_RET(__wt_config_subgets(session, meta_item, "id", &cval));
    checkpoint_id = (uint64_t)cval.val;
    WT_RET(__wt_config_subgets(session, meta_item, "metadata_lsn", &cval));
    metadata_lsn = (uint64_t)cval.val;

    if (idp != NULL)
        *idp = checkpoint_id;

    /* Now actually pick up the checkpoint. */
    return (__disagg_pick_up_checkpoint(session, metadata_lsn, checkpoint_id));
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

    WT_STAT_CONN_SET(session, layered_table_manager_running, 1);
    __wt_verbose_level(
      session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5, "%s", "__wt_layered_table_manager_start");
    FLD_SET(conn->server_flags, WT_CONN_SERVER_LAYERED);

    /* Now that everything is setup, allow the manager to be used. */
    __wt_atomic_store32(&manager->state, WT_LAYERED_TABLE_MANAGER_RUNNING);
    return (0);

err:
    /* Quit the layered table server. */
    WT_TRET(__wt_layered_table_manager_destroy(session));
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
    entry->ingest_uri = layered->ingest_uri;
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
__layered_table_manager_remove_table_inlock(WT_SESSION_IMPL *session, uint32_t ingest_id)
{
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;

    manager = &S2C(session)->layered_table_manager;

    if ((entry = manager->entries[ingest_id]) != NULL) {
        WT_STAT_CONN_DECR(session, layered_table_manager_tables);
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "__wt_layered_table_manager_remove_table stable_uri=%s ingest_id=%" PRIu32,
          entry->stable_uri, ingest_id);

        __wt_free(session, entry);
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
    __layered_table_manager_remove_table_inlock(session, ingest_id);

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

    WT_ACQUIRE_READ(global_ckpt_id, conn->disaggregated_storage.global_checkpoint_id);
    if (global_ckpt_id > entry->read_checkpoint)
        cfg[2] = "force=true";

    /* Open the cursor and keep a reference in the manager entry and our caller */
    WT_RET(__wt_open_cursor(session, entry->stable_uri, NULL, cfg, &stable_cursor));
    entry->read_checkpoint = global_ckpt_id;
    *cursorp = stable_cursor;

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
    WT_SESSION_IMPL *session;

    WT_UNUSED(session_shared);
    session = thread->session;
    WT_ASSERT(session, session->id != 0);

    WT_STAT_CONN_SET(session, layered_table_manager_active, 1);

    /* TODO: now we just sleep. In the future, do whatever we need to do here. */
    __wt_sleep(1, 0);

    WT_STAT_CONN_SET(session, layered_table_manager_active, 0);

    return (0);
}

/*
 * __wt_layered_table_manager_destroy --
 *     Destroy the layered table manager thread(s)
 */
int
__wt_layered_table_manager_destroy(WT_SESSION_IMPL *session)
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
            __layered_table_manager_remove_table_inlock(session, i);
    }
    __wt_free(session, manager->entries);
    manager->open_layered_table_count = 0;

    __wt_atomic_store32(&manager->state, WT_LAYERED_TABLE_MANAGER_OFF);
    WT_STAT_CONN_SET(session, layered_table_manager_running, 0);
    __wt_spin_unlock(session, &manager->layered_table_lock);

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
    WT_ERR(__wt_session_create(
      internal_session, WT_DISAGG_METADATA_URI, "key_format=S,value_format=S,log=(enabled=false)"));

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

    /* Reconfigure-only settings. */
    if (reconfig) {

        /* Pick up a new checkpoint (followers only). */
        WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_meta", &cval));
        if (cval.len > 0) {
            if (leader)
                WT_ERR(EINVAL); /* Leaders can't pick up new checkpoints. */
            else {
                WT_WITH_CHECKPOINT_LOCK(
                  session, ret = __disagg_pick_up_checkpoint_meta(session, &cval, &checkpoint_id));
                WT_ERR(ret);
            }
        } else {
            /* Legacy method (will be deprecated). */
            WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_id", &cval));
            if (cval.len > 0 && cval.val >= 0) {
                if (leader)
                    WT_ERR(EINVAL); /* Leaders can't pick up new checkpoints. */
                else {
                    checkpoint_id = (uint64_t)cval.val;
                    WT_WITH_CHECKPOINT_LOCK(
                      session, ret = __disagg_pick_up_checkpoint(session, 0, checkpoint_id));
                    WT_ERR(ret);
                }
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
            if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE) {
                WT_ACQUIRE_READ(
                  next_checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
                next_checkpoint_id++;
            }
            if (next_checkpoint_id == WT_DISAGG_CHECKPOINT_ID_NONE)
                next_checkpoint_id = WT_DISAGG_CHECKPOINT_ID_FIRST;
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __wt_disagg_begin_checkpoint(session, next_checkpoint_id));
            WT_ERR(ret);

            /* Drain the ingest tables before switching to leader. */
            WT_ERR(__layered_drain_ingest_tables(session));
        }
    }

    /* Connection init settings only. */

    if (reconfig)
        return (0);

    /* Remember the configuration. */
    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.page_log", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &conn->disaggregated_storage.page_log));

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
        WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_meta", &cval));
        if (cval.len > 0) {
            WT_WITH_CHECKPOINT_LOCK(
              session, ret = __disagg_pick_up_checkpoint_meta(session, &cval, &checkpoint_id));
            WT_ERR(ret);
        } else {
            WT_ERR(__wt_config_gets(session, cfg, "disaggregated.checkpoint_id", &cval));
            if (cval.len > 0 && cval.val >= 0) {
                checkpoint_id = (uint64_t)cval.val;
                WT_WITH_CHECKPOINT_LOCK(
                  session, ret = __disagg_pick_up_checkpoint(session, 0, checkpoint_id));
                WT_ERR(ret);
            } else
                /*
                 * TODO: If we are starting with local files, get the checkpoint ID from them?
                 * Alternatively, maybe we should just fail if the checkpoint ID is not specified?
                 */
                checkpoint_id = WT_DISAGG_CHECKPOINT_ID_NONE;
        }

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
    if (ret != 0 && reconfig && !was_leader && leader)
        return (__wt_panic(session, ret, "failed to step-up as primary"));
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
    return (ret);
}

/*
 * __wt_disagg_get_meta --
 *     Read metadata from disaggregated storage.
 */
int
__wt_disagg_get_meta(
  WT_SESSION_IMPL *session, uint64_t page_id, uint64_t lsn, uint64_t checkpoint_id, WT_ITEM *item)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    WT_PAGE_LOG_GET_ARGS get_args;
    u_int count, retry;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    WT_CLEAR(get_args);
    get_args.lsn = lsn;

    if (disagg->page_log_meta != NULL) {
        retry = 0;
        for (;;) {
            count = 1;
            WT_RET(disagg->page_log_meta->plh_get(disagg->page_log_meta, &session->iface, page_id,
              checkpoint_id, &get_args, item, &count));
            WT_ASSERT(session, count <= 1); /* TODO: corrupt data */

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
__wt_disagg_put_meta(WT_SESSION_IMPL *session, uint64_t page_id, uint64_t checkpoint_id,
  const WT_ITEM *item, uint64_t *lsnp)
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
        if (lsnp != NULL)
            *lsnp = put_args.lsn;
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
    WT_DECL_ITEM(meta);
    WT_DECL_RET;
    WT_DISAGGREGATED_STORAGE *disagg;
    wt_timestamp_t checkpoint_timestamp;
    uint64_t checkpoint_id, meta_lsn;

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    WT_RET(__wt_scr_alloc(session, 0, &meta));

    WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock);

    /* Only the leader can advance the global checkpoint ID. */
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0);

    WT_ACQUIRE_READ(meta_lsn, conn->disaggregated_storage.last_checkpoint_meta_lsn);
    WT_ACQUIRE_READ(checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);
    WT_ACQUIRE_READ(checkpoint_timestamp, conn->disaggregated_storage.cur_checkpoint_timestamp);
    WT_ASSERT(session, meta_lsn > 0); /* The metadata page should be written by now. */
    WT_ASSERT(session, checkpoint_id >= WT_DISAGG_CHECKPOINT_ID_FIRST);

    if (ckpt_success) {
        if (disagg->npage_log->page_log->pl_complete_checkpoint_ext == NULL)
            /* Use the legacy method if the new one is not yet available (will be deprecated). */
            WT_ERR(disagg->npage_log->page_log->pl_complete_checkpoint(
              disagg->npage_log->page_log, &session->iface, checkpoint_id));
        else {
            /*
             * Important: To keep testing simple, keep the metadata to be a valid configuration
             * string without quotation marks or escape characters.
             */
            WT_ERR(__wt_buf_fmt(
              session, meta, "id=%" PRIu64 ",metadata_lsn=%" PRIu64, checkpoint_id, meta_lsn));
            WT_ERR(
              disagg->npage_log->page_log->pl_complete_checkpoint_ext(disagg->npage_log->page_log,
                &session->iface, checkpoint_id, (uint64_t)checkpoint_timestamp, meta, NULL));
        }
        WT_RELEASE_WRITE(
          conn->disaggregated_storage.last_checkpoint_timestamp, checkpoint_timestamp);
    }

    WT_ERR(__wt_disagg_begin_checkpoint(session, checkpoint_id + 1));

err:
    __wt_scr_free(session, &meta);
    return (ret);
}

/*
 * __layered_move_updates --
 *     Move the updates of a key to the stable table
 */
static int
__layered_move_updates(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *key, WT_UPDATE *upds)
{
    WT_DECL_RET;

    /* Search the page. */
    WT_WITH_PAGE_INDEX(session, ret = __wt_row_search(cbt, key, true, NULL, false, NULL));
    WT_ERR(ret);

    /* Apply the modification. */
    WT_ERR(__wt_row_modify(cbt, key, NULL, &upds, WT_UPDATE_INVALID, false, false));

err:
    WT_TRET(__wt_btcur_reset(cbt));
    return (ret);
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
    wt_timestamp_t last_checkpoint_timestamp;
    uint8_t flags, location, prepare, type;
    int cmp;
    char buf[256], buf2[64];
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL, NULL, NULL};

    stable_cursor = version_cursor = NULL;
    prev_upd = tombstone = upd = upds = NULL;
    WT_TIME_WINDOW_INIT(&tw);

    WT_ACQUIRE_READ(
      last_checkpoint_timestamp, S2C(session)->disaggregated_storage.last_checkpoint_timestamp);
    WT_RET(__layered_table_get_constituent_cursor(session, entry->ingest_id, &stable_cursor));
    cbt = (WT_CURSOR_BTREE *)stable_cursor;
    if (last_checkpoint_timestamp != WT_TS_NONE)
        WT_ERR(__wt_snprintf(
          buf2, sizeof(buf2), "start_timestamp=%" PRIx64 "", last_checkpoint_timestamp));
    else
        buf2[0] = '\0';
    WT_ERR(__wt_snprintf(buf, sizeof(buf),
      "debug=(dump_version=(enabled=true,raw_key_value=true,visible_only=true,timestamp_order=true,"
      "%s))",
      buf2));
    cfg[1] = buf;
    WT_ERR(__wt_open_cursor(session, entry->ingest_uri, NULL, cfg, &version_cursor));

    WT_ERR(__wt_scr_alloc(session, 0, &key));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp_key));
    WT_ERR(__wt_scr_alloc(session, 0, &value));

    for (;;) {
        tombstone = upd = NULL;
        WT_ERR_NOTFOUND_OK(version_cursor->next(version_cursor), true);
        if (ret == WT_NOTFOUND) {
            if (key->size > 0 && upds != NULL) {
                WT_WITH_DHANDLE(
                  session, cbt->dhandle, ret = __layered_move_updates(session, cbt, key, upds));
                WT_ERR(ret);
                upds = NULL;
            } else
                ret = 0;
            break;
        }

        WT_ERR(version_cursor->get_key(version_cursor, tmp_key));
        WT_ERR(__wt_compare(session, CUR2BT(cbt)->collator, key, tmp_key, &cmp));
        if (cmp != 0) {
            WT_ASSERT(session, cmp <= 0);

            if (upds != NULL) {
                WT_WITH_DHANDLE(
                  session, cbt->dhandle, ret = __layered_move_updates(session, cbt, key, upds));
                WT_ERR(ret);
            }

            upds = NULL;
            prev_upd = NULL;
            WT_ERR(__wt_buf_set(session, key, tmp_key->data, tmp_key->size));
        }

        WT_ERR(version_cursor->get_value(version_cursor, &tw.start_txn, &tw.start_ts,
          &tw.durable_start_ts, &tw.stop_txn, &tw.stop_ts, &tw.durable_stop_ts, &type, &prepare,
          &flags, &location, value));
        /* We shouldn't see any prepared updates. */
        WT_ASSERT(session, prepare == 0);

        /* We assume the updates returned will be in timestamp order. */
        if (prev_upd != NULL) {
            /* If we see a single tombstone in the previous iteration, we must be reaching the end
             * and should never be here. */
            WT_ASSERT(session, prev_upd->type == WT_UPDATE_STANDARD);
            WT_ASSERT(session,
              tw.stop_txn <= prev_upd->txnid && tw.stop_ts <= prev_upd->start_ts &&
                tw.durable_stop_ts <= prev_upd->durable_ts);
            WT_ASSERT(session,
              tw.start_txn <= prev_upd->txnid && tw.start_ts <= prev_upd->start_ts &&
                tw.durable_start_ts <= prev_upd->durable_ts);
            if (tw.stop_txn != prev_upd->txnid || tw.stop_ts != prev_upd->start_ts ||
              tw.durable_stop_ts != prev_upd->durable_ts)
                WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));
        } else if (WT_TIME_WINDOW_HAS_STOP(&tw))
            WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, NULL));

        /*
         * It is possible to see a full value that is smaller than or equal to the last checkpoint
         * timestamp with a tombstone that is larger than the last checkpoint timestamp. Ignore the
         * update in this case.
         */
        if (tw.durable_start_ts > last_checkpoint_timestamp) {
            WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
            upd->txnid = tw.start_txn;
            upd->start_ts = tw.start_ts;
            upd->durable_ts = tw.durable_start_ts;
        } else
            WT_ASSERT(session, tombstone != NULL);

        if (tombstone != NULL) {
            tombstone->txnid = tw.stop_txn;
            tombstone->start_ts = tw.start_ts;
            tombstone->durable_ts = tw.durable_start_ts;
            tombstone->next = upd;

            WT_ASSERT(session, tombstone->durable_ts > last_checkpoint_timestamp);

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
 * __layered_drain_ingest_tables --
 *     Moving all the data from the ingest tables to the stable tables
 */
static int
__layered_drain_ingest_tables(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LAYERED_TABLE_MANAGER *manager;
    WT_LAYERED_TABLE_MANAGER_ENTRY *entry;
    WT_SESSION_IMPL *internal_session;
    size_t i;

    conn = S2C(session);
    manager = &conn->layered_table_manager;

    WT_RET(__wt_open_internal_session(conn, "disagg-drain", false, 0, 0, &internal_session));

    /*
     * The table count never shrinks, so this is safe. It probably needs the layered table lock.
     *
     * TODO: skip empty ingest tables.
     */
    for (i = 0; i < manager->open_layered_table_count; i++) {
        if ((entry = manager->entries[i]) != NULL)
            WT_ERR(__layered_drain_ingest_table(internal_session, entry));
    }

err:
    WT_TRET(__wt_session_close_internal(internal_session));
    return (ret);
}
