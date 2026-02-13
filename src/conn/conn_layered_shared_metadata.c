/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __disagg_update_metadata_free --
 *     Free an entry in the update metadata queue.
 */
static void
__disagg_update_metadata_free(WT_SESSION_IMPL *session, WT_DISAGG_UPDATE_METADATA **entry)
{
    if (*entry == NULL)
        return;
    __wt_free(session, (*entry)->stable_uri);
    __wt_free(session, (*entry)->table_name);
    __wt_free(session, (*entry)->colgroup_value);
    __wt_free(session, (*entry)->layered_value);
    __wt_free(session, (*entry)->stable_value);
    __wt_free(session, (*entry)->table_value);
    __wt_free(session, *entry);
    *entry = NULL;
}

/*
 * __disagg_save_metadata --
 *     Fetch a metadata key/value pair from the metadata table and save the value.
 */
static int
__disagg_save_metadata(WT_SESSION_IMPL *session, WT_CURSOR *md_cursor, const char *prefix,
  const char *key, char **valuep)
{
    WT_DECL_ITEM(md_key);
    WT_DECL_RET;
    const char *md_value;

    WT_ERR(__wt_scr_alloc(session, 0, &md_key));
    WT_ERR(__wt_buf_fmt(session, md_key, "%s%s", prefix, key));

    md_cursor->set_key(md_cursor, md_key->data);
    WT_ERR_NOTFOUND_OK(md_cursor->search(md_cursor), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND)) {
        WT_ERR(md_cursor->get_value(md_cursor, &md_value));
        WT_ERR(__wt_strdup(session, md_value, valuep));
    }

err:
    __wt_scr_free(session, &md_key);
    return (ret);
}

/*
 * __wt_disagg_update_metadata_later --
 *     Copy the metadata that belongs to the given URI into the shared metadata table at the next
 *     checkpoint.
 */
int
__wt_disagg_update_metadata_later(
  WT_SESSION_IMPL *session, const char *stable_uri, const char *table_name)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_DISAGG_UPDATE_METADATA *entry;

    conn = S2C(session);
    cursor = NULL;
    entry = NULL;

    /*
     * Ensure that the schema lock is held. We cannot check this via spinlock ownership, because
     * this function might be called from an internal session, while the lock was acquired by its
     * parent session.
     */
    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_SCHEMA));

    /* Allocate the entry structure. */
    WT_ERR(__wt_calloc_one(session, &entry));
    WT_ERR(__wt_strdup(session, stable_uri, &entry->stable_uri));
    WT_ERR(__wt_strdup(session, table_name, &entry->table_name));

    /* Get the table metadata. */
    WT_ERR(__wt_metadata_cursor(session, &cursor));

    /* Fetch the relevant data from the metadata table and save it in the entry. */
    WT_ERR(
      __disagg_save_metadata(session, cursor, "colgroup:", table_name, &entry->colgroup_value));
    WT_ERR(__disagg_save_metadata(session, cursor, "layered:", table_name, &entry->layered_value));
    WT_ERR(__disagg_save_metadata(session, cursor, "table:", table_name, &entry->table_value));
    WT_ERR(__disagg_save_metadata(session, cursor, "", stable_uri, &entry->stable_value));

    /* Cannot fail past this point. */
    __wt_spin_lock(session, &conn->disaggregated_storage.update_metadata_lock);
    TAILQ_INSERT_TAIL(&conn->disaggregated_storage.update_metadata_qh, entry, q);
    __wt_spin_unlock(session, &conn->disaggregated_storage.update_metadata_lock);

    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE,
      "Scheduled copying disaggregated metadata for table \"%s\" (stable URI \"%s\") to shared "
      "metadata table at next checkpoint:",
      table_name, stable_uri);
    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE, "  colgroup: %s",
      entry->colgroup_value == NULL ? "<none>" : entry->colgroup_value);
    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE, "  layered: %s",
      entry->layered_value == NULL ? "<none>" : entry->layered_value);
    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE, "  table: %s",
      entry->table_value == NULL ? "<none>" : entry->table_value);
    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE, "  stable: %s",
      entry->stable_value == NULL ? "<none>" : entry->stable_value);

    /* No need to free the entry structure here as it has been added to the queue. */
    entry = NULL;

err:
    __disagg_update_metadata_free(session, &entry);

    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __wti_disagg_update_metadata_clear --
 *     Clear the update metadata list.
 */
void
__wti_disagg_update_metadata_clear(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGG_UPDATE_METADATA *entry, *tmp;

    conn = S2C(session);

    __wt_spin_lock(session, &conn->disaggregated_storage.update_metadata_lock);

    WT_TAILQ_SAFE_REMOVE_BEGIN(entry, &conn->disaggregated_storage.update_metadata_qh, q, tmp)
    {
        TAILQ_REMOVE(&conn->disaggregated_storage.update_metadata_qh, entry, q);
        __disagg_update_metadata_free(session, &entry);
    }
    WT_TAILQ_SAFE_REMOVE_END

    __wt_spin_unlock(session, &conn->disaggregated_storage.update_metadata_lock);
}

/*
 * __disagg_update_shared_metadata_helper --
 *     Update the shared metadata.
 */
static int
__disagg_update_shared_metadata_helper(WT_SESSION_IMPL *session, const char *key, const char *value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL};

    WT_ASSERT(session, S2C(session)->layered_table_manager.leader);

    cursor = NULL;

    WT_ERR(__wt_open_cursor(session, WT_DISAGG_METADATA_URI, NULL, cfg, &cursor));
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, value);
    WT_ERR(cursor->insert(cursor));

    __wt_verbose_debug2(session, WT_VERB_DISAGGREGATED_STORAGE,
      "Updated disaggregated shared metadata: key=\"%s\" value=\"%s\"", key, value);

err:
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));
    return (ret);
}

/*
 * __disagg_update_shared_metadata --
 *     Update metadata in the shared metadata table.
 */
static int
__disagg_update_shared_metadata(
  WT_SESSION_IMPL *session, const char *prefix, const char *key, const char *value)
{
    WT_DECL_ITEM(md_key);
    WT_DECL_RET;

    if (value == NULL)
        return (0);

    WT_ERR(__wt_scr_alloc(session, 0, &md_key));
    WT_ERR(__wt_buf_fmt(session, md_key, "%s%s", prefix, key));

    WT_SAVE_DHANDLE(
      session, ret = __disagg_update_shared_metadata_helper(session, md_key->data, value));
    WT_ERR(ret);

err:
    __wt_scr_free(session, &md_key);
    return (ret);
}

/*
 * __wt_disagg_update_metadata_process --
 *     Process the update metadata list.
 */
int
__wt_disagg_update_metadata_process(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_DISAGG_UPDATE_METADATA *entry, *tmp;

    conn = S2C(session);

    /*
     * This requires schema lock to ensure that we capture a consistent snapshot of metadata entries
     * related to the given shared table, e.g., the various file, colgroup, table, and layered
     * entries.
     */
    WT_ASSERT_SPINLOCK_OWNED(session, &conn->schema_lock);

    __wt_spin_lock(session, &conn->disaggregated_storage.update_metadata_lock);

    TAILQ_FOREACH_SAFE(entry, &conn->disaggregated_storage.update_metadata_qh, q, tmp)
    {
        WT_ERR(__disagg_update_shared_metadata(
          session, "colgroup:", entry->table_name, entry->colgroup_value));
        WT_ERR(__disagg_update_shared_metadata(
          session, "layered:", entry->table_name, entry->layered_value));
        WT_ERR(__disagg_update_shared_metadata(
          session, "table:", entry->table_name, entry->table_value));
        WT_ERR(
          __disagg_update_shared_metadata(session, "", entry->stable_uri, entry->stable_value));

        TAILQ_REMOVE(&conn->disaggregated_storage.update_metadata_qh, entry, q);
        __disagg_update_metadata_free(session, &entry);
    }

err:
    __wt_spin_unlock(session, &conn->disaggregated_storage.update_metadata_lock);

    return (ret);
}

/*
 * __wti_disagg_metadata_table_init --
 *     Initialize the shared metadata table.
 */
int
__wti_disagg_metadata_table_init(WT_SESSION_IMPL *session)
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
