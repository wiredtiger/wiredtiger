/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __disagg_truncate_free --
 *     Free an entry in the update metadata queue.
 */
static void
__disagg_truncate_free(WT_SESSION_IMPL *session, WT_TRUNCATE **entry)
{
    if (entry == NULL)
        return;

    __wt_free(session, (*entry)->uri);
    __wt_free(session, (*entry)->start_key);
    __wt_free(session, (*entry)->stop_key);
    __wt_free(session, *entry);
    *entry = NULL;
}

/*
 * __wt_insert_truncate_entry --
 *     Insert a truncate entry into the session's truncate list.
 */
int
__wt_insert_truncate_entry(
  WT_SESSION_IMPL *session, const char *uri, WT_ITEM *start_key, WT_ITEM *stop_key)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *t;

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     */
    WT_RET_NOTFOUND_OK(__wt_session_get_dhandle(session, uri, NULL, NULL, 0));
    if (ret == WT_NOTFOUND) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "Truncate %s: Layered table was not found.", uri);
        return (0);
    }
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    WT_RET(__wt_calloc_def(session, sizeof(WT_TRUNCATE), &t));
    WT_ERR(__wt_strdup(session, uri, &t->uri));
    WT_ERR(__wt_buf_set(session, &t->start_key, start_key->data, start_key->size));
    WT_ERR(__wt_buf_set(session, &t->stop_key, stop_key->data, stop_key->size));

    /* Required to update max_upd_txn. */
    WT_ERR(__wt_session_get_dhandle(session, layered_table->ingest_uri, NULL, NULL, 0));
    WT_ERR(__wt_txn_truncate(session, t));
    WT_ERR(__wt_session_release_dhandle(session));

    session->dhandle = (WT_DATA_HANDLE *)layered_table;
    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_INSERT_TAIL(&layered_table->truncateqh, t, q);
    __wt_spin_unlock(session, &layered_table->truncate_lock);

    if (0) {
err:
        __disagg_truncate_free(session, &t);
    }
    WT_TRET(__wt_session_release_dhandle(session));

    return (ret);
}

/*
 * __layered_table_truncate_detect_write_conflict --
 *     Search for a truncate entry in the session's truncate list. Must use WT_SAVE_DHANDLE to get
 *     the layered table handle before calling this function, and hold the layered table lock.
 */
int
__layered_table_truncate_detect_write_conflict_v2(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_ITEM *key, WT_TRUNCATE **tp)
{
    WT_TRUNCATE *entry;
    int start_cmp, stop_cmp;

    WT_ASSERT(session, WT_PREFIX_MATCH(layered_table->iface.name, "layered:"));
    WT_COLLATOR *collator = ((WT_LAYERED_TABLE *)layered_table)->collator;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (entry, &layered_table->truncateqh, q) {
        if (__wt_txn_visible(session, entry->txn_id, entry->start_ts, entry->durable_ts))
            continue;

        WT_RET(__wt_compare(session, collator, key, &entry->start_key, &start_cmp));
        WT_RET(__wt_compare(session, collator, key, &entry->stop_key, &stop_cmp));

        if (start_cmp >= 0 && stop_cmp <= 0) {
            if (tp != NULL)
                *tp = entry;
            __wt_spin_unlock(session, &layered_table->truncate_lock);
            return (WT_WRITE_CONFLICT);
        }
    }

    __wt_spin_unlock(session, &layered_table->truncate_lock);
    return (0);
}

/*
 * __layered_table_truncate_detect_write_conflict --
 *     Search for a truncate entry in the session's truncate list. Must use WT_SAVE_DHANDLE to get
 *     the layered table handle before calling this function, and hold the layered table lock.
 */
int
__layered_table_truncate_detect_write_conflict(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_ITEM *key, WT_TRUNCATE **tp)
{
    WT_TRUNCATE *entry;
    int start_cmp, stop_cmp;

    WT_ASSERT(session, WT_PREFIX_MATCH(layered_table->iface.name, "layered:"));
    WT_COLLATOR *collator = ((WT_LAYERED_TABLE *)layered_table)->collator;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (entry, &layered_table->truncateqh, q) {
        if (__wt_txn_visible(session, entry->txn_id, entry->start_ts, entry->durable_ts))
            continue;

        WT_RET(__wt_compare(session, collator, key, &entry->start_key, &start_cmp));
        WT_RET(__wt_compare(session, collator, key, &entry->stop_key, &stop_cmp));

        if (start_cmp >= 0 && stop_cmp <= 0) {
            if (tp != NULL)
                *tp = entry;
            __wt_spin_unlock(session, &layered_table->truncate_lock);
            return (WT_WRITE_CONFLICT);
        }
    }

    __wt_spin_unlock(session, &layered_table->truncate_lock);
    return (0);
}

/*
 * __search_layered_table_truncate_list --
 *     Search for a truncate entry in the session's truncate list. Must use WT_SAVE_DHANDLE to get
 *     the layered table handle before calling this function, and hold the layered table lock.
 */
int
__search_layered_table_truncate_list(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_ITEM *key, WT_TRUNCATE **tp)
{
    WT_TRUNCATE *entry;
    int start_cmp, stop_cmp;

    WT_ASSERT(session, WT_PREFIX_MATCH(layered_table->iface.name, "layered:"));
    WT_COLLATOR *collator = ((WT_LAYERED_TABLE *)layered_table)->collator;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (entry, &layered_table->truncateqh, q) {
        if (!__wt_txn_visible(session, entry->txn_id, entry->start_ts, entry->durable_ts))
            continue;

        WT_RET(__wt_compare(session, collator, key, &entry->start_key, &start_cmp));
        WT_RET(__wt_compare(session, collator, key, &entry->stop_key, &stop_cmp));

        if (start_cmp >= 0 && stop_cmp <= 0) {
            if (tp != NULL)
                *tp = entry;
            __wt_spin_unlock(session, &layered_table->truncate_lock);
            return (0);
        }
    }

    __wt_spin_unlock(session, &layered_table->truncate_lock);
    return (WT_NOTFOUND);
}

/*
 * __wti_mark_committed_truncate_table --
 *     Search for a truncate entry in the session's truncate list.
 */
int
__wti_mark_committed_truncate_table(WT_SESSION_IMPL *session, WT_TXN_OP *op)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *entry;

    layered_table = NULL;
    entry = op->u.follower_truncate.t;

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     */
    WT_RET_NOTFOUND_OK(__wt_session_get_dhandle(session, entry->uri, NULL, NULL, 0));
    if (ret == WT_NOTFOUND) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "Truncate %s: Layered table was not found.", entry->uri);
        return (0);
    }
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    entry->txn_id = session->txn->id;
    entry->start_ts = session->txn->commit_timestamp;
    entry->durable_ts = session->txn->durable_timestamp;
    __wt_spin_unlock(session, &layered_table->truncate_lock);
    WT_TRET(__wt_session_release_dhandle(session));
    return (0);
}

/*
 * __wt_truncate_delete_visible_check --
 *     Search for a truncate entry in the session's truncate list.
 */
int
__wt_truncate_delete_visible_check(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_ITEM *key, WT_TRUNCATE **tp)
{
    return (__search_layered_table_truncate_list(session, layered_table, key, tp));
}

/*
 * __wt_layered_table_truncate_clear --
 *     Search for a truncate entry in the session's truncate list.
 */
void
__wt_layered_table_truncate_clear(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table)
{
    WT_TRUNCATE *entry;

    entry = NULL;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_FOREACH (entry, &layered_table->truncateqh, q) {
        TAILQ_REMOVE(&layered_table->truncateqh, entry, q);
        __disagg_truncate_free(session, &entry);
    }

    __wt_spin_unlock(session, &layered_table->truncate_lock);
}

/*
 * __wti_layered_table_truncate_rollback --
 *     Search for a truncate entry in the session's truncate list.
 */
int
__wti_layered_table_truncate_rollback(WT_SESSION_IMPL *session, WT_TXN_OP *op)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *entry;

    layered_table = NULL;
    entry = op->u.follower_truncate.t;

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     */
    WT_RET_NOTFOUND_OK(__wt_session_get_dhandle(session, entry->uri, NULL, NULL, 0));
    if (ret == WT_NOTFOUND) {
        __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_5,
          "Truncate %s: Layered table was not found.", entry->uri);
        return (WT_ERROR);
    }
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    __wt_spin_lock(session, &layered_table->truncate_lock);
    TAILQ_REMOVE(&layered_table->truncateqh, entry, q);
    __wt_spin_unlock(session, &layered_table->truncate_lock);

    WT_TRET(__wt_session_release_dhandle(session));
    return (0);
}
