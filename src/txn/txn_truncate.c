/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Selects which truncate-list entries __truncate_search considers: those visible to the calling
 * transaction (committed truncates we may need to honor) or those not visible (uncommitted
 * truncates that may conflict with our writes).
 */
typedef enum { WT_TRUNCATE_SEARCH_VISIBLE, WT_TRUNCATE_SEARCH_NOT_VISIBLE } WT_TRUNCATE_SEARCH_MODE;

/*
 * __disagg_truncate_free --
 *     Free an entry in the layered dhandle truncate list.
 */
static void
__disagg_truncate_free(WT_SESSION_IMPL *session, WT_TRUNCATE **entry)
{
    WT_ASSERT(session, __wt_process.disagg_fast_truncate_2026 == true);

    if (entry == NULL || *entry == NULL)
        return;

    __wt_free(session, (*entry)->uri);
    __wt_buf_free(session, &(*entry)->start_key);
    __wt_buf_free(session, &(*entry)->stop_key);

    __wt_free(session, *entry);
    *entry = NULL;
}

/*
 * __truncate_ski_insert --
 *     Insert a node into the truncate skip list, maintaining start_key sort order.
 *     Mirrors the WT_INSERT insertion pattern.
 */
static int
__truncate_ski_insert(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_TRUNCATE *t)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *update[WT_SKIP_MAXDEPTH], *cur, *fwd;
    int cmp;
    uint32_t i, lvl;

    collator = layered_table->collator;
    cur = NULL; /* NULL == head sentinel */

    /* Standard O(log N) descent: find the predecessor at each level. */
    for (i = WT_SKIP_MAXDEPTH; i > 0; i--) {
        lvl = i - 1;
        fwd = (cur == NULL ? layered_table->truncate_head.head[lvl] : cur->next[lvl]);
        while (fwd != NULL) {
            WT_RET(__wt_compare(session, collator, &fwd->start_key, &t->start_key, &cmp));
            if (cmp >= 0)
                break;
            cur = fwd;
            fwd = cur->next[lvl];
        }
        update[lvl] = cur;
    }

    /* Splice t into the list at each level of its tower. */
    for (i = 0; i < t->next_depth; i++) {
        if (update[i] == NULL) {
            t->next[i] = layered_table->truncate_head.head[i];
            layered_table->truncate_head.head[i] = t;
        } else {
            t->next[i] = update[i]->next[i];
            update[i]->next[i] = t;
        }
    }

    return (0);
}

/*
 * __truncate_ski_remove --
 *     Remove a node from the truncate skip list.
 */
static int
__truncate_ski_remove(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_TRUNCATE *t)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *update[WT_SKIP_MAXDEPTH], *cur, *fwd;
    int cmp;
    uint32_t i, lvl;

    collator = layered_table->collator;
    cur = NULL;

    /* Find the predecessor at each level using the same descent as insert. */
    for (i = WT_SKIP_MAXDEPTH; i > 0; i--) {
        lvl = i - 1;
        fwd = (cur == NULL ? layered_table->truncate_head.head[lvl] : cur->next[lvl]);
        while (fwd != NULL && fwd != t) {
            WT_RET(__wt_compare(session, collator, &fwd->start_key, &t->start_key, &cmp));
            if (cmp > 0)
                break;
            cur = fwd;
            fwd = cur->next[lvl];
        }
        update[lvl] = cur;
    }

    /* Unlink t at each level where it appears. */
    for (i = 0; i < t->next_depth; i++) {
        WT_TRUNCATE **pred_next =
          (update[i] == NULL ? &layered_table->truncate_head.head[i] : &update[i]->next[i]);
        if (*pred_next == t)
            *pred_next = t->next[i];
    }

    return (0);
}

/*
 * __truncate_ski_stab --
 *     Scan the truncate skip list for an entry (per mode) whose range [start_key, stop_key]
 *     contains key.  Nodes are sorted by start_key ascending, so the scan stops as soon as
 *     start_key exceeds key.
 */
static int
__truncate_ski_stab(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, const WT_ITEM *key,
  const WT_TRUNCATE_SEARCH_MODE mode, WT_TRUNCATE **tp, bool *is_foundp)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *t;
    bool is_visible;
    int cmp;

    WT_ASSERT(session, is_foundp != NULL);
    *is_foundp = false;

    collator = layered_table->collator;

    WT_TRUNC_SKIP_FOREACH(t, &layered_table->truncate_head) {
        /* Sorted by start_key: once start_key > key no further node can cover key. */
        WT_RET(__wt_compare(session, collator, &t->start_key, key, &cmp));
        if (cmp > 0)
            break;

        /* start_key <= key; check if stop_key >= key. */
        WT_RET(__wt_compare(session, collator, &t->stop_key, key, &cmp));
        if (cmp < 0)
            continue;

        /* start_key <= key <= stop_key — check visibility. */
        is_visible = __wt_txn_visible(session, t->txn_id, t->start_ts, t->durable_ts);
        if ((mode == WT_TRUNCATE_SEARCH_VISIBLE && is_visible) ||
          (mode == WT_TRUNCATE_SEARCH_NOT_VISIBLE && !is_visible)) {
            if (tp != NULL)
                *tp = t;
            *is_foundp = true;
            return (0);
        }
    }

    return (0);
}

/*
 * __truncate_ski_clear --
 *     Free all nodes in the truncate skip list and zero the head.
 */
static void
__truncate_ski_clear(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table)
{
    WT_TRUNCATE *t, *next;

    for (t = WT_TRUNC_SKIP_FIRST(&layered_table->truncate_head); t != NULL; t = next) {
        next = WT_TRUNC_SKIP_NEXT(t);
        __disagg_truncate_free(session, &t);
    }
    memset(&layered_table->truncate_head, 0, sizeof(layered_table->truncate_head));
}

/*
 * __txn_insert_truncate_entry_helper --
 *     Register a truncate entry to the latest transaction and store it in the truncate list.
 */
static int
__txn_insert_truncate_entry_helper(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_TRUNCATE **tp)
{
    WT_DECL_RET;
    WT_TRUNCATE *t;

    t = *tp;

    WT_RET(__wt_session_get_dhandle(session, layered_table->ingest_uri, NULL, NULL, 0));
    WT_ERR(__wt_txn_truncate(session, t));

    __wt_writelock(session, &layered_table->truncate_lock);
    ret = __truncate_ski_insert(session, layered_table, t);
    __wt_writeunlock(session, &layered_table->truncate_lock);
    WT_ERR(ret);

    /* Ownership transferred to the txn op and truncate skip list. */
    *tp = NULL;

err:
    WT_TRET(__wt_session_release_dhandle(session));
    return (ret);
}

/*
 * __wt_insert_truncate_entry --
 *     Insert a truncate entry into the layered dhandle truncate list.
 */
int
__wt_insert_truncate_entry(
  WT_SESSION_IMPL *session, const char *uri, WT_ITEM *start_key, WT_ITEM *stop_key)
{
    WT_DECL_ITEM(start_buf);
    WT_DECL_ITEM(stop_buf);
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *t = NULL;
    u_int skipdepth;

    WT_ASSERT(session, __wt_process.disagg_fast_truncate_2026 == true);

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     *
     * FIXME-WT-16789: Disallow sweep server or follower mode to clean up the dhandle from the
     * dhandle list, if there are entries in the truncate list.
     */
    WT_ASSERT_ALWAYS(session, __wt_session_get_dhandle(session, uri, NULL, NULL, 0) == 0,
      "failed to get layered dhandle for truncate entry insert");
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    /* Caller resolves open-ended ranges to concrete keys before reaching us. */
    WT_ASSERT(session, start_key != NULL && stop_key != NULL);
    WT_ASSERT(session, start_key->size != 0 && stop_key->size != 0);

    WT_RET(__wt_scr_alloc(session, 0, &start_buf));
    WT_RET(__wt_scr_alloc(session, 0, &stop_buf));
    __wt_verbose_level(session, WT_VERB_LAYERED, WT_VERBOSE_DEBUG_3,
      "insert entry into truncate list on table %s: start=%s stop=%s", uri,
      __wt_key_string(
        session, start_key->data, start_key->size, layered_table->key_format, start_buf),
      __wt_key_string(
        session, stop_key->data, stop_key->size, layered_table->key_format, stop_buf));

    /* Allocate the node with a flexible next[] array sized to the chosen tower height. */
    skipdepth = __wt_skip_choose_depth(session);
    WT_ERR(__wt_calloc(session, 1, sizeof(WT_TRUNCATE) + skipdepth * sizeof(WT_TRUNCATE *), &t));
    t->next_depth = (uint8_t)skipdepth;

    WT_ERR(__wt_strdup(session, uri, &t->uri));
    WT_ERR(__wt_buf_set(session, &t->start_key, start_key->data, start_key->size));
    WT_ERR(__wt_buf_set(session, &t->stop_key, stop_key->data, stop_key->size));

    /*
     * Mark the WT_TRUNCATE object modified by the current transaction. Also required to update the
     * max_upd_txn.
     */
    WT_SAVE_DHANDLE(session, ret = __txn_insert_truncate_entry_helper(session, layered_table, &t));
    WT_ERR(ret);

    if (0) {
err:
        __disagg_truncate_free(session, &t);
    }
    WT_TRET(__wt_session_release_dhandle(session));
    __wt_scr_free(session, &start_buf);
    __wt_scr_free(session, &stop_buf);

    return (ret);
}

/*
 * __truncate_search --
 *     Walk the layered table truncate list looking for a committed or uncommitted entry (depending
 *     on the search mode) whose range covers the given key. The matched entry is returned through
 *     the output parameter when non-NULL.
 */
static int
__truncate_search(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, const WT_ITEM *key,
  const WT_TRUNCATE_SEARCH_MODE mode, WT_TRUNCATE **tp, bool *is_foundp)
{
    return (__truncate_ski_stab(session, layered_table, key, mode, tp, is_foundp));
}

/*
 * __wt_layered_table_truncate_detect_write_conflict --
 *     Search if the current key we are modifying conflicts with any uncommitted truncates in the
 *     layered table truncate list.
 *
 * FIXME-WT-16812: Investigate whether this function can be called below the cursor layer. Doing so
 *     would remove the write cursor operations dependency on the truncate list.
 */
int
__wt_layered_table_truncate_detect_write_conflict(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, const WT_ITEM *key)
{
    WT_DECL_RET;
    bool is_found = false;

    if (!__wt_process.disagg_fast_truncate_2026)
        return (0);

    WT_ASSERT(session, WT_PREFIX_MATCH(layered_table->iface.name, "layered:"));

    __wt_readlock(session, &layered_table->truncate_lock);

    /*
     * The truncate entry has already been committed if it is visible to this transaction. We can
     * ignore these entries.
     */
    ret = __truncate_search(
      session, layered_table, key, WT_TRUNCATE_SEARCH_NOT_VISIBLE, NULL, &is_found);

    __wt_readunlock(session, &layered_table->truncate_lock);
    WT_RET(ret);

    if (is_found) {
        WT_STAT_CONN_INCR(session, txn_update_conflict);
        __wt_session_set_last_error(
          session, WT_ROLLBACK, WT_WRITE_CONFLICT, WT_TXN_ROLLBACK_REASON_CONFLICT);
        return (WT_ROLLBACK);
    }

    return (0);
}

/*
 * __wt_truncate_delete_visible_check --
 *     Search if the given key has been deleted in the layered table truncate list.
 */
int
__wt_truncate_delete_visible_check(
  WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_ITEM *key, WT_TRUNCATE **tp)
{
    WT_DECL_RET;
    bool is_found = false;

    if (!__wt_process.disagg_fast_truncate_2026)
        return (WT_NOTFOUND);

    WT_ASSERT(session, WT_PREFIX_MATCH(layered_table->iface.name, "layered:"));

    __wt_readlock(session, &layered_table->truncate_lock);

    /*
     * Ignore all truncate entries that haven't been committed. They won't be visible to this
     * transaction.
     */
    ret = __truncate_search(session, layered_table, key, WT_TRUNCATE_SEARCH_VISIBLE, tp, &is_found);

    __wt_readunlock(session, &layered_table->truncate_lock);
    WT_RET(ret);

    return (is_found ? 0 : WT_NOTFOUND);
}

/*
 * __wti_mark_committed_truncate_table --
 *     Mark a truncate table entry as committed, updating truncate entries timestamp information.
 */
int
__wti_mark_committed_truncate_table(WT_SESSION_IMPL *session, WT_TXN_OP *op)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *entry;

    layered_table = NULL;
    entry = op->u.follower_truncate.t;

    WT_ASSERT(session, __wt_process.disagg_fast_truncate_2026 == true);

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     *
     * FIXME-WT-16789: Disallow sweep server or follower mode to clean up the dhandle from the
     * dhandle list, if there are entries in the truncate list.
     */
    WT_ASSERT_ALWAYS(session, __wt_session_get_dhandle(session, entry->uri, NULL, NULL, 0) == 0,
      "failed to get layered dhandle when marking truncate committed");
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    __wt_writelock(session, &layered_table->truncate_lock);
    entry->txn_id = session->txn->time_point.id;
    entry->start_ts = session->txn->time_point.commit_timestamp;
    entry->durable_ts = session->txn->time_point.durable_timestamp;
    __wt_writeunlock(session, &layered_table->truncate_lock);

    WT_TRET(__wt_session_release_dhandle(session));
    return (ret);
}

/*
 * __wti_layered_table_truncate_rollback --
 *     Perform transaction rollback for a truncate operation, removing the truncate entry from the
 *     layered table truncate list.
 */
int
__wti_layered_table_truncate_rollback(WT_SESSION_IMPL *session, WT_TXN_OP *op)
{
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered_table;
    WT_TRUNCATE *entry;

    layered_table = NULL;
    entry = op->u.follower_truncate.t;

    WT_ASSERT(session, __wt_process.disagg_fast_truncate_2026 == true);

    /*
     * Get the layered table from the provided URI. We don't hold any global locks so that's
     * possible that it was already removed.
     *
     * FIXME-WT-16789: Disallow sweep server or follower mode to clean up the dhandle from the
     * dhandle list, if there are entries in the truncate list.
     */
    WT_ASSERT_ALWAYS(session, __wt_session_get_dhandle(session, entry->uri, NULL, NULL, 0) == 0,
      "failed to get layered dhandle during truncate rollback");
    layered_table = (WT_LAYERED_TABLE *)session->dhandle;

    __wt_writelock(session, &layered_table->truncate_lock);
    ret = __truncate_ski_remove(session, layered_table, entry);
    __wt_writeunlock(session, &layered_table->truncate_lock);

    if (ret == 0)
        __disagg_truncate_free(session, &entry);
    op->u.follower_truncate.t = NULL;

    WT_TRET(__wt_session_release_dhandle(session));
    return (ret);
}

/*
 * __wt_layered_table_truncate_clear --
 *     Clear all entries in the layered dhandle truncate list.
 */
void
__wt_layered_table_truncate_clear(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table)
{
    __wt_writelock(session, &layered_table->truncate_lock);
    __truncate_ski_clear(session, layered_table);
    __wt_writeunlock(session, &layered_table->truncate_lock);
}
