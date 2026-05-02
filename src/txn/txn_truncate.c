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

    for (uint32_t i = 0; i < WT_SKIP_MAXDEPTH; i++)
        __wt_buf_free(session, &(*entry)->fwd_max_stop[i]);

    __wt_free(session, *entry);
    *entry = NULL;
}

/*
 * __truncate_item_cmp --
 *     Compare two WT_ITEM values using the table collator. Returns negative/zero/positive.
 *     An empty item (size == 0) is treated as less than any non-empty item.
 */
static int
__truncate_item_cmp(WT_SESSION_IMPL *session, WT_COLLATOR *collator, const WT_ITEM *a,
  const WT_ITEM *b, int *cmpp)
{
    if (a->size == 0 && b->size == 0) {
        *cmpp = 0;
        return (0);
    }
    if (a->size == 0) {
        *cmpp = -1;
        return (0);
    }
    if (b->size == 0) {
        *cmpp = 1;
        return (0);
    }
    return (__wt_compare(session, collator, a, b, cmpp));
}

/*
 * __truncate_max_stop_item --
 *     Return a pointer to the larger of two WT_ITEM stop keys (treating empty as -infinity).
 */
static const WT_ITEM *
__truncate_max_stop_item(WT_SESSION_IMPL *session, WT_COLLATOR *collator, const WT_ITEM *a,
  const WT_ITEM *b)
{
    int cmp;

    if (__truncate_item_cmp(session, collator, a, b, &cmp) != 0)
        return (a->size >= b->size ? a : b); /* fallback on error */
    return (cmp >= 0 ? a : b);
}

/*
 * __truncate_ski_insert --
 *     Insert a node into the augmented interval skip list, maintaining start_key order and
 *     updating fwd_max_stop augmentation at each level.
 */
static int
__truncate_ski_insert(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_TRUNCATE *t)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *update[WT_SKIP_MAXDEPTH];
    WT_TRUNCATE *fwd;
    const WT_ITEM *max_item;
    int cmp;
    uint32_t i, new_height;

    collator = layered_table->collator;

    /* Choose tower height for this node. */
    t->ski_height = __wt_skip_choose_depth(session);
    new_height = t->ski_height;

    /* Grow the list height if needed. */
    if (new_height > layered_table->truncate_ski_height)
        layered_table->truncate_ski_height = new_height;

    /*
     * Standard skip list descent: track a current node (NULL = head sentinel). At each level,
     * advance as far as possible while start_key < target, then record the stopping point as
     * update[lvl]. Descending reuses the same node, so lower levels start where the upper level
     * stopped.
     */
    {
        WT_TRUNCATE *cur = NULL; /* NULL represents the head sentinel */

        for (i = layered_table->truncate_ski_height; i > 0; i--) {
            uint32_t lvl = i - 1;
            fwd = (cur == NULL ? layered_table->truncate_ski[lvl] : cur->ski_next[lvl]);
            while (fwd != NULL) {
                WT_RET(__wt_compare(session, collator, &fwd->start_key, &t->start_key, &cmp));
                if (cmp >= 0)
                    break;
                cur = fwd;
                fwd = cur->ski_next[lvl];
            }
            update[lvl] = cur;
        }

        /* Levels above the list height have no predecessors. */
        for (i = layered_table->truncate_ski_height; i < WT_SKIP_MAXDEPTH; i++)
            update[i] = NULL;
    }

    /* Splice t into the list at each level up to its height. */
    for (i = 0; i < new_height; i++) {
        if (update[i] == NULL) {
            t->ski_next[i] = layered_table->truncate_ski[i];
            layered_table->truncate_ski[i] = t;
        } else {
            t->ski_next[i] = update[i]->ski_next[i];
            update[i]->ski_next[i] = t;
        }
    }

    /*
     * Set fwd_max_stop[i] on t: max of t->stop_key and the next node's fwd_max_stop[i].
     * Walk bottom-up so higher levels can use lower-level values if needed.
     */
    for (i = 0; i < new_height; i++) {
        fwd = t->ski_next[i];
        if (fwd != NULL && fwd->fwd_max_stop[i].size > 0)
            max_item = __truncate_max_stop_item(session, collator, &t->stop_key,
              &fwd->fwd_max_stop[i]);
        else
            max_item = &t->stop_key;
        WT_RET(__wt_buf_set(session, &t->fwd_max_stop[i], max_item->data, max_item->size));
    }

    /*
     * Propagate t's stop_key up the update path: for each predecessor that now has t reachable,
     * update their fwd_max_stop if t->stop_key is larger.
     */
    for (i = 0; i < layered_table->truncate_ski_height; i++) {
        WT_ITEM *head_max = &layered_table->truncate_ski_max_stop[i];
        max_item = __truncate_max_stop_item(session, collator, head_max, &t->stop_key);
        if (max_item == &t->stop_key)
            WT_RET(__wt_buf_set(session, head_max, t->stop_key.data, t->stop_key.size));

        if (update[i] != NULL) {
            WT_ITEM *pred_max = &update[i]->fwd_max_stop[i];
            max_item = __truncate_max_stop_item(session, collator, pred_max, &t->stop_key);
            if (max_item == &t->stop_key)
                WT_RET(
                  __wt_buf_set(session, pred_max, t->stop_key.data, t->stop_key.size));
        }
    }

    return (0);
}

/*
 * __truncate_ski_recompute_max_stop --
 *     Recompute fwd_max_stop[level] for a node by scanning its forward chain at that level.
 *     Used after removal to restore the augmentation invariant.
 */
static int
__truncate_ski_recompute_max_stop(WT_SESSION_IMPL *session, WT_COLLATOR *collator,
  WT_ITEM *result_item, WT_TRUNCATE *start_node, uint32_t level)
{
    WT_TRUNCATE *cur;
    const WT_ITEM *running_max;
    WT_ITEM empty;

    WT_CLEAR(empty);

    running_max = &empty;
    for (cur = start_node; cur != NULL; cur = cur->ski_next[level]) {
        running_max = __truncate_max_stop_item(session, collator, running_max, &cur->stop_key);
    }

    if (running_max->size == 0)
        __wt_buf_free(session, result_item);
    else
        WT_RET(__wt_buf_set(session, result_item, running_max->data, running_max->size));

    return (0);
}

/*
 * __truncate_ski_remove --
 *     Remove a node from the augmented interval skip list and recompute augmentation.
 */
static int
__truncate_ski_remove(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, WT_TRUNCATE *t)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *update[WT_SKIP_MAXDEPTH];
    WT_TRUNCATE *fwd;
    int cmp;
    uint32_t i;

    collator = layered_table->collator;

    /* Find the predecessor at each level. */
    for (i = 0; i < WT_SKIP_MAXDEPTH; i++)
        update[i] = NULL;

    for (i = layered_table->truncate_ski_height; i > 0; i--) {
        uint32_t lvl = i - 1;

        fwd = (update[lvl] == NULL ? layered_table->truncate_ski[lvl] :
                                     update[lvl]->ski_next[lvl]);

        while (fwd != NULL && fwd != t) {
            WT_RET(__wt_compare(session, collator, &fwd->start_key, &t->start_key, &cmp));
            if (cmp > 0)
                break;
            update[lvl] = fwd;
            fwd = fwd->ski_next[lvl];
        }
    }

    /* Unlink t at each level where it appears. */
    for (i = 0; i < t->ski_height; i++) {
        WT_TRUNCATE **pred_next =
          (update[i] == NULL ? &layered_table->truncate_ski[i] : &update[i]->ski_next[i]);
        if (*pred_next == t)
            *pred_next = t->ski_next[i];
    }

    /* Recompute fwd_max_stop for each predecessor and head at affected levels. */
    for (i = 0; i < layered_table->truncate_ski_height; i++) {
        WT_ITEM *head_max = &layered_table->truncate_ski_max_stop[i];
        WT_RET(__truncate_ski_recompute_max_stop(
          session, collator, head_max, layered_table->truncate_ski[i], i));

        if (update[i] != NULL) {
            WT_RET(__truncate_ski_recompute_max_stop(
              session, collator, &update[i]->fwd_max_stop[i], update[i]->ski_next[i], i));
        }
    }

    /* Trim the list height if top levels are now empty. */
    while (layered_table->truncate_ski_height > 0 &&
      layered_table->truncate_ski[layered_table->truncate_ski_height - 1] == NULL)
        layered_table->truncate_ski_height--;

    return (0);
}

/*
 * __truncate_ski_stab --
 *     Augmented stabbing search: find the first visible/not-visible truncate entry (per mode) whose
 *     range [start_key, stop_key] contains key. Uses fwd_max_stop pruning for O(log N + k).
 */
static int
__truncate_ski_stab(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table, const WT_ITEM *key,
  const WT_TRUNCATE_SEARCH_MODE mode, WT_TRUNCATE **tp, bool *is_foundp)
{
    WT_COLLATOR *collator;
    WT_TRUNCATE *node, *fwd;
    int cmp;

    WT_ASSERT(session, is_foundp != NULL);
    *is_foundp = false;

    if (layered_table->truncate_ski_height == 0)
        return (0);

    collator = layered_table->collator;
    node = NULL; /* current best predecessor */

    for (int level = (int)layered_table->truncate_ski_height - 1; level >= 0; level--) {
        fwd = (node == NULL ? layered_table->truncate_ski[level] : node->ski_next[level]);

        while (fwd != NULL) {
            /* Pruning: if fwd's max reachable stop < key, no match possible at this level. */
            if (fwd->fwd_max_stop[level].size > 0) {
                WT_RET(
                  __truncate_item_cmp(session, collator, &fwd->fwd_max_stop[level], key, &cmp));
                if (cmp < 0)
                    break;
            }

            /* If start_key > key, all further nodes at this level are also past key. */
            WT_RET(__wt_compare(session, collator, &fwd->start_key, key, &cmp));
            if (cmp > 0)
                break;

            /* start_key <= key; advance node pointer to fwd. */
            node = fwd;
            fwd = node->ski_next[level];

            /* Check whether this node's range actually covers key. */
            WT_RET(__wt_compare(session, collator, &node->stop_key, key, &cmp));
            if (cmp >= 0) {
                /* node->start_key <= key <= node->stop_key — check visibility. */
                const bool is_visible =
                  __wt_txn_visible(session, node->txn_id, node->start_ts, node->durable_ts);
                if ((mode == WT_TRUNCATE_SEARCH_VISIBLE && is_visible) ||
                  (mode == WT_TRUNCATE_SEARCH_NOT_VISIBLE && !is_visible)) {
                    if (tp != NULL)
                        *tp = node;
                    *is_foundp = true;
                    return (0);
                }
            }
        }
    }

    return (0);
}

/*
 * __truncate_ski_clear --
 *     Free all entries in the augmented interval skip list and reset head state.
 */
static void
__truncate_ski_clear(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table)
{
    WT_TRUNCATE *cur, *next;
    uint32_t i;

    cur = layered_table->truncate_ski[0];
    while (cur != NULL) {
        next = cur->ski_next[0];
        __disagg_truncate_free(session, &cur);
        cur = next;
    }

    for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        layered_table->truncate_ski[i] = NULL;
        __wt_buf_free(session, &layered_table->truncate_ski_max_stop[i]);
    }
    layered_table->truncate_ski_height = 0;
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

    WT_ERR(__wt_calloc_one(session, &t));
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
