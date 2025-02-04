/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __clayered_copy_bounds(WT_CURSOR_LAYERED *);
static int __clayered_lookup(WT_CURSOR_LAYERED *, WT_ITEM *);
static int __clayered_open_cursors(WT_CURSOR_LAYERED *, bool);
static int __clayered_reset_cursors(WT_CURSOR_LAYERED *, bool);
static int __clayered_search_near(WT_CURSOR *cursor, int *exactp);

/*
 * We need a tombstone to mark deleted records, and we use the special value below for that purpose.
 * We use two 0x14 (Device Control 4) bytes to minimize the likelihood of colliding with an
 * application-chosen encoding byte, if the application uses two leading DC4 byte for some reason,
 * we'll do a wasted data copy each time a new value is inserted into the object.
 */
static const WT_ITEM __tombstone = {"\x14\x14", 3, NULL, 0, 0};

/*
 * __clayered_deleted --
 *     Check whether the current value is a tombstone.
 */
static WT_INLINE bool
__clayered_deleted(const WT_ITEM *item)
{
    return (item->size == __tombstone.size &&
      memcmp(item->data, __tombstone.data, __tombstone.size) == 0);
}

/*
 * __clayered_deleted_encode --
 *     Encode values that are in the encoded name space.
 */
static WT_INLINE int
__clayered_deleted_encode(
  WT_SESSION_IMPL *session, const WT_ITEM *value, WT_ITEM *final_value, WT_ITEM **tmpp)
{
    WT_ITEM *tmp;

    /*
     * If value requires encoding, get a scratch buffer of the right size and create a copy of the
     * data with the first byte of the tombstone appended.
     */
    if (value->size >= __tombstone.size &&
      memcmp(value->data, __tombstone.data, __tombstone.size) == 0) {
        WT_RET(__wt_scr_alloc(session, value->size + 1, tmpp));
        tmp = *tmpp;

        memcpy(tmp->mem, value->data, value->size);
        memcpy((uint8_t *)tmp->mem + value->size, __tombstone.data, 1);
        final_value->data = tmp->mem;
        final_value->size = value->size + 1;
    } else {
        final_value->data = value->data;
        final_value->size = value->size;
    }

    return (0);
}

/*
 * __clayered_deleted_decode --
 *     Decode values that start with the tombstone.
 */
static WT_INLINE void
__clayered_deleted_decode(WT_ITEM *value)
{
    if (value->size > __tombstone.size &&
      memcmp(value->data, __tombstone.data, __tombstone.size) == 0)
        --value->size;
}

/*
 * __clayered_get_collator --
 *     Retrieve the collator for a layered cursor. Wrapped in a function, since in the future the
 *     collator might live in a constituent cursor instead of the handle.
 */
static void
__clayered_get_collator(WT_CURSOR_LAYERED *clayered, WT_COLLATOR **collatorp)
{
    *collatorp = ((WT_LAYERED_TABLE *)clayered->dhandle)->collator;
}

/*
 * __clayered_cursor_compare --
 *     Compare two constituent cursors in a layered tree
 */
static int
__clayered_cursor_compare(WT_CURSOR_LAYERED *clayered, WT_CURSOR *c1, WT_CURSOR *c2, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);

    WT_ASSERT_ALWAYS(session, F_ISSET(c1, WT_CURSTD_KEY_SET) && F_ISSET(c2, WT_CURSTD_KEY_SET),
      "Can only compare cursors with keys available in layered tree");

    __clayered_get_collator(clayered, &collator);
    return (__wt_compare(session, collator, &c1->key, &c2->key, cmpp));
}

/*
 * __clayered_enter --
 *     Start an operation on a layered cursor.
 */
static WT_INLINE int
__clayered_enter(WT_CURSOR_LAYERED *clayered, bool reset, bool update)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);

    if (reset) {
        WT_ASSERT(session, !F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT));
        WT_RET(__clayered_reset_cursors(clayered, false));
    }

    for (;;) {
        /*
         * Stop when we are up-to-date, as long as this is:
         *   - an update operation with a ingest cursor, or
         *   - a read operation and the cursor is open for reading.
         */
        if ((update && clayered->ingest_cursor != NULL) ||
          (!update && F_ISSET(clayered, WT_CLAYERED_OPEN_READ)))
            break;

        WT_WITH_SCHEMA_LOCK(session, ret = __clayered_open_cursors(clayered, update));
        WT_RET(ret);
    }

    if (!F_ISSET(clayered, WT_CLAYERED_ACTIVE)) {
        /*
         * Opening this layered cursor has opened a number of btree cursors, ensure other code
         * doesn't think this is the first cursor in a session.
         */
        ++session->ncursors;
        WT_RET(__cursor_enter(session));
        F_SET(clayered, WT_CLAYERED_ACTIVE);
    }

    return (0);
}

/*
 * __clayered_leave --
 *     Finish an operation on a layered cursor.
 */
static void
__clayered_leave(WT_CURSOR_LAYERED *clayered)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);

    if (F_ISSET(clayered, WT_CLAYERED_ACTIVE)) {
        --session->ncursors;
        __cursor_leave(session);
        F_CLR(clayered, WT_CLAYERED_ACTIVE);
    }
}

/*
 * __clayered_close_cursors --
 *     Close any btree cursors that are not needed.
 */
static int
__clayered_close_cursors(WT_CURSOR_LAYERED *clayered)
{
    WT_CURSOR *c;

    clayered->current_cursor = NULL;
    if ((c = clayered->ingest_cursor) != NULL) {
        WT_RET(c->close(c));
        clayered->ingest_cursor = NULL;
    }
    if ((c = clayered->stable_cursor) != NULL) {
        WT_RET(c->close(c));
        clayered->stable_cursor = NULL;
    }

    clayered->flags = 0;
    return (0);
}

/*
 * __clayered_open_cursors --
 *     Open cursors for the current set of files.
 */
static int
__clayered_open_cursors(WT_CURSOR_LAYERED *clayered, bool update)
{
    WT_CURSOR *c;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    WT_SESSION_IMPL *session;
    u_int cfg_pos;
    char random_config[1024];
    const char *ckpt_cfg[4];
    bool defer_stable, leader;

    c = &clayered->iface;
    defer_stable = false;
    session = CUR2S(clayered);
    layered = (WT_LAYERED_TABLE *)session->dhandle;

    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->schema_lock);

    /*
     * Query operations need a full set of cursors. Overwrite cursors do queries in service of
     * updates.
     */
    if (!update || !F_ISSET(c, WT_CURSTD_OVERWRITE))
        F_SET(clayered, WT_CLAYERED_OPEN_READ);

    /*
     * Cursors open for updates only open the ingest cursor, cursors open for read open both. If the
     * right cursors are already open we are done. NOTE: This should become more complex as the
     * stable cursor can have the checkpoint updated in that case this code will close the current
     * stable cursor and open a new one to get the more recent checkpoint information and allow for
     * garbage collection.
     */
    if (clayered->ingest_cursor != NULL && clayered->stable_cursor != NULL)
        return (0);

    cfg_pos = 0;
    ckpt_cfg[cfg_pos++] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
    /*
     * If the layered cursor is configured with next_random, we'll need to open any constituent
     * cursors with the same configuration that is relevant for random cursors.
     */
    if (F_ISSET(clayered, WT_CLAYERED_RANDOM)) {
        WT_RET(__wt_snprintf(random_config, sizeof(random_config),
          "next_random=true,next_random_seed=%" PRId64 ",next_random_sample_size=%" PRIu64,
          clayered->next_random_seed, (uint64_t)clayered->next_random_sample_size));
        ckpt_cfg[cfg_pos++] = random_config;
    }

    /*
     * If the key is pointing to memory that is pinned by a chunk cursor, take a copy before closing
     * cursors.
     */
    if (F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_RET(__cursor_needkey(c));

    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    /* Always open both the ingest and stable cursors */
    if (clayered->ingest_cursor == NULL) {
        ckpt_cfg[cfg_pos] = NULL;
        WT_RET(__wt_open_cursor(
          session, layered->ingest_uri, &clayered->iface, ckpt_cfg, &clayered->ingest_cursor));
        F_SET(clayered->ingest_cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);
    }

    if (clayered->stable_cursor == NULL) {
        leader = S2C(session)->layered_table_manager.leader;
        if (!leader)
            /*
             * We may have a stable chunk with no checkpoint yet. If that's the case then open a
             * cursor on stable without a checkpoint. It will never return an invalid result (it's
             * content is by definition trailing the ingest cursor. It is just slightly less
             * efficient, and also not an accurate reflection of what we want in terms of sharing
             * checkpoint across different WiredTiger instances eventually.
             */
            ckpt_cfg[cfg_pos++] = ",raw,checkpoint_use_history=false,force=true";
        ckpt_cfg[cfg_pos] = NULL;
        ret = __wt_open_cursor(
          session, layered->stable_uri, &clayered->iface, ckpt_cfg, &clayered->stable_cursor);

        if (ret == WT_NOTFOUND && !leader) {
            ckpt_cfg[1] = "";
            ret = __wt_open_cursor(
              session, layered->stable_uri, &clayered->iface, ckpt_cfg, &clayered->stable_cursor);
            if (ret == 0)
                F_SET(clayered, WT_CLAYERED_STABLE_NO_CKPT);
        } else if (ret == ENOENT && !leader) {
            /* This is fine, we may not have seen a checkpoint with this table yet. */
            ret = 0;
            defer_stable = true;
        } else if (ret == WT_NOTFOUND)
            WT_RET(__wt_panic(
              session, WT_PANIC, "Layered table could not access stable table on leader"));

        WT_RET(ret);
        if (clayered->stable_cursor != NULL)
            F_SET(clayered->stable_cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);
    }

    if (F_ISSET(clayered, WT_CLAYERED_RANDOM)) {
        /*
         * Cursors configured with next_random only allow the next method to be called. But our
         * implementation of random requires search_near to be called on the two constituent
         * cursors, so explicitly allow that here.
         */
        WT_ASSERT(session, WT_PREFIX_MATCH(clayered->ingest_cursor->uri, "file:"));
        clayered->ingest_cursor->search_near = __wti_curfile_search_near;

        /* TODO SLS-1052 make sure this gets set if the stable constituent appears later. */
        if (!defer_stable) {
            clayered->stable_cursor->search_near = __wti_curfile_search_near;
            WT_ASSERT(session, WT_PREFIX_MATCH(clayered->stable_cursor->uri, "file:"));
        }
    }

    WT_RET(__clayered_copy_bounds(clayered));

    return (ret);
}

/*
 * __clayered_get_current --
 *     Find the smallest / largest of the cursors and copy its key/value.
 */
static int
__clayered_get_current(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, bool smallest, bool *deletedp)
{
    WT_COLLATOR *collator;
    WT_CURSOR *c, *current;
    int cmp;
    bool ingest_positioned, stable_positioned;

    c = &clayered->iface;
    current = NULL;
    ingest_positioned = stable_positioned = false;

    /*
     * There are a couple of cases to deal with here: Some cursors don't have both ingest and stable
     * cursors. Some cursor positioning operations will only have one positioned cursor (e.g a walk
     * has exhausted one cursor but not the other).
     */
    if (clayered->ingest_cursor != NULL && F_ISSET(clayered->ingest_cursor, WT_CURSTD_KEY_INT))
        ingest_positioned = true;

    if (clayered->stable_cursor != NULL && F_ISSET(clayered->stable_cursor, WT_CURSTD_KEY_INT))
        stable_positioned = true;

    if (!ingest_positioned && !stable_positioned) {
        F_CLR(clayered, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        return (WT_NOTFOUND);
    }

    __clayered_get_collator(clayered, &collator);

    if (ingest_positioned && stable_positioned) {
        WT_RET(__wt_compare(
          session, collator, &clayered->ingest_cursor->key, &clayered->stable_cursor->key, &cmp));
        if (smallest ? cmp < 0 : cmp > 0)
            current = clayered->ingest_cursor;
        else if (cmp == 0)
            current = clayered->ingest_cursor;
        else
            current = clayered->stable_cursor;

        /*
         * If the cursors are equal, choose the ingest cursor to return the result but remember not
         * to later return the same result from the stable cursor.
         */
        if (cmp == 0)
            F_SET(clayered, WT_CLAYERED_MULTIPLE);
        else
            F_CLR(clayered, WT_CLAYERED_MULTIPLE);

    } else if (ingest_positioned)
        current = clayered->ingest_cursor;
    else if (stable_positioned)
        current = clayered->stable_cursor;

    if ((clayered->current_cursor = current) == NULL) {
        F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        return (WT_NOTFOUND);
    }

    WT_RET(current->get_key(current, &c->key));
    WT_RET(current->get_value(current, &c->value));

    F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if ((*deletedp = __clayered_deleted(&c->value)) == false)
        F_SET(c, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    return (0);
}

/*
 * __clayered_compare --
 *     WT_CURSOR->compare implementation for the layered cursor type.
 */
static int
__clayered_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /* There's no need to sync with the layered tree, avoid layered enter. */
    clayered = (WT_CURSOR_LAYERED *)a;
    CURSOR_API_CALL(a, session, ret, compare, clayered->dhandle);

    /*
     * Confirm both cursors refer to the same source and have keys, then compare the keys.
     */
    if (strcmp(a->uri, b->uri) != 0)
        WT_ERR_MSG(session, EINVAL, "comparison method cursors must reference the same object");

    /* Both cursors are from the same tree - they share the same collator */
    __clayered_get_collator(clayered, &collator);

    WT_ERR(__wt_compare(session, collator, &a->key, &b->key, cmpp));

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_position_constituent --
 *     Position a constituent cursor.
 */
static int
__clayered_position_constituent(WT_CURSOR_LAYERED *clayered, WT_CURSOR *c, bool forward, int *cmpp)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    cursor = &clayered->iface;
    session = CUR2S(cursor);

    c->set_key(c, &cursor->key);
    WT_RET(c->search_near(c, cmpp));

    while (forward ? *cmpp < 0 : *cmpp > 0) {
        WT_RET(forward ? c->next(c) : c->prev(c));

        /*
         * With higher isolation levels, where we have stable reads, we're done: the cursor is now
         * positioned as expected.
         *
         * With read-uncommitted isolation, a new record could have appeared in between the search
         * and stepping forward / back. In that case, keep going until we see a key in the expected
         * range.
         */
        if (session->txn->isolation != WT_ISO_READ_UNCOMMITTED)
            return (0);

        WT_RET(__clayered_cursor_compare(clayered, c, cursor, cmpp));
    }

    return (0);
}

/*
 * __clayered_iterate_constituent --
 *     Move a constituent cursor of a layered tree and setup the general positioning necessary to
 *     reflect that.
 */
static int
__clayered_iterate_constituent(WT_CURSOR_LAYERED *clayered, WT_CURSOR *constituent, bool forward)
{
    WT_DECL_RET;
    int cmp;

    /* We may not have this table yet, e.g. for a stable cursor on a secondary. */
    if (constituent == NULL)
        return (0);

    /* To iterate a layered cursor, which has two constituent cursors, we are in one of a few
     * states:
     * * Neither constituent is positioned - in which case both cursors need to be moved to the
     * start (or end) of the tree.
     * * Both cursors are positioned, one of which is the "current" cursor, which means it was used
     * to return the position on the prior iteration. That current cursor needs to be moved forward
     * one spot.
     * * Both cursors are positioned, the constituent cursor being checked wasn't the current, so it
     * has been moved to a position that hasn't yet been returned to the application. It does not
     * need to be moved forward.
     */
    if (!F_ISSET(constituent, WT_CURSTD_KEY_SET)) {
        WT_RET(constituent->reset(constituent));
        ret = forward ? constituent->next(constituent) : constituent->prev(constituent);
    } else if (constituent != clayered->current_cursor &&
      (ret = __clayered_position_constituent(clayered, constituent, true, &cmp)) == 0 && cmp == 0 &&
      clayered->current_cursor == NULL)
        clayered->current_cursor = constituent;
    WT_RET_NOTFOUND_OK(ret);

    return (0);
}

/*
 * __clayered_next --
 *     WT_CURSOR->next method for the layered cursor type.
 */
static int
__clayered_next(WT_CURSOR *cursor)
{
    WT_CURSOR *alternate_cursor, *c;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp;
    bool deleted;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, next, clayered->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false));

    /* If we aren't positioned for a forward scan, get started. */
    if (clayered->current_cursor == NULL || !F_ISSET(clayered, WT_CLAYERED_ITERATE_NEXT)) {
        WT_ERR(__clayered_iterate_constituent(clayered, clayered->ingest_cursor, true));
        WT_ERR(__clayered_iterate_constituent(clayered, clayered->stable_cursor, true));
        F_SET(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_MULTIPLE);
        F_CLR(clayered, WT_CLAYERED_ITERATE_PREV);

        /* We just positioned *at* the key, now move. */
        if (clayered->current_cursor != NULL)
            goto retry;
    } else {
retry:
        /* If there are multiple cursors on that key, move them forward. */
        if (clayered->current_cursor == clayered->stable_cursor)
            alternate_cursor = clayered->ingest_cursor;
        else
            alternate_cursor = clayered->stable_cursor;

        if (alternate_cursor != NULL && F_ISSET(alternate_cursor, WT_CURSTD_KEY_INT)) {
            if (alternate_cursor != clayered->current_cursor) {
                WT_ERR(__clayered_cursor_compare(
                  clayered, alternate_cursor, clayered->current_cursor, &cmp));
                if (cmp == 0)
                    WT_ERR_NOTFOUND_OK(alternate_cursor->next(alternate_cursor), false);
            }
        }

        /* Move the smallest cursor forward. */
        c = clayered->current_cursor;
        WT_ERR_NOTFOUND_OK(c->next(c), false);
    }

    /* Find the cursor(s) with the smallest key. */
    if ((ret = __clayered_get_current(session, clayered, true, &deleted)) == 0 && deleted)
        goto retry;

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_next);
    if (clayered->current_cursor == clayered->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_next_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_next_stable);

err:
    __clayered_leave(clayered);
    if (ret == 0)
        __clayered_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __layered_prev_int --
 *     WT_CURSOR->prev method for the layered cursor type (internal version).
 */
static int
__layered_prev_int(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
{
    WT_CURSOR *alternate_cursor, *c;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    int cmp;
    bool deleted;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false));

    /* If we aren't positioned for a reverse scan, get started. */
    if (clayered->current_cursor == NULL || !F_ISSET(clayered, WT_CLAYERED_ITERATE_PREV)) {
        WT_ERR(__clayered_iterate_constituent(clayered, clayered->ingest_cursor, false));
        WT_ERR(__clayered_iterate_constituent(clayered, clayered->stable_cursor, false));
        F_SET(clayered, WT_CLAYERED_ITERATE_PREV | WT_CLAYERED_MULTIPLE);
        F_CLR(clayered, WT_CLAYERED_ITERATE_PREV);

        /* We just positioned *at* the key, now move. */
        if (clayered->current_cursor != NULL)
            goto retry;
    } else {
retry:
        /* If there are multiple cursors on that key, move them backwards. */
        if (clayered->current_cursor == clayered->stable_cursor)
            alternate_cursor = clayered->ingest_cursor;
        else
            alternate_cursor = clayered->stable_cursor;

        if (alternate_cursor != NULL && F_ISSET(alternate_cursor, WT_CURSTD_KEY_INT)) {
            if (alternate_cursor != clayered->current_cursor) {
                WT_ERR(__clayered_cursor_compare(
                  clayered, alternate_cursor, clayered->current_cursor, &cmp));
                if (cmp == 0)
                    WT_ERR_NOTFOUND_OK(alternate_cursor->prev(alternate_cursor), false);
            }
        }

        /* Move the smallest cursor forward. */
        c = clayered->current_cursor;
        WT_ERR_NOTFOUND_OK(c->prev(c), false);
    }

    /* Find the cursor(s) with the largest key. */
    if ((ret = __clayered_get_current(session, clayered, false, &deleted)) == 0 && deleted)
        goto retry;

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev);
    if (clayered->current_cursor == clayered->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev_stable);

err:
    __clayered_leave(clayered);
    if (ret == 0)
        __clayered_deleted_decode(&cursor->value);
    return (ret);
}

/*
 * __layered_prev --
 *     WT_CURSOR->prev method for the layered cursor type.
 */
static int
__layered_prev(WT_CURSOR *cursor)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    CURSOR_API_CALL(cursor, session, ret, prev, ((WT_CURSOR_LAYERED *)cursor)->dhandle);
    WT_ERR(__layered_prev_int(session, cursor));

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_reset_cursors --
 *     Reset any positioned constituent cursors. If the skip parameter is non-NULL, that cursor is
 *     about to be used, so there is no need to reset it.
 */
static int
__clayered_reset_cursors(WT_CURSOR_LAYERED *clayered, bool skip_ingest)
{
    WT_CURSOR *c;
    WT_DECL_RET;

    /* Fast path if the cursor is not positioned. */
    if (clayered->current_cursor == NULL &&
      !F_ISSET(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV))
        return (0);

    c = clayered->stable_cursor;
    if (c != NULL && F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_TRET(c->reset(c));

    c = clayered->ingest_cursor;
    if (!skip_ingest && F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_TRET(c->reset(c));

    clayered->current_cursor = NULL;
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    return (ret);
}

/*
 * __clayered_reset --
 *     WT_CURSOR->reset method for the layered cursor type.
 */
static int
__clayered_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __clayered_enter path: that is wasted work when all we want to do is
     * give up our position.
     */
    clayered = (WT_CURSOR_LAYERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, clayered->dhandle);

    /* Reset any bounds on the top level cursor, and propagate that to constituents */
    __wt_cursor_bound_reset(cursor);
    WT_ERR(__clayered_copy_bounds(clayered));

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_TRET(__clayered_reset_cursors(clayered, false));

    /* In case we were left positioned, clear that. */
    __clayered_leave(clayered);

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_copy_constituent_bound --
 *     Copy the top level bound into a single constituent cursor
 */
static int
__clayered_copy_constituent_bound(WT_CURSOR_LAYERED *clayered, WT_CURSOR *constituent)
{
    WT_CURSOR *base_cursor;
    WT_ITEM *layered_bound, *constituent_bound;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);
    base_cursor = (WT_CURSOR *)clayered;

    if (constituent == NULL)
        return (0);

    /* Note that the inclusive flag is additive to upper/lower, so no need to check it as well */
    if (F_ISSET(base_cursor, WT_CURSTD_BOUND_UPPER)) {
        /* If an upper bound is already present on the constituent, make sure it matches */
        if (F_ISSET(constituent, WT_CURSTD_BOUND_UPPER)) {
            layered_bound = &base_cursor->upper_bound;
            constituent_bound = &constituent->upper_bound;
            WT_ASSERT_ALWAYS(session,
              layered_bound->size == constituent_bound->size &&
                memcmp(layered_bound->data, constituent_bound->data, layered_bound->size) == 0,
              "Setting an upper bound on a layered cursor and a constituent already has a "
              "different bound");
        } else
            WT_RET(__wt_buf_set(session, &constituent->upper_bound, base_cursor->upper_bound.data,
              base_cursor->upper_bound.size));
    } else {
        __wt_buf_free(session, &constituent->upper_bound);
        WT_CLEAR(constituent->upper_bound);
    }
    if (F_ISSET(base_cursor, WT_CURSTD_BOUND_LOWER)) {
        /* If a lower bound is already present on the constituent, make sure it matches */
        if (F_ISSET(constituent, WT_CURSTD_BOUND_LOWER)) {
            layered_bound = &base_cursor->lower_bound;
            constituent_bound = &constituent->lower_bound;
            WT_ASSERT_ALWAYS(session,
              layered_bound->size == constituent_bound->size &&
                memcmp(layered_bound->data, constituent_bound->data, layered_bound->size) == 0,
              "Setting a lower bound on a layered cursor and a constituent already has a different "
              "bound");
        } else
            WT_RET(__wt_buf_set(session, &constituent->lower_bound, base_cursor->lower_bound.data,
              base_cursor->lower_bound.size));
    } else {
        __wt_buf_free(session, &constituent->lower_bound);
        WT_CLEAR(constituent->lower_bound);
    }
    /* Copy across all the bound configurations */
    F_CLR(constituent, WT_CURSTD_BOUND_ALL);
    F_SET(constituent, F_MASK(base_cursor, WT_CURSTD_BOUND_ALL));
    return (0);
}

/*
 * __clayered_copy_bounds --
 *     A method for copying (or clearing) bounds on constituent cursors within a layered cursor
 */
static int
__clayered_copy_bounds(WT_CURSOR_LAYERED *clayered)
{
    WT_RET(__clayered_copy_constituent_bound(clayered, clayered->ingest_cursor));
    WT_RET(__clayered_copy_constituent_bound(clayered, clayered->stable_cursor));
    return (0);
}

/*
 * __clayered_bound --
 *     WT_CURSOR->bound method for the layered cursor type.
 */
static int
__clayered_bound(WT_CURSOR *cursor, const char *config)
{
    WT_COLLATOR *collator;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_CONF(WT_CURSOR, bound, conf);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    /*
     * The bound interface operates on an unpositioned cursor, so skip entering the layered cursor
     * for this API.
     */
    CURSOR_API_CALL(cursor, session, ret, bound, clayered->dhandle);

    WT_ERR(__wt_conf_compile_api_call(session, WT_CONFIG_REF(session, WT_CURSOR_bound),
      WT_CONFIG_ENTRY_WT_CURSOR_bound, config, &_conf, sizeof(_conf), &conf));

    __clayered_get_collator(clayered, &collator);
    /* Setup bounds on this top level cursor */
    WT_ERR(__wti_cursor_bound(cursor, conf, collator));

    /*
     * Copy those bounds into the constituents. Note that the constituent cursors may not be open
     * yet, and that would be fine, the layered cursor open interface handles setting up configured
     * bounds as well.
     */
    WT_ERR(__clayered_copy_bounds(clayered));

err:
    if (ret != 0) {
        /* Free any bounds we set on the top level cursor before the error */
        if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER)) {
            __wt_buf_free(session, &cursor->upper_bound);
            WT_CLEAR(cursor->upper_bound);
        }
        if (F_ISSET(cursor, WT_CURSTD_BOUND_LOWER)) {
            __wt_buf_free(session, &cursor->lower_bound);
            WT_CLEAR(cursor->lower_bound);
        }
        F_CLR(cursor, WT_CURSTD_BOUND_ALL);
        /* Ensure the bounds are cleaned up on any constituents */
        WT_TRET(__clayered_copy_bounds(clayered));
    }
    API_END_RET(session, ret);
}

/*
 * __clayered_lookup --
 *     Position a layered cursor.
 */
static int
__clayered_lookup(WT_CURSOR_LAYERED *clayered, WT_ITEM *value)
{
    WT_CURSOR *c, *cursor;
    WT_DECL_RET;
    bool found;

    c = NULL;
    cursor = &clayered->iface;
    found = false;

    c = clayered->ingest_cursor;
    c->set_key(c, &cursor->key);
    if ((ret = c->search(c)) == 0) {
        WT_ERR(c->get_key(c, &cursor->key));
        WT_ERR(c->get_value(c, value));
        if (__clayered_deleted(value))
            ret = WT_NOTFOUND;
        /*
         * Even a tombstone is considered found here - the delete overrides any remaining record in
         * the stable constituent.
         */
        found = true;
    }
    WT_ERR_NOTFOUND_OK(ret, true);
    if (!found)
        F_CLR(c, WT_CURSTD_KEY_SET);

    /*
     * If the key didn't exist in the ingest constituent and the cursor is setup for reading, check
     * the stable constituent.
     */
    if (!found && F_ISSET(clayered, WT_CLAYERED_OPEN_READ) && clayered->stable_cursor != NULL) {
        c = clayered->stable_cursor;
        c->set_key(c, &cursor->key);
        if ((ret = c->search(c)) == 0) {
            WT_ERR(c->get_key(c, &cursor->key));
            WT_ERR(c->get_value(c, value));
            if (__clayered_deleted(value))
                ret = WT_NOTFOUND;
            found = true;
        }
        WT_ERR_NOTFOUND_OK(ret, true);
        if (!found)
            F_CLR(c, WT_CURSTD_KEY_SET);
    }

err:
    if (ret == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT);
        clayered->current_cursor = c;
        if (value == &cursor->value)
            F_SET(cursor, WT_CURSTD_VALUE_INT);
    } else if (c != NULL)
        WT_TRET(c->reset(c));

    return (ret);
}

/*
 * __clayered_search --
 *     WT_CURSOR->search method for the layered cursor type.
 */
static int
__clayered_search(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, search, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, true, false));
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    ret = __clayered_lookup(clayered, &cursor->value);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_search);
    if (clayered->current_cursor == clayered->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_stable);

err:
    __clayered_leave(clayered);
    if (ret == 0)
        __clayered_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __clayered_search_near --
 *     WT_CURSOR->search_near method for the layered cursor type.
 */
static int
__clayered_search_near(WT_CURSOR *cursor, int *exactp)
{
    WT_CURSOR *closest;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp, ingest_cmp, stable_cmp;
    bool deleted, ingest_found, stable_found;

    closest = NULL;
    clayered = (WT_CURSOR_LAYERED *)cursor;
    ingest_cmp = stable_cmp = 0;
    stable_found = false;

    CURSOR_API_CALL(cursor, session, ret, search_near, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, true, false));
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    /*
     * search_near is somewhat fiddly: we can't just use a nearby key from the current constituent
     * because there could be a closer key in the other table.
     *
     * The semantics are:
     * * An exact match always wins.
     * * Otherwise a larger key is preferred if one exists.
     * * Otherwise a smaller key should be returned.
     * If both constituents have a larger key available, return the one closes to the search term.
     */
    clayered->ingest_cursor->set_key(clayered->ingest_cursor, &cursor->key);
    WT_ERR_NOTFOUND_OK(
      clayered->ingest_cursor->search_near(clayered->ingest_cursor, &ingest_cmp), true);
    ingest_found = ret != WT_NOTFOUND;

    /* If there wasn't an exact match, check the stable table as well */
    if ((!ingest_found || ingest_cmp != 0) && clayered->stable_cursor != NULL) {
        clayered->stable_cursor->set_key(clayered->stable_cursor, &cursor->key);
        WT_ERR_NOTFOUND_OK(
          clayered->stable_cursor->search_near(clayered->stable_cursor, &stable_cmp), true);
        stable_found = ret != WT_NOTFOUND;
    }

    if (!ingest_found && !stable_found) {
        ret = WT_NOTFOUND;
        goto err;
    } else if (!stable_found)
        closest = clayered->ingest_cursor;
    else if (!ingest_found)
        closest = clayered->stable_cursor;

    /* Now that we know there are two positioned cursors - choose the one with the best match */
    if (closest == NULL) {
        if (ingest_cmp == 0)
            closest = clayered->ingest_cursor;
        else if (stable_cmp == 0)
            closest = clayered->stable_cursor;
        else if (ingest_cmp > 0 && stable_cmp > 0) {
            WT_ERR(__clayered_cursor_compare(
              clayered, clayered->ingest_cursor, clayered->stable_cursor, &cmp));
            if (cmp < 0)
                closest = clayered->stable_cursor;
            else
                /* If the cursors were identical, or ingest was closer choose ingest. */
                closest = clayered->ingest_cursor;
        } else if (ingest_cmp > 0)
            closest = clayered->ingest_cursor;
        else if (stable_cmp > 0)
            closest = clayered->stable_cursor;
        else { /* Both cursors were smaller than the search key - choose the bigger one */
            WT_ERR(__clayered_cursor_compare(
              clayered, clayered->ingest_cursor, clayered->stable_cursor, &cmp));
            if (cmp > 0) {
                closest = clayered->stable_cursor;
            } else {
                /* If the cursors were identical, or ingest was closer choose ingest. */
                closest = clayered->ingest_cursor;
            }
        }
    }

    /*
     * If we land on a deleted item, try going forwards or backwards to find one that isn't deleted.
     * If the whole tree is empty, we'll end up with WT_NOTFOUND, as expected.
     */
    WT_ASSERT_ALWAYS(session, closest != NULL, "Layered search near should have found something");
    WT_ERR(closest->get_key(closest, &cursor->key));
    WT_ERR(closest->get_value(closest, &cursor->value));

    /* Get prepared for finalizing the result before fixing up for tombstones. */
    if (closest == clayered->ingest_cursor)
        cmp = ingest_cmp;
    else
        cmp = stable_cmp;
    clayered->current_cursor = closest;
    closest = NULL;

    deleted = __clayered_deleted(&cursor->value);
    if (!deleted)
        __clayered_deleted_decode(&cursor->value);
    else {
        /*
         * We have a key pointing at memory that is pinned by the current chunk cursor. In the
         * unlikely event that we have to reopen cursors to move to the next record, make sure the
         * cursor flags are set so a copy is made before the current chunk cursor releases its
         * position.
         */
        F_CLR(cursor, WT_CURSTD_KEY_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT);
        /* Advance past the deleted record using normal cursor traversal interface */
        if ((ret = __clayered_next(cursor)) == 0) {
            cmp = 1;
            deleted = false;
        }
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (deleted) {
        clayered->current_cursor = NULL;
        WT_ERR(__layered_prev(cursor));
        cmp = -1;
    }
    *exactp = cmp;

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near);
    if (clayered->current_cursor == clayered->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near_stable);

err:
    __clayered_leave(clayered);
    if (closest != NULL)
        WT_TRET(closest->reset(closest));

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if (ret == 0) {
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else
        clayered->current_cursor = NULL;

    API_END_RET(session, ret);
}

/*
 * __clayered_put --
 *     Put an entry into the desired tree.
 */
static WT_INLINE int
__clayered_put(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key,
  const WT_ITEM *value, bool position, bool reserve)
{
    WT_CURSOR *c;
    int (*func)(WT_CURSOR *);

    /*
     * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
     * anyway.
     */
    WT_RET(__clayered_reset_cursors(clayered, true));

    if (S2C(session)->layered_table_manager.leader)
        c = clayered->stable_cursor;
    else
        c = clayered->ingest_cursor;

    /* If necessary, set the position for future scans. */
    if (position)
        clayered->current_cursor = c;

    c->set_key(c, key);
    func = c->insert;
    if (position)
        func = reserve ? c->reserve : c->update;
    if (func != c->reserve)
        c->set_value(c, value);
    WT_RET(func(c));

    /* TODO: Need something to add a log record? */

    return (0);
}

/*
 * __clayered_modify_int --
 *     Put an modify into the desired tree.
 */
static WT_INLINE int
__clayered_modify_int(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key,
  WT_MODIFY *entries, int nentries)
{
    WT_CURSOR *c;

    /*
     * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
     * anyway.
     */
    WT_RET(__clayered_reset_cursors(clayered, true));

    if (S2C(session)->layered_table_manager.leader)
        c = clayered->stable_cursor;
    else
        c = clayered->ingest_cursor;

    clayered->current_cursor = c;

    c->set_key(c, key);
    WT_RET(c->modify(c, entries, nentries));

    /* TODO: Need something to add a log record? */

    return (0);
}

/*
 * __clayered_insert --
 *     WT_CURSOR->insert method for the layered cursor type.
 */
static int
__clayered_insert(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, insert, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__clayered_enter(clayered, false, true));

    /*
     * It isn't necessary to copy the key out after the lookup in this case because any non-failed
     * lookup results in an error, and a failed lookup leaves the original key intact.
     */
    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
      (ret = __clayered_lookup(clayered, &value)) != WT_NOTFOUND) {
        if (ret == 0)
            ret = WT_DUPLICATE_KEY;
        goto err;
    }

    WT_ERR(__clayered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__clayered_put(session, clayered, &cursor->key, &value, false, false));

    /*
     * WT_CURSOR.insert doesn't leave the cursor positioned, and the application may want to free
     * the memory used to configure the insert; don't read that memory again (matching the
     * underlying file object cursor insert semantics).
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_insert);
err:
    __wt_scr_free(session, &buf);
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_update --
 *     WT_CURSOR->update method for the layered cursor type.
 */
static int
__clayered_update(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, update, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__clayered_enter(clayered, false, true));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__clayered_lookup(clayered, &value));
        /*
         * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__clayered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__clayered_put(session, clayered, &cursor->key, &value, true, false));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_ITEM_SET(cursor->key, clayered->current_cursor->key);
    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    WT_ASSERT(session, F_MASK(clayered->current_cursor, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(
      session, F_MASK(clayered->current_cursor, WT_CURSTD_VALUE_SET) == WT_CURSTD_VALUE_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_update);

err:
    __wt_scr_free(session, &buf);
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_remove --
 *     WT_CURSOR->remove method for the layered cursor type.
 */
static int
__clayered_remove(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;
    bool positioned;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    /* Remember if the cursor is currently positioned. */
    positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT);

    CURSOR_REMOVE_API_CALL(cursor, session, ret, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);

    /*
     * Remove fails if the key doesn't exist, do a search first. This requires a second pair of
     * layered enter/leave calls as we search the full stack, but updates are limited to the
     * top-level.
     */
    WT_ERR(__clayered_enter(clayered, false, false));
    WT_ERR(__clayered_lookup(clayered, &value));
    __clayered_leave(clayered);

    WT_ERR(__clayered_enter(clayered, false, true));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__clayered_put(session, clayered, &cursor->key, &__tombstone, true, false));

    /*
     * If the cursor was positioned, it stays positioned with a key but no value, otherwise, there's
     * no position, key or value. This isn't just cosmetic, without a reset, iteration on this
     * cursor won't start at the beginning/end of the table.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if (positioned)
        F_SET(cursor, WT_CURSTD_KEY_INT);
    else
        WT_TRET(cursor->reset(cursor));
    WT_STAT_CONN_DSRC_INCR(session, layered_curs_remove);

err:
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_reserve --
 *     WT_CURSOR->reserve method for the layered cursor type.
 */
static int
__clayered_reserve(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, reserve, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__wt_txn_context_check(session, true));
    WT_ERR(__clayered_enter(clayered, false, true));

    WT_ERR(__clayered_lookup(clayered, &value));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    ret = __clayered_put(session, clayered, &cursor->key, NULL, true, true);

err:
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);

    /*
     * The application might do a WT_CURSOR.get_value call when we return, so we need a value and
     * the underlying functions didn't set one up. For various reasons, those functions may not have
     * done a search and any previous value in the cursor might race with WT_CURSOR.reserve (and in
     * cases like layered tables, the reserve never encountered the original key). For simplicity,
     * repeat the search here.
     */
    return (ret == 0 ? cursor->search(cursor) : ret);
}

/*
 * __clayered_largest_key --
 *     WT_CURSOR->largest_key implementation for layered tables.
 */
static int
__clayered_largest_key(WT_CURSOR *cursor)
{
    WT_COLLATOR *collator;
    WT_CURSOR *larger_cursor, *ingest_cursor, *stable_cursor;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(key);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp;
    bool ingest_found, stable_found;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    ingest_found = stable_found = false;

    CURSOR_API_CALL(cursor, session, ret, largest_key, clayered->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false));

    ingest_cursor = clayered->ingest_cursor;
    stable_cursor = clayered->stable_cursor;

    WT_ERR(__wt_scr_alloc(session, 0, &key));

    WT_ERR_NOTFOUND_OK(ingest_cursor->largest_key(ingest_cursor), true);
    if (ret == 0)
        ingest_found = true;

    if (stable_cursor != NULL) {
        WT_ERR_NOTFOUND_OK(stable_cursor->largest_key(stable_cursor), true);
        if (ret == 0)
            stable_found = true;
    }

    if (!ingest_found && !stable_found) {
        ret = WT_NOTFOUND;
        goto err;
    }

    if (ingest_found && !stable_found)
        larger_cursor = ingest_cursor;
    else if (!ingest_found && stable_found) {
        larger_cursor = stable_cursor;
    } else {
        __clayered_get_collator(clayered, &collator);
        if (stable_cursor == NULL)
            larger_cursor = ingest_cursor;
        else {
            WT_ERR(__wt_compare(session, collator, &ingest_cursor->key, &stable_cursor->key, &cmp));
            if (cmp <= 0)
                larger_cursor = stable_cursor;
            else
                larger_cursor = ingest_cursor;
        }
    }

    /* Copy the key as we will reset the cursor after that. */
    WT_ERR(__wt_buf_set(session, key, larger_cursor->key.data, larger_cursor->key.size));
    WT_ERR(cursor->reset(cursor));
    WT_ERR(__wt_buf_set(session, &cursor->key, key->data, key->size));
    /* Set the key as external. */
    F_SET(cursor, WT_CURSTD_KEY_EXT);

err:
    __clayered_leave(clayered);
    __wt_scr_free(session, &key);
    if (ret != 0)
        WT_TRET(cursor->reset(cursor));
    API_END_RET_STAT(session, ret, cursor_largest_key);
}

/*
 * __clayered_close_int --
 *     Close a layered cursor
 */
static int
__clayered_close_int(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(cursor);
    WT_ASSERT_ALWAYS(session, session->dhandle->type == WT_DHANDLE_TYPE_LAYERED,
      "Valid layered dhandle is required to close a cursor");
    clayered = (WT_CURSOR_LAYERED *)cursor;

    /*
     * If this close is via a connection close the constituent cursors will be closed by a scan of
     * cursors in the session. It might be better to keep them out of the session cursor list, but I
     * don't know how to do that? Probably opening a file cursor directly instead of a table cursor?
     */
    WT_TRET(__clayered_close_cursors(clayered));

    /* In case we were somehow left positioned, clear that. */
    __clayered_leave(clayered);

    __wt_cursor_close(cursor);

    WT_TRET(__wt_session_release_dhandle(session));

    return (ret);
}

/*
 * __wt_clayered_close --
 *     WT_CURSOR->close method for the layered cursor type.
 */
int
__wt_clayered_close(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __clayered_enter path: that is wasted work when closing, and the cursor
     * may never have been used.
     */
    clayered = (WT_CURSOR_LAYERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, clayered->dhandle);
err:

    WT_TRET(__clayered_close_int(cursor));

    API_END_RET(session, ret);
}

/*
 * __clayered_next_random --
 *     WT_CURSOR->next_random method for the layered cursor type.
 */
static int
__clayered_next_random(WT_CURSOR *cursor)
{
    WT_CURSOR *c;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int exact;

    c = NULL; /* Workaround for compilers reporting it as used uninitialized. */
    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, next, clayered->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false));

    for (;;) {
        /* TODO: consider the size of ingest table in the future. */
        if (clayered->stable_cursor != NULL) {
            c = clayered->stable_cursor;
            /*
             * This call to next_random on the layered table can potentially end in WT_NOTFOUND if
             * the layered table is empty. When that happens, use the ingest table.
             */
            WT_ERR_NOTFOUND_OK(__wt_curfile_next_random(c), true);
        } else
            ret = WT_NOTFOUND;

        /* The stable table was either empty or missing. */
        if (ret == WT_NOTFOUND) {
            c = clayered->ingest_cursor;
            WT_ERR(__wt_curfile_next_random(c));
        }

        F_SET(cursor, WT_CURSTD_KEY_INT);
        WT_ERR(c->get_key(c, &cursor->key));

        /*
         * Search near the current key to resolve any tombstones and position to a valid document.
         * If we see a WT_NOTFOUND here that is valid, as the tree has no documents visible to us.
         */
        WT_ERR(__clayered_search_near(cursor, &exact));
        break;
    }

err:
    __clayered_leave(clayered);
    API_END_RET(session, ret);
}

/*
 * __clayered_modify --
 *     WT_CURSOR->modify method for the layered cursor type. This function assumes the modify will
 *     be done on the btree that we originally calculate the diff from. Currently, we only allow
 *     writes to the stable table so the assumption holds. TODO: revisit this when we enable writing
 *     to the ingest table.
 */
static int
__clayered_modify(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, modify, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__clayered_enter(clayered, false, true));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__clayered_lookup(clayered, &value));
        /*
         * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__clayered_modify_int(session, clayered, &cursor->key, entries, nentries));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_ITEM_SET(cursor->key, clayered->current_cursor->key);
    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    WT_ASSERT(session, F_MASK(clayered->current_cursor, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(
      session, F_MASK(clayered->current_cursor, WT_CURSTD_VALUE_SET) == WT_CURSTD_VALUE_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_update);

err:
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __wt_clayered_open --
 *     WT_SESSION->open_cursor method for layered cursors.
 */
int
__wt_clayered_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CONFIG_ITEM cval;
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wt_cursor_get_value,                          /* get-value */
      __wt_cursor_get_raw_key_value,                  /* get-value */
      __wt_cursor_set_key,                            /* set-key */
      __wt_cursor_set_value,                          /* set-value */
      __clayered_compare,                             /* compare */
      __wt_cursor_equals,                             /* equals */
      __clayered_next,                                /* next */
      __layered_prev,                                 /* prev */
      __clayered_reset,                               /* reset */
      __clayered_search,                              /* search */
      __clayered_search_near,                         /* search-near */
      __clayered_insert,                              /* insert */
      __clayered_modify,                              /* modify */
      __clayered_update,                              /* update */
      __clayered_remove,                              /* remove */
      __clayered_reserve,                             /* reserve */
      __wt_cursor_reconfigure,                        /* reconfigure */
      __clayered_largest_key,                         /* largest_key */
      __clayered_bound,                               /* bound */
      __wt_cursor_notsup,                             /* cache */
      __wt_cursor_reopen_notsup,                      /* reopen */
      __wt_cursor_checkpoint_id,                      /* checkpoint ID */
      __wt_clayered_close);                           /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;

    WT_VERIFY_OPAQUE_POINTER(WT_CURSOR_LAYERED);

    clayered = NULL;
    cursor = NULL;

    if (!WT_PREFIX_MATCH(uri, "layered:"))
        return (__wt_unexpected_object_type(session, uri, "layered:"));

    WT_RET(__wt_inmem_unsupported_op(session, "Layered trees"));

    WT_RET(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
    if (cval.len != 0)
        WT_RET_MSG(session, EINVAL, "Layered trees do not support opening by checkpoint");

    WT_RET(__wt_config_gets_def(session, cfg, "bulk", 0, &cval));
    if (cval.val != 0)
        WT_RET_MSG(session, EINVAL, "Layered trees do not support bulk loading");

    /* Get the layered tree, and hold a reference to it until the cursor is closed. */
    WT_RET(__wt_session_get_dhandle(session, uri, NULL, cfg, 0));

    layered = (WT_LAYERED_TABLE *)session->dhandle;
    WT_ASSERT_ALWAYS(session, layered->ingest_uri != NULL && layered->key_format != NULL,
      "Layered handle not setup");

    WT_ERR(__wt_calloc_one(session, &clayered));
    clayered->dhandle = session->dhandle;

    cursor = (WT_CURSOR *)clayered;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    cursor->key_format = layered->key_format;
    cursor->value_format = layered->value_format;

    WT_ERR(__wt_cursor_init(cursor, uri, owner, cfg, cursorp));

    WT_ERR(__wt_config_gets_def(session, cfg, "next_random", 0, &cval));
    if (cval.val != 0) {
        F_SET(clayered, WT_CLAYERED_RANDOM);
        __wt_cursor_set_notsup(cursor);
        cursor->next = __clayered_next_random;

        WT_ERR(__wt_config_gets_def(session, cfg, "next_random_seed", 0, &cval));
        clayered->next_random_seed = cval.val;

        WT_ERR(__wt_config_gets_def(session, cfg, "next_random_sample_size", 0, &cval));
        clayered->next_random_sample_size = (u_int)cval.val;
    }

    if (0) {
err:
        if (clayered != NULL)
            WT_TRET(__wt_clayered_close(cursor));
        WT_TRET(__wt_session_release_dhandle(session));

        *cursorp = NULL;
    }

    return (ret);
}
