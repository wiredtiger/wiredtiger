/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __coligarch_lookup(WT_CURSOR_OLIGARCH *, WT_ITEM *);
static int __coligarch_open_cursors(WT_CURSOR_OLIGARCH *, bool);
static int __coligarch_reset_cursors(WT_CURSOR_OLIGARCH *, bool);
static int __coligarch_search_near(WT_CURSOR *cursor, int *exactp);

/*
 * We need a tombstone to mark deleted records, and we use the special value below for that purpose.
 * We use two 0x14 (Device Control 4) bytes to minimize the likelihood of colliding with an
 * application-chosen encoding byte, if the application uses two leading DC4 byte for some reason,
 * we'll do a wasted data copy each time a new value is inserted into the object.
 */
static const WT_ITEM __tombstone = {"\x14\x14", 2, NULL, 0, 0};

/*
 * __coligarch_deleted --
 *     Check whether the current value is a tombstone.
 */
static WT_INLINE bool
__coligarch_deleted(const WT_ITEM *item)
{
    return (item->size == __tombstone.size &&
      memcmp(item->data, __tombstone.data, __tombstone.size) == 0);
}

/*
 * __coligarch_deleted_encode --
 *     Encode values that are in the encoded name space.
 */
static WT_INLINE int
__coligarch_deleted_encode(
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
 * __coligarch_deleted_decode --
 *     Decode values that start with the tombstone.
 */
static WT_INLINE void
__coligarch_deleted_decode(WT_ITEM *value)
{
    if (value->size > __tombstone.size &&
      memcmp(value->data, __tombstone.data, __tombstone.size) == 0)
        --value->size;
}

/*
 * __coligarch_get_collator --
 *     Retrieve the collator for an oligarch cursor. Wrapped in a function, since in the future the
 *     collator might live in a constituent cursor instead of the handle.
 */
static void
__coligarch_get_collator(WT_CURSOR_OLIGARCH *coligarch, WT_COLLATOR **collatorp)
{
    *collatorp = ((WT_OLIGARCH *)coligarch->dhandle)->collator;
}

/*
 * __coligarch_cursor_compare --
 *     Compare two constituent cursors in an oligarch tree
 */
static int
__coligarch_cursor_compare(WT_CURSOR_OLIGARCH *coligarch, WT_CURSOR *c1, WT_CURSOR *c2, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_SESSION_IMPL *session;

    session = CUR2S(coligarch);

    WT_ASSERT_ALWAYS(session, F_ISSET(c1, WT_CURSTD_KEY_SET) && F_ISSET(c2, WT_CURSTD_KEY_SET),
      "Can only compare cursors with keys available in oligarch tree");

    __coligarch_get_collator(coligarch, &collator);
    return (__wt_compare(session, collator, &c1->key, &c2->key, cmpp));
}

/*
 * __coligarch_enter --
 *     Start an operation on an oligarch cursor.
 */
static WT_INLINE int
__coligarch_enter(WT_CURSOR_OLIGARCH *coligarch, bool reset, bool update)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(coligarch);

    if (reset) {
        WT_ASSERT(session, !F_ISSET(&coligarch->iface, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT));
        WT_RET(__coligarch_reset_cursors(coligarch, false));
    }

    for (;;) {
        /*
         * Stop when we are up-to-date, as long as this is:
         *   - an update operation with a ingest cursor, or
         *   - a read operation and the cursor is open for reading.
         */
        if ((update && coligarch->ingest_cursor != NULL) ||
          (!update && F_ISSET(coligarch, WT_COLIGARCH_OPEN_READ)))
            break;

        WT_WITH_SCHEMA_LOCK(session, ret = __coligarch_open_cursors(coligarch, update));
        WT_RET(ret);
    }

    if (!F_ISSET(coligarch, WT_COLIGARCH_ACTIVE)) {
        /*
         * Opening this oligarch cursor has opened a number of btree cursors, ensure other code
         * doesn't think this is the first cursor in a session.
         */
        ++session->ncursors;
        WT_RET(__cursor_enter(session));
        F_SET(coligarch, WT_COLIGARCH_ACTIVE);
    }

    return (0);
}

/*
 * __coligarch_leave --
 *     Finish an operation on an oligarch cursor.
 */
static void
__coligarch_leave(WT_CURSOR_OLIGARCH *coligarch)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(coligarch);

    if (F_ISSET(coligarch, WT_COLIGARCH_ACTIVE)) {
        --session->ncursors;
        __cursor_leave(session);
        F_CLR(coligarch, WT_COLIGARCH_ACTIVE);
    }
}

/*
 * __coligarch_close_cursors --
 *     Close any btree cursors that are not needed.
 */
static int
__coligarch_close_cursors(WT_CURSOR_OLIGARCH *coligarch)
{
    WT_CURSOR *c;

    coligarch->current_cursor = NULL;
    if ((c = coligarch->ingest_cursor) != NULL) {
        WT_RET(c->close(c));
        coligarch->ingest_cursor = NULL;
    }
    if ((c = coligarch->stable_cursor) != NULL) {
        WT_RET(c->close(c));
        coligarch->stable_cursor = NULL;
    }

    coligarch->flags = 0;
    return (0);
}

/*
 * __coligarch_open_cursors --
 *     Open cursors for the current set of files.
 */
static int
__coligarch_open_cursors(WT_CURSOR_OLIGARCH *coligarch, bool update)
{
    WT_CURSOR *c;
    WT_DECL_RET;
    WT_OLIGARCH *oligarch;
    WT_SESSION_IMPL *session;
    const char *ckpt_cfg[3];

    c = &coligarch->iface;
    session = CUR2S(coligarch);
    oligarch = (WT_OLIGARCH *)session->dhandle;

    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->schema_lock);

    /*
     * Query operations need a full set of cursors. Overwrite cursors do queries in service of
     * updates.
     */
    if (!update || !F_ISSET(c, WT_CURSTD_OVERWRITE))
        F_SET(coligarch, WT_COLIGARCH_OPEN_READ);

    /*
     * Cursors open for updates only open the ingest cursor, cursors open for read open both. If the
     * right cursors are already open we are done. NOTE: This should become more complex as the
     * stable cursor can have the checkpoint updated in that case this code will close the current
     * stable cursor and open a new one to get the more recent checkpoint information and allow for
     * garbage collection.
     */
    if (coligarch->ingest_cursor != NULL &&
      (!F_ISSET(coligarch, WT_COLIGARCH_OPEN_READ) || coligarch->stable_cursor != NULL))
        return (0);

    /*
     * If the key is pointing to memory that is pinned by a chunk cursor, take a copy before closing
     * cursors.
     */
    if (F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_RET(__cursor_needkey(c));

    F_CLR(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_ITERATE_PREV);

    /* Always open the ingest cursor */
    if (coligarch->ingest_cursor == NULL) {
        WT_RET(__wt_open_cursor(
          session, oligarch->ingest_uri, &coligarch->iface, NULL, &coligarch->ingest_cursor));
        F_SET(coligarch->ingest_cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);
    }

    if (coligarch->stable_cursor == NULL && F_ISSET(coligarch, WT_COLIGARCH_OPEN_READ)) {
        ckpt_cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_open_cursor);
        ckpt_cfg[1] = "checkpoint=" WT_CHECKPOINT ",raw,checkpoint_use_history=false";
        ckpt_cfg[2] = NULL;

        /*
         * We may have a stable chunk with no checkpoint yet. If that's the case then open a cursor
         * on stable without a checkpoint. It will never return an invalid result (it's content is
         * by definition trailing the ingest cursor. It is just slightly less efficient, and also
         * not an accurate reflection of what we want in terms of sharing checkpoint across
         * different WiredTiger instances eventually.
         */
        ret = __wt_open_cursor(
          session, oligarch->stable_uri, &coligarch->iface, ckpt_cfg, &coligarch->stable_cursor);
        if (ret == WT_NOTFOUND) {
            ret = __wt_open_cursor(
              session, oligarch->stable_uri, &coligarch->iface, NULL, &coligarch->stable_cursor);
            if (ret == WT_NOTFOUND)
                WT_RET(
                  __wt_panic(session, WT_PANIC, "Oligarch table could not access stable table"));
            if (ret == 0)
                F_SET(coligarch, WT_COLIGARCH_STABLE_NO_CKPT);
        }
        WT_RET(ret);
        if (coligarch->stable_cursor != NULL)
            F_SET(coligarch->stable_cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);
    }

    return (ret);
}

/*
 * __coligarch_get_current --
 *     Find the smallest / largest of the cursors and copy its key/value.
 */
static int
__coligarch_get_current(
  WT_SESSION_IMPL *session, WT_CURSOR_OLIGARCH *coligarch, bool smallest, bool *deletedp)
{
    WT_COLLATOR *collator;
    WT_CURSOR *c, *current;
    int cmp;
    bool ingest_positioned, stable_positioned;

    c = &coligarch->iface;
    current = NULL;
    ingest_positioned = stable_positioned = false;

    /*
     * There are a couple of cases to deal with here: Some cursors don't have both ingest and stable
     * cursors. Some cursor positioning operations will only have one positioned cursor (e.g a walk
     * has exhausted one cursor but not the other).
     */
    if (coligarch->ingest_cursor != NULL && F_ISSET(coligarch->ingest_cursor, WT_CURSTD_KEY_INT))
        ingest_positioned = true;

    if (coligarch->stable_cursor != NULL && F_ISSET(coligarch->stable_cursor, WT_CURSTD_KEY_INT))
        stable_positioned = true;

    if (!ingest_positioned && !stable_positioned) {
        F_CLR(coligarch, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        return (WT_NOTFOUND);
    }

    __coligarch_get_collator(coligarch, &collator);

    if (ingest_positioned && stable_positioned) {
        WT_RET(__wt_compare(
          session, collator, &coligarch->ingest_cursor->key, &coligarch->stable_cursor->key, &cmp));
        if (smallest ? cmp < 0 : cmp > 0)
            current = coligarch->ingest_cursor;
        else if (cmp == 0)
            current = coligarch->ingest_cursor;
        else
            current = coligarch->stable_cursor;

        /*
         * If the cursors are equal, choose the ingest cursor to return the result but remember not
         * to later return the same result from the stable cursor.
         */
        if (cmp == 0)
            F_SET(coligarch, WT_COLIGARCH_MULTIPLE);
        else
            F_CLR(coligarch, WT_COLIGARCH_MULTIPLE);

    } else if (ingest_positioned)
        current = coligarch->ingest_cursor;
    else if (stable_positioned)
        current = coligarch->stable_cursor;

    if ((coligarch->current_cursor = current) == NULL) {
        F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        return (WT_NOTFOUND);
    }

    WT_RET(current->get_key(current, &c->key));
    WT_RET(current->get_value(current, &c->value));

    F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if ((*deletedp = __coligarch_deleted(&c->value)) == false)
        F_SET(c, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    return (0);
}

/*
 * __coligarch_compare --
 *     WT_CURSOR->compare implementation for the oligarch cursor type.
 */
static int
__coligarch_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_CURSOR_OLIGARCH *acoligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /* There's no need to sync with the oligarch tree, avoid oligarch enter. */
    acoligarch = (WT_CURSOR_OLIGARCH *)a;
    CURSOR_API_CALL(a, session, ret, compare, acoligarch->dhandle);

    /*
     * Confirm both cursors refer to the same source and have keys, then compare the keys.
     */
    if (strcmp(a->uri, b->uri) != 0)
        WT_ERR_MSG(session, EINVAL, "comparison method cursors must reference the same object");

    WT_ERR(__cursor_needkey(a));
    WT_ERR(__cursor_needkey(b));

    /* Both cursors are from the same tree - they share the same collator */
    __coligarch_get_collator(acoligarch, &collator);

    WT_ERR(__wt_compare(session, collator, &a->key, &b->key, cmpp));

err:
    API_END_RET(session, ret);
}

/*
 * __coligarch_position_constituent --
 *     Position a constituent cursor.
 */
static int
__coligarch_position_constituent(
  WT_CURSOR_OLIGARCH *coligarch, WT_CURSOR *c, bool forward, int *cmpp)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    cursor = &coligarch->iface;
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

        WT_RET(__coligarch_cursor_compare(coligarch, c, cursor, cmpp));
    }

    return (0);
}

/*
 * __coligarch_iterate_constituent --
 *     Move a constituent cursor of an oligarch tree and setup the general positioning necessary to
 *     reflect that.
 */
static int
__coligarch_iterate_constituent(WT_CURSOR_OLIGARCH *coligarch, WT_CURSOR *constituent, bool forward)
{
    WT_DECL_RET;
    int cmp;

    /* To iterate an oligarch cursor, which has two constituent cursors, we are in one of a few
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
    } else if (constituent != coligarch->current_cursor &&
      (ret = __coligarch_position_constituent(coligarch, constituent, true, &cmp)) == 0 &&
      cmp == 0 && coligarch->current_cursor == NULL)
        coligarch->current_cursor = constituent;
    WT_RET_NOTFOUND_OK(ret);

    return (0);
}

/*
 * __coligarch_next --
 *     WT_CURSOR->next method for the oligarch cursor type.
 */
static int
__coligarch_next(WT_CURSOR *cursor)
{
    WT_CURSOR *alternate_cursor, *c;
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp;
    bool deleted;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_API_CALL(cursor, session, ret, next, coligarch->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__coligarch_enter(coligarch, false, false));

    /* If we aren't positioned for a forward scan, get started. */
    if (coligarch->current_cursor == NULL || !F_ISSET(coligarch, WT_COLIGARCH_ITERATE_NEXT)) {
        WT_ERR(__coligarch_iterate_constituent(coligarch, coligarch->ingest_cursor, true));
        WT_ERR(__coligarch_iterate_constituent(coligarch, coligarch->stable_cursor, true));
        F_SET(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_MULTIPLE);
        F_CLR(coligarch, WT_COLIGARCH_ITERATE_PREV);

        /* We just positioned *at* the key, now move. */
        if (coligarch->current_cursor != NULL)
            goto retry;
    } else {
retry:
        /* If there are multiple cursors on that key, move them forward. */
        if (coligarch->current_cursor == coligarch->stable_cursor)
            alternate_cursor = coligarch->ingest_cursor;
        else
            alternate_cursor = coligarch->stable_cursor;

        if (F_ISSET(alternate_cursor, WT_CURSTD_KEY_INT)) {
            if (alternate_cursor != coligarch->current_cursor) {
                WT_ERR(__coligarch_cursor_compare(
                  coligarch, alternate_cursor, coligarch->current_cursor, &cmp));
                if (cmp == 0)
                    WT_ERR_NOTFOUND_OK(alternate_cursor->next(alternate_cursor), false);
            }
        }

        /* Move the smallest cursor forward. */
        c = coligarch->current_cursor;
        WT_ERR_NOTFOUND_OK(c->next(c), false);
    }

    /* Find the cursor(s) with the smallest key. */
    if ((ret = __coligarch_get_current(session, coligarch, true, &deleted)) == 0 && deleted)
        goto retry;

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_next);
    if (coligarch->current_cursor == coligarch->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_next_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_next_stable);

err:
    __coligarch_leave(coligarch);
    if (ret == 0)
        __coligarch_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __coligarch_prev --
 *     WT_CURSOR->prev method for the oligarch cursor type.
 */
static int
__coligarch_prev(WT_CURSOR *cursor)
{
    WT_CURSOR *alternate_cursor, *c;
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp;
    bool deleted;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_API_CALL(cursor, session, ret, prev, coligarch->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__coligarch_enter(coligarch, false, false));

    /* If we aren't positioned for a reverse scan, get started. */
    if (coligarch->current_cursor == NULL || !F_ISSET(coligarch, WT_COLIGARCH_ITERATE_PREV)) {
        WT_ERR(__coligarch_iterate_constituent(coligarch, coligarch->ingest_cursor, false));
        WT_ERR(__coligarch_iterate_constituent(coligarch, coligarch->stable_cursor, false));
        F_SET(coligarch, WT_COLIGARCH_ITERATE_PREV | WT_COLIGARCH_MULTIPLE);
        F_CLR(coligarch, WT_COLIGARCH_ITERATE_PREV);

        /* We just positioned *at* the key, now move. */
        if (coligarch->current_cursor != NULL)
            goto retry;
    } else {
retry:
        /* If there are multiple cursors on that key, move them backwards. */
        if (coligarch->current_cursor == coligarch->stable_cursor)
            alternate_cursor = coligarch->ingest_cursor;
        else
            alternate_cursor = coligarch->stable_cursor;

        if (F_ISSET(alternate_cursor, WT_CURSTD_KEY_INT)) {
            if (alternate_cursor != coligarch->current_cursor) {
                WT_ERR(__coligarch_cursor_compare(
                  coligarch, alternate_cursor, coligarch->current_cursor, &cmp));
                if (cmp == 0)
                    WT_ERR_NOTFOUND_OK(alternate_cursor->prev(alternate_cursor), false);
            }
        }

        /* Move the smallest cursor forward. */
        c = coligarch->current_cursor;
        WT_ERR_NOTFOUND_OK(c->prev(c), false);
    }

    /* Find the cursor(s) with the largest key. */
    if ((ret = __coligarch_get_current(session, coligarch, false, &deleted)) == 0 && deleted)
        goto retry;

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_prev);
    if (coligarch->current_cursor == coligarch->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_prev_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_prev_stable);

err:
    __coligarch_leave(coligarch);
    if (ret == 0)
        __coligarch_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __coligarch_reset_cursors --
 *     Reset any positioned constituent cursors. If the skip parameter is non-NULL, that cursor is
 *     about to be used, so there is no need to reset it.
 */
static int
__coligarch_reset_cursors(WT_CURSOR_OLIGARCH *coligarch, bool skip_ingest)
{
    WT_CURSOR *c;
    WT_DECL_RET;

    /* Fast path if the cursor is not positioned. */
    if (coligarch->current_cursor == NULL &&
      !F_ISSET(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_ITERATE_PREV))
        return (0);

    c = coligarch->stable_cursor;
    if (c != NULL && F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_TRET(c->reset(c));

    c = coligarch->ingest_cursor;
    if (!skip_ingest && F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_TRET(c->reset(c));

    coligarch->current_cursor = NULL;
    F_CLR(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_ITERATE_PREV);

    return (ret);
}

/*
 * __coligarch_reset --
 *     WT_CURSOR->reset method for the oligarch cursor type.
 */
static int
__coligarch_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __coligarch_enter path: that is wasted work when all we want to do is
     * give up our position.
     */
    coligarch = (WT_CURSOR_OLIGARCH *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, coligarch->dhandle);
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_TRET(__coligarch_reset_cursors(coligarch, false));

    /* In case we were left positioned, clear that. */
    __coligarch_leave(coligarch);

err:
    API_END_RET(session, ret);
}

/*
 * __coligarch_lookup --
 *     Position an oligarch cursor.
 */
static int
__coligarch_lookup(WT_CURSOR_OLIGARCH *coligarch, WT_ITEM *value)
{
    WT_CURSOR *c, *cursor;
    WT_DECL_RET;
    bool found;

    c = NULL;
    cursor = &coligarch->iface;
    found = false;

    c = coligarch->ingest_cursor;
    c->set_key(c, &cursor->key);
    if ((ret = c->search(c)) == 0) {
        WT_ERR(c->get_key(c, &cursor->key));
        WT_ERR(c->get_value(c, value));
        if (__coligarch_deleted(value))
            ret = WT_NOTFOUND;
        /*
         * Even a tombstone is considered found here - the delete overrides any remaining record in
         * the stable constituent.
         */
        found = true;
    }
    WT_ERR_NOTFOUND_OK(ret, false);
    if (!found)
        F_CLR(c, WT_CURSTD_KEY_SET);

    /*
     * If the key didn't exist in the ingest constituent and the cursor is setup for reading, check
     * the stable constituent.
     */
    if (!found && F_ISSET(coligarch, WT_COLIGARCH_OPEN_READ)) {
        c = coligarch->stable_cursor;
        c->set_key(c, &cursor->key);
        if ((ret = c->search(c)) == 0) {
            WT_ERR(c->get_key(c, &cursor->key));
            WT_ERR(c->get_value(c, value));
            if (__coligarch_deleted(value))
                ret = WT_NOTFOUND;
            found = true;
        }
        WT_ERR_NOTFOUND_OK(ret, false);
        if (!found)
            F_CLR(c, WT_CURSTD_KEY_SET);
    }

err:
    if (ret == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT);
        coligarch->current_cursor = c;
        if (value == &cursor->value)
            F_SET(cursor, WT_CURSTD_VALUE_INT);
    } else if (c != NULL)
        WT_TRET(c->reset(c));

    return (ret);
}

/*
 * __coligarch_search --
 *     WT_CURSOR->search method for the oligarch cursor type.
 */
static int
__coligarch_search(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_API_CALL(cursor, session, ret, search, coligarch->dhandle);
    fprintf(stderr, "__coligarch_search enter\n");
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__coligarch_enter(coligarch, true, false));
    F_CLR(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_ITERATE_PREV);

    ret = __coligarch_lookup(coligarch, &cursor->value);

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search);
    if (coligarch->current_cursor == coligarch->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search_stable);

err:
    __coligarch_leave(coligarch);
    fprintf(stderr, "__coligarch_search leave\n");
    if (ret == 0)
        __coligarch_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __coligarch_search_near --
 *     WT_CURSOR->search_near method for the oligarch cursor type.
 */
static int
__coligarch_search_near(WT_CURSOR *cursor, int *exactp)
{
    WT_CURSOR *closest;
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp, ingest_cmp, stable_cmp;
    bool deleted, ingest_found, stable_found;

    closest = NULL;
    coligarch = (WT_CURSOR_OLIGARCH *)cursor;
    ingest_cmp = stable_cmp = 0;
    ingest_found = stable_found = false;

    CURSOR_API_CALL(cursor, session, ret, search_near, coligarch->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__coligarch_enter(coligarch, true, false));
    F_CLR(coligarch, WT_COLIGARCH_ITERATE_NEXT | WT_COLIGARCH_ITERATE_PREV);

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
    coligarch->ingest_cursor->set_key(coligarch->ingest_cursor, &cursor->key);
    WT_ERR_NOTFOUND_OK(
      coligarch->ingest_cursor->search_near(coligarch->ingest_cursor, &ingest_cmp), true);
    ingest_found = ret != WT_NOTFOUND;

    /* If there wasn't an exact match, check the stable table as well */
    if (ingest_cmp != 0) {
        coligarch->stable_cursor->set_key(coligarch->stable_cursor, &cursor->key);
        WT_ERR_NOTFOUND_OK(
          coligarch->stable_cursor->search_near(coligarch->stable_cursor, &stable_cmp), true);
        stable_found = ret != WT_NOTFOUND;
    }

    if (!ingest_found && !stable_found) {
        ret = WT_NOTFOUND;
        goto err;
    } else if (!stable_found)
        closest = coligarch->ingest_cursor;
    else if (!ingest_found)
        closest = coligarch->stable_cursor;

    /* Now that we know there are two positioned cursors - choose the one with the best match */
    if (closest == NULL) {
        if (ingest_cmp == 0)
            closest = coligarch->ingest_cursor;
        else if (stable_cmp == 0)
            closest = coligarch->stable_cursor;
        else if (ingest_cmp > 0 && stable_cmp > 0) {
            WT_ERR(__coligarch_cursor_compare(
              coligarch, coligarch->ingest_cursor, coligarch->stable_cursor, &cmp));
            if (cmp < 0)
                closest = coligarch->stable_cursor;
            else
                /* If the cursors were identical, or ingest was closer choose ingest. */
                closest = coligarch->ingest_cursor;
        } else if (ingest_cmp > 0)
            closest = coligarch->ingest_cursor;
        else if (stable_cmp > 0)
            closest = coligarch->stable_cursor;
        else { /* Both cursors were smaller than the search key - choose the bigger one */
            WT_ERR(__coligarch_cursor_compare(
              coligarch, coligarch->ingest_cursor, coligarch->stable_cursor, &cmp));
            if (cmp > 0) {
                closest = coligarch->stable_cursor;
            } else {
                /* If the cursors were identical, or ingest was closer choose ingest. */
                closest = coligarch->ingest_cursor;
            }
        }
    }

    /*
     * If we land on a deleted item, try going forwards or backwards to find one that isn't deleted.
     * If the whole tree is empty, we'll end up with WT_NOTFOUND, as expected.
     */
    WT_ASSERT_ALWAYS(session, closest != NULL, "Oligarch search near should have found something");
    WT_ERR(closest->get_key(closest, &cursor->key));
    WT_ERR(closest->get_value(closest, &cursor->value));

    /* Get prepared for finalizing the result before fixing up for tombstones. */
    if (closest == coligarch->ingest_cursor)
        cmp = ingest_cmp;
    else
        cmp = stable_cmp;
    coligarch->current_cursor = closest;
    closest = NULL;

    deleted = __coligarch_deleted(&cursor->value);
    if (!deleted)
        __coligarch_deleted_decode(&cursor->value);
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
        if ((ret = __coligarch_next(cursor)) == 0) {
            cmp = 1;
            deleted = false;
        }
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (deleted) {
        coligarch->current_cursor = NULL;
        WT_ERR(__coligarch_prev(cursor));
        cmp = -1;
    }
    *exactp = cmp;

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search_near);
    if (coligarch->current_cursor == coligarch->ingest_cursor)
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search_near_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_search_near_stable);

err:
    __coligarch_leave(coligarch);
    if (closest != NULL)
        WT_TRET(closest->reset(closest));

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if (ret == 0) {
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else
        coligarch->current_cursor = NULL;

    API_END_RET(session, ret);
}

static int
__coligarch_modify_check(WT_SESSION_IMPL *session)
{
    if (!S2C(session)->oligarch_manager.leader)
        return (EINVAL);
    return (0);
}

/*
 * __coligarch_put --
 *     Put an entry into the ingest tree, and make sure it's available for replay into stable.
 */
static WT_INLINE int
__coligarch_put(WT_SESSION_IMPL *session, WT_CURSOR_OLIGARCH *coligarch, const WT_ITEM *key, const WT_ITEM *value,
  bool position, bool reserve)
{
    WT_CURSOR *c;
    int (*func)(WT_CURSOR *);

    /*
     * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
     * anyway.
     */
    WT_RET(__coligarch_reset_cursors(coligarch, true));

    WT_RET(__coligarch_modify_check(session));

    /* If necessary, set the position for future scans. */
    if (position)
        coligarch->current_cursor = coligarch->ingest_cursor;

    c = coligarch->ingest_cursor;
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
 * __coligarch_insert --
 *     WT_CURSOR->insert method for the oligarch cursor type.
 */
static int
__coligarch_insert(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, insert, coligarch->dhandle);
    WT_RET(__coligarch_modify_check(session));
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__coligarch_enter(coligarch, false, true));

    /*
     * It isn't necessary to copy the key out after the lookup in this case because any non-failed
     * lookup results in an error, and a failed lookup leaves the original key intact.
     */
    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
      (ret = __coligarch_lookup(coligarch, &value)) != WT_NOTFOUND) {
        if (ret == 0)
            ret = WT_DUPLICATE_KEY;
        goto err;
    }

    WT_ERR(__coligarch_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__coligarch_put(session, coligarch, &cursor->key, &value, false, false));

    /*
     * WT_CURSOR.insert doesn't leave the cursor positioned, and the application may want to free
     * the memory used to configure the insert; don't read that memory again (matching the
     * underlying file object cursor insert semantics).
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_insert);
err:
    __wt_scr_free(session, &buf);
    __coligarch_leave(coligarch);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __coligarch_update --
 *     WT_CURSOR->update method for the oligarch cursor type.
 */
static int
__coligarch_update(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, update, coligarch->dhandle);
    WT_RET(__coligarch_modify_check(session));
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__coligarch_enter(coligarch, false, true));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__coligarch_lookup(coligarch, &value));
        /*
         * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__coligarch_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__coligarch_put(session, coligarch, &cursor->key, &value, true, false));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_ITEM_SET(cursor->key, coligarch->current_cursor->key);
    WT_ITEM_SET(cursor->value, coligarch->current_cursor->value);
    WT_ASSERT(session, F_MASK(coligarch->current_cursor, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(
      session, F_MASK(coligarch->current_cursor, WT_CURSTD_VALUE_SET) == WT_CURSTD_VALUE_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_update);

err:
    __wt_scr_free(session, &buf);
    __coligarch_leave(coligarch);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __coligarch_remove --
 *     WT_CURSOR->remove method for the oligarch cursor type.
 */
static int
__coligarch_remove(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;
    bool positioned;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    /* Remember if the cursor is currently positioned. */
    positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT);

    CURSOR_REMOVE_API_CALL(cursor, session, ret, coligarch->dhandle);
    WT_RET(__coligarch_modify_check(session));
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);

    /*
     * Remove fails if the key doesn't exist, do a search first. This requires a second pair of
     * oligarch enter/leave calls as we search the full stack, but updates are limited to the
     * top-level.
     */
    WT_ERR(__coligarch_enter(coligarch, false, false));
    WT_ERR(__coligarch_lookup(coligarch, &value));
    __coligarch_leave(coligarch);

    WT_ERR(__coligarch_enter(coligarch, false, true));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__coligarch_put(session, coligarch, &cursor->key, &__tombstone, true, false));

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
    WT_STAT_CONN_DSRC_INCR(session, oligarch_curs_remove);

err:
    __coligarch_leave(coligarch);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __coligarch_reserve --
 *     WT_CURSOR->reserve method for the oligarch cursor type.
 */
static int
__coligarch_reserve(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, reserve, coligarch->dhandle);
    WT_RET(__coligarch_modify_check(session));
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__wt_txn_context_check(session, true));
    WT_ERR(__coligarch_enter(coligarch, false, true));

    WT_ERR(__coligarch_lookup(coligarch, &value));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    ret = __coligarch_put(session, coligarch, &cursor->key, NULL, true, true);

err:
    __coligarch_leave(coligarch);
    CURSOR_UPDATE_API_END(session, ret);

    /*
     * The application might do a WT_CURSOR.get_value call when we return, so we need a value and
     * the underlying functions didn't set one up. For various reasons, those functions may not have
     * done a search and any previous value in the cursor might race with WT_CURSOR.reserve (and in
     * cases like oligarch, the reserve never encountered the original key). For simplicity, repeat
     * the search here.
     */
    return (ret == 0 ? cursor->search(cursor) : ret);
}

/*
 * __coligarch_close_int --
 *     Close an oligarch cursor
 */
static int
__coligarch_close_int(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(cursor);
    WT_ASSERT_ALWAYS(session, session->dhandle->type == WT_DHANDLE_TYPE_OLIGARCH,
      "Valid oligarch dhandle is required to close a cursor");
    coligarch = (WT_CURSOR_OLIGARCH *)cursor;

    /*
     * If this close is via a connection close the constituent cursors will be closed by a scan of
     * cursors in the session. It might be better to keep them out of the session cursor list, but I
     * don't know how to do that? Probably opening a file cursor directly instead of a table cursor?
     */
    WT_TRET(__coligarch_close_cursors(coligarch));

    /* In case we were somehow left positioned, clear that. */
    __coligarch_leave(coligarch);

    __wt_cursor_close(cursor);

    WT_TRET(__wt_session_release_dhandle(session));

    return (ret);
}

/*
 * __wt_coligarch_close --
 *     WT_CURSOR->close method for the oligarch cursor type.
 */
int
__wt_coligarch_close(WT_CURSOR *cursor)
{
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __coligarch_enter path: that is wasted work when closing, and the cursor
     * may never have been used.
     */
    coligarch = (WT_CURSOR_OLIGARCH *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, coligarch->dhandle);
err:

    WT_TRET(__coligarch_close_int(cursor));

    API_END_RET(session, ret);
}

/*
 * __wt_coligarch_open --
 *     WT_SESSION->open_cursor method for oligarch cursors.
 */
int
__wt_coligarch_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CONFIG_ITEM cval;
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wt_cursor_get_value,                          /* get-value */
      __wt_cursor_get_raw_key_value,                  /* get-value */
      __wt_cursor_set_key,                            /* set-key */
      __wt_cursor_set_value,                          /* set-value */
      __coligarch_compare,                            /* compare */
      __wt_cursor_equals,                             /* equals */
      __coligarch_next,                               /* next */
      __coligarch_prev,                               /* prev */
      __coligarch_reset,                              /* reset */
      __coligarch_search,                             /* search */
      __coligarch_search_near,                        /* search-near */
      __coligarch_insert,                             /* insert */
      __wt_cursor_modify_value_format_notsup,         /* modify */
      __coligarch_update,                             /* update */
      __coligarch_remove,                             /* remove */
      __coligarch_reserve,                            /* reserve */
      __wt_cursor_reconfigure,                        /* reconfigure */
      __wt_cursor_notsup,                             /* largest_key */
      __wt_cursor_config_notsup,                      /* bound */
      __wt_cursor_notsup,                             /* cache */
      __wt_cursor_reopen_notsup,                      /* reopen */
      __wt_cursor_checkpoint_id,                      /* checkpoint ID */
      __wt_coligarch_close);                          /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_OLIGARCH *coligarch;
    WT_DECL_RET;
    WT_OLIGARCH *oligarch;
    bool got_dhandle;

    WT_VERIFY_OPAQUE_POINTER(WT_CURSOR_OLIGARCH);

    coligarch = NULL;
    cursor = NULL;
    got_dhandle = false;

    if (!WT_PREFIX_MATCH(uri, "oligarch:"))
        return (__wt_unexpected_object_type(session, uri, "oligarch:"));

    WT_RET(__wt_inmem_unsupported_op(session, "Oligarch trees"));

    WT_RET(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
    if (cval.len != 0)
        WT_RET_MSG(session, EINVAL, "Oligarch trees do not support opening by checkpoint");

    WT_RET(__wt_config_gets_def(session, cfg, "bulk", 0, &cval));
    if (cval.val != 0)
        WT_RET_MSG(session, EINVAL, "Oligarch trees do not support bulk loading");

    WT_RET(__wt_config_gets_def(session, cfg, "next_random", 0, &cval));
    if (cval.val != 0)
        WT_RET_MSG(session, EINVAL, "Oligarch trees do not support random positioning");

    /* Get the oligarch tree, and hold a reference to it until the cursor is closed. */
    WT_RET(__wt_session_get_dhandle(session, uri, NULL, cfg, 0));
    got_dhandle = true;

    oligarch = (WT_OLIGARCH *)session->dhandle;
    WT_ASSERT_ALWAYS(session, oligarch->ingest_uri != NULL && oligarch->key_format != NULL,
      "Oligarch handle not setup");

    WT_ERR(__wt_calloc_one(session, &coligarch));
    coligarch->dhandle = session->dhandle;

    cursor = (WT_CURSOR *)coligarch;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    cursor->key_format = oligarch->key_format;
    cursor->value_format = oligarch->value_format;

    WT_ERR(__wt_cursor_init(cursor, uri, owner, cfg, cursorp));

    if (0) {
err:
        if (coligarch != NULL)
            WT_TRET(__wt_coligarch_close(cursor));
        else if (got_dhandle)
            WT_TRET(__wt_session_release_dhandle(session));

        *cursorp = NULL;
    }

    return (ret);
}
