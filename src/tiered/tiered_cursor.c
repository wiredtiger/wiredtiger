/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_FORALL_CURSORS(curtiered, c, i)  \
    for ((i) = 0; i < WT_TIERED_MAX_TIERS;) \
        if (((c) = (curtiered)->cursors[(i)++]) != NULL)

#define WT_TIERED_CURCMP(s, tiered, c1, c2, cmp) \
    __wt_compare(s, (tiered)->collator, &(c1)->key, &(c2)->key, &(cmp))

/*
 * __curtiered_open_cursors --
 *     Open cursors for the current set of files.
 */
static int
__curtiered_open_cursors(WT_CURSOR_TIERED *curtiered)
{
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_TIERED *tiered;
    u_int i;

    cursor = &curtiered->iface;
    session = CUR2S(curtiered);
    dhandle = NULL;
    tiered = curtiered->tiered;

    /*
     * If the key is pointing to memory that is pinned by a tier cursor, take a copy before closing
     * cursors.
     */
    if (F_ISSET(cursor, WT_CURSTD_KEY_INT))
        WT_ERR(__cursor_needkey(cursor));

    F_CLR(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_ITERATE_PREV);

    WT_ASSERT(session, curtiered->cursors == NULL);
    WT_ERR(__wt_calloc_def(session, WT_TIERED_MAX_TIERS, &curtiered->cursors));

    /* Open the cursors for tiers that have changed. */
    __wt_verbose(session, WT_VERB_TIERED,
      "tiered opening cursor session(%p):tiered cursor(%p), tiers: %d", (void *)session,
      (void *)curtiered, (int)WT_TIERED_MAX_TIERS);
    for (i = 0; i < WT_TIERED_MAX_TIERS; i++) {
        dhandle = tiered->tiers[i].tier;
        if (dhandle == NULL)
            continue;

        /*
         * Read from the checkpoint if the file has been written. Once all cursors switch, the
         * in-memory tree can be evicted.
         */
        WT_ASSERT(session, curtiered->cursors[i] == NULL);
        WT_ERR(__wt_open_cursor(session, dhandle->name, cursor, NULL, &curtiered->cursors[i]));

        /* Child cursors always use overwrite and raw mode. */
        F_SET(curtiered->cursors[i], WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);
    }

err:
    return (ret);
}

/*
 * __curtiered_close_cursors --
 *     Close any btree cursors that are not needed.
 */
static int
__curtiered_close_cursors(WT_SESSION_IMPL *session, WT_CURSOR_TIERED *curtiered)
{
    WT_CURSOR *c;
    u_int i;

    __wt_verbose(session, WT_VERB_TIERED, "tiered close cursors session(%p):tiered cursor(%p)",
      (void *)session, (void *)curtiered);

    if (curtiered->cursors == NULL)
        return (0);

    /* Walk the cursors, closing them. */
    for (i = 0; i < WT_TIERED_MAX_TIERS; i++) {
        if ((c = (curtiered)->cursors[i]) != NULL) {
            curtiered->cursors[i] = NULL;
            WT_RET(c->close(c));
        }
    }

    __wt_free(session, curtiered->cursors);
    return (0);
}

/*
 * __curtiered_reset_cursors --
 *     Reset any positioned tier cursors. If the skip parameter is non-NULL, that cursor is about to
 *     be used, so there is no need to reset it.
 */
static int
__curtiered_reset_cursors(WT_CURSOR_TIERED *curtiered, WT_CURSOR *skip)
{
    WT_CURSOR *c;
    WT_DECL_RET;
    u_int i;

    /* Fast path if the cursor is not positioned. */
    if ((curtiered->current == NULL || curtiered->current == skip) &&
      !F_ISSET(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_ITERATE_PREV))
        return (0);

    WT_FORALL_CURSORS(curtiered, c, i)
    {
        if (c == skip)
            continue;
        if (F_ISSET(c, WT_CURSTD_KEY_INT))
            WT_TRET(c->reset(c));
    }

    curtiered->current = NULL;
    F_CLR(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_ITERATE_PREV);

    return (ret);
}

/*
 * __curtiered_enter --
 *     Start an operation on a tiered cursor.
 */
static inline int
__curtiered_enter(WT_CURSOR_TIERED *curtiered, bool reset)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(curtiered);

    if (curtiered->cursors == NULL)
        WT_RET(__curtiered_open_cursors(curtiered));

    if (reset) {
        WT_ASSERT(session, !F_ISSET(&curtiered->iface, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT));
        WT_RET(__curtiered_reset_cursors(curtiered, NULL));
    }

    if (!F_ISSET(curtiered, WT_CURTIERED_ACTIVE)) {
        /*
         * Opening this tiered cursor has opened a number of other cursors, ensure we don't mistake
         * this as the first cursor in a session.
         */
        ++session->ncursors;
        WT_RET(__cursor_enter(session));
        F_SET(curtiered, WT_CURTIERED_ACTIVE);
    }

    return (0);
}
/*
 * __curtiered_leave --
 *     Finish an operation on a tiered cursor.
 */
static void
__curtiered_leave(WT_CURSOR_TIERED *curtiered)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(curtiered);

    if (F_ISSET(curtiered, WT_CURTIERED_ACTIVE)) {
        --session->ncursors;
        __cursor_leave(session);
        F_CLR(curtiered, WT_CURTIERED_ACTIVE);
    }
}

/*
 * We need a tombstone to mark deleted records, and we use the special value below for that purpose.
 * We use two 0x14 (Device Control 4) bytes to minimize the likelihood of colliding with an
 * application-chosen encoding byte, if the application uses two leading DC4 byte for some reason,
 * we'll do a wasted data copy each time a new value is inserted into the object.
 */
static const WT_ITEM __tombstone = {"\x14\x14", 2, NULL, 0, 0};

/*
 * __curtiered_deleted --
 *     Check whether the current value is a tombstone.
 */
static inline bool
__curtiered_deleted(WT_CURSOR_TIERED *curtiered, const WT_ITEM *item)
{
    WT_UNUSED(curtiered);
    return (item->size == __tombstone.size &&
      memcmp(item->data, __tombstone.data, __tombstone.size) == 0);
}

/*
 * __curtiered_deleted_encode --
 *     Encode values that are in the encoded name space.
 */
static inline int
__curtiered_deleted_encode(
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
 * __curtiered_deleted_decode --
 *     Decode values that start with the tombstone.
 */
static inline void
__curtiered_deleted_decode(WT_CURSOR_TIERED *curtiered, WT_ITEM *value)
{
    WT_UNUSED(curtiered);
    /*
     * Take care with this check: when a tiered cursor is used for a merge, it is valid to return
     * the tombstone value.
     */
    if (value->size > __tombstone.size &&
      memcmp(value->data, __tombstone.data, __tombstone.size) == 0)
        --value->size;
}

/*
 * __wt_curtiered_close --
 *     WT_CURSOR->close method for the tiered cursor type.
 */
int
__wt_curtiered_close(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __curtiered_enter path: that is wasted work when closing, and the cursor
     * may never have been used.
     */
    curtiered = (WT_CURSOR_TIERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, NULL);
err:
    WT_TRET(__curtiered_close_cursors(session, curtiered));

    /* In case we were somehow left positioned, clear that. */
    __curtiered_leave(curtiered);

    if (curtiered->tiered != NULL)
        WT_WITH_DHANDLE(session, (WT_DATA_HANDLE *)curtiered->tiered,
          WT_TRET(__wt_session_release_dhandle(session)));
    __wt_cursor_close(cursor);

    API_END_RET(session, ret);
}

/*
 * __curtiered_get_current --
 *     Find the smallest / largest of the cursors and copy its key/value.
 */
static int
__curtiered_get_current(
  WT_SESSION_IMPL *session, WT_CURSOR_TIERED *curtiered, bool smallest, bool *deletedp)
{
    WT_CURSOR *c, *current;
    u_int i;
    int cmp;
    bool multiple;

    current = NULL;
    multiple = false;

    WT_FORALL_CURSORS(curtiered, c, i)
    {
        if (!F_ISSET(c, WT_CURSTD_KEY_INT))
            continue;
        if (current == NULL) {
            current = c;
            continue;
        }
        WT_RET(WT_TIERED_CURCMP(session, curtiered->tiered, c, current, cmp));
        if (smallest ? cmp < 0 : cmp > 0) {
            current = c;
            multiple = false;
        } else if (cmp == 0)
            multiple = true;
    }

    c = &curtiered->iface;
    if ((curtiered->current = current) == NULL) {
        F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        return (WT_NOTFOUND);
    }

    if (multiple)
        F_SET(curtiered, WT_CURTIERED_MULTIPLE);
    else
        F_CLR(curtiered, WT_CURTIERED_MULTIPLE);

    WT_RET(current->get_key(current, &c->key));
    WT_RET(current->get_value(current, &c->value));

    F_CLR(c, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if ((*deletedp = __curtiered_deleted(curtiered, &c->value)) == false)
        F_SET(c, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    return (0);
}

/*
 * __curtiered_compare --
 *     WT_CURSOR->compare implementation for the tiered cursor type.
 */
static int
__curtiered_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
    WT_CURSOR_TIERED *atiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    atiered = (WT_CURSOR_TIERED *)a;
    CURSOR_API_CALL(a, session, compare, NULL);

    /*
     * Confirm both cursors refer to the same source and have keys, then compare the keys.
     */
    if (strcmp(a->uri, b->uri) != 0)
        WT_ERR_MSG(session, EINVAL, "comparison method cursors must reference the same object");

    WT_ERR(__cursor_needkey(a));
    WT_ERR(__cursor_needkey(b));

    WT_ERR(__wt_compare(session, atiered->tiered->collator, &a->key, &b->key, cmpp));

err:
    API_END_RET(session, ret);
}

/*
 * __curtiered_position_tier --
 *     Position a tier cursor.
 */
static int
__curtiered_position_tier(WT_CURSOR_TIERED *curtiered, WT_CURSOR *c, bool forward, int *cmpp)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    cursor = &curtiered->iface;
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

        WT_RET(WT_TIERED_CURCMP(session, curtiered->tiered, c, cursor, *cmpp));
    }

    return (0);
}

/*
 * __curtiered_next --
 *     WT_CURSOR->next method for the tiered cursor type.
 */
static int
__curtiered_next(WT_CURSOR *cursor)
{
    WT_CURSOR *c;
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i;
    int cmp;
    bool deleted;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_API_CALL(cursor, session, next, NULL);
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, false));

    /* If we aren't positioned for a forward scan, get started. */
    if (curtiered->current == NULL || !F_ISSET(curtiered, WT_CURTIERED_ITERATE_NEXT)) {
        WT_FORALL_CURSORS(curtiered, c, i)
        {
            if (!F_ISSET(cursor, WT_CURSTD_KEY_SET)) {
                WT_ERR(c->reset(c));
                ret = c->next(c);
            } else if (c != curtiered->current &&
              (ret = __curtiered_position_tier(curtiered, c, true, &cmp)) == 0 && cmp == 0 &&
              curtiered->current == NULL)
                curtiered->current = c;
            WT_ERR_NOTFOUND_OK(ret, false);
        }
        F_SET(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_MULTIPLE);
        F_CLR(curtiered, WT_CURTIERED_ITERATE_PREV);

        /* We just positioned *at* the key, now move. */
        if (curtiered->current != NULL)
            goto retry;
    } else {
retry:
        /*
         * If there are multiple cursors on that key, move them forward.
         */
        if (F_ISSET(curtiered, WT_CURTIERED_MULTIPLE)) {
            WT_FORALL_CURSORS(curtiered, c, i)
            {
                if (!F_ISSET(c, WT_CURSTD_KEY_INT))
                    continue;
                if (c != curtiered->current) {
                    WT_ERR(
                      WT_TIERED_CURCMP(session, curtiered->tiered, c, curtiered->current, cmp));
                    if (cmp == 0)
                        WT_ERR_NOTFOUND_OK(c->next(c), false);
                }
            }
        }

        /* Move the smallest cursor forward. */
        c = curtiered->current;
        WT_ERR_NOTFOUND_OK(c->next(c), false);
    }

    /* Find the cursor(s) with the smallest key. */
    if ((ret = __curtiered_get_current(session, curtiered, true, &deleted)) == 0 && deleted)
        goto retry;

err:
    __curtiered_leave(curtiered);
    if (ret == 0)
        __curtiered_deleted_decode(curtiered, &cursor->value);
    API_END_RET(session, ret);
}

/*
 * __curtiered_prev --
 *     WT_CURSOR->prev method for the tiered cursor type.
 */
static int
__curtiered_prev(WT_CURSOR *cursor)
{
    WT_CURSOR *c;
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i;
    int cmp;
    bool deleted;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_API_CALL(cursor, session, prev, NULL);
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, false));

    /* If we aren't positioned for a reverse scan, get started. */
    if (curtiered->current == NULL || !F_ISSET(curtiered, WT_CURTIERED_ITERATE_PREV)) {
        WT_FORALL_CURSORS(curtiered, c, i)
        {
            if (!F_ISSET(cursor, WT_CURSTD_KEY_SET)) {
                WT_ERR(c->reset(c));
                ret = c->prev(c);
            } else if (c != curtiered->current &&
              (ret = __curtiered_position_tier(curtiered, c, false, &cmp)) == 0 && cmp == 0 &&
              curtiered->current == NULL)
                curtiered->current = c;
            WT_ERR_NOTFOUND_OK(ret, false);
        }
        F_SET(curtiered, WT_CURTIERED_ITERATE_PREV | WT_CURTIERED_MULTIPLE);
        F_CLR(curtiered, WT_CURTIERED_ITERATE_NEXT);

        /* We just positioned *at* the key, now move. */
        if (curtiered->current != NULL)
            goto retry;
    } else {
retry:
        /*
         * If there are multiple cursors on that key, move them backwards.
         */
        if (F_ISSET(curtiered, WT_CURTIERED_MULTIPLE)) {
            WT_FORALL_CURSORS(curtiered, c, i)
            {
                if (!F_ISSET(c, WT_CURSTD_KEY_INT))
                    continue;
                if (c != curtiered->current) {
                    WT_ERR(
                      WT_TIERED_CURCMP(session, curtiered->tiered, c, curtiered->current, cmp));
                    if (cmp == 0)
                        WT_ERR_NOTFOUND_OK(c->prev(c), false);
                }
            }
        }

        /* Move the largest cursor backwards. */
        c = curtiered->current;
        WT_ERR_NOTFOUND_OK(c->prev(c), false);
    }

    /* Find the cursor(s) with the largest key. */
    if ((ret = __curtiered_get_current(session, curtiered, false, &deleted)) == 0 && deleted)
        goto retry;

err:
    __curtiered_leave(curtiered);
    if (ret == 0)
        __curtiered_deleted_decode(curtiered, &cursor->value);
    API_END_RET(session, ret);
}

/*
 * __curtiered_reset --
 *     WT_CURSOR->reset method for the tiered cursor type.
 */
static int
__curtiered_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __curtiered_enter path: that is wasted work when all we want to do is
     * give up our position.
     */
    curtiered = (WT_CURSOR_TIERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, NULL);
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_TRET(__curtiered_reset_cursors(curtiered, NULL));

    /* In case we were left positioned, clear that. */
    __curtiered_leave(curtiered);

err:
    API_END_RET(session, ret);
}

/*
 * __curtiered_lookup --
 *     Position a tiered cursor.
 */
static int
__curtiered_lookup(WT_CURSOR_TIERED *curtiered, WT_ITEM *value)
{
    WT_CURSOR *c, *cursor;
    WT_DECL_RET;
    u_int i;

    c = NULL;
    cursor = &curtiered->iface;

    WT_FORALL_CURSORS(curtiered, c, i)
    {
        c->set_key(c, &cursor->key);
        if ((ret = c->search(c)) == 0) {
            WT_ERR(c->get_key(c, &cursor->key));
            WT_ERR(c->get_value(c, value));
            if (__curtiered_deleted(curtiered, value))
                ret = WT_NOTFOUND;
            goto done;
        }
        WT_ERR_NOTFOUND_OK(ret, false);
        F_CLR(c, WT_CURSTD_KEY_SET);
    }
    WT_ERR(WT_NOTFOUND);

done:
err:
    if (ret == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT);
        curtiered->current = c;
        if (value == &cursor->value)
            F_SET(cursor, WT_CURSTD_VALUE_INT);
    } else if (c != NULL)
        WT_TRET(c->reset(c));

    return (ret);
}

/*
 * __curtiered_search --
 *     WT_CURSOR->search method for the tiered cursor type.
 */
static int
__curtiered_search(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_API_CALL(cursor, session, search, NULL);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, true));
    F_CLR(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_ITERATE_PREV);

    ret = __curtiered_lookup(curtiered, &cursor->value);

err:
    __curtiered_leave(curtiered);
    if (ret == 0)
        __curtiered_deleted_decode(curtiered, &cursor->value);
    API_END_RET(session, ret);
}

/*
 * __curtiered_search_near --
 *     WT_CURSOR->search_near method for the tiered cursor type.
 */
static int
__curtiered_search_near(WT_CURSOR *cursor, int *exactp)
{
    WT_CURSOR *c, *closest;
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i;
    int cmp, exact;
    bool deleted;

    closest = NULL;
    curtiered = (WT_CURSOR_TIERED *)cursor;
    exact = 0;

    CURSOR_API_CALL(cursor, session, search_near, NULL);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, true));
    F_CLR(curtiered, WT_CURTIERED_ITERATE_NEXT | WT_CURTIERED_ITERATE_PREV);

    /*
     * search_near is somewhat fiddly: we can't just use a nearby key from the first tier because
     * there could be a closer key in a lower tier.
     *
     * As we search down the tiers, we stop as soon as we find an exact match. Otherwise, we
     * maintain the smallest cursor larger than the search key and the largest cursor smaller than
     * the search key. At the end, we prefer the larger cursor, but if no record is larger, position
     * on the last record in the tree.
     */
    WT_FORALL_CURSORS(curtiered, c, i)
    {
        c->set_key(c, &cursor->key);
        if ((ret = c->search_near(c, &cmp)) == WT_NOTFOUND) {
            ret = 0;
            continue;
        }
        if (ret != 0)
            goto err;

        /* Do we have an exact match? */
        if (cmp == 0) {
            closest = c;
            exact = 1;
            break;
        }

        /*
         * Prefer larger cursors. There are two reasons: (1) we expect prefix searches to be a
         * common case (as in our own indices); and (2) we need a way to unambiguously know we have
         * the "closest" result.
         */
        if (cmp < 0) {
            if ((ret = c->next(c)) == WT_NOTFOUND) {
                ret = 0;
                continue;
            }
            if (ret != 0)
                goto err;
        }

        /*
         * We are trying to find the smallest cursor greater than the search key.
         */
        if (closest == NULL)
            closest = c;
        else {
            WT_ERR(WT_TIERED_CURCMP(session, curtiered->tiered, c, closest, cmp));
            if (cmp < 0)
                closest = c;
        }
    }

    /*
     * At this point, we either have an exact match, or closest is the smallest cursor larger than
     * the search key, or it is NULL if the search key is larger than any record in the tree.
     */
    cmp = exact ? 0 : 1;

    /*
     * If we land on a deleted item, try going forwards or backwards to find one that isn't deleted.
     * If the whole tree is empty, we'll end up with WT_NOTFOUND, as expected.
     */
    if (closest == NULL)
        deleted = true;
    else {
        WT_ERR(closest->get_key(closest, &cursor->key));
        WT_ERR(closest->get_value(closest, &cursor->value));
        curtiered->current = closest;
        closest = NULL;
        deleted = __curtiered_deleted(curtiered, &cursor->value);
        if (!deleted)
            __curtiered_deleted_decode(curtiered, &cursor->value);
        else {
            /*
             * We have a key pointing at memory that is pinned by the current tier cursor. In the
             * unlikely event that we have to reopen cursors to move to the next record, make sure
             * the cursor flags are set so a copy is made before the current tier cursor releases
             * its position.
             */
            F_CLR(cursor, WT_CURSTD_KEY_SET);
            F_SET(cursor, WT_CURSTD_KEY_INT);
            /*
             * We call __curtiered_next here as we want to advance forward. If we are a random
             * tiered cursor calling next on the cursor will not advance as we intend.
             */
            if ((ret = __curtiered_next(cursor)) == 0) {
                cmp = 1;
                deleted = false;
            }
        }
        WT_ERR_NOTFOUND_OK(ret, false);
    }
    if (deleted) {
        curtiered->current = NULL;
        /*
         * We call prev directly here as cursor->prev may be "invalid" if this is a random cursor.
         */
        WT_ERR(__curtiered_prev(cursor));
        cmp = -1;
    }
    *exactp = cmp;

err:
    __curtiered_leave(curtiered);
    if (closest != NULL)
        WT_TRET(closest->reset(closest));

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if (ret == 0) {
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else
        curtiered->current = NULL;

    API_END_RET(session, ret);
}

/*
 * __curtiered_put --
 *     Put an entry into the primary tree.
 */
static inline int
__curtiered_put(WT_CURSOR_TIERED *curtiered, const WT_ITEM *key, const WT_ITEM *value,
  bool position, bool reserve)
{
    WT_CURSOR *primary;
    int (*func)(WT_CURSOR *);

    /*
     * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
     * anyway.
     */
    primary = curtiered->cursors[WT_TIERED_INDEX_LOCAL];
    WT_RET(__curtiered_reset_cursors(curtiered, primary));

    /* If necessary, set the position for future scans. */
    if (position)
        curtiered->current = primary;

    primary->set_key(primary, key);

    /* Our API always leaves the cursor positioned after a reserve call. */
    WT_ASSERT(CUR2S(curtiered), !reserve || position);
    func = primary->insert;
    if (position)
        func = reserve ? primary->reserve : primary->update;
    if (!reserve)
        primary->set_value(primary, value);
    return (func(primary));
}

/*
 * __curtiered_insert --
 *     WT_CURSOR->insert method for the tiered cursor type.
 */
static int
__curtiered_insert(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, insert);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__curtiered_enter(curtiered, false));

    /*
     * It isn't necessary to copy the key out after the lookup in this case because any non-failed
     * lookup results in an error, and a failed lookup leaves the original key intact.
     */
    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
      (ret = __curtiered_lookup(curtiered, &value)) != WT_NOTFOUND) {
        if (ret == 0)
            ret = WT_DUPLICATE_KEY;
        goto err;
    }

    WT_ERR(__curtiered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__curtiered_put(curtiered, &cursor->key, &value, false, false));

    /*
     * WT_CURSOR.insert doesn't leave the cursor positioned, and the application may want to free
     * the memory used to configure the insert; don't read that memory again (matching the
     * underlying file object cursor insert semantics).
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

err:
    __wt_scr_free(session, &buf);
    __curtiered_leave(curtiered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __curtiered_update --
 *     WT_CURSOR->update method for the tiered cursor type.
 */
static int
__curtiered_update(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, update);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__curtiered_enter(curtiered, false));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__curtiered_lookup(curtiered, &value));
        /*
         * Copy the key out, since the insert resets non-primary tier cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__curtiered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__curtiered_put(curtiered, &cursor->key, &value, true, false));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_ITEM_SET(cursor->key, curtiered->current->key);
    WT_ITEM_SET(cursor->value, curtiered->current->value);
    WT_ASSERT(session, F_MASK(curtiered->current, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(session, F_MASK(curtiered->current, WT_CURSTD_VALUE_SET) == WT_CURSTD_VALUE_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

err:
    __wt_scr_free(session, &buf);
    __curtiered_leave(curtiered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __curtiered_remove --
 *     WT_CURSOR->remove method for the tiered cursor type.
 */
static int
__curtiered_remove(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;
    bool positioned;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    /* Check if the cursor is positioned. */
    positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT);

    CURSOR_REMOVE_API_CALL(cursor, session, NULL);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, false));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__curtiered_lookup(curtiered, &value));
        /*
         * Copy the key out, since the insert resets non-primary tier cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__curtiered_put(curtiered, &cursor->key, &__tombstone, positioned, false));

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

err:
    __curtiered_leave(curtiered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __curtiered_reserve --
 *     WT_CURSOR->reserve method for the tiered cursor type.
 */
static int
__curtiered_reserve(WT_CURSOR *cursor)
{
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, reserve);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__wt_txn_context_check(session, true));
    WT_ERR(__curtiered_enter(curtiered, false));

    WT_ERR(__curtiered_lookup(curtiered, &value));
    /*
     * Copy the key out, since the insert resets non-primary tier cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    ret = __curtiered_put(curtiered, &cursor->key, NULL, true, true);

err:
    __curtiered_leave(curtiered);
    CURSOR_UPDATE_API_END(session, ret);

    /*
     * The application might do a WT_CURSOR.get_value call when we return, so we need a value and
     * the underlying functions didn't set one up. For various reasons, those functions may not have
     * done a search and any previous value in the cursor might race with WT_CURSOR.reserve (and in
     * cases like tiered, the reserve never encountered the original key). For simplicity, repeat
     * the search here.
     */
    return (ret == 0 ? cursor->search(cursor) : ret);
}

/*
 * __curtiered_next_random --
 *     WT_CURSOR->next method for the tiered cursor type when configured with next_random.
 */
static int
__curtiered_next_random(WT_CURSOR *cursor)
{
    WT_CURSOR *c;
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i, tier;
    int exact;

    c = NULL;
    curtiered = (WT_CURSOR_TIERED *)cursor;

    CURSOR_API_CALL(cursor, session, next, NULL);
    __cursor_novalue(cursor);
    WT_ERR(__curtiered_enter(curtiered, false));

    /*
     * Select a random tier. If it is empty, try the next tier and so on, wrapping around until we
     * find something or run out of tiers.
     */
    tier = __wt_random(&session->rnd) % WT_TIERED_MAX_TIERS;
    for (i = 0; i < WT_TIERED_MAX_TIERS; i++) {
        c = curtiered->cursors[tier];
        WT_ERR_NOTFOUND_OK(__wt_curfile_next_random(c), true);
        if (ret == WT_NOTFOUND) {
            if (++tier == WT_TIERED_MAX_TIERS)
                tier = 0;
            continue;
        }

        F_SET(cursor, WT_CURSTD_KEY_INT);
        WT_ERR(c->get_key(c, &cursor->key));
        /*
         * Search near the current key to resolve any tombstones and position to a valid record. If
         * we see a WT_NOTFOUND here that is valid, as the tree has no documents visible to us.
         */
        WT_ERR(__curtiered_search_near(cursor, &exact));
        break;
    }

err:
    if (ret != 0) {
        /* We didn't find a valid record. Don't leave cursor positioned */
        F_CLR(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    }
    __curtiered_leave(curtiered);
    API_END_RET(session, ret);
}

/*
 * __curtiered_insert_bulk --
 *     WT_CURSOR->insert method for tiered bulk cursors.
 */
static int
__curtiered_insert_bulk(WT_CURSOR *cursor)
{
    WT_CURSOR *bulk_cursor;
    WT_CURSOR_TIERED *curtiered;
    WT_SESSION_IMPL *session;

    curtiered = (WT_CURSOR_TIERED *)cursor;
    session = CUR2S(curtiered);
    bulk_cursor = curtiered->cursors[WT_TIERED_INDEX_LOCAL];

    WT_ASSERT(session, bulk_cursor != NULL);
    bulk_cursor->set_key(bulk_cursor, &cursor->key);
    bulk_cursor->set_value(bulk_cursor, &cursor->value);
    WT_RET(bulk_cursor->insert(bulk_cursor));

    return (0);
}

/*
 * __curtiered_open_bulk --
 *     WT_SESSION->open_cursor method for tiered bulk cursors.
 */
static int
__curtiered_open_bulk(WT_CURSOR_TIERED *curtiered, const char *cfg[])
{
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_TIERED *tiered;

    cursor = &curtiered->iface;
    session = CUR2S(curtiered);
    tiered = curtiered->tiered;

    /* Bulk cursors only support insert and close. */
    __wt_cursor_set_notsup(cursor);
    cursor->insert = __curtiered_insert_bulk;
    cursor->close = __wt_curtiered_close;

    WT_ASSERT(session, curtiered->cursors == NULL);
    WT_ERR(__wt_calloc_def(session, WT_TIERED_MAX_TIERS, &curtiered->cursors));

    /* Open a bulk cursor on the local tier. */
    dhandle = tiered->tiers[WT_TIERED_INDEX_LOCAL].tier;
    WT_ASSERT(session, dhandle != NULL);
    WT_ERR(__wt_open_cursor(
      session, dhandle->name, cursor, cfg, &curtiered->cursors[WT_TIERED_INDEX_LOCAL]));

    /* Child cursors always use overwrite and raw mode. */
    F_SET(curtiered->cursors[WT_TIERED_INDEX_LOCAL], WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);

    if (0) {
err:
        __wt_free(session, curtiered->cursors);
    }
    return (ret);
}

/*
 * __wt_curtiered_open --
 *     WT_SESSION->open_cursor method for tiered cursors.
 */
int
__wt_curtiered_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CONFIG_ITEM cval;
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wt_cursor_get_value,                          /* get-value */
      __wt_cursor_set_key,                            /* set-key */
      __wt_cursor_set_value,                          /* set-value */
      __curtiered_compare,                            /* compare */
      __wt_cursor_equals,                             /* equals */
      __curtiered_next,                               /* next */
      __curtiered_prev,                               /* prev */
      __curtiered_reset,                              /* reset */
      __curtiered_search,                             /* search */
      __curtiered_search_near,                        /* search-near */
      __curtiered_insert,                             /* insert */
      __wt_cursor_modify_value_format_notsup,         /* modify */
      __curtiered_update,                             /* update */
      __curtiered_remove,                             /* remove */
      __curtiered_reserve,                            /* reserve */
      __wt_cursor_reconfigure,                        /* reconfigure */
      __wt_cursor_notsup,                             /* cache */
      __wt_cursor_reopen_notsup,                      /* reopen */
      __wt_curtiered_close);                          /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_TIERED *curtiered;
    WT_DECL_RET;
    WT_TIERED *tiered;
    bool bulk;

    WT_STATIC_ASSERT(offsetof(WT_CURSOR_TIERED, iface) == 0);

    curtiered = NULL;
    cursor = NULL;
    tiered = NULL;

    if (!WT_PREFIX_MATCH(uri, "tiered:"))
        return (__wt_unexpected_object_type(session, uri, "tiered:"));

    WT_RET(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
    if (cval.len != 0)
        WT_RET_MSG(session, EINVAL, "tiered tables do not support opening by checkpoint");

    WT_RET(__wt_config_gets_def(session, cfg, "bulk", 0, &cval));
    bulk = cval.val != 0;

    /* Get the tiered data handle. */
    ret = __wt_session_get_dhandle(session, uri, NULL, cfg, bulk ? WT_DHANDLE_EXCLUSIVE : 0);

    /* Check whether the exclusive open for a bulk load succeeded. */
    if (bulk && ret == EBUSY)
        ret = EINVAL;
    /* Flag any errors from the tree get. */
    WT_ERR(ret);

    tiered = (WT_TIERED *)session->dhandle;

    /* Make sure we have exclusive access if and only if we want it */
    WT_ASSERT(session, !bulk || tiered->iface.excl_session != NULL);

    WT_ERR(__wt_calloc_one(session, &curtiered));
    cursor = (WT_CURSOR *)curtiered;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    WT_ERR(__wt_strdup(session, tiered->iface.name, &cursor->uri));
    cursor->key_format = tiered->key_format;
    cursor->value_format = tiered->value_format;

    curtiered->tiered = tiered;
    tiered = NULL;

    /* If the next_random option is set, configure a random cursor */
    WT_ERR(__wt_config_gets_def(session, cfg, "next_random", 0, &cval));
    if (cval.val != 0) {
        __wt_cursor_set_notsup(cursor);
        cursor->next = __curtiered_next_random;
    }

    WT_ERR(__wt_cursor_init(cursor, cursor->uri, owner, cfg, cursorp));

    if (bulk)
        WT_ERR(__curtiered_open_bulk(curtiered, cfg));

    if (0) {
err:
        if (curtiered != NULL)
            WT_TRET(__wt_curtiered_close(cursor));
        else if (tiered != NULL)
            WT_WITH_DHANDLE(
              session, (WT_DATA_HANDLE *)tiered, WT_TRET(__wt_session_release_dhandle(session)));

        *cursorp = NULL;
    }

    return (ret);
}
