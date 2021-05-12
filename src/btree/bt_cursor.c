/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * When returning an error, we need to restore the cursor to a valid state, the upper-level cursor
 * code is likely to retry. This structure and the associated functions are used save and restore
 * the cursor state.
 */
typedef struct {
    WT_ITEM key;
    WT_ITEM value;
    uint64_t recno;
    uint32_t flags;
} WT_CURFILE_STATE;

/*
 * __cursor_state_save --
 *     Save the cursor's external state.
 */
static inline void
__cursor_state_save(WT_CURSOR *cursor, WT_CURFILE_STATE *state)
{
    WT_ITEM_SET(state->key, cursor->key);
    WT_ITEM_SET(state->value, cursor->value);
    state->recno = cursor->recno;
    state->flags = cursor->flags;
}

/*
 * __cursor_state_restore --
 *     Restore the cursor's external state.
 */
static inline void
__cursor_state_restore(WT_CURSOR *cursor, WT_CURFILE_STATE *state)
{
    if (F_ISSET(state, WT_CURSTD_KEY_EXT))
        WT_ITEM_SET(cursor->key, state->key);
    if (F_ISSET(state, WT_CURSTD_VALUE_EXT))
        WT_ITEM_SET(cursor->value, state->value);
    cursor->recno = state->recno;
    F_CLR(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    F_SET(cursor, F_MASK(state, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT));
}

/*
 * __cursor_page_pinned --
 *     Return if we have a page pinned.
 */
static inline bool
__cursor_page_pinned(WT_CURSOR_BTREE *cbt, bool search_operation)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    cursor = &cbt->iface;
    session = CUR2S(cbt);

    /*
     * Check the page active flag, asserting the page reference with any external key.
     */
    if (!F_ISSET(cbt, WT_CBT_ACTIVE)) {
        WT_ASSERT(session, cbt->ref == NULL && !F_ISSET(cursor, WT_CURSTD_KEY_INT));
        return (false);
    }

    /*
     * Check if the key references an item on a page. When returning from search, the page is pinned
     * and the key is internal. After the application sets a key, the key becomes external. For the
     * search and search-near operations, we assume locality and check any pinned page first on each
     * new search operation. For operations other than search and search-near, check if we have an
     * internal key. If the page is pinned and we're pointing into the page, we don't need to search
     * at all, we can proceed with the operation. However, if the key has been set, that is, it's an
     * external key, we're going to have to do a full search.
     */
    if (!search_operation && !F_ISSET(cursor, WT_CURSTD_KEY_INT))
        return (false);

    /*
     * XXX No fast-path searches at read-committed isolation. Underlying transactional functions
     * called by the fast and slow path search code handle transaction IDs differently, resulting in
     * different search results at read-committed isolation. This makes no difference for the update
     * functions, but in the case of a search, we will see different results based on the cursor's
     * initial location. See WT-5134 for the details.
     */
    if (search_operation && session->txn->isolation == WT_ISO_READ_COMMITTED)
        return (false);

    /*
     * Fail if the page is flagged for forced eviction (so we periodically release pages grown too
     * large).
     */
    if (cbt->ref->page->read_gen == WT_READGEN_OLDEST)
        return (false);

    return (true);
}

/*
 * __cursor_size_chk --
 *     Return if an inserted item is too large.
 */
static inline int
__cursor_size_chk(WT_SESSION_IMPL *session, WT_ITEM *kv)
{
    WT_BM *bm;
    WT_BTREE *btree;
    WT_DECL_RET;
    size_t size;

    btree = S2BT(session);
    bm = btree->bm;

    if (btree->type == BTREE_COL_FIX) {
        /* Fixed-size column-stores take a single byte. */
        if (kv->size != 1)
            WT_RET_MSG(session, EINVAL,
              "item size of %" WT_SIZET_FMT
              " does not match fixed-length file requirement of 1 byte",
              kv->size);
        return (0);
    }

    /* Don't waste effort, 1GB is always cool. */
    if (kv->size <= WT_GIGABYTE)
        return (0);

    /* Check what we are willing to store in the tree. */
    if (kv->size > WT_BTREE_MAX_OBJECT_SIZE)
        WT_RET_MSG(session, EINVAL,
          "item size of %" WT_SIZET_FMT
          " exceeds the maximum supported WiredTiger size of %" PRIu32,
          kv->size, WT_BTREE_MAX_OBJECT_SIZE);

    /* Check what the block manager can actually write. */
    size = kv->size;
    if ((ret = bm->write_size(bm, session, &size)) != 0)
        WT_RET_MSG(
          session, ret, "item size of %" WT_SIZET_FMT " refused by block manager", kv->size);

    return (0);
}

/*
 * __cursor_fix_implicit --
 *     Return if search went past the end of the tree.
 */
static inline bool
__cursor_fix_implicit(WT_BTREE *btree, WT_CURSOR_BTREE *cbt)
{
    /*
     * When there's no exact match, column-store search returns the key nearest the searched-for key
     * (continuing past keys smaller than the searched-for key to return the next-largest key).
     * Therefore, if the returned comparison is -1, the searched-for key was larger than any row on
     * the page's standard information or column-store insert list.
     *
     * If the returned comparison is NOT -1, there was a row equal to or larger than the
     * searched-for key, and we implicitly create missing rows.
     */
    return (btree->type == BTREE_COL_FIX && cbt->compare != -1);
}

/*
 * __wt_cursor_valid --
 *     Return if the cursor references an valid key/value pair.
 */
int
__wt_cursor_valid(WT_CURSOR_BTREE *cbt, WT_ITEM *key, uint64_t recno, bool *valid)
{
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_COL *cip;
    WT_PAGE *page;
    WT_SESSION_IMPL *session;

    *valid = false;

    btree = CUR2BT(cbt);
    page = cbt->ref->page;
    session = CUR2S(cbt);

    /*
     * We may be pointing to an insert object, and we may have a page with
     * existing entries.  Insert objects always have associated update
     * objects (the value).  Any update object may be deleted, or invisible
     * to us.  In the case of an on-page entry, there is by definition a
     * value that is visible to us, the original page cell.
     *
     * If we find a visible update structure, return our caller a reference
     * to it because we don't want to repeatedly search for the update, it
     * might suddenly become invisible (imagine a read-uncommitted session
     * with another session's aborted insert), and we don't want to handle
     * that potential error every time we look at the value.
     *
     * Unfortunately, the objects we might have and their relationships are
     * different for the underlying page types.
     *
     * In the case of row-store, an insert object implies ignoring any page
     * objects, no insert object can have the same key as an on-page object.
     * For row-store:
     *	if there's an insert object:
     *		if there's a visible update:
     *			exact match
     *		else
     *			no exact match
     *	else
     *		use the on-page object (which may have an associated
     *		update object that may or may not be visible to us).
     *
     * Column-store is more complicated because an insert object can have
     * the same key as an on-page object: updates to column-store rows
     * are insert/object pairs, and an invisible update isn't the end as
     * there may be an on-page object that is visible.  This changes the
     * logic to:
     *	if there's an insert object:
     *		if there's a visible update:
     *			exact match
     *		else if the on-page object's key matches the insert key
     *			use the on-page object
     *	else
     *		use the on-page object
     *
     * First, check for an insert object with a visible update (a visible
     * update that's been deleted is not a valid key/value pair).
     */
    if (cbt->ins != NULL) {
        WT_RET(__wt_txn_read_upd_list(session, cbt, cbt->ins->upd));
        if (cbt->upd_value->type != WT_UPDATE_INVALID) {
            if (cbt->upd_value->type == WT_UPDATE_TOMBSTONE)
                return (0);
            *valid = true;
            return (0);
        }
    }

    /*
     * Clean out any stale value here. Calling a transaction read helper automatically clears this
     * but we have some code paths that don't do this (fixed length column store is one example).
     */
    __wt_upd_value_clear(cbt->upd_value);

    /*
     * If we don't have an insert object, or in the case of column-store, there's an insert object
     * but no update was visible to us and the key on the page is the same as the insert object's
     * key, and the slot as set by the search function is valid, we can use the original page
     * information.
     */
    switch (btree->type) {
    case BTREE_COL_FIX:
        /*
         * If search returned an insert object, there may or may not be a matching on-page object,
         * we have to check. Fixed-length column-store pages don't have slots, but map one-to-one to
         * keys, check for retrieval past the end of the page.
         */
        if (cbt->recno >= cbt->ref->ref_recno + page->entries)
            return (0);

        *valid = true;
        /*
         * An update would have appeared as an "insert" object; no further checks to do.
         */
        break;
    case BTREE_COL_VAR:
        /* The search function doesn't check for empty pages. */
        if (page->entries == 0)
            return (0);
        /*
         * In case of prepare conflict, the slot might not have a valid value, if the update in the
         * insert list of a new page scanned is in prepared state.
         */
        WT_ASSERT(session, cbt->slot == UINT32_MAX || cbt->slot < page->entries);

        /*
         * Column-store updates are stored as "insert" objects. If search returned an insert object
         * we can't return, the returned on-page object must be checked for a match.
         */
        if (cbt->ins != NULL && !F_ISSET(cbt, WT_CBT_VAR_ONPAGE_MATCH))
            return (0);

        /*
         * Although updates would have appeared as an "insert" objects, variable-length column store
         * deletes are written into the backing store; check the cell for a record already deleted
         * when read.
         */
        cip = &page->pg_var[cbt->slot];
        cell = WT_COL_PTR(page, cip);
        if (__wt_cell_type(cell) == WT_CELL_DEL)
            return (0);

        /*
         * Check for an update ondisk or in the history store. For column store, an insert object
         * can have the same key as an on-page or history store object.
         */
        WT_RET(__wt_txn_read(session, cbt, key, recno, NULL, NULL));
        if (cbt->upd_value->type != WT_UPDATE_INVALID) {
            if (cbt->upd_value->type == WT_UPDATE_TOMBSTONE)
                return (0);
            *valid = true;
        }
        break;
    case BTREE_ROW:
        /* The search function doesn't check for empty pages. */
        if (page->entries == 0)
            return (0);
        /*
         * In case of prepare conflict, the slot might not have a valid value, if the update in the
         * insert list of a new page scanned is in prepared state.
         */
        WT_ASSERT(session, cbt->slot == UINT32_MAX || cbt->slot < page->entries);

        /*
         * See above: for row-store, no insert object can have the same key as an on-page object,
         * we're done.
         */
        if (cbt->ins != NULL)
            return (0);

        /* Check for an update. */
        WT_RET(__wt_txn_read(session, cbt, key, WT_RECNO_OOB,
          (page->modify != NULL && page->modify->mod_row_update != NULL) ?
            page->modify->mod_row_update[cbt->slot] :
            NULL,
          NULL));
        if (cbt->upd_value->type != WT_UPDATE_INVALID) {
            if (cbt->upd_value->type == WT_UPDATE_TOMBSTONE)
                return (0);
            *valid = true;
        }
        break;
    }
    return (0);
}

/*
 * __cursor_col_search --
 *     Column-store search from a cursor.
 */
static inline int
__cursor_col_search(WT_CURSOR_BTREE *cbt, WT_REF *leaf, bool *leaf_foundp)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(cbt);

#ifdef HAVE_DIAGNOSTIC
    /*
     * Turn off cursor-order checks in all cases on search. The search/search-near functions turn
     * them back on after a successful search.
     */
    __wt_cursor_key_order_reset(cbt);
#endif

    WT_WITH_PAGE_INDEX(
      session, ret = __wt_col_search(cbt, cbt->iface.recno, leaf, false, leaf_foundp));
    return (ret);
}

/*
 * __cursor_row_search --
 *     Row-store search from a cursor.
 */
static inline int
__cursor_row_search(WT_CURSOR_BTREE *cbt, bool insert, WT_REF *leaf, bool *leaf_foundp)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(cbt);

#ifdef HAVE_DIAGNOSTIC
    /*
     * Turn off cursor-order checks in all cases on search. The search/search-near functions turn
     * them back on after a successful search.
     */
    __wt_cursor_key_order_reset(cbt);
#endif

    WT_WITH_PAGE_INDEX(
      session, ret = __wt_row_search(cbt, &cbt->iface.key, insert, leaf, false, leaf_foundp));
    return (ret);
}

/*
 * __cursor_col_modify --
 *     Column-store modify from a cursor.
 */
static inline int
__cursor_col_modify(WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
    return (__wt_col_modify(cbt, cbt->iface.recno, value, NULL, modify_type, false));
}

/*
 * __cursor_row_modify --
 *     Row-store modify from a cursor.
 */
static inline int
__cursor_row_modify(WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
    return (__wt_row_modify(cbt, &cbt->iface.key, value, NULL, modify_type, false));
}

/*
 * __cursor_restart --
 *     Common cursor restart handling.
 */
static void
__cursor_restart(WT_SESSION_IMPL *session, uint64_t *yield_count, uint64_t *sleep_usecs)
{
    __wt_spin_backoff(yield_count, sleep_usecs);
    WT_STAT_CONN_DATA_INCR(session, cursor_restart);
}

/*
 * __wt_btcur_reset --
 *     Invalidate the cursor position.
 */
int
__wt_btcur_reset(WT_CURSOR_BTREE *cbt)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    cursor = &cbt->iface;
    session = CUR2S(cbt);

    WT_STAT_CONN_DATA_INCR(session, cursor_reset);

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    return (__cursor_reset(cbt));
}

/*
 * __wt_btcur_search_prepared --
 *     Search and return exact matching records only.
 */
int
__wt_btcur_search_prepared(WT_CURSOR *cursor, WT_UPDATE **updp)
{
    WT_BTREE *btree;
    WT_CURSOR_BTREE *cbt;
    WT_UPDATE *upd;

    *updp = NULL;

    cbt = (WT_CURSOR_BTREE *)cursor;
    btree = CUR2BT(cbt);
    upd = NULL; /* -Wuninitialized */

    /*
     * Not calling the cursor initialization functions, we don't want to be tapped for eviction nor
     * do we want other standard cursor semantics like snapshots, just discard the hazard pointer
     * from the last operation. This also depends on the fact we're not setting the cursor's active
     * flag, this is really a special chunk of code and not to be modified without careful thought.
     */
    WT_RET(__cursor_reset(cbt));

    WT_RET(btree->type == BTREE_ROW ? __cursor_row_search(cbt, false, NULL, NULL) :
                                      __cursor_col_search(cbt, NULL, NULL));

    /*
     * Ideally an exact match will be found, as this transaction is searching for updates done by
     * itself. But, we cannot be sure of finding one, as pre-processing of the updates could have
     * happened as part of resolving earlier transaction operations.
     */
    if (cbt->compare != 0)
        return (0);

    /* Get any uncommitted update from the in-memory page. */
    switch (btree->type) {
    case BTREE_ROW:
        /*
         * Any update must be either in the insert list, in which case search will have returned a
         * pointer for us, or as an update in a particular key's update list, in which case the slot
         * will be returned to us. In either case, we want the most recent update (any update
         * attempted after the prepare would have failed).
         */
        if (cbt->ins != NULL)
            upd = cbt->ins->upd;
        else if (cbt->ref->page->modify != NULL && cbt->ref->page->modify->mod_row_update != NULL)
            upd = cbt->ref->page->modify->mod_row_update[cbt->slot];
        break;
    case BTREE_COL_FIX:
    case BTREE_COL_VAR:
        /*
         * Any update must be in the insert list and we want the most recent update (any update
         * attempted after the prepare would have failed).
         */
        if (cbt->ins != NULL)
            upd = cbt->ins->upd;
        break;
    }

    *updp = upd;
    return (0);
}

/*
 * __wt_btcur_search --
 *     Search for a matching record in the tree.
 */
int
__wt_btcur_search(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool leaf_found, valid;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    session = CUR2S(cbt);

    WT_STAT_CONN_DATA_INCR(session, cursor_search);

    WT_RET(__wt_txn_search_check(session));
    __cursor_state_save(cursor, &state);

    /*
     * The pinned page goes away if we search the tree, get a local copy of any pinned key and
     * discard any pinned value, then re-save the cursor state. Done before searching pinned pages
     * (unlike other cursor functions), because we don't anticipate applications searching for a key
     * they currently have pinned.)
     */
    WT_ERR(__cursor_localkey(cursor));
    __cursor_novalue(cursor);
    __cursor_state_save(cursor, &state);

    /*
     * If we have a page pinned, search it; if we don't have a page pinned, or the search of the
     * pinned page doesn't find an exact match, search from the root.
     */
    valid = false;
    if (__cursor_page_pinned(cbt, true)) {
        __wt_txn_cursor_op(session);

        if (btree->type == BTREE_ROW) {
            WT_ERR(__cursor_row_search(cbt, false, cbt->ref, &leaf_found));
            if (leaf_found && cbt->compare == 0)
                WT_ERR(__wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
        } else {
            WT_ERR(__cursor_col_search(cbt, cbt->ref, &leaf_found));
            if (leaf_found && cbt->compare == 0)
                WT_ERR(__wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
        }
    }
    if (!valid) {
        WT_ERR(__cursor_func_init(cbt, true));

        if (btree->type == BTREE_ROW) {
            WT_ERR(__cursor_row_search(cbt, false, NULL, NULL));
            if (cbt->compare == 0)
                WT_ERR(__wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
        } else {
            WT_ERR(__cursor_col_search(cbt, NULL, NULL));
            if (cbt->compare == 0)
                WT_ERR(__wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
        }
    }

    if (valid)
        ret = __cursor_kv_return(cbt, cbt->upd_value);
    else if (__cursor_fix_implicit(btree, cbt)) {
        /*
         * Creating a record past the end of the tree in a fixed-length column-store implicitly
         * fills the gap with empty records.
         */
        cbt->recno = cursor->recno;
        cbt->v = 0;
        cursor->value.data = &cbt->v;
        cursor->value.size = 1;
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else
        ret = WT_NOTFOUND;

#ifdef HAVE_DIAGNOSTIC
    if (ret == 0)
        WT_ERR(__wt_cursor_key_order_init(cbt));
#endif

err:
    if (ret != 0) {
        WT_TRET(__cursor_reset(cbt));
        __cursor_state_restore(cursor, &state);
    }
    return (ret);
}

/*
 * __wt_btcur_search_near --
 *     Search for a record in the tree.
 */
int
__wt_btcur_search_near(WT_CURSOR_BTREE *cbt, int *exactp)
{
    WT_BTREE *btree;
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int exact;
    bool leaf_found, valid;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    session = CUR2S(cbt);
    exact = 0;

    WT_STAT_CONN_DATA_INCR(session, cursor_search_near);

    WT_RET(__wt_txn_search_check(session));
    __cursor_state_save(cursor, &state);

    /*
     * The pinned page goes away if we search the tree, get a local copy of any pinned key and
     * discard any pinned value, then re-save the cursor state. Done before searching pinned pages
     * (unlike other cursor functions), because we don't anticipate applications searching for a key
     * they currently have pinned.)
     */
    WT_ERR(__cursor_localkey(cursor));
    __cursor_novalue(cursor);
    __cursor_state_save(cursor, &state);

    /*
     * If we have a row-store page pinned, search it; if we don't have a page pinned, or the search
     * of the pinned page doesn't find an exact match, search from the root. Unlike
     * WT_CURSOR.search, ignore pinned pages in the case of column-store, search-near isn't an
     * interesting enough case for column-store to add the complexity needed to avoid the tree
     * search.
     */
    valid = false;
    if (btree->type == BTREE_ROW && __cursor_page_pinned(cbt, true)) {
        __wt_txn_cursor_op(session);

        /*
         * Set the "insert" flag for row-store search; we may intend to position the cursor at the
         * the end of the tree, rather than match an existing record. (LSM requires this semantic.)
         */
        WT_ERR(__cursor_row_search(cbt, true, cbt->ref, &leaf_found));

        /*
         * Only use the pinned page search results if search returns an exact match or a slot other
         * than the page's boundary slots, if that's not the case, a neighbor page might offer a
         * better match. This test is simplistic as we're ignoring append lists (there may be no
         * page slots or we might be legitimately positioned after the last page slot). Ignore those
         * cases, it makes things too complicated.
         */
        if (leaf_found &&
          (cbt->compare == 0 || (cbt->slot != 0 && cbt->slot != cbt->ref->page->entries - 1)))
            WT_ERR(__wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
    }
    if (!valid) {
        WT_ERR(__cursor_func_init(cbt, true));

        /*
         * Set the "insert" flag for row-store search; we may intend to position the cursor at the
         * the end of the tree, rather than match an existing record. (LSM requires this semantic.)
         */
        if (btree->type == BTREE_ROW) {
            WT_ERR(__cursor_row_search(cbt, true, NULL, NULL));
            WT_ERR(__wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
        } else {
            WT_ERR(__cursor_col_search(cbt, NULL, NULL));
            WT_ERR(__wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
        }
    }

    /*
     * If we find a valid key, return it.
     *
     * Else, creating a record past the end of the tree in a fixed-length column-store implicitly
     * fills the gap with empty records. In this case, we instantiate the empty record, it's an
     * exact match.
     *
     * Else, move to the next key in the tree (bias for prefix searches). Cursor next skips invalid
     * rows, so we don't have to test for them again.
     *
     * Else, redo the search and move to the previous key in the tree. Cursor previous skips invalid
     * rows, so we don't have to test for them again.
     *
     * If that fails, quit, there's no record to return.
     */
    if (valid) {
        exact = cbt->compare;
        ret = __cursor_kv_return(cbt, cbt->upd_value);
    } else if (__cursor_fix_implicit(btree, cbt)) {
        cbt->recno = cursor->recno;
        cbt->v = 0;
        cursor->value.data = &cbt->v;
        cursor->value.size = 1;
        exact = 0;
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else {
        /*
         * We didn't find an exact match: try after the search key, then before. We have to loop
         * here because at low isolation levels, new records could appear as we are stepping through
         * the tree.
         */
        while ((ret = __wt_btcur_next_prefix(cbt, &state.key, false)) != WT_NOTFOUND) {
            WT_ERR(ret);
            if (btree->type == BTREE_ROW)
                WT_ERR(__wt_compare(session, btree->collator, &cursor->key, &state.key, &exact));
            else
                exact = cbt->recno < state.recno ? -1 : cbt->recno == state.recno ? 0 : 1;
            if (exact >= 0)
                goto done;
        }

        /*
         * We walked to the end of the tree without finding a match. Walk backwards instead.
         */
        while ((ret = __wt_btcur_prev_prefix(cbt, &state.key, false)) != WT_NOTFOUND) {
            WT_ERR(ret);
            if (btree->type == BTREE_ROW)
                WT_ERR(__wt_compare(session, btree->collator, &cursor->key, &state.key, &exact));
            else
                exact = cbt->recno < state.recno ? -1 : cbt->recno == state.recno ? 0 : 1;
            if (exact <= 0)
                goto done;
        }
    }

done:
err:
    if (ret == 0 && exactp != NULL)
        *exactp = exact;

#ifdef HAVE_DIAGNOSTIC
    if (ret == 0)
        WT_TRET(__wt_cursor_key_order_init(cbt));
#endif

    if (ret != 0) {
        /*
         * It is important that this reset is kept as the cursor state is modified in the above prev
         * and next loops. Those internally do reset the cursor but not when performing a prefix
         * search near.
         */
        WT_TRET(__cursor_reset(cbt));
        __cursor_state_restore(cursor, &state);
    }
    return (ret);
}

/*
 * __wt_btcur_insert --
 *     Insert a record into the tree.
 */
int
__wt_btcur_insert(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    size_t insert_bytes;
    uint64_t yield_count, sleep_usecs;
    bool append_key, valid;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    insert_bytes = cursor->key.size + cursor->value.size;
    session = CUR2S(cbt);
    yield_count = sleep_usecs = 0;

    WT_STAT_CONN_DATA_INCR(session, cursor_insert);
    WT_STAT_CONN_DATA_INCRV(session, cursor_insert_bytes, insert_bytes);

    if (btree->type == BTREE_ROW)
        WT_RET(__cursor_size_chk(session, &cursor->key));
    WT_RET(__cursor_size_chk(session, &cursor->value));

    /* It's no longer possible to bulk-load into the tree. */
    __wt_cursor_disable_bulk(session);

    /*
     * Insert a new record if WT_CURSTD_APPEND configured, (ignoring any application set record
     * number). Although append can't be configured for a row-store, this code would break if it
     * were, and that's owned by the upper cursor layer, be cautious.
     */
    append_key = F_ISSET(cursor, WT_CURSTD_APPEND) && btree->type != BTREE_ROW;

    /* Save the cursor state. */
    __cursor_state_save(cursor, &state);

    /*
     * If inserting with overwrite configured, and positioned to an on-page key, the update doesn't
     * require another search. Cursors configured for append aren't included, regardless of whether
     * or not they meet all other criteria.
     *
     * Fixed-length column store can never use a positioned cursor to update because the cursor may
     * not be positioned to the correct record in the case of implicit records in the append list.
     */
    if (btree->type != BTREE_COL_FIX && __cursor_page_pinned(cbt, false) &&
      F_ISSET(cursor, WT_CURSTD_OVERWRITE) && !append_key) {
        WT_ERR(__wt_txn_autocommit_check(session));
        /*
         * The cursor position may not be exact (the cursor's comparison value not equal to zero).
         * Correct to an exact match so we can update whatever we're pointing at.
         */
        cbt->compare = 0;
        ret = btree->type == BTREE_ROW ?
          __cursor_row_modify(cbt, &cbt->iface.value, WT_UPDATE_STANDARD) :
          __cursor_col_modify(cbt, &cbt->iface.value, WT_UPDATE_STANDARD);
        if (ret == 0)
            goto done;

        /*
         * The pinned page goes away if we fail for any reason, get a local copy of any pinned key
         * or value. (Restart could still use the pinned page, but that's an unlikely path.) Re-save
         * the cursor state: we may retry but eventually fail.
         */
        WT_TRET(__cursor_localkey(cursor));
        WT_TRET(__cursor_localvalue(cursor));
        __cursor_state_save(cursor, &state);
        goto err;
    }

    /*
     * The pinned page goes away if we do a search, get a local copy of any pinned key or value.
     * Re-save the cursor state: we may retry but eventually fail.
     */
    WT_ERR(__cursor_localkey(cursor));
    WT_ERR(__cursor_localvalue(cursor));
    __cursor_state_save(cursor, &state);

retry:
    WT_ERR(__cursor_func_init(cbt, true));

    if (btree->type == BTREE_ROW) {
        WT_ERR(__cursor_row_search(cbt, true, NULL, NULL));
        /*
         * If not overwriting, fail if the key exists, else insert the key/value pair.
         */
        if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) && cbt->compare == 0) {
            WT_WITH_UPDATE_VALUE_SKIP_BUF(
              ret = __wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
            WT_ERR(ret);
            if (valid)
                WT_ERR(WT_DUPLICATE_KEY);
        }

        ret = __cursor_row_modify(cbt, &cbt->iface.value, WT_UPDATE_STANDARD);
    } else if (append_key) {
        /*
         * Optionally insert a new record (ignoring the application's record number). The real
         * record number is allocated by the serialized append operation.
         */
        cbt->iface.recno = WT_RECNO_OOB;
        cbt->compare = 1;
        WT_ERR(__cursor_col_search(cbt, NULL, NULL));
        WT_ERR(__cursor_col_modify(cbt, &cbt->iface.value, WT_UPDATE_STANDARD));
        cursor->recno = cbt->recno;
    } else {
        WT_ERR(__cursor_col_search(cbt, NULL, NULL));

        /*
         * If not overwriting, fail if the key exists. Creating a record past the end of the tree in
         * a fixed-length column-store implicitly fills the gap with empty records. Fail in that
         * case, the record exists.
         */
        if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
            if (cbt->compare == 0) {
                WT_WITH_UPDATE_VALUE_SKIP_BUF(
                  ret = __wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
                WT_ERR(ret);
                if (valid)
                    WT_ERR(WT_DUPLICATE_KEY);
            } else if (__cursor_fix_implicit(btree, cbt))
                WT_ERR(WT_DUPLICATE_KEY);
        }

        WT_ERR(__cursor_col_modify(cbt, &cbt->iface.value, WT_UPDATE_STANDARD));
    }

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    /* Insert doesn't maintain a position across calls, clear resources. */
    if (ret == 0) {
done:
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        if (append_key)
            F_SET(cursor, WT_CURSTD_KEY_EXT);
    }
    WT_TRET(__cursor_reset(cbt));
    if (ret != 0)
        __cursor_state_restore(cursor, &state);

    return (ret);
}

/*
 * __curfile_update_check --
 *     Check whether an update would conflict. This function expects the cursor to already be
 *     positioned. It should be called before deciding whether to skip an update operation based on
 *     existence of a visible update for a key --
 *     even if there is no value visible to the transaction, an update could still conflict.
 */
static int
__curfile_update_check(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_PAGE *page;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;

    btree = CUR2BT(cbt);
    page = cbt->ref->page;
    session = CUR2S(cbt);
    upd = NULL;

    if (cbt->compare != 0)
        return (0);

    if (cbt->ins != NULL)
        upd = cbt->ins->upd;
    else if (btree->type == BTREE_ROW && page->modify != NULL &&
      page->modify->mod_row_update != NULL)
        upd = page->modify->mod_row_update[cbt->slot];
    else if (btree->type != BTREE_COL_VAR)
        return (0);

    return (__wt_txn_update_check(session, cbt, upd, NULL));
}

/*
 * __wt_btcur_insert_check --
 *     Check whether an update would conflict. This can replace WT_CURSOR::insert, so it only checks
 *     for conflicts without updating the tree. It is used to maintain snapshot isolation for
 *     transactions that span multiple chunks in an LSM tree.
 */
int
__wt_btcur_insert_check(WT_CURSOR_BTREE *cbt)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t yield_count, sleep_usecs;

    cursor = &cbt->iface;
    session = CUR2S(cbt);
    yield_count = sleep_usecs = 0;

    WT_ASSERT(session, CUR2BT(cbt)->type == BTREE_ROW);

    /*
     * The pinned page goes away if we do a search, get a local copy of any pinned key and discard
     * any pinned value. Unlike most of the btree cursor routines, we don't have to save/restore the
     * cursor key state, none of the work done here changes the cursor state.
     */
    WT_ERR(__cursor_localkey(cursor));
    __cursor_novalue(cursor);

retry:
    WT_ERR(__cursor_func_init(cbt, true));
    WT_ERR(__cursor_row_search(cbt, true, NULL, NULL));

    /* Just check for conflicts. */
    ret = __curfile_update_check(cbt);

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    /* Insert doesn't maintain a position across calls, clear resources. */
    if (ret == 0)
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_TRET(__cursor_reset(cbt));

    return (ret);
}

/*
 * __wt_btcur_remove --
 *     Remove a record from the tree.
 */
int
__wt_btcur_remove(WT_CURSOR_BTREE *cbt, bool positioned)
{
    WT_BTREE *btree;
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t yield_count, sleep_usecs;
    bool iterating, searched, valid;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    session = CUR2S(cbt);
    yield_count = sleep_usecs = 0;
    iterating = F_ISSET(cbt, WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV);
    searched = false;

    WT_STAT_CONN_DATA_INCR(session, cursor_remove);
    WT_STAT_CONN_DATA_INCRV(session, cursor_remove_bytes, cursor->key.size);

    /* Save the cursor state. */
    __cursor_state_save(cursor, &state);

    /*
     * If remove positioned to an on-page key, the remove doesn't require another search. We don't
     * care about the "overwrite" configuration because regardless of the overwrite setting, any
     * existing record is removed, and the record must exist with a positioned cursor.
     *
     * There's trickiness in the page-pinned check. By definition a remove operation leaves a cursor
     * positioned if it's initially positioned. However, if every item on the page is deleted and we
     * unpin the page, eviction might delete the page and our search will re-instantiate an empty
     * page for us. Cursor remove returns not-found whether or not that eviction/deletion happens
     * and it's OK unless cursor-overwrite is configured (which means we return success even if
     * there's no item to delete). In that case, we'll fail when we try to point the cursor at the
     * key on the page to satisfy the positioned requirement. It's arguably safe to simply leave the
     * key initialized in the cursor (as that's all a positioned cursor implies), but it's probably
     * safer to avoid page eviction entirely in the positioned case.
     *
     * Fixed-length column store can never use a positioned cursor to update because the cursor may
     * not be positioned to the correct record in the case of implicit records in the append list.
     */
    if (btree->type != BTREE_COL_FIX && __cursor_page_pinned(cbt, false)) {
        WT_ERR(__wt_txn_autocommit_check(session));

        /*
         * The cursor position may not be exact (the cursor's comparison value not equal to zero).
         * Correct to an exact match so we can remove whatever we're pointing at.
         */
        cbt->compare = 0;
        ret = btree->type == BTREE_ROW ? __cursor_row_modify(cbt, NULL, WT_UPDATE_TOMBSTONE) :
                                         __cursor_col_modify(cbt, NULL, WT_UPDATE_TOMBSTONE);
        if (ret == 0)
            goto done;
        goto err;
    }

retry:
    /*
     * Note these steps must be repeatable, we'll continue to take this path as long as we encounter
     * WT_RESTART.
     *
     * Any pinned page goes away if we do a search, including as a result of a restart. Get a local
     * copy of any pinned key and re-save the cursor state: we may retry but eventually fail.
     */
    WT_ERR(__cursor_localkey(cursor));
    __cursor_state_save(cursor, &state);
    searched = true;

    WT_ERR(__cursor_func_init(cbt, true));

    if (btree->type == BTREE_ROW) {
        WT_ERR_NOTFOUND_OK(__cursor_row_search(cbt, false, NULL, NULL), true);
        if (ret == WT_NOTFOUND)
            goto search_notfound;

        /* Check whether an update would conflict. */
        WT_ERR(__curfile_update_check(cbt));

        if (cbt->compare != 0)
            goto search_notfound;
        WT_WITH_UPDATE_VALUE_SKIP_BUF(ret = __wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
        WT_ERR(ret);
        if (!valid)
            goto search_notfound;

        ret = __cursor_row_modify(cbt, NULL, WT_UPDATE_TOMBSTONE);
    } else {
        WT_ERR_NOTFOUND_OK(__cursor_col_search(cbt, NULL, NULL), true);
        if (ret == WT_NOTFOUND)
            goto search_notfound;

        /*
         * If we find a matching record, check whether an update would conflict. Do this before
         * checking if the update is visible in __wt_cursor_valid, or we can miss conflict.
         */
        WT_ERR(__curfile_update_check(cbt));

        /* Remove the record if it exists. */
        valid = false;
        if (cbt->compare == 0) {
            WT_WITH_UPDATE_VALUE_SKIP_BUF(ret = __wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
            WT_ERR(ret);
        }
        if (cbt->compare != 0 || !valid) {
            if (!__cursor_fix_implicit(btree, cbt))
                goto search_notfound;
            /*
             * Creating a record past the end of the tree in a fixed-length column-store implicitly
             * fills the gap with empty records. Return success in that case, the record was deleted
             * successfully.
             *
             * Correct the btree cursor's location: the search will have pointed us at the
             * previous/next item, and that's not correct.
             */
            cbt->recno = cursor->recno;
        } else
            ret = __cursor_col_modify(cbt, NULL, WT_UPDATE_TOMBSTONE);
    }

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    if (ret == 0) {
        /*
         * If positioned originally, but we had to do a search, acquire a position so we can return
         * success.
         *
         * If not positioned originally, leave it that way, clear any key and reset the cursor.
         */
        if (positioned) {
            if (searched)
                WT_TRET(__wt_key_return(cbt));
        } else {
            F_CLR(cursor, WT_CURSTD_KEY_SET);
            WT_TRET(__cursor_reset(cbt));
        }

        /*
         * Check the return status again as we might have encountered an error setting the return
         * key or resetting the cursor after an otherwise successful remove.
         */
        if (ret != 0) {
            WT_TRET(__cursor_reset(cbt));
            __cursor_state_restore(cursor, &state);
        }
    } else {
        /*
         * If the cursor is configured for overwrite and search returned not-found, that is what we
         * want, try to return success. We can do that as long as it's not an iterating or
         * positioned cursor. (Iterating or positioned cursors would have been forced to give up any
         * pinned page, and when the search failed we've lost the cursor position. Since no
         * subsequent iteration can succeed, we cannot return success.)
         */
        if (0) {
search_notfound:
            ret = WT_NOTFOUND;
            if (!iterating && !positioned && F_ISSET(cursor, WT_CURSTD_OVERWRITE))
                ret = 0;
        }

        /*
         * Reset the cursor and restore the original cursor key: done after clearing the return
         * value in the clause immediately above so we don't lose an error value if cursor reset
         * fails.
         */
        WT_TRET(__cursor_reset(cbt));
        __cursor_state_restore(cursor, &state);
    }

done:
    /*
     * Upper level cursor removes don't expect the cursor value to be set after a successful remove
     * (and check in diagnostic mode). Error handling may have converted failure to a success, do a
     * final check.
     */
    if (ret == 0)
        F_CLR(cursor, WT_CURSTD_VALUE_SET);

    return (ret);
}

/*
 * __btcur_update --
 *     Update a record in the tree.
 */
static int
__btcur_update(WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
    WT_BTREE *btree;
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t yield_count, sleep_usecs;
    bool valid;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    session = CUR2S(cbt);
    yield_count = sleep_usecs = 0;

    /* It's no longer possible to bulk-load into the tree. */
    __wt_cursor_disable_bulk(session);

    /* Save the cursor state. */
    __cursor_state_save(cursor, &state);

    /*
     * If update positioned to an on-page key, the update doesn't require another search. We don't
     * care about the "overwrite" configuration because regardless of the overwrite setting, any
     * existing record is updated, and the record must exist with a positioned cursor.
     *
     * Fixed-length column store can never use a positioned cursor to update because the cursor may
     * not be positioned to the correct record in the case of implicit records in the append list.
     */
    if (btree->type != BTREE_COL_FIX && __cursor_page_pinned(cbt, false)) {
        WT_ERR(__wt_txn_autocommit_check(session));

        /*
         * The cursor position may not be exact (the cursor's comparison value not equal to zero).
         * Correct to an exact match so we can update whatever we're pointing at.
         */
        cbt->compare = 0;
        ret = btree->type == BTREE_ROW ? __cursor_row_modify(cbt, value, modify_type) :
                                         __cursor_col_modify(cbt, value, modify_type);
        if (ret == 0)
            goto done;

        /*
         * The pinned page goes away if we fail for any reason, get a local copy of any pinned key
         * or value. (Restart could still use the pinned page, but that's an unlikely path.) Re-save
         * the cursor state: we may retry but eventually fail.
         */
        WT_TRET(__cursor_localkey(cursor));
        WT_TRET(__cursor_localvalue(cursor));
        __cursor_state_save(cursor, &state);
        goto err;
    }

    /*
     * The pinned page goes away if we do a search, get a local copy of any pinned key or value.
     * Re-save the cursor state: we may retry but eventually fail.
     */
    WT_ERR(__cursor_localkey(cursor));
    WT_ERR(__cursor_localvalue(cursor));
    __cursor_state_save(cursor, &state);

retry:
    WT_ERR(__cursor_func_init(cbt, true));
    WT_ERR(btree->type == BTREE_ROW ? __cursor_row_search(cbt, true, NULL, NULL) :
                                      __cursor_col_search(cbt, NULL, NULL));

    if (btree->type == BTREE_ROW) {
        /*
         * If not overwriting, check for conflicts and fail if the key does not exist.
         */
        if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
            WT_ERR(__curfile_update_check(cbt));
            if (cbt->compare != 0)
                WT_ERR(WT_NOTFOUND);
            WT_WITH_UPDATE_VALUE_SKIP_BUF(
              ret = __wt_cursor_valid(cbt, cbt->tmp, WT_RECNO_OOB, &valid));
            WT_ERR(ret);
            if (!valid)
                WT_ERR(WT_NOTFOUND);
        }
        ret = __cursor_row_modify(cbt, value, modify_type);
    } else {
        /*
         * If not overwriting, fail if the key doesn't exist. If we find an update for the key,
         * check for conflicts. Update the record if it exists. Creating a record past the end of
         * the tree in a fixed-length column-store implicitly fills the gap with empty records.
         * Update the record in that case, the record exists.
         */
        if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
            WT_ERR(__curfile_update_check(cbt));
            valid = false;
            if (cbt->compare == 0) {
                WT_WITH_UPDATE_VALUE_SKIP_BUF(
                  ret = __wt_cursor_valid(cbt, NULL, cbt->recno, &valid));
                WT_ERR(ret);
            }
            if ((cbt->compare != 0 || !valid) && !__cursor_fix_implicit(btree, cbt))
                WT_ERR(WT_NOTFOUND);
        }
        ret = __cursor_col_modify(cbt, value, modify_type);
    }

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    /*
     * If successful, point the cursor at internal copies of the data. We could shuffle memory in
     * the cursor so the key/value pair are in local buffer memory, but that's a data copy. We don't
     * want to do another search (and we might get a different update structure if we race). To make
     * this work, we add a field to the btree cursor to pass back a pointer to the modify function's
     * allocated update structure.
     */
    if (ret == 0) {
done:
        switch (modify_type) {
        case WT_UPDATE_STANDARD:
            /*
             * WT_CURSOR.update returns a key and a value.
             */
            ret = __cursor_kv_return(cbt, cbt->modify_update);
            break;
        case WT_UPDATE_RESERVE:
            /*
             * WT_CURSOR.reserve doesn't return any value.
             */
            F_CLR(cursor, WT_CURSTD_VALUE_SET);
        /* FALLTHROUGH */
        case WT_UPDATE_MODIFY:
            /*
             * WT_CURSOR.modify has already created the return value and our job is to leave it
             * untouched.
             */
            ret = __wt_key_return(cbt);
            break;
        case WT_UPDATE_TOMBSTONE:
        default:
            return (__wt_illegal_value(session, modify_type));
        }
    }

    if (ret != 0) {
        WT_TRET(__cursor_reset(cbt));
        __cursor_state_restore(cursor, &state);
    }

    return (ret);
}

/*
 * __cursor_chain_exceeded --
 *     Return if the update chain has exceeded the limit.
 */
static bool
__cursor_chain_exceeded(WT_CURSOR_BTREE *cbt)
{
    WT_CURSOR *cursor;
    WT_PAGE *page;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;
    size_t upd_size;
    int i;

    cursor = &cbt->iface;
    page = cbt->ref->page;
    session = CUR2S(cbt);

    upd = NULL;
    if (cbt->ins != NULL)
        upd = cbt->ins->upd;
    else if (CUR2BT(cbt)->type == BTREE_ROW && page->modify != NULL &&
      page->modify->mod_row_update != NULL)
        upd = page->modify->mod_row_update[cbt->slot];

    /*
     * Step through the modify operations at the beginning of the chain.
     *
     * Deleted or standard updates are anticipated to be sufficient to base the modify (although
     * that's not guaranteed: they may not be visible or might abort before we read them). Also,
     * this is not a hard limit, threads can race modifying updates.
     *
     * If the total size in bytes of the updates exceeds some factor of the underlying value size
     * (which we know because the cursor is positioned), create a new full copy of the value. This
     * limits the cache pressure from creating full copies to that factor: with the default factor
     * of 1, the total size in memory of a set of modify updates is limited to double the size of
     * the modifies.
     *
     * Otherwise, limit the length of the update chain to a fixed size to bound the cost of
     * rebuilding the value during reads. When history has to be maintained, creating extra copies
     * of large documents multiplies cache pressure because the old ones cannot be freed, so allow
     * the modify chain to grow.
     */
    for (i = 0, upd_size = 0; upd != NULL && upd->type == WT_UPDATE_MODIFY; ++i, upd = upd->next) {
        upd_size += WT_UPDATE_MEMSIZE(upd);
        if (i >= WT_MAX_MODIFY_UPDATE && upd_size * WT_MODIFY_MEM_FRACTION >= cursor->value.size)
            return (true);
    }
    if (i >= WT_MAX_MODIFY_UPDATE && upd != NULL && upd->type == WT_UPDATE_STANDARD &&
      __wt_txn_upd_visible_all(session, upd))
        return (true);
    return (false);
}

/*
 * __wt_btcur_modify --
 *     Modify a record in the tree.
 */
int
__wt_btcur_modify(WT_CURSOR_BTREE *cbt, WT_MODIFY *entries, int nentries)
{
    WT_CURFILE_STATE state;
    WT_CURSOR *cursor;
    WT_DECL_ITEM(modify);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    size_t orig, new;
    bool overwrite;

    cursor = &cbt->iface;
    session = CUR2S(cbt);

    /* Save the cursor state. */
    __cursor_state_save(cursor, &state);

    /*
     * Get the current value and apply the modification to it, for a few reasons: first, we set the
     * updated value so the application can retrieve the cursor's value; second, we use the updated
     * value as the update if the update chain is too long; third, there's a check if the updated
     * value is too large to store; fourth, to simplify the count of bytes being added/removed;
     * fifth, we can get into serious trouble if we attempt to modify a value that doesn't exist or
     * read a value that might not exist in the future. For the fifth reason, fail if in anything
     * other than a snapshot transaction, read-committed and read-uncommitted imply values that
     * might disappear out from under us or an inability to repeat point-in-time reads.
     *
     * Also, an application might read a value outside of a transaction and then call modify. For
     * that to work, the read must be part of the transaction that performs the update for
     * correctness, otherwise we could race with another thread and end up modifying the wrong
     * value. A clever application could get this right (imagine threads that only updated
     * non-overlapping, fixed-length byte strings), but it's unsafe because it will work most of the
     * time and the failure is unlikely to be detected. Require explicit transactions for modify
     * operations.
     */
    if (session->txn->isolation != WT_ISO_SNAPSHOT)
        WT_ERR_MSG(
          session, ENOTSUP, "not supported in read-committed or read-uncommitted transactions");
    if (F_ISSET(session->txn, WT_TXN_AUTOCOMMIT))
        WT_ERR_MSG(session, ENOTSUP, "not supported in implicit transactions");

    if (!F_ISSET(cursor, WT_CURSTD_KEY_INT) || !F_ISSET(cursor, WT_CURSTD_VALUE_INT))
        WT_ERR(__wt_btcur_search(cbt));

    WT_ERR(__wt_modify_pack(cursor, entries, nentries, &modify));

    orig = cursor->value.size;
    WT_ERR(__wt_modify_apply_item(session, cursor->value_format, &cursor->value, modify->data));
    new = cursor->value.size;
    WT_ERR(__cursor_size_chk(session, &cursor->value));

    WT_STAT_CONN_DATA_INCRV(
      session, cursor_update_bytes_changed, new > orig ? new - orig : orig - new);

    /*
     * WT_CURSOR.modify is update-without-overwrite.
     *
     * Use the modify buffer as the update if the data package saves us some memory and the update
     * chain is under the limit, else use the complete value.
     */
    overwrite = F_ISSET(cursor, WT_CURSTD_OVERWRITE);
    F_CLR(cursor, WT_CURSTD_OVERWRITE);
    if (cursor->value.size <= 64 || __cursor_chain_exceeded(cbt))
        ret = __btcur_update(cbt, &cursor->value, WT_UPDATE_STANDARD);
    else
        ret = __btcur_update(cbt, modify, WT_UPDATE_MODIFY);
    if (overwrite)
        F_SET(cursor, WT_CURSTD_OVERWRITE);

    /*
     * We have our own cursor state restoration because we've modified the cursor before calling the
     * underlying cursor update function and we need to restore it to its original state. This means
     * multiple calls to reset the cursor, but that shouldn't be a problem.
     */
    if (ret != 0) {
err:
        WT_TRET(__cursor_reset(cbt));
        __cursor_state_restore(cursor, &state);
    }

    __wt_scr_free(session, &modify);
    return (ret);
}

/*
 * __wt_btcur_reserve --
 *     Reserve a record in the tree.
 */
int
__wt_btcur_reserve(WT_CURSOR_BTREE *cbt)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool overwrite;

    cursor = &cbt->iface;
    session = CUR2S(cbt);

    WT_STAT_CONN_DATA_INCR(session, cursor_reserve);

    /* WT_CURSOR.reserve is update-without-overwrite and a special value. */
    overwrite = F_ISSET(cursor, WT_CURSTD_OVERWRITE);
    F_CLR(cursor, WT_CURSTD_OVERWRITE);
    ret = __btcur_update(cbt, NULL, WT_UPDATE_RESERVE);
    if (overwrite)
        F_SET(cursor, WT_CURSTD_OVERWRITE);
    return (ret);
}

/*
 * __wt_btcur_update --
 *     Update a record in the tree.
 */
int
__wt_btcur_update(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session;

    btree = CUR2BT(cbt);
    cursor = &cbt->iface;
    session = CUR2S(cbt);

    WT_STAT_CONN_DATA_INCR(session, cursor_update);
    WT_STAT_CONN_DATA_INCRV(session, cursor_update_bytes, cursor->key.size + cursor->value.size);

    if (btree->type == BTREE_ROW)
        WT_RET(__cursor_size_chk(session, &cursor->key));
    WT_RET(__cursor_size_chk(session, &cursor->value));

    return (__btcur_update(cbt, &cursor->value, WT_UPDATE_STANDARD));
}

/*
 * __wt_btcur_compare --
 *     Return a comparison between two cursors.
 */
int
__wt_btcur_compare(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *cmpp)
{
    WT_BTREE *btree;
    WT_CURSOR *a, *b;
    WT_SESSION_IMPL *session;

    btree = CUR2BT(a_arg);
    a = (WT_CURSOR *)a_arg;
    b = (WT_CURSOR *)b_arg;
    session = CUR2S(a_arg);

    /* Confirm both cursors reference the same object. */
    if (CUR2BT(a_arg) != CUR2BT(b_arg))
        WT_RET_MSG(session, EINVAL, "cursors must reference the same object");

    switch (btree->type) {
    case BTREE_COL_FIX:
    case BTREE_COL_VAR:
        /*
         * Compare the interface's cursor record, not the underlying cursor reference: the
         * interface's cursor reference is the one being returned to the application.
         */
        if (a->recno < b->recno)
            *cmpp = -1;
        else if (a->recno == b->recno)
            *cmpp = 0;
        else
            *cmpp = 1;
        break;
    case BTREE_ROW:
        WT_RET(__wt_compare(session, btree->collator, &a->key, &b->key, cmpp));
        break;
    }
    return (0);
}

/*
 * __cursor_equals --
 *     Return if two cursors reference the same row.
 */
static inline bool
__cursor_equals(WT_CURSOR_BTREE *a, WT_CURSOR_BTREE *b)
{
    switch (CUR2BT(a)->type) {
    case BTREE_COL_FIX:
    case BTREE_COL_VAR:
        /*
         * Compare the interface's cursor record, not the underlying cursor reference: the
         * interface's cursor reference is the one being returned to the application.
         */
        if (((WT_CURSOR *)a)->recno == ((WT_CURSOR *)b)->recno)
            return (true);
        break;
    case BTREE_ROW:
        if (a->ref != b->ref)
            return (false);
        if (a->ins != NULL || b->ins != NULL) {
            if (a->ins == b->ins)
                return (true);
            break;
        }
        if (a->slot == b->slot)
            return (true);
        break;
    }
    return (false);
}

/*
 * __wt_btcur_equals --
 *     Return an equality comparison between two cursors.
 */
int
__wt_btcur_equals(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *equalp)
{
    WT_CURSOR *a, *b;
    WT_SESSION_IMPL *session;
    int cmp;

    a = (WT_CURSOR *)a_arg;
    b = (WT_CURSOR *)b_arg;
    session = CUR2S(a_arg);
    cmp = 0;

    /* Confirm both cursors reference the same object. */
    if (CUR2BT(a_arg) != CUR2BT(b_arg))
        WT_RET_MSG(session, EINVAL, "cursors must reference the same object");

    /*
     * The reason for an equals method is because we can avoid doing a full key comparison in some
     * cases. If both cursors point into the tree, take the fast path, otherwise fall back to the
     * slower compare method; in both cases, return 1 if the cursors are equal, 0 if they are not.
     */
    if (F_ISSET(a, WT_CURSTD_KEY_INT) && F_ISSET(b, WT_CURSTD_KEY_INT))
        *equalp = __cursor_equals(a_arg, b_arg);
    else {
        WT_RET(__wt_btcur_compare(a_arg, b_arg, &cmp));
        *equalp = (cmp == 0) ? 1 : 0;
    }
    return (0);
}

/*
 * __cursor_truncate --
 *     Discard a cursor range from row-store or variable-width column-store tree.
 */
static int
__cursor_truncate(
  WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop, int (*rmfunc)(WT_CURSOR_BTREE *, WT_ITEM *, u_int))
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t yield_count, sleep_usecs;

    session = CUR2S(start);
    yield_count = sleep_usecs = 0;

/*
 * First, call the cursor search method to re-position the cursor: we may not have a cursor position
 * (if the higher-level truncate code switched the cursors to have an "external" cursor key, and
 * because we don't save a copy of the page's write generation information, which we need to remove
 * records).
 *
 * Once that's done, we can delete records without a full search, unless we encounter a restart
 * error because the page was modified by some other thread of control; in that case, repeat the
 * full search to refresh the page's modification information.
 *
 * If this is a row-store, we delete leaf pages having no overflow items without reading them; for
 * that to work, we have to ensure we read the page referenced by the ending cursor, since we may be
 * deleting only a partial page at the end of the truncation. Our caller already fully instantiated
 * the end cursor, so we know that page is pinned in memory and we can proceed without concern.
 */
retry:
    WT_ERR(__wt_btcur_search(start));
    WT_ASSERT(session, F_MASK((WT_CURSOR *)start, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);

    for (;;) {
        WT_ERR(rmfunc(start, NULL, WT_UPDATE_TOMBSTONE));

        if (stop != NULL && __cursor_equals(start, stop))
            return (0);

        WT_ERR(__wt_btcur_next(start, true));

        start->compare = 0; /* Exact match */
    }

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    WT_RET_NOTFOUND_OK(ret);
    return (0);
}

/*
 * __cursor_truncate_fix --
 *     Discard a cursor range from fixed-width column-store tree.
 */
static int
__cursor_truncate_fix(
  WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop, int (*rmfunc)(WT_CURSOR_BTREE *, WT_ITEM *, u_int))
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t yield_count, sleep_usecs;
    const uint8_t *value;

    session = CUR2S(start);
    yield_count = sleep_usecs = 0;

/*
 * Handle fixed-length column-store objects separately: for row-store and variable-length
 * column-store objects we have "deleted" values and so returned objects actually exist:
 * fixed-length column-store objects are filled-in if they don't exist, that is, if you create
 * record 37, records 1-36 magically appear. Those records can't be deleted, which means we have to
 * ignore already "deleted" records.
 *
 * First, call the cursor search method to re-position the cursor: we may not have a cursor position
 * (if the higher-level truncate code switched the cursors to have an "external" cursor key, and
 * because we don't save a copy of the page's write generation information, which we need to remove
 * records).
 *
 * Once that's done, we can delete records without a full search, unless we encounter a restart
 * error because the page was modified by some other thread of control; in that case, repeat the
 * full search to refresh the page's modification information.
 */
retry:
    WT_ERR(__wt_btcur_search(start));
    WT_ASSERT(session, F_MASK((WT_CURSOR *)start, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);

    for (;;) {
        value = (const uint8_t *)start->iface.value.data;
        if (*value != 0)
            WT_ERR(rmfunc(start, NULL, WT_UPDATE_TOMBSTONE));

        if (stop != NULL && __cursor_equals(start, stop))
            return (0);

        WT_ERR(__wt_btcur_next(start, true));

        start->compare = 0; /* Exact match */
    }

err:
    if (ret == WT_RESTART) {
        __cursor_restart(session, &yield_count, &sleep_usecs);
        goto retry;
    }

    WT_RET_NOTFOUND_OK(ret);
    return (0);
}

/*
 * __wt_btcur_range_truncate --
 *     Discard a cursor range from the tree.
 */
int
__wt_btcur_range_truncate(WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    btree = CUR2BT(start);
    session = CUR2S(start);
    WT_STAT_DATA_INCR(session, cursor_truncate);

    WT_RET(__wt_txn_autocommit_check(session));

    /*
     * For recovery, log the start and stop keys for a truncate operation, not the individual
     * records removed. On the other hand, for rollback we need to keep track of all the in-memory
     * operations.
     *
     * We deal with this here by logging the truncate range first, then (in the logging code)
     * disabling writing of the in-memory remove records to disk.
     */
    if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
        WT_RET(__wt_txn_truncate_log(session, start, stop));

    switch (btree->type) {
    case BTREE_COL_FIX:
        WT_ERR(__cursor_truncate_fix(start, stop, __cursor_col_modify));
        break;
    case BTREE_COL_VAR:
        WT_ERR(__cursor_truncate(start, stop, __cursor_col_modify));
        break;
    case BTREE_ROW:
        /*
         * The underlying cursor comparison routine requires cursors be fully instantiated when
         * truncating row-store objects because it's comparing page and/or skiplist positions, not
         * keys. (Key comparison would work, it's only that a key comparison would be relatively
         * expensive, especially with custom collators. Column-store objects have record number
         * keys, so the key comparison is cheap.) The session truncate code did cursor searches when
         * setting up the truncate so we're good to go: if that ever changes, we'd need to do
         * something here to ensure a fully instantiated cursor.
         */
        WT_ERR(__cursor_truncate(start, stop, __cursor_row_modify));
        break;
    }

err:
    if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
        __wt_txn_truncate_end(session);
    return (ret);
}

/*
 * __wt_btcur_init --
 *     Initialize a cursor used for internal purposes.
 */
void
__wt_btcur_init(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
    memset(cbt, 0, sizeof(WT_CURSOR_BTREE));

    cbt->iface.session = &session->iface;
    cbt->dhandle = session->dhandle;
}

/*
 * __wt_btcur_open --
 *     Open a btree cursor.
 */
void
__wt_btcur_open(WT_CURSOR_BTREE *cbt)
{
    cbt->row_key = &cbt->_row_key;
    cbt->tmp = &cbt->_tmp;
    cbt->modify_update = &cbt->_modify_update;
    cbt->upd_value = &cbt->_upd_value;

#ifdef HAVE_DIAGNOSTIC
    cbt->lastkey = &cbt->_lastkey;
    cbt->lastrecno = WT_RECNO_OOB;
#endif
}

/*
 * __wt_btcur_cache --
 *     Discard buffers when caching a cursor.
 */
void
__wt_btcur_cache(WT_CURSOR_BTREE *cbt)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(cbt);

    __wt_buf_free(session, &cbt->_row_key);
    __wt_buf_free(session, &cbt->_tmp);
    __wt_buf_free(session, &cbt->_modify_update.buf);
    __wt_buf_free(session, &cbt->_upd_value.buf);
}

/*
 * __wt_btcur_close --
 *     Close a btree cursor.
 */
int
__wt_btcur_close(WT_CURSOR_BTREE *cbt, bool lowlevel)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(cbt);

    /*
     * The in-memory split and history store table code creates low-level btree cursors to
     * search/modify leaf pages. Those cursors don't hold hazard pointers, nor are they counted in
     * the session handle's cursor count. Skip the usual cursor tear-down in that case.
     */
    if (!lowlevel)
        ret = __cursor_reset(cbt);

    __wt_buf_free(session, &cbt->_row_key);
    __wt_buf_free(session, &cbt->_tmp);
    __wt_buf_free(session, &cbt->_modify_update.buf);
    __wt_buf_free(session, &cbt->_upd_value.buf);
#ifdef HAVE_DIAGNOSTIC
    __wt_buf_free(session, &cbt->_lastkey);
#endif

    return (ret);
}
