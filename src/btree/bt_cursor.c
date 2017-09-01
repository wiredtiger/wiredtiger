/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * When returning an error, we need to restore the cursor to a valid state, the
 * upper-level cursor code is likely to retry. This structure and the associated
 * functions are used save and restore the cursor state.
 */
typedef struct {
	WT_ITEM key;
	WT_ITEM value;
	uint64_t recno;
	uint32_t flags;
} WT_CURFILE_STATE;

/*
 * __cursor_state_save --
 *	Save the cursor's external state.
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
 *	Restore the cursor's external state.
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
 *	Return if we have a page pinned and it's not been flagged for forced
 * eviction (the forced eviction test is so we periodically release pages
 * grown too large).
 */
static inline bool
__cursor_page_pinned(WT_CURSOR_BTREE *cbt)
{
	return (F_ISSET(cbt, WT_CBT_ACTIVE) &&
	    cbt->ref->page->read_gen != WT_READGEN_OLDEST);
}

/*
 * __cursor_size_chk --
 *	Return if an inserted item is too large.
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
			    "item size of %" WT_SIZET_FMT " does not match "
			    "fixed-length file requirement of 1 byte",
			    kv->size);
		return (0);
	}

	/* Don't waste effort, 1GB is always cool. */
	if (kv->size <= WT_GIGABYTE)
		return (0);

	/* Check what we are willing to store in the tree. */
	if (kv->size > WT_BTREE_MAX_OBJECT_SIZE)
		WT_RET_MSG(session, EINVAL,
		    "item size of %" WT_SIZET_FMT " exceeds the maximum "
		    "supported WiredTiger size of %" PRIu32,
		    kv->size, WT_BTREE_MAX_OBJECT_SIZE);

	/* Check what the block manager can actually write. */
	size = kv->size;
	if ((ret = bm->write_size(bm, session, &size)) != 0)
		WT_RET_MSG(session, ret,
		    "item size of %" WT_SIZET_FMT " refused by block manager",
		    kv->size);

	return (0);
}

/*
 * __cursor_disable_bulk --
 *	Disable bulk loads into a tree.
 */
static inline void
__cursor_disable_bulk(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
	/*
	 * Once a tree (other than the LSM primary) is no longer empty, eviction
	 * should pay attention to it, and it's no longer possible to bulk-load
	 * into it.
	 */
	if (!btree->original)
		return;
	if (btree->lsm_primary) {
		btree->original = 0;		/* Make the next test faster. */
		return;
	}

	/*
	 * We use a compare-and-swap here to avoid races among the first inserts
	 * into a tree.  Eviction is disabled when an empty tree is opened, and
	 * it must only be enabled once.
	 */
	if (__wt_atomic_cas8(&btree->original, 1, 0)) {
		btree->evict_disabled_open = false;
		__wt_evict_file_exclusive_off(session);
	}
}

/*
 * __cursor_fix_implicit --
 *	Return if search went past the end of the tree.
 */
static inline int
__cursor_fix_implicit(WT_BTREE *btree, WT_CURSOR_BTREE *cbt)
{
	/*
	 * When there's no exact match, column-store search returns the key
	 * nearest the searched-for key (continuing past keys smaller than the
	 * searched-for key to return the next-largest key). Therefore, if the
	 * returned comparison is -1, the searched-for key was larger than any
	 * row on the page's standard information or column-store insert list.
	 *
	 * If the returned comparison is NOT -1, there was a row equal to or
	 * larger than the searched-for key, and we implicitly create missing
	 * rows.
	 */
	return (btree->type == BTREE_COL_FIX && cbt->compare != -1);
}

/*
 * __wt_cursor_valid --
 *	Return if the cursor references an valid key/value pair.
 */
bool
__wt_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_COL *cip;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	btree = cbt->btree;
	page = cbt->ref->page;
	session = (WT_SESSION_IMPL *)cbt->iface.session;
	if (updp != NULL)
		*updp = NULL;

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
	if (cbt->ins != NULL &&
	    (upd = __wt_txn_read(session, cbt->ins->upd)) != NULL) {
		if (upd->type == WT_UPDATE_DELETED)
			return (false);
		if (updp != NULL)
			*updp = upd;
		return (true);
	}

	/*
	 * If we don't have an insert object, or in the case of column-store,
	 * there's an insert object but no update was visible to us and the key
	 * on the page is the same as the insert object's key, and the slot as
	 * set by the search function is valid, we can use the original page
	 * information.
	 */
	switch (btree->type) {
	case BTREE_COL_FIX:
		/*
		 * If search returned an insert object, there may or may not be
		 * a matching on-page object, we have to check.  Fixed-length
		 * column-store pages don't have slots, but map one-to-one to
		 * keys, check for retrieval past the end of the page.
		 */
		if (cbt->recno >= cbt->ref->ref_recno + page->entries)
			return (false);

		/*
		 * An update would have appeared as an "insert" object; no
		 * further checks to do.
		 */
		break;
	case BTREE_COL_VAR:
		/* The search function doesn't check for empty pages. */
		if (page->entries == 0)
			return (false);
		WT_ASSERT(session, cbt->slot < page->entries);

		/*
		 * Column-store updates are stored as "insert" objects. If
		 * search returned an insert object we can't return, the
		 * returned on-page object must be checked for a match.
		 */
		if (cbt->ins != NULL && !F_ISSET(cbt, WT_CBT_VAR_ONPAGE_MATCH))
			return (false);

		/*
		 * Although updates would have appeared as an "insert" objects,
		 * variable-length column store deletes are written into the
		 * backing store; check the cell for a record already deleted
		 * when read.
		 */
		cip = &page->pg_var[cbt->slot];
		if ((cell = WT_COL_PTR(page, cip)) == NULL ||
		    __wt_cell_type(cell) == WT_CELL_DEL)
			return (false);
		break;
	case BTREE_ROW:
		/* The search function doesn't check for empty pages. */
		if (page->entries == 0)
			return (false);
		WT_ASSERT(session, cbt->slot < page->entries);

		/*
		 * See above: for row-store, no insert object can have the same
		 * key as an on-page object, we're done.
		 */
		if (cbt->ins != NULL)
			return (false);

		/* Check for an update. */
		if (page->modify != NULL &&
		    page->modify->mod_row_update != NULL &&
		    (upd = __wt_txn_read(session,
		    page->modify->mod_row_update[cbt->slot])) != NULL) {
			if (upd->type == WT_UPDATE_DELETED)
				return (false);
			if (updp != NULL)
				*updp = upd;
		}
		break;
	}
	return (true);
}

/*
 * __cursor_kv_return --
 *	Return a page referenced key/value pair to the application.
 */
static inline int
__cursor_kv_return(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_RET(__wt_key_return(session, cbt));
	WT_RET(__wt_value_return(session, cbt, upd));

	return (0);
}

/*
 * __cursor_col_search --
 *	Column-store search from a cursor.
 */
static inline int
__cursor_col_search(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_REF *leaf)
{
	WT_DECL_RET;

	WT_WITH_PAGE_INDEX(session,
	    ret = __wt_col_search(session, cbt->iface.recno, leaf, cbt));
	return (ret);
}

/*
 * __cursor_row_search --
 *	Row-store search from a cursor.
 */
static inline int
__cursor_row_search(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_REF *leaf, bool insert)
{
	WT_DECL_RET;

	WT_WITH_PAGE_INDEX(session,
	    ret = __wt_row_search(session, &cbt->iface.key, leaf, cbt, insert));
	return (ret);
}

/*
 * __cursor_col_modify_v --
 *	Column-store modify from a cursor, with a separate value.
 */
static inline int
__cursor_col_modify_v(WT_SESSION_IMPL *session,
    WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
	return (__wt_col_modify(session, cbt,
	    cbt->iface.recno, value, NULL, modify_type, false));
}

/*
 * __cursor_row_modify_v --
 *	Row-store modify from a cursor, with a separate value.
 */
static inline int
__cursor_row_modify_v(WT_SESSION_IMPL *session,
    WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
	return (__wt_row_modify(session, cbt,
	    &cbt->iface.key, value, NULL, modify_type, false));
}

/*
 * __cursor_col_modify --
 *	Column-store modify from a cursor.
 */
static inline int
__cursor_col_modify(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, u_int modify_type)
{
	return (__wt_col_modify(session, cbt,
	    cbt->iface.recno, &cbt->iface.value, NULL, modify_type, false));
}

/*
 * __cursor_row_modify --
 *	Row-store modify from a cursor.
 */
static inline int
__cursor_row_modify(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, u_int modify_type)
{
	return (__wt_row_modify(session, cbt,
	    &cbt->iface.key, &cbt->iface.value, NULL, modify_type, false));
}

/*
 * __wt_btcur_reset --
 *	Invalidate the cursor position.
 */
int
__wt_btcur_reset(WT_CURSOR_BTREE *cbt)
{
	WT_CURSOR *cursor;
	WT_SESSION_IMPL *session;

	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_STAT_CONN_INCR(session, cursor_reset);
	WT_STAT_DATA_INCR(session, cursor_reset);

	F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

	return (__cursor_reset(cbt));
}

/*
 * __wt_btcur_search --
 *	Search for a matching record in the tree.
 */
int
__wt_btcur_search(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_CURFILE_STATE state;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	bool valid;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;
	upd = NULL;					/* -Wuninitialized */

	WT_STAT_CONN_INCR(session, cursor_search);
	WT_STAT_DATA_INCR(session, cursor_search);

	__cursor_state_save(cursor, &state);

	/*
	 * The pinned page goes away if we search the tree, get a local copy of
	 * any pinned key and discard any pinned value, then re-save the cursor
	 * state. Done before searching pinned pages (unlike other cursor
	 * functions), because we don't anticipate applications searching for a
	 * key they currently have pinned.)
	 */
	WT_ERR(__cursor_localkey(cursor));
	__cursor_novalue(cursor);
	__cursor_state_save(cursor, &state);

	/*
	 * If we have a page pinned, search it; if we don't have a page pinned,
	 * or the search of the pinned page doesn't find an exact match, search
	 * from the root.
	 */
	valid = false;
	if (__cursor_page_pinned(cbt)) {
		__wt_txn_cursor_op(session);

		WT_ERR(btree->type == BTREE_ROW ?
		    __cursor_row_search(session, cbt, cbt->ref, false) :
		    __cursor_col_search(session, cbt, cbt->ref));
		valid = cbt->compare == 0 && __wt_cursor_valid(cbt, &upd);
	}
	if (!valid) {
		WT_ERR(__cursor_func_init(cbt, true));

		WT_ERR(btree->type == BTREE_ROW ?
		    __cursor_row_search(session, cbt, NULL, false) :
		    __cursor_col_search(session, cbt, NULL));
		valid = cbt->compare == 0 && __wt_cursor_valid(cbt, &upd);
	}

	if (valid)
		ret = __cursor_kv_return(session, cbt, upd);
	else if (__cursor_fix_implicit(btree, cbt)) {
		/*
		 * Creating a record past the end of the tree in a fixed-length
		 * column-store implicitly fills the gap with empty records.
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
		WT_ERR(__wt_cursor_key_order_init(session, cbt));
#endif

err:	if (ret != 0) {
		WT_TRET(__cursor_reset(cbt));
		__cursor_state_restore(cursor, &state);
	}
	return (ret);
}

/*
 * __wt_btcur_search_near --
 *	Search for a record in the tree.
 */
int
__wt_btcur_search_near(WT_CURSOR_BTREE *cbt, int *exactp)
{
	WT_BTREE *btree;
	WT_CURFILE_STATE state;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	int exact;
	bool valid;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;
	upd = NULL;					/* -Wuninitialized */
	exact = 0;

	WT_STAT_CONN_INCR(session, cursor_search_near);
	WT_STAT_DATA_INCR(session, cursor_search_near);

	__cursor_state_save(cursor, &state);

	/*
	 * The pinned page goes away if we search the tree, get a local copy of
	 * any pinned key and discard any pinned value, then re-save the cursor
	 * state. Done before searching pinned pages (unlike other cursor
	 * functions), because we don't anticipate applications searching for a
	 * key they currently have pinned.)
	 */
	WT_ERR(__cursor_localkey(cursor));
	__cursor_novalue(cursor);
	__cursor_state_save(cursor, &state);

	/*
	 * If we have a row-store page pinned, search it; if we don't have a
	 * page pinned, or the search of the pinned page doesn't find an exact
	 * match, search from the root. Unlike WT_CURSOR.search, ignore pinned
	 * pages in the case of column-store, search-near isn't an interesting
	 * enough case for column-store to add the complexity needed to avoid
	 * the tree search.
	 *
	 * Set the "insert" flag for the btree row-store search; we may intend
	 * to position the cursor at the end of the tree, rather than match an
	 * existing record.
	 */
	valid = false;
	if (btree->type == BTREE_ROW && __cursor_page_pinned(cbt)) {
		__wt_txn_cursor_op(session);

		WT_ERR(__cursor_row_search(session, cbt, cbt->ref, true));

		/*
		 * Search-near is trickier than search when searching an already
		 * pinned page. If search returns the first or last page slots,
		 * discard the results and search the full tree as the neighbor
		 * pages might offer better matches. This test is simplistic as
		 * we're ignoring append lists (there may be no page slots or we
		 * might be legitimately positioned after the last page slot).
		 * Ignore those cases, it makes things too complicated.
		 */
		if (cbt->slot != 0 && cbt->slot != cbt->ref->page->entries - 1)
			valid = __wt_cursor_valid(cbt, &upd);
	}
	if (!valid) {
		WT_ERR(__cursor_func_init(cbt, true));
		WT_ERR(btree->type == BTREE_ROW ?
		    __cursor_row_search(session, cbt, NULL, true) :
		    __cursor_col_search(session, cbt, NULL));
		valid = __wt_cursor_valid(cbt, &upd);
	}

	/*
	 * If we find a valid key, return it.
	 *
	 * Else, creating a record past the end of the tree in a fixed-length
	 * column-store implicitly fills the gap with empty records.  In this
	 * case, we instantiate the empty record, it's an exact match.
	 *
	 * Else, move to the next key in the tree (bias for prefix searches).
	 * Cursor next skips invalid rows, so we don't have to test for them
	 * again.
	 *
	 * Else, redo the search and move to the previous key in the tree.
	 * Cursor previous skips invalid rows, so we don't have to test for
	 * them again.
	 *
	 * If that fails, quit, there's no record to return.
	 */
	if (valid) {
		exact = cbt->compare;
		ret = __cursor_kv_return(session, cbt, upd);
	} else if (__cursor_fix_implicit(btree, cbt)) {
		cbt->recno = cursor->recno;
		cbt->v = 0;
		cursor->value.data = &cbt->v;
		cursor->value.size = 1;
		exact = 0;
		F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
		F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
	} else if ((ret = __wt_btcur_next(cbt, false)) != WT_NOTFOUND)
		exact = 1;
	else {
		WT_ERR(__cursor_func_init(cbt, true));
		WT_ERR(btree->type == BTREE_ROW ?
		    __cursor_row_search(session, cbt, NULL, true) :
		    __cursor_col_search(session, cbt, NULL));
		if (__wt_cursor_valid(cbt, &upd)) {
			exact = cbt->compare;
			ret = __cursor_kv_return(session, cbt, upd);
		} else if ((ret = __wt_btcur_prev(cbt, false)) != WT_NOTFOUND)
			exact = -1;
	}

err:	if (ret == 0 && exactp != NULL)
		*exactp = exact;

#ifdef HAVE_DIAGNOSTIC
	if (ret == 0)
		WT_TRET(__wt_cursor_key_order_init(session, cbt));
#endif

	if (ret != 0) {
		WT_TRET(__cursor_reset(cbt));
		__cursor_state_restore(cursor, &state);
	}
	return (ret);
}

/*
 * __wt_btcur_insert --
 *	Insert a record into the tree.
 */
int
__wt_btcur_insert(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_CURFILE_STATE state;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	bool append_key;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_CONN_INCR(session, cursor_insert);
	WT_STAT_DATA_INCR(session, cursor_insert);
	WT_STAT_DATA_INCRV(session,
	    cursor_insert_bytes, cursor->key.size + cursor->value.size);

	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));
	WT_RET(__cursor_size_chk(session, &cursor->value));

	/* It's no longer possible to bulk-load into the tree. */
	__cursor_disable_bulk(session, btree);

	/*
	 * Insert a new record if WT_CURSTD_APPEND configured, (ignoring any
	 * application set record number). Although append can't be configured
	 * for a row-store, this code would break if it were, and that's owned
	 * by the upper cursor layer, be cautious.
	 */
	append_key =
	    F_ISSET(cursor, WT_CURSTD_APPEND) && btree->type != BTREE_ROW;

	/* Save the cursor state. */
	__cursor_state_save(cursor, &state);

	/*
	 * If inserting with overwrite configured, and positioned to an on-page
	 * key, the update doesn't require another search. The cursor won't be
	 * positioned on a page with an external key set, but be sure. Cursors
	 * configured for append aren't included, regardless of whether or not
	 * they meet all other criteria.
	 */
	if (__cursor_page_pinned(cbt) &&
	    F_ISSET_ALL(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_OVERWRITE) &&
	    !append_key) {
		WT_ERR(__wt_txn_autocommit_check(session));
		/*
		 * The cursor position may not be exact (the cursor's comparison
		 * value not equal to zero). Correct to an exact match so we can
		 * update whatever we're pointing at.
		 */
		cbt->compare = 0;
		ret = btree->type == BTREE_ROW ?
		    __cursor_row_modify(session, cbt, WT_UPDATE_STANDARD) :
		    __cursor_col_modify(session, cbt, WT_UPDATE_STANDARD);
		if (ret == 0)
			goto done;

		/*
		 * The pinned page goes away if we fail for any reason, get a
		 * local copy of any pinned key or value. (Restart could still
		 * use the pinned page, but that's an unlikely path.) Re-save
		 * the cursor state: we may retry but eventually fail.
		 */
		WT_TRET(__cursor_localkey(cursor));
		WT_TRET(__cursor_localvalue(cursor));
		__cursor_state_save(cursor, &state);
		goto err;
	}

	/*
	 * The pinned page goes away if we do a search, get a local copy of any
	 * pinned key or value. Re-save the cursor state: we may retry but
	 * eventually fail.
	 */
	WT_ERR(__cursor_localkey(cursor));
	WT_ERR(__cursor_localvalue(cursor));
	__cursor_state_save(cursor, &state);

retry:	WT_ERR(__cursor_func_init(cbt, true));

	if (btree->type == BTREE_ROW) {
		WT_ERR(__cursor_row_search(session, cbt, NULL, true));
		/*
		 * If not overwriting, fail if the key exists, else insert the
		 * key/value pair.
		 */
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
		    cbt->compare == 0 && __wt_cursor_valid(cbt, NULL))
			WT_ERR(WT_DUPLICATE_KEY);

		ret = __cursor_row_modify(session, cbt, WT_UPDATE_STANDARD);
	} else {
		/*
		 * Optionally insert a new record (ignoring the application's
		 * record number). The real record number is allocated by the
		 * serialized append operation.
		 */
		if (append_key)
			cbt->iface.recno = WT_RECNO_OOB;

		WT_ERR(__cursor_col_search(session, cbt, NULL));

		/*
		 * If not overwriting, fail if the key exists.  Creating a
		 * record past the end of the tree in a fixed-length
		 * column-store implicitly fills the gap with empty records.
		 * Fail in that case, the record exists.
		 */
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
		    ((cbt->compare == 0 && __wt_cursor_valid(cbt, NULL)) ||
		    (cbt->compare != 0 && __cursor_fix_implicit(btree, cbt))))
			WT_ERR(WT_DUPLICATE_KEY);

		WT_ERR(__cursor_col_modify(session, cbt, WT_UPDATE_STANDARD));

		if (append_key)
			cbt->iface.recno = cbt->recno;
	}

err:	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
		goto retry;
	}

done:	/* Insert doesn't maintain a position across calls, clear resources. */
	if (ret == 0) {
		F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
		if (append_key)
			F_SET(cursor, WT_CURSTD_KEY_INT);
	}
	WT_TRET(__cursor_reset(cbt));
	if (ret != 0)
		__cursor_state_restore(cursor, &state);

	return (ret);
}

/*
 * __curfile_update_check --
 *	Check whether an update would conflict.
 *
 *	This function expects the cursor to already be positioned.  It should
 *	be called before deciding whether to skip an update operation based on
 *	existence of a visible update for a key -- even if there is no value
 *	visible to the transaction, an update could still conflict.
 */
static int
__curfile_update_check(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	session = (WT_SESSION_IMPL *)cbt->iface.session;

	if (cbt->compare != 0)
		return (0);
	if (cbt->ins != NULL)
		return (__wt_txn_update_check(session, cbt->ins->upd));

	if (btree->type == BTREE_ROW &&
	    cbt->ref->page->modify != NULL &&
	    cbt->ref->page->modify->mod_row_update != NULL)
		return (__wt_txn_update_check(session,
		    cbt->ref->page->modify->mod_row_update[cbt->slot]));
	return (0);
}

/*
 * __wt_btcur_insert_check --
 *	Check whether an update would conflict.
 *
 * This can replace WT_CURSOR::insert, so it only checks for conflicts without
 * updating the tree. It is used to maintain snapshot isolation for transactions
 * that span multiple chunks in an LSM tree.
 */
int
__wt_btcur_insert_check(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	cursor = &cbt->iface;
	btree = cbt->btree;
	session = (WT_SESSION_IMPL *)cursor->session;

	/*
	 * The pinned page goes away if we do a search, get a local copy of any
	 * pinned key and discard any pinned value. Unlike most of the btree
	 * cursor routines, we don't have to save/restore the cursor key state,
	 * none of the work done here changes the cursor state.
	 */
	WT_ERR(__cursor_localkey(cursor));
	__cursor_novalue(cursor);

retry:	WT_ERR(__cursor_func_init(cbt, true));

	if (btree->type == BTREE_ROW) {
		WT_ERR(__cursor_row_search(session, cbt, NULL, true));

		/* Just check for conflicts. */
		ret = __curfile_update_check(cbt);
	} else
		WT_ERR(__wt_illegal_value(session, NULL));

err:	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
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
 *	Remove a record from the tree.
 */
int
__wt_btcur_remove(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_CURFILE_STATE state;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	bool positioned;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_CONN_INCR(session, cursor_remove);
	WT_STAT_DATA_INCR(session, cursor_remove);
	WT_STAT_DATA_INCRV(session, cursor_remove_bytes, cursor->key.size);

	/*
	 * WT_CURSOR.remove has a unique semantic, the cursor stays positioned
	 * if it starts positioned, otherwise clear the cursor on completion.
	 */
	positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT);

	/* Save the cursor state. */
	__cursor_state_save(cursor, &state);

	/*
	 * If remove positioned to an on-page key, the remove doesn't require
	 * another search. We don't care about the "overwrite" configuration
	 * because regardless of the overwrite setting, any existing record is
	 * removed, and the record must exist with a positioned cursor. The
	 * cursor won't be positioned on a page with an external key set, but
	 * be sure.
	 */
	if (__cursor_page_pinned(cbt) && F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
		WT_ERR(__wt_txn_autocommit_check(session));

		/*
		 * The cursor position may not be exact (the cursor's comparison
		 * value not equal to zero). Correct to an exact match so we can
		 * remove whatever we're pointing at.
		 */
		cbt->compare = 0;
		ret = btree->type == BTREE_ROW ?
		    __cursor_row_modify(session, cbt, WT_UPDATE_DELETED) :
		    __cursor_col_modify(session, cbt, WT_UPDATE_DELETED);
		if (ret == 0)
			goto done;

		/*
		 * The pinned page goes away if we fail for any reason, get a
		 * local copy of any pinned key and discard any value (remove
		 * discards any previous value on success or failure). (Restart
		 * could still use the pinned page, but that's an unlikely
		 * path.) Re-save the cursor state: we may retry but eventually
		 * fail.
		 */
		WT_TRET(__cursor_localkey(cursor));
		F_CLR(cursor, WT_CURSTD_VALUE_SET);
		__cursor_state_save(cursor, &state);
		goto err;
	}

	/*
	 * The pinned page goes away if we do a search, get a local copy of any
	 * pinned key and discard any value (remove discards any previous
	 * value on success or failure). Re-save the cursor state: we may retry
	 * but eventually fail.
	 */
	WT_ERR(__cursor_localkey(cursor));
	F_CLR(cursor, WT_CURSTD_VALUE_SET);
	__cursor_state_save(cursor, &state);

retry:	WT_ERR(__cursor_func_init(cbt, true));

	if (btree->type == BTREE_ROW) {
		WT_ERR(__cursor_row_search(session, cbt, NULL, false));

		/* Check whether an update would conflict. */
		WT_ERR(__curfile_update_check(cbt));

		if (cbt->compare != 0 || !__wt_cursor_valid(cbt, NULL))
			WT_ERR(WT_NOTFOUND);

		ret = __cursor_row_modify(session, cbt, WT_UPDATE_DELETED);
	} else {
		WT_ERR(__cursor_col_search(session, cbt, NULL));

		/*
		 * If we find a matching record, check whether an update would
		 * conflict.  Do this before checking if the update is visible
		 * in __wt_cursor_valid, or we can miss conflict.
		 */
		WT_ERR(__curfile_update_check(cbt));

		/* Remove the record if it exists. */
		if (cbt->compare != 0 || !__wt_cursor_valid(cbt, NULL)) {
			if (!__cursor_fix_implicit(btree, cbt))
				WT_ERR(WT_NOTFOUND);
			/*
			 * Creating a record past the end of the tree in a
			 * fixed-length column-store implicitly fills the
			 * gap with empty records.  Return success in that
			 * case, the record was deleted successfully.
			 *
			 * Correct the btree cursor's location: the search
			 * will have pointed us at the previous/next item,
			 * and that's not correct.
			 */
			cbt->recno = cursor->recno;
		} else
			ret = __cursor_col_modify(
			    session, cbt, WT_UPDATE_DELETED);
	}

err:	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
		goto retry;
	}

	/*
	 * If the cursor is configured to overwrite and the record is not found,
	 * that is exactly what we want, return success.
	 */
	if (F_ISSET(cursor, WT_CURSTD_OVERWRITE) && ret == WT_NOTFOUND)
		ret = 0;

done:	/*
	 * If the cursor was positioned, it stays positioned, point the cursor
	 * at an internal copy of the key. Otherwise, there's no position or
	 * key/value.
	 */
	if (ret == 0)
		F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
	if (ret == 0 && positioned)
		WT_TRET(__wt_key_return(session, cbt));
	else
		WT_TRET(__cursor_reset(cbt));
	if (ret != 0)
		__cursor_state_restore(cursor, &state);

	return (ret);
}

/*
 * __btcur_update --
 *	Update a record in the tree.
 */
static int
__btcur_update(WT_CURSOR_BTREE *cbt, WT_ITEM *value, u_int modify_type)
{
	WT_BTREE *btree;
	WT_CURFILE_STATE state;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	/* It's no longer possible to bulk-load into the tree. */
	__cursor_disable_bulk(session, btree);

	/* Save the cursor state. */
	__cursor_state_save(cursor, &state);

	/*
	 * If update positioned to an on-page key, the update doesn't require
	 * another search. We don't care about the "overwrite" configuration
	 * because regardless of the overwrite setting, any existing record is
	 * updated, and the record must exist with a positioned cursor. The
	 * cursor won't be positioned on a page with an external key set, but
	 * be sure.
	 */
	if (__cursor_page_pinned(cbt) && F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
		WT_ERR(__wt_txn_autocommit_check(session));

		/*
		 * The cursor position may not be exact (the cursor's comparison
		 * value not equal to zero). Correct to an exact match so we can
		 * update whatever we're pointing at.
		 */
		cbt->compare = 0;
		ret = btree->type == BTREE_ROW ?
		    __cursor_row_modify_v(session, cbt, value, modify_type) :
		    __cursor_col_modify_v(session, cbt, value, modify_type);
		if (ret == 0)
			goto done;

		/*
		 * The pinned page goes away if we fail for any reason, get a
		 * a local copy of any pinned key or value. (Restart could still
		 * use the pinned page, but that's an unlikely path.) Re-save
		 * the cursor state: we may retry but eventually fail.
		 */
		WT_TRET(__cursor_localkey(cursor));
		WT_TRET(__cursor_localvalue(cursor));
		__cursor_state_save(cursor, &state);
		goto err;
	}

	/*
	 * The pinned page goes away if we do a search, get a local copy of any
	 * pinned key or value. Re-save the cursor state: we may retry but
	 * eventually fail.
	 */
	WT_ERR(__cursor_localkey(cursor));
	WT_ERR(__cursor_localvalue(cursor));
	__cursor_state_save(cursor, &state);

retry:	WT_ERR(__cursor_func_init(cbt, true));

	if (btree->type == BTREE_ROW) {
		WT_ERR(__cursor_row_search(session, cbt, NULL, true));

		/*
		 * If not overwriting, check for conflicts and fail if the key
		 * does not exist.
		 */
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
			WT_ERR(__curfile_update_check(cbt));
			if (cbt->compare != 0 || !__wt_cursor_valid(cbt, NULL))
				WT_ERR(WT_NOTFOUND);
		}
		ret = __cursor_row_modify_v(session, cbt, value, modify_type);
	} else {
		WT_ERR(__cursor_col_search(session, cbt, NULL));

		/*
		 * If not overwriting, fail if the key doesn't exist.  If we
		 * find an update for the key, check for conflicts.  Update the
		 * record if it exists.  Creating a record past the end of the
		 * tree in a fixed-length column-store implicitly fills the gap
		 * with empty records.  Update the record in that case, the
		 * record exists.
		 */
		if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
			WT_ERR(__curfile_update_check(cbt));
			if ((cbt->compare != 0 ||
			    !__wt_cursor_valid(cbt, NULL)) &&
			    !__cursor_fix_implicit(btree, cbt))
				WT_ERR(WT_NOTFOUND);
		}
		ret = __cursor_col_modify_v(session, cbt, value, modify_type);
	}

err:	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
		goto retry;
	}

	/*
	 * If successful, point the cursor at internal copies of the data.  We
	 * could shuffle memory in the cursor so the key/value pair are in local
	 * buffer memory, but that's a data copy.  We don't want to do another
	 * search (and we might get a different update structure if we race).
	 * To make this work, we add a field to the btree cursor to pass back a
	 * pointer to the modify function's allocated update structure.
	 */
done:	if (ret == 0)
		switch (modify_type) {
		case WT_UPDATE_STANDARD:
			/*
			 * WT_CURSOR.update returns a key and a value.
			 */
			WT_TRET(__cursor_kv_return(
			    session, cbt, cbt->modify_update));
			break;
		case WT_UPDATE_RESERVED:
			/*
			 * WT_CURSOR.reserve doesn't return any value.
			 */
			F_CLR(cursor, WT_CURSTD_VALUE_SET);
			/* FALLTHROUGH */
		case WT_UPDATE_MODIFIED:
			/*
			 * WT_CURSOR.modify has already created the return value
			 * and our job is to leave it untouched.
			 */
			WT_TRET(__wt_key_return(session, cbt));
			break;
		case WT_UPDATE_DELETED:
		default:
			WT_TRET(__wt_illegal_value(session, NULL));
			break;
		}

	if (ret != 0) {
		WT_TRET(__cursor_reset(cbt));
		__cursor_state_restore(cursor, &state);
	}

	return (ret);
}

/*
 * __cursor_chain_exceeded --
 *	Return if the update chain has exceeded the limit. Deleted or standard
 * updates are anticipated to be sufficient to base the modify (although that's
 * not guaranteed, they may not be visible or might abort before we read them).
 * Also, this is not a hard limit, threads can race modifying updates.
 */
static bool
__cursor_chain_exceeded(WT_CURSOR_BTREE *cbt)
{
	WT_PAGE *page;
	WT_UPDATE *upd;
	int i;

	page = cbt->ref->page;

	upd = NULL;
	if (cbt->ins != NULL)
		upd = cbt->ins->upd;
	else if (cbt->btree->type == BTREE_ROW &&
	    page->modify != NULL && page->modify->mod_row_update != NULL)
		upd = page->modify->mod_row_update[cbt->slot];

	for (i = 0; upd != NULL; ++i, upd = upd->next) {
		if (WT_UPDATE_DATA_VALUE(upd))
			return (false);
		if (i >= WT_MAX_MODIFY_UPDATE)
			return (true);
	}
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
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_CONN_INCR(session, cursor_modify);
	WT_STAT_DATA_INCR(session, cursor_modify);

	/* Save the cursor state. */
	__cursor_state_save(cursor, &state);

	/*
	 * Get the current value and apply the modification to it, for a few
	 * reasons: first, we set the updated value so the application can
	 * retrieve the cursor's value; second, we use the updated value as
	 * the update if the update chain is too long; third, there's a check
	 * if the updated value is too large to store; fourth, to simplify the
	 * count of bytes being added/removed; fifth, we can get into serious
	 * trouble if we attempt to modify a value that doesn't exist. For the
	 * fifth reason, verify we're not in a read-uncommitted transaction,
	 * that implies a value that might disappear out from under us.
	 */
	if (session->txn.isolation == WT_ISO_READ_UNCOMMITTED)
		WT_ERR_MSG(session, ENOTSUP,
		    "not supported in read-uncommitted transactions");

	WT_ERR(__wt_btcur_search(cbt));
	orig = cursor->value.size;
	WT_ERR(__wt_modify_apply_api(
	    session, &cursor->value, entries, nentries));
	new = cursor->value.size;
	WT_ERR(__cursor_size_chk(session, &cursor->value));
	if (new > orig)
		WT_STAT_DATA_INCRV(session, cursor_update_bytes, new - orig);
	else
		WT_STAT_DATA_DECRV(session, cursor_update_bytes, orig - new);

	/*
	 * WT_CURSOR.modify is update-without-overwrite.
	 *
	 * Use the modify buffer as the update if the data package saves us some
	 * memory and the update chain is under the limit, else use the complete
	 * value.
	 */
	overwrite = F_ISSET(cursor, WT_CURSTD_OVERWRITE);
	F_CLR(cursor, WT_CURSTD_OVERWRITE);
	if (cursor->value.size <= 64 || __cursor_chain_exceeded(cbt))
		ret = __btcur_update(cbt, &cursor->value, WT_UPDATE_STANDARD);
	else if ((ret =
	    __wt_modify_pack(session, &modify, entries, nentries)) == 0)
		ret = __btcur_update(cbt, modify, WT_UPDATE_MODIFIED);
	if (overwrite)
	       F_SET(cursor, WT_CURSTD_OVERWRITE);

	/*
	 * We have our own cursor state restoration because we've modified the
	 * cursor before calling the underlying cursor update function and we
	 * need to restore it to its original state. This means multiple calls
	 * to reset the cursor, but that shouldn't be a problem.
	 */
	if (ret != 0) {
err:		WT_TRET(__cursor_reset(cbt));
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
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_CONN_INCR(session, cursor_reserve);
	WT_STAT_DATA_INCR(session, cursor_reserve);

	/* WT_CURSOR.reserve is update-without-overwrite and a special value. */
	overwrite = F_ISSET(cursor, WT_CURSTD_OVERWRITE);
	F_CLR(cursor, WT_CURSTD_OVERWRITE);
	ret = __btcur_update(cbt, &cursor->value, WT_UPDATE_RESERVED);
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

	btree = cbt->btree;
	cursor = &cbt->iface;
	session = (WT_SESSION_IMPL *)cursor->session;

	WT_STAT_CONN_INCR(session, cursor_update);
	WT_STAT_DATA_INCR(session, cursor_update);
	WT_STAT_DATA_INCRV(session, cursor_update_bytes, cursor->value.size);

	if (btree->type == BTREE_ROW)
		WT_RET(__cursor_size_chk(session, &cursor->key));
	WT_RET(__cursor_size_chk(session, &cursor->value));

	return (__btcur_update(cbt, &cursor->value, WT_UPDATE_STANDARD));
}

/*
 * __wt_btcur_compare --
 *	Return a comparison between two cursors.
 */
int
__wt_btcur_compare(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *cmpp)
{
	WT_CURSOR *a, *b;
	WT_SESSION_IMPL *session;

	a = (WT_CURSOR *)a_arg;
	b = (WT_CURSOR *)b_arg;
	session = (WT_SESSION_IMPL *)a->session;

	/* Confirm both cursors reference the same object. */
	if (a_arg->btree != b_arg->btree)
		WT_RET_MSG(
		    session, EINVAL, "Cursors must reference the same object");

	switch (a_arg->btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		/*
		 * Compare the interface's cursor record, not the underlying
		 * cursor reference: the interface's cursor reference is the
		 * one being returned to the application.
		 */
		if (a->recno < b->recno)
			*cmpp = -1;
		else if (a->recno == b->recno)
			*cmpp = 0;
		else
			*cmpp = 1;
		break;
	case BTREE_ROW:
		WT_RET(__wt_compare(
		    session, a_arg->btree->collator, &a->key, &b->key, cmpp));
		break;
	}
	return (0);
}

/*
 * __cursor_equals --
 *	Return if two cursors reference the same row.
 */
static inline bool
__cursor_equals(WT_CURSOR_BTREE *a, WT_CURSOR_BTREE *b)
{
	switch (a->btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		/*
		 * Compare the interface's cursor record, not the underlying
		 * cursor reference: the interface's cursor reference is the
		 * one being returned to the application.
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
 *	Return an equality comparison between two cursors.
 */
int
__wt_btcur_equals(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *equalp)
{
	WT_CURSOR *a, *b;
	WT_SESSION_IMPL *session;
	int cmp;

	a = (WT_CURSOR *)a_arg;
	b = (WT_CURSOR *)b_arg;
	cmp = 0;
	session = (WT_SESSION_IMPL *)a->session;

	/* Confirm both cursors reference the same object. */
	if (a_arg->btree != b_arg->btree)
		WT_RET_MSG(
		    session, EINVAL, "Cursors must reference the same object");

	/*
	 * The reason for an equals method is because we can avoid doing
	 * a full key comparison in some cases. If both cursors point into the
	 * tree, take the fast path, otherwise fall back to the slower compare
	 * method; in both cases, return 1 if the cursors are equal, 0 if they
	 * are not.
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
 *	Discard a cursor range from row-store or variable-width column-store
 * tree.
 */
static int
__cursor_truncate(WT_SESSION_IMPL *session,
    WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop,
    int (*rmfunc)(WT_SESSION_IMPL *, WT_CURSOR_BTREE *, u_int))
{
	WT_DECL_RET;

	/*
	 * First, call the cursor search method to re-position the cursor: we
	 * may not have a cursor position (if the higher-level truncate code
	 * switched the cursors to have an "external" cursor key, and because
	 * we don't save a copy of the page's write generation information,
	 * which we need to remove records.
	 *
	 * Once that's done, we can delete records without a full search, unless
	 * we encounter a restart error because the page was modified by some
	 * other thread of control; in that case, repeat the full search to
	 * refresh the page's modification information.
	 *
	 * If this is a row-store, we delete leaf pages having no overflow items
	 * without reading them; for that to work, we have to ensure we read the
	 * page referenced by the ending cursor, since we may be deleting only a
	 * partial page at the end of the truncation.  Our caller already fully
	 * instantiated the end cursor, so we know that page is pinned in memory
	 * and we can proceed without concern.
	 */
retry:	WT_RET(__wt_btcur_search(start));
	WT_ASSERT(session,
	    F_MASK((WT_CURSOR *)start, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);

	for (;;) {
		if ((ret = rmfunc(session, start, WT_UPDATE_DELETED)) != 0)
			break;

		if (stop != NULL && __cursor_equals(start, stop))
			break;
		if ((ret = __wt_btcur_next(start, true)) != 0)
			break;
		start->compare = 0;		/* Exact match */
	}

	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
		goto retry;
	}

	WT_RET_NOTFOUND_OK(ret);
	return (0);
}

/*
 * __cursor_truncate_fix --
 *	Discard a cursor range from fixed-width column-store tree.
 */
static int
__cursor_truncate_fix(WT_SESSION_IMPL *session,
    WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop,
    int (*rmfunc)(WT_SESSION_IMPL *, WT_CURSOR_BTREE *, u_int))
{
	WT_DECL_RET;
	const uint8_t *value;

	/*
	 * Handle fixed-length column-store objects separately: for row-store
	 * and variable-length column-store objects we have "deleted" values
	 * and so returned objects actually exist: fixed-length column-store
	 * objects are filled-in if they don't exist, that is, if you create
	 * record 37, records 1-36 magically appear.  Those records can't be
	 * deleted, which means we have to ignore already "deleted" records.
	 *
	 * First, call the cursor search method to re-position the cursor: we
	 * may not have a cursor position (if the higher-level truncate code
	 * switched the cursors to have an "external" cursor key, and because
	 * we don't save a copy of the page's write generation information,
	 * which we need to remove records.
	 *
	 * Once that's done, we can delete records without a full search, unless
	 * we encounter a restart error because the page was modified by some
	 * other thread of control; in that case, repeat the full search to
	 * refresh the page's modification information.
	 */
retry:	WT_RET(__wt_btcur_search(start));
	WT_ASSERT(session,
	    F_MASK((WT_CURSOR *)start, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);

	for (;;) {
		value = (const uint8_t *)start->iface.value.data;
		if (*value != 0 &&
		    (ret = rmfunc(session, start, WT_UPDATE_DELETED)) != 0)
			break;

		if (stop != NULL && __cursor_equals(start, stop))
			break;
		if ((ret = __wt_btcur_next(start, true)) != 0)
			break;
		start->compare = 0;	/* Exact match */
	}

	if (ret == WT_RESTART) {
		WT_STAT_CONN_INCR(session, cursor_restart);
		WT_STAT_DATA_INCR(session, cursor_restart);
		goto retry;
	}

	WT_RET_NOTFOUND_OK(ret);
	return (0);
}

/*
 * __wt_btcur_range_truncate --
 *	Discard a cursor range from the tree.
 */
int
__wt_btcur_range_truncate(WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)start->iface.session;
	btree = start->btree;
	WT_STAT_DATA_INCR(session, cursor_truncate);

	/*
	 * For recovery, log the start and stop keys for a truncate operation,
	 * not the individual records removed.  On the other hand, for rollback
	 * we need to keep track of all the in-memory operations.
	 *
	 * We deal with this here by logging the truncate range first, then (in
	 * the logging code) disabling writing of the in-memory remove records
	 * to disk.
	 */
	if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
		WT_RET(__wt_txn_truncate_log(session, start, stop));

	switch (btree->type) {
	case BTREE_COL_FIX:
		WT_ERR(__cursor_truncate_fix(
		    session, start, stop, __cursor_col_modify));
		break;
	case BTREE_COL_VAR:
		WT_ERR(__cursor_truncate(
		    session, start, stop, __cursor_col_modify));
		break;
	case BTREE_ROW:
		/*
		 * The underlying cursor comparison routine requires cursors be
		 * fully instantiated when truncating row-store objects because
		 * it's comparing page and/or skiplist positions, not keys. (Key
		 * comparison would work, it's only that a key comparison would
		 * be relatively expensive, especially with custom collators.
		 * Column-store objects have record number keys, so the key
		 * comparison is cheap.)  The session truncate code did cursor
		 * searches when setting up the truncate so we're good to go: if
		 * that ever changes, we'd need to do something here to ensure a
		 * fully instantiated cursor.
		 */
		WT_ERR(__cursor_truncate(
		    session, start, stop, __cursor_row_modify));
		break;
	}

err:	if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED))
		__wt_txn_truncate_end(session);
	return (ret);
}

/*
 * __wt_btcur_init --
 *	Initialize a cursor used for internal purposes.
 */
void
__wt_btcur_init(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
	memset(cbt, 0, sizeof(WT_CURSOR_BTREE));

	cbt->iface.session = &session->iface;
	cbt->btree = S2BT(session);
}

/*
 * __wt_btcur_open --
 *	Open a btree cursor.
 */
void
__wt_btcur_open(WT_CURSOR_BTREE *cbt)
{
	cbt->row_key = &cbt->_row_key;
	cbt->tmp = &cbt->_tmp;

#ifdef HAVE_DIAGNOSTIC
	cbt->lastkey = &cbt->_lastkey;
	cbt->lastrecno = WT_RECNO_OOB;
#endif
}

/*
 * __wt_btcur_close --
 *	Close a btree cursor.
 */
int
__wt_btcur_close(WT_CURSOR_BTREE *cbt, bool lowlevel)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	/*
	 * The in-memory split and lookaside table code creates low-level btree
	 * cursors to search/modify leaf pages. Those cursors don't hold hazard
	 * pointers, nor are they counted in the session handle's cursor count.
	 * Skip the usual cursor tear-down in that case.
	 */
	if (!lowlevel)
		ret = __cursor_reset(cbt);

	__wt_buf_free(session, &cbt->_row_key);
	__wt_buf_free(session, &cbt->_tmp);
#ifdef HAVE_DIAGNOSTIC
	__wt_buf_free(session, &cbt->_lastkey);
#endif

	return (ret);
}
