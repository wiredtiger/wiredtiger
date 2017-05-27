/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __key_return --
 *	Change the cursor to reference an internal return key.
 */
static inline int
__key_return(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
	WT_CURSOR *cursor;
	WT_ITEM *tmp;
	WT_PAGE *page;
	WT_ROW *rip;

	page = cbt->ref->page;
	cursor = &cbt->iface;

	if (page->type == WT_PAGE_ROW_LEAF) {
		rip = &page->pg_row[cbt->slot];

		/*
		 * If the cursor references a WT_INSERT item, take its key.
		 * Else, if we have an exact match, we copied the key in the
		 * search function, take it from there.
		 * If we don't have an exact match, take the key from the
		 * original page.
		 */
		if (cbt->ins != NULL) {
			cursor->key.data = WT_INSERT_KEY(cbt->ins);
			cursor->key.size = WT_INSERT_KEY_SIZE(cbt->ins);
			return (0);
		}

		if (cbt->compare == 0) {
			/*
			 * If not in an insert list and there's an exact match,
			 * the row-store search function built the key we want
			 * to return in the cursor's temporary buffer. Swap the
			 * cursor's search-key and temporary buffers so we can
			 * return it (it's unsafe to return the temporary buffer
			 * itself because our caller might do another search in
			 * this table using the key we return, and we'd corrupt
			 * the search key during any subsequent search that used
			 * the temporary buffer.
			 */
			tmp = cbt->row_key;
			cbt->row_key = cbt->tmp;
			cbt->tmp = tmp;

			cursor->key.data = cbt->row_key->data;
			cursor->key.size = cbt->row_key->size;
			return (0);
		}
		return (__wt_row_leaf_key(
		    session, page, rip, &cursor->key, false));
	}

	/*
	 * WT_PAGE_COL_FIX, WT_PAGE_COL_VAR:
	 *	The interface cursor's record has usually been set, but that
	 * isn't universally true, specifically, cursor.search_near may call
	 * here without first setting the interface cursor.
	 */
	cursor->recno = cbt->recno;
	return (0);
}

/*
 * __value_return --
 *	Change the cursor to reference an internal original-page return value.
 */
static inline int
__value_return(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_CURSOR *cursor;
	WT_PAGE *page;
	WT_ROW *rip;
	uint8_t v;

	btree = S2BT(session);

	page = cbt->ref->page;
	cursor = &cbt->iface;

	if (page->type == WT_PAGE_ROW_LEAF) {
		rip = &page->pg_row[cbt->slot];

		/* Simple values have their location encoded in the WT_ROW. */
		if (__wt_row_leaf_value(page, rip, &cursor->value))
			return (0);

		/*
		 * Take the value from the original page cell (which may be
		 * empty).
		 */
		if ((cell =
		    __wt_row_leaf_value_cell(page, rip, NULL)) == NULL) {
			cursor->value.size = 0;
			return (0);
		}
		__wt_cell_unpack(cell, &unpack);
		return (__wt_page_cell_data_ref(
		    session, page, &unpack, &cursor->value));

	}

	if (page->type == WT_PAGE_COL_VAR) {
		/* Take the value from the original page cell. */
		cell = WT_COL_PTR(page, &page->pg_var[cbt->slot]);
		__wt_cell_unpack(cell, &unpack);
		return (__wt_page_cell_data_ref(
		    session, page, &unpack, &cursor->value));
	}

	/* WT_PAGE_COL_FIX: Take the value from the original page. */
	v = __bit_getv_recno(cbt->ref, cursor->recno, btree->bitcnt);
	return (__wt_buf_set(session, &cursor->value, &v, 1));
}

/*
 * __value_return_upd --
 *	Change the cursor to reference an internal update structure return
 * value.
 */
static inline int
__value_return_upd(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_CURSOR *cursor;
	void **listp, *list[WT_MAX_MODIFY_UPDATE + 5];

	cursor = &cbt->iface;

	/* Fast path standard updates. */
	if (upd->type == WT_UPDATE_STANDARD) {
		cursor->value.data = WT_UPDATE_DATA(upd);
		cursor->value.size = upd->size;
		return (0);
	}

	/*
	 * Find a complete update that's visible to us, tracking modifications
	 * and skipping aborted and reserved updates.
	 */
	for (listp = list; upd != NULL; upd = upd->next) {
		switch (upd->type) {
		case WT_UPDATE_STANDARD:
			/*
			 * Visibility checks aren't cheap, and standard updates
			 * should be visible to us, but we have to skip aborted
			 * updates anyway and it's less fragile to check using
			 * the standard API than roll my own test.
			 */
			if (!__wt_txn_visible(session, upd->txnid))
				continue;
			break;
		case WT_UPDATE_DELETED:
			/*
			 * We should never see a deleted record, it must have
			 * been aborted for us to get here.
			 */
			WT_ASSERT(session,
			    !__wt_txn_visible(session, upd->txnid));
			continue;
		case WT_UPDATE_MODIFIED:
			*listp++ = WT_UPDATE_DATA(upd);
			continue;
		case WT_UPDATE_RESERVED:
			continue;
		}
		break;
	}

	/*
	 * If we hit the end of the chain, roll forward from the update item we
	 * found, otherwise, from the original page's value.
	 */
	if (upd == NULL) {
		WT_RET(__value_return(session, cbt));
		WT_RET(__cursor_localvalue(cursor));
	} else
		WT_RET(__wt_buf_set(session,
		    &cursor->value, WT_UPDATE_DATA(upd), upd->size));

	while (listp > list)
		WT_RET(__wt_modify_apply(session, &cursor->value, *--listp));
	return (0);
}

/*
 * __wt_key_return --
 *	Change the cursor to reference an internal return key.
 */
int
__wt_key_return(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
	WT_CURSOR *cursor;

	cursor = &cbt->iface;

	/*
	 * We may already have an internal key and the cursor may not be set up
	 * to get another copy, so we have to leave it alone. Consider a cursor
	 * search followed by an update: the update doesn't repeat the search,
	 * it simply updates the currently referenced key's value. We will end
	 * up here with the correct internal key, but we can't "return" the key
	 * again even if we wanted to do the additional work, the cursor isn't
	 * set up for that because we didn't just complete a search.
	 */
	F_CLR(cursor, WT_CURSTD_KEY_EXT);
	if (!F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
		WT_RET(__key_return(session, cbt));
		F_SET(cursor, WT_CURSTD_KEY_INT);
	}
	return (0);
}

/*
 * __wt_value_return --
 *	Change the cursor to reference an internal return value.
 */
int
__wt_value_return(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_CURSOR *cursor;

	cursor = &cbt->iface;

	F_CLR(cursor, WT_CURSTD_VALUE_EXT);
	if (upd == NULL)
		WT_RET(__value_return(session, cbt));
	else
		WT_RET(__value_return_upd(session, cbt, upd));
	F_SET(cursor, WT_CURSTD_VALUE_INT);
	return (0);
}
