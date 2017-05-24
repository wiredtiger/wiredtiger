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
static int
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
static int
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
 * __value_modify_apply_one --
 *	Apply a single modify structure change to the buffer.
 */
static int
__value_modify_apply_one(WT_SESSION_IMPL *session, WT_ITEM *value,
    const uint8_t *data, size_t data_size, size_t offset, size_t size)
{
	uint8_t *p, *t;

	/*
	 * Fast-path the expected case, where we're overwriting a set of bytes
	 * that already exist in the buffer.
	 */
	if (value->size > offset + data_size && data_size == size) {
		memmove((uint8_t *)value->data + offset, data, data_size);
		return (0);
	}

	/*
	 * Grow the buffer to the maximum size we'll need. This is pessimistic
	 * because it ignores replacement bytes, but it's a simpler calculation.
	 */
	WT_RET(__wt_buf_grow(
	    session, value, WT_MAX(value->size, offset) + data_size));

	/*
	 * If appending bytes past the end of the value, initialize gap bytes
	 * and copy the new bytes into place.
	 */
	if (value->size <= offset) {
		if (value->size < offset)
			memset((uint8_t *)value->data +
			    value->size, '\0', offset - value->size);
		memmove((uint8_t *)value->data + offset, data, data_size);
		value->size = offset + data_size;
		return (0);
	}

	/*
	 * Correct the size if it's nonsense, we can't replace more bytes than
	 * remain in the value.
	 */
	if (value->size < offset + size)
		size = value->size - offset;

	if (data_size == size) {			/* Overwrite */
		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

		/*
		 * The new data must overlap the buffer's end (else, we'd use
		 * the fast-path code above). Grow the buffer size to include
		 * the new data.
		 */
		value->size = offset + data_size;
	} else {					/* Shrink or grow */
		/* Shift the current data into its new location. */
		p = (uint8_t *)value->data + offset + size;
		t = (uint8_t *)value->data + offset + data_size;
		memmove(t, p, value->size - (offset + size));

		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

		/* Fix the size. */
		if (data_size > size)
			value->size += (data_size - size);
		else
			value->size -= (size - data_size);
	}

	return (0);
}

/*
 * __wt_value_modify_apply --
 *	Apply a single update structure's WT_MODIFY changes to the buffer.
 */
int
__wt_value_modify_apply(
    WT_SESSION_IMPL *session, WT_ITEM *value, const void *modify)
{
	const size_t *p;
	int nentries;
	const uint8_t *data;

	/*
	 * Get the number of entries, and set a second pointer to reference the
	 * change data.
	 */
	p = modify;
	nentries = (int)*p++;
	data = (uint8_t *)modify +
	    sizeof(size_t) + ((u_int)nentries * 3 * sizeof(size_t));

	/* Step through the list of entries, applying them in order. */
	for (; nentries-- > 0; data += p[0], p += 3)
		WT_RET(__value_modify_apply_one(
		    session, value, data, p[0], p[1], p[2]));

	return (0);
}

/*
 * __value_return_upd --
 *	Change the cursor to reference an internal update structure return
 * value.
 */
static int
__value_return_upd(
    WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_CURSOR *cursor;
	void **listp, *list[100];			/* XXX KEITH: 100 */

	cursor = &cbt->iface;

	/* Fast path standard updates. */
	if (upd->type == WT_UPDATE_STANDARD) {
		cursor->value.data = WT_UPDATE_DATA(upd);
		cursor->value.size = upd->size;
		return (0);
	}

	/*
	 * Find a complete update, skipping reserved updates and tracking other
	 * modifications (we shouldn't ever see a deleted update).
	 *
	 * XXX KEITH: is there any way we can be here with an update that
	 * isn't visible to us?
	 */
	for (listp = list; upd != NULL; upd = upd->next) {
		if (upd->type == WT_UPDATE_STANDARD)
			break;

		WT_ASSERT(session, upd->type != WT_UPDATE_DELETED);

		if (upd->type == WT_UPDATE_MODIFIED)
			*listp++ = WT_UPDATE_DATA(upd);
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
		WT_RET(__wt_value_modify_apply(
		    session, &cursor->value, *--listp));
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

/*
 * __wt_kv_return --
 *	Return a page referenced key/value pair to the application.
 */
int
__wt_kv_return(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd)
{
	WT_RET(__wt_key_return(session, cbt));
	WT_RET(__wt_value_return(session, cbt, upd));

	return (0);
}
