/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_rec_bulk_init --
 *	Bulk insert initialization.
 *	TODO: This doesn't belong here.
 */
int
__wt_rec_bulk_init(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_PAGE_INDEX *pindex;
	WT_RECONCILE *r;
	uint64_t recno;

	btree = S2BT(session);
	/*
	 * Bulk-load is only permitted on newly created files, not any empty
	 * file -- see the checkpoint code for a discussion.
	 */
	if (!btree->bulk_load_ok)
		WT_RET_MSG(session, EINVAL,
		    "bulk-load is only possible for newly created trees");

	/*
	 * Get a reference to the empty leaf page; we have exclusive access so
	 * we can take a copy of the page, confident the parent won't split.
	 */
	pindex = WT_INTL_INDEX_GET_SAFE(btree->root.page);
	cbulk->ref = pindex->index[0];
	cbulk->leaf = cbulk->ref->page;

	WT_RET(__wt_rec_write_init(
	    session, cbulk->ref, 0, NULL, &cbulk->reconcile));
	r = cbulk->reconcile;
	r->is_bulk_load = true;

	switch (btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		recno = 1;
		break;
	case BTREE_ROW:
		recno = WT_RECNO_OOB;
		break;
	WT_ILLEGAL_VALUE(session);
	}

	return (__wt_rec_split_init(
	    session, r, cbulk->leaf, recno, btree->maxleafpage));
}

/*
 * __wt_rec_bulk_wrapup --
 *	Bulk insert cleanup.
 */
int
__wt_rec_bulk_wrapup(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_PAGE *parent;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);

	switch (btree->type) {
	case BTREE_COL_FIX:
		if (cbulk->entry != 0)
			__wt_rec_incr(session, r, cbulk->entry,
			    __bitstr_size(
			    (size_t)cbulk->entry * btree->bitcnt));
		break;
	case BTREE_COL_VAR:
		if (cbulk->rle != 0)
			WT_RET(__wt_rec_bulk_insert_var(session, cbulk));
		break;
	case BTREE_ROW:
		break;
	WT_ILLEGAL_VALUE(session);
	}

	WT_RET(__wt_rec_split_finish(session, r));
	WT_RET(__wt_rec_write_wrapup(session, r, r->page));
	WT_RET(__wt_rec_write_status(session, r, r->page));

	/* Mark the page's parent and the tree dirty. */
	parent = r->ref->home;
	WT_RET(__wt_page_modify_init(session, parent));
	__wt_page_modify_set(session, parent);

	__wt_rec_destroy(session, &cbulk->reconcile);

	return (0);
}

/*
 * __rec_bulk_col_fix_insert_split_check --
 *	Check if a bulk-loaded fixed-length column store page needs to split.
 */
static inline int
__rec_bulk_col_fix_insert_split_check(WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_RECONCILE *r;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbulk->cbt.iface.session;
	r = cbulk->reconcile;
	btree = S2BT(session);

	if (cbulk->entry == cbulk->nrecs) {
		if (cbulk->entry != 0) {
			/*
			 * If everything didn't fit, update the counters and
			 * split.
			 *
			 * Boundary: split or write the page.
			 */
			__wt_rec_incr(session, r, cbulk->entry,
			    __bitstr_size(
			    (size_t)cbulk->entry * btree->bitcnt));
			WT_RET(__wt_rec_split(session, r, 0));
		}
		cbulk->entry = 0;
		cbulk->nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
	}
	return (0);
}

/*
 * __wt_rec_bulk_insert_row --
 *	Row-store bulk insert.
 */
int
__wt_rec_bulk_insert_row(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_KV *key, *val;
	WT_RECONCILE *r;
	bool ovfl_key;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	key = &r->k;
	val = &r->v;
	WT_RET(__wt_rec_cell_build_leaf_key(session, r,	/* Build key cell */
	    cursor->key.data, cursor->key.size, &ovfl_key));
	/* Build value cell */
	WT_RET(__wt_rec_cell_build_val(session, r,
	    cursor->value.data, cursor->value.size, (uint64_t)0));

	/* Boundary: split or write the page. */
	if (key->len + val->len > r->space_avail) {
		if (r->raw_compression)
			WT_RET(__wt_rec_split_raw(
			    session, r, key->len + val->len));
		else {
			/*
			 * Turn off prefix compression until a full key written
			 * to the new page, and (unless already working with an
			 * overflow key), rebuild the key without compression.
			 */
			if (r->key_pfx_compress_conf) {
				r->key_pfx_compress = false;
				if (!ovfl_key)
					WT_RET(__wt_rec_cell_build_leaf_key(
					    session, r, NULL, 0, &ovfl_key));
			}

			WT_RET(__wt_rec_split(session, r, key->len + val->len));
		}
	}

	/* Copy the key/value pair onto the page. */
	__wt_rec_copy_incr(session, r, key);
	if (val->len == 0)
		r->any_empty_value = true;
	else {
		r->all_empty_value = false;
		if (btree->dictionary)
			WT_RET(__wt_rec_dictionary_replace(session, r, 0, val));
		__wt_rec_copy_incr(session, r, val);
	}

	/* Update compression state. */
	__wt_rec_row_key_state_update(r, ovfl_key);

	return (0);
}

/*
 * __wt_rec_bulk_insert_fix --
 *	Fixed-length column-store bulk insert.
 */
int
__wt_rec_bulk_insert_fix(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_RECONCILE *r;
	uint32_t entries, offset, page_entries, page_size;
	const uint8_t *data;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	if (cbulk->bitmap) {
		if (((r->recno - 1) * btree->bitcnt) & 0x7)
			WT_RET_MSG(session, EINVAL,
			    "Bulk bitmap load not aligned on a byte boundary");
		for (data = cursor->value.data,
		    entries = (uint32_t)cursor->value.size;
		    entries > 0;
		    entries -= page_entries, data += page_size) {
			WT_RET(__rec_bulk_col_fix_insert_split_check(cbulk));

			page_entries =
			    WT_MIN(entries, cbulk->nrecs - cbulk->entry);
			page_size = __bitstr_size(page_entries * btree->bitcnt);
			offset = __bitstr_size(cbulk->entry * btree->bitcnt);
			memcpy(r->first_free + offset, data, page_size);
			cbulk->entry += page_entries;
			r->recno += page_entries;
		}
		return (0);
	}

	WT_RET(__rec_bulk_col_fix_insert_split_check(cbulk));

	__bit_setv(r->first_free,
	    cbulk->entry, btree->bitcnt, ((uint8_t *)cursor->value.data)[0]);
	++cbulk->entry;
	++r->recno;

	return (0);
}

/*
 * __wt_rec_bulk_insert_var --
 *	Variable-length column-store bulk insert.
 */
int
__wt_rec_bulk_insert_var(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_KV *val;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);

	/*
	 * Store the bulk cursor's last buffer, not the current value, we're
	 * creating a duplicate count, which means we want the previous value
	 * seen, not the current value.
	 */
	val = &r->v;
	WT_RET(__wt_rec_cell_build_val(
	    session, r, cbulk->last.data, cbulk->last.size, cbulk->rle));

	/* Boundary: split or write the page. */
	if (val->len > r->space_avail)
		WT_RET(r->raw_compression ?
		    __wt_rec_split_raw(session, r, val->len) :
		    __wt_rec_split(session, r, val->len));

	/* Copy the value onto the page. */
	if (btree->dictionary)
		WT_RET(
		    __wt_rec_dictionary_replace(session, r, cbulk->rle, val));
	__wt_rec_copy_incr(session, r, val);

	/* Update the starting record number in case we split. */
	r->recno += cbulk->rle;

	return (0);
}

