/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int  __inmem_col_fix(WT_SESSION_IMPL *, WT_PAGE *);
static int  __inmem_col_int(WT_SESSION_IMPL *, WT_PAGE *, size_t *);
static int  __inmem_col_var(WT_SESSION_IMPL *, WT_PAGE *, size_t *);
static int  __inmem_row_int(WT_SESSION_IMPL *, WT_PAGE *, size_t *);
static int  __inmem_row_leaf(WT_SESSION_IMPL *, WT_PAGE *, size_t *);

/*
 * __wt_page_in --
 *	Acquire a hazard reference to a page; if the page is not in-memory,
 *	read it from the disk and build an in-memory version.
 */
int
__wt_page_in_func(
    WT_SESSION_IMPL *session, WT_PAGE *parent, WT_REF *ref
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	WT_DECL_RET;
	WT_PAGE *page;
	int busy, read_lockout, wake;

	/*
	 * Only wake the eviction server the first time through here (if the
	 * cache is too full), or after we fail to evict a page.  Otherwise, we
	 * are just wasting effort and making a busy mutex busier.
	 */
	wake = 1;

	for (;;) {
		switch (ref->state) {
		case WT_REF_DISK:
		case WT_REF_DELETED:
			/* The page isn't in memory, attempt to read it. */

			/* Check if there is space in the cache. */
			__wt_eviction_check(session, &read_lockout, wake);
			wake = 0;

			/*
			 * If the cache is full, give up, but only if we are
			 * not holding the schema lock.  The schema lock can
			 * block checkpoints, and thus eviction, so it is not
			 * safe to wait for eviction if we are holding it.
			 */
			if (read_lockout &&
			   !F_ISSET(session, WT_SESSION_SCHEMA_LOCKED))
				break;

			WT_RET(__wt_cache_read(session, parent, ref));
			continue;
		case WT_REF_EVICT_FORCE:
		case WT_REF_LOCKED:
		case WT_REF_READING:
			/*
			 * The page is being read or considered for eviction --
			 * wait for that to be resolved.
			 */
			break;
		case WT_REF_EVICT_WALK:
		case WT_REF_MEM:
			/*
			 * The page is in memory: get a hazard reference, update
			 * the page's LRU and return.  The expected reason we
			 * can't get a hazard reference is because the page is
			 * being evicted; yield and try again.
			 */
#ifdef HAVE_DIAGNOSTIC
			WT_RET(
			    __wt_hazard_set(session, ref, &busy, file, line));
#else
			WT_RET(__wt_hazard_set(session, ref, &busy));
#endif
			if (busy)
				break;

			page = ref->page;
			WT_ASSERT(session, !WT_PAGE_IS_ROOT(page));

			/*
			 * Ensure the page doesn't have ancient updates on it.
			 * If it did, reading the page could ignore committed
			 * updates.  This should be extremely unlikely in real
			 * applications, force eviction of the page to avoid
			 * the issue.
			 */
			if (page->modify != NULL &&
			    __wt_txn_ancient(session, page->modify->first_id)) {
				__wt_evict_page_request(session, page);
				__wt_hazard_clear(session, page);
				__wt_evict_server_wake(session);
				break;
			}

			/* Check if we need an autocommit transaction. */
			if ((ret = __wt_txn_autocommit_check(session)) != 0) {
				__wt_hazard_clear(session, page);
				return (ret);
			}

			page->read_gen = __wt_cache_read_gen(session);
			return (0);
		WT_ILLEGAL_VALUE(session);
		}

		/* Find a page to evict -- if the page is busy, keep trying. */
		if ((ret = __wt_evict_lru_page(session, 1)) == EBUSY)
			__wt_yield();
		else if (ret == WT_NOTFOUND)
			wake = 1;
		else
			WT_RET(ret);
	}
}

/*
 * __wt_page_inmem --
 *	Build in-memory page information.
 */
int
__wt_page_inmem(WT_SESSION_IMPL *session,
    WT_PAGE *parent, WT_REF *parent_ref, WT_PAGE_HEADER *dsk, WT_PAGE **pagep)
{
	WT_DECL_RET;
	WT_PAGE *page;
	size_t inmem_size;

	WT_ASSERT_RET(session, dsk->u.entries > 0);

	*pagep = NULL;

	/*
	 * Allocate and initialize the WT_PAGE.
	 * Set the LRU so the page is not immediately selected for eviction.
	 * Set the read generation (which can't match a search where the write
	 * generation wasn't set, that is, remained 0).
	 */
	WT_RET(__wt_calloc_def(session, 1, &page));
	page->parent = parent;
	page->ref = parent_ref;
	page->dsk = dsk;
	page->read_gen = __wt_cache_read_gen(session);
	page->type = dsk->type;

	inmem_size = 0;
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		page->u.col_fix.recno = dsk->recno;
		WT_ERR(__inmem_col_fix(session, page));
		break;
	case WT_PAGE_COL_INT:
		page->u.intl.recno = dsk->recno;
		WT_ERR(__inmem_col_int(session, page, &inmem_size));
		break;
	case WT_PAGE_COL_VAR:
		page->u.col_var.recno = dsk->recno;
		WT_ERR(__inmem_col_var(session, page, &inmem_size));
		break;
	case WT_PAGE_ROW_INT:
		WT_ERR(__inmem_row_int(session, page, &inmem_size));
		break;
	case WT_PAGE_ROW_LEAF:
		WT_ERR(__inmem_row_leaf(session, page, &inmem_size));
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}

	__wt_cache_page_read(
	    session, page, sizeof(WT_PAGE) + dsk->size + inmem_size);

	*pagep = page;
	return (0);

err:	/*
	 * Our caller (specifically salvage) may have special concerns about the
	 * underlying disk image, the caller owns that problem.
	 */
	__wt_page_out(session, &page, WT_PAGE_FREE_IGNORE_DISK);
	return (ret);
}

/*
 * __inmem_cell_addr_del --
 *	WT_CELL_ADDR_DEL support.
 */
static int
__inmem_cell_addr_del(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BTREE *btree;

	btree = session->btree;

	/*
	 * A cell may reference a deleted address, which happens in two ways.
	 *
	 * First, a cell on a row-store internal page may reference a deleted
	 * leaf page: if a leaf page was deleted without first being read, and
	 * the deletion committed, but some transaction in the system required
	 * the previous version of the page to be available, a special address
	 * deleted cell is written.  If we crash and recover to a page with a
	 * deleted-address cell, we now want to delete the leaf page (because
	 * it was never deleted, but by definition no earlier transaction might
	 * need it).  Second, a cell in a variable-length column-store or row-
	 * store page may reference a deleted overflow chunk: when a key
	 * references an overflow item, the overflow value is updated and the
	 * update commits, we want to discard the overflow chunk.  Again, if
	 * some transaction in the system required the original version of the
	 * overflow value to be available, a special deleted-address type cell
	 * is written.
	 *
	 * In both cases, if we crash and recover to a version of the page with
	 * address-deleted cells, it's time to delete the leaf page or overflow
	 * chunk (we crashed, so by definition no earlier transaction can need
	 * the original page/chunk).
	 *
	 * Should we find any WT_CELL_ADDR_DEL cells, we give the page a modify
	 * structure and set the transaction ID for the first update to the
	 * page (WT_TXN_NONE because the transaction is committed and visible).
	 *
	 * If the tree is already dirty and so will be written, mark the page
	 * dirty.  (We'd like to free the referenced pages right here, but if
	 * the handle is read-only and/or the application never modifies the
	 * tree, we're not able to do so).
	 */
	WT_RET(__wt_page_modify_init(session, page));
	page->modify->first_id = WT_TXN_NONE;
	if (btree->modified)
		__wt_page_modify_set(page);
	return (0);
}

/*
 * __inmem_col_fix --
 *	Build in-memory index for fixed-length column-store leaf pages.
 */
static int
__inmem_col_fix(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BTREE *btree;
	WT_PAGE_HEADER *dsk;

	btree = session->btree;
	dsk = page->dsk;

	page->u.col_fix.bitf = WT_PAGE_HEADER_BYTE(btree, dsk);
	page->entries = dsk->u.entries;
	return (0);
}

/*
 * __inmem_col_int --
 *	Build in-memory index for column-store internal pages.
 */
static int
__inmem_col_int(WT_SESSION_IMPL *session, WT_PAGE *page, size_t *inmem_sizep)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_PAGE_HEADER *dsk;
	WT_REF *ref;
	uint32_t i;

	btree = session->btree;
	dsk = page->dsk;
	unpack = &_unpack;

	/*
	 * Column-store page entries map one-to-one to the number of physical
	 * entries on the page (each physical entry is a offset object).
	 */
	WT_RET(__wt_calloc_def(
	    session, (size_t)dsk->u.entries, &page->u.intl.t));
	if (inmem_sizep != NULL)
		*inmem_sizep += dsk->u.entries * sizeof(*page->u.intl.t);

	/*
	 * Walk the page, building references: the page contains value items.
	 * The value items are on-page items (WT_CELL_VALUE).
	 */
	ref = page->u.intl.t;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);
		ref->addr = cell;
		ref->u.recno = unpack->v;
		++ref;
	}

	page->entries = dsk->u.entries;
	return (0);
}

/*
 * __inmem_col_var --
 *	Build in-memory index for variable-length, data-only leaf pages in
 *	column-store trees.
 */
static int
__inmem_col_var(WT_SESSION_IMPL *session, WT_PAGE *page, size_t *inmem_sizep)
{
	WT_BTREE *btree;
	WT_COL *cip;
	WT_COL_RLE *repeats;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_PAGE_HEADER *dsk;
	uint64_t recno, rle;
	size_t bytes_allocated;
	uint32_t i, nindx, max_repeats, nrepeats;
	int addrdel;

	btree = session->btree;
	dsk = page->dsk;
	unpack = &_unpack;
	repeats = NULL;
	bytes_allocated = max_repeats = nrepeats = 0;
	recno = page->u.col_var.recno;

	/*
	 * Column-store page entries normally map one-to-one to the number of
	 * physical entries on the page (each physical entry is a data item).
	 * There might be address-deleted cells on the page, but they're so
	 * rare it's not worth doing any kind of correction for that case.
	 */
	WT_RET(__wt_calloc_def(
	    session, (size_t)dsk->u.entries, &page->u.col_var.d));
	if (inmem_sizep != NULL)
		*inmem_sizep += dsk->u.entries * sizeof(*page->u.col_var.d);

	/*
	 * Walk the page, building references: the page contains unsorted value
	 * items.  The value items are on-page (WT_CELL_VALUE), overflow
	 * (WT_CELL_VALUE_OVFL), deleted (WT_CELL_DEL) or address-deleted
	 * (WT_CELL_ADDR_DEL) items.
	 */
	cip = page->u.col_var.d;
	nindx = 0;
	addrdel = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);

		/* Ignore address-deleted cells. */
		if (unpack->raw == WT_CELL_ADDR_DEL) {
			addrdel = 1;
			continue;
		}

		(cip++)->__value = WT_PAGE_DISK_OFFSET(page, cell);

		/*
		 * Add records with repeat counts greater than 1 to an array we
		 * use for fast lookups.
		 */
		rle = __wt_cell_rle(unpack);
		if (rle > 1) {
			if (nrepeats == max_repeats) {
				max_repeats = (max_repeats == 0) ?
				    10 : 2 * max_repeats;
				WT_RET(__wt_realloc(session, &bytes_allocated,
				    max_repeats * sizeof(WT_COL_RLE),
				    &repeats));
			}
			repeats[nrepeats].indx = nindx;
			repeats[nrepeats].recno = recno;
			repeats[nrepeats++].rle = rle;
		}
		nindx++;
		recno += rle;
	}

	/* Found an address-deleted cell. */
	if (addrdel) {
		WT_RET(__inmem_cell_addr_del(session, page));
		F_SET(page->modify, WT_PM_ADDR_DEL);
	}

	page->u.col_var.repeats = repeats;
	page->u.col_var.nrepeats = nrepeats;
	page->entries = nindx;
	if (inmem_sizep != NULL)
		*inmem_sizep += bytes_allocated;
	return (0);
}

/*
 * __inmem_row_int --
 *	Build in-memory index for row-store internal pages.
 */
static int
__inmem_row_int(WT_SESSION_IMPL *session, WT_PAGE *page, size_t *inmem_sizep)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_ITEM(current);
	WT_DECL_ITEM(last);
	WT_DECL_RET;
	WT_ITEM *tmp;
	WT_PAGE_HEADER *dsk;
	WT_REF *ref;
	uint32_t i, nindx, prefix;
	int addrdel;
	void *huffman;

	btree = session->btree;
	unpack = &_unpack;
	dsk = page->dsk;
	huffman = btree->huffman_key;

	WT_ERR(__wt_scr_alloc(session, 0, &current));
	WT_ERR(__wt_scr_alloc(session, 0, &last));

	/*
	 * Internal row-store page entries map one-to-two to the number of
	 * physical entries on the page (each in-memory entry is a key item
	 * and location cookie).
	 */
	nindx = dsk->u.entries / 2;
	WT_ERR((__wt_calloc_def(session, (size_t)nindx, &page->u.intl.t)));
	if (inmem_sizep != NULL)
		*inmem_sizep += nindx * sizeof(*page->u.intl.t);

	/*
	 * Set the number of elements now -- we're about to allocate memory,
	 * and if we fail in the middle of the page, we want to discard that
	 * memory properly.
	 */
	page->entries = nindx;

	/*
	 * Walk the page, instantiating keys: the page contains sorted key and
	 * location cookie pairs.  Keys are on-page/overflow items and location
	 * cookies are WT_CELL_ADDR or WT_CELL_ADDR_DEL items.
	 */
	ref = page->u.intl.t;
	addrdel = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
			break;
		case WT_CELL_ADDR:
			/*
			 * An address-deleted cell, re-create the deleted node.
			 *
			 * It's tempting to change this in a couple of ways:
			 * first, we could change reconciliation to write only a
			 * single address-deleted cell instead of a key/value
			 * pair (where the value is an address-deleted cell).
			 * Reconciliation makes that difficult: dealing with
			 * split key promotions and prefix compression won't
			 * be pleasant.  Second, it's tempting to skip over the
			 * address-deleted cells here, letting reconciliation
			 * delete the same way we delete overflow chunks (that
			 * would give us more compact trees, too, because an
			 * insert wouldn't instantiate an entire page, it would
			 * simply insert into the appropriate leaf page.  The
			 * problem with that change is a large truncation will
			 * result in empty internal pages, which the rest of the
			 * system isn't prepared to deal with.
			 */
			if (unpack->raw == WT_CELL_ADDR_DEL) {
				addrdel = 1;
				ref->state = WT_REF_DELETED;
				ref->txnid = WT_TXN_NONE;
			}

			ref->addr = cell;
			++ref;
			continue;
		WT_ILLEGAL_VALUE_ERR(session);
		}

		/*
		 * If Huffman decoding is required or it's an overflow record,
		 * use the heavy-weight __wt_cell_unpack_copy() call to build
		 * the key.  Else, we can do it faster internally as we don't
		 * have to shuffle memory around as much.
		 */
		prefix = unpack->prefix;
		if (huffman != NULL || unpack->ovfl) {
			WT_ERR(__wt_cell_unpack_copy(session, unpack, current));

			/*
			 * If there's a prefix, make sure there's enough buffer
			 * space, then shift the decoded data past the prefix
			 * and copy the prefix into place.
			 */
			if (prefix != 0) {
				WT_ERR(__wt_buf_grow(
				    session, current, prefix + current->size));
				memmove((uint8_t *)current->data +
				    prefix, current->data, current->size);
				memcpy(
				    (void *)current->data, last->data, prefix);
				current->size += prefix;
			}
		} else {
			/*
			 * Get the cell's data/length and make sure we have
			 * enough buffer space.
			 */
			WT_ERR(__wt_buf_grow(
			    session, current, prefix + unpack->size));

			/* Copy the prefix then the data into place. */
			if (prefix != 0)
				memcpy((void *)
				    current->data, last->data, prefix);
			memcpy((uint8_t *)
			    current->data + prefix, unpack->data, unpack->size);
			current->size = prefix + unpack->size;
		}

		/*
		 * Allocate and initialize the instantiated key.
		 */
		WT_ERR(__wt_row_ikey_alloc(session,
		    WT_PAGE_DISK_OFFSET(page, cell),
		    current->data, current->size, &ref->u.key));
		if (inmem_sizep != NULL)
			*inmem_sizep += sizeof(WT_IKEY) + current->size;

		/*
		 * Swap buffers if it's not an overflow key, we have a new
		 * prefix-compressed key.
		 */
		if (!unpack->ovfl) {
			tmp = last;
			last = current;
			current = tmp;
		}
	}

	/* Found an address-deleted cell. */
	if (addrdel)
		WT_ERR(__inmem_cell_addr_del(session, page));

err:	__wt_scr_free(&current);
	__wt_scr_free(&last);
	return (ret);
}

/*
 * __inmem_row_leaf --
 *	Build in-memory index for row-store leaf pages.
 */
static int
__inmem_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page, size_t *inmem_sizep)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_PAGE_HEADER *dsk;
	WT_ROW *rip;
	uint32_t i, nindx;
	int addrdel;

	btree = session->btree;
	dsk = page->dsk;
	unpack = &_unpack;

	/*
	 * Leaf row-store page entries map to a maximum of two-to-one to the
	 * number of physical entries on the page (each physical entry might be
	 * a key without a subsequent data item).  To avoid over-allocation in
	 * workloads with large numbers of empty data items, first walk the page
	 * counting the number of keys, then allocate the indices.
	 *
	 * The page contains key/data pairs.  Keys are on-page (WT_CELL_KEY) or
	 * overflow (WT_CELL_KEY_OVFL) items, data are either a single on-page
	 * (WT_CELL_VALUE), overflow (WT_CELL_VALUE_OVFL) or address-deleted
	 * (WT_CELL_ADDR_DEL) items.
	 */
	nindx = 0;
	addrdel = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
			++nindx;
			break;
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
			break;
		case WT_CELL_ADDR:
			/*
			 * Ignore address-deleted cells.
			 * Sanity: the raw type better be address-deleted.
			 */
			if (unpack->raw == WT_CELL_ADDR_DEL) {
				addrdel = 1;
				break;
			}
			/* FALLTHROUGH */
		WT_ILLEGAL_VALUE(session);
		}
	}

	/* Found an address-deleted cell. */
	if (addrdel) {
		WT_RET(__inmem_cell_addr_del(session, page));

		F_SET(page->modify, WT_PM_ADDR_DEL);
	}

	WT_RET((__wt_calloc_def(session, (size_t)nindx, &page->u.row.d)));
	if (inmem_sizep != NULL)
		*inmem_sizep += nindx * sizeof(*page->u.row.d);

	/* Walk the page again, building indices. */
	rip = page->u.row.d;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
			WT_ROW_KEY_SET(rip, cell);
			++rip;
			break;
		case WT_CELL_ADDR:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
			break;
		WT_ILLEGAL_VALUE(session);
		}
	}

	page->entries = nindx;

	/*
	 * If the keys are Huffman encoded, instantiate some set of them.  It
	 * doesn't matter if we are randomly searching the page or scanning a
	 * cursor through it, there isn't a fast-path to getting keys off the
	 * page.
	 */
	return (btree->huffman_key == NULL ?
	    0 : __wt_row_leaf_keys(session, page));
}
