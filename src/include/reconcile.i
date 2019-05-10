/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	WT_CROSSING_MIN_BND(r, next_len)				\
	((r)->cur_ptr->min_offset == 0 &&				\
	    (next_len) > (r)->min_space_avail)
#define	WT_CROSSING_SPLIT_BND(r, next_len) ((next_len) > (r)->space_avail)
#define	WT_CHECK_CROSSING_BND(r, next_len)				\
	(WT_CROSSING_MIN_BND(r, next_len) || WT_CROSSING_SPLIT_BND(r, next_len))

/*
 * __wt_rec_need_split --
 *	Check whether adding some bytes to the page requires a split.
 */
static inline bool
__wt_rec_need_split(WT_RECONCILE *r, size_t len)
{
	/*
	 * In the case of a row-store leaf page, trigger a split if a threshold
	 * number of saved updates is reached. This allows pages to split for
	 * update/restore and lookaside eviction when there is no visible data
	 * causing the disk image to grow.
	 *
	 * In the case of small pages or large keys, we might try to split when
	 * a page has no updates or entries, which isn't possible. To consider
	 * update/restore or lookaside information, require either page entries
	 * or updates that will be attached to the image. The limit is one of
	 * either, but it doesn't make sense to create pages or images with few
	 * entries or updates, even where page sizes are small (especially as
	 * updates that will eventually become overflow items can throw off our
	 * calculations). Bound the combination at something reasonable.
	 */
	if (r->page->type == WT_PAGE_ROW_LEAF && r->entries + r->supd_next > 10)
		len += r->supd_memsize;

	/* Check for the disk image crossing a boundary. */
	return (WT_CHECK_CROSSING_BND(r, len));
}

/*
 * __wt_rec_addr_ts_init --
 *	Initialize an address timestamp triplet.
 */
static inline void
__wt_rec_addr_ts_init(WT_RECONCILE *r, wt_timestamp_t *newest_durable_ts,
    wt_timestamp_t *oldest_start_tsp, uint64_t *oldest_start_txnp,
    wt_timestamp_t *newest_stop_tsp, uint64_t *newest_stop_txnp)
{
	/*
	 * If the page format supports address timestamps (and not fixed-length
	 * column-store, where we don't maintain timestamps at all), set the
	 * oldest/newest timestamps to values at the end of their expected range
	 * so they're corrected as we process key/value items. Otherwise, set
	 * the oldest/newest timestamps to simple durability.
	 */
	*newest_durable_ts = WT_TS_NONE;
	*oldest_start_tsp = WT_TS_MAX;
	*oldest_start_txnp = WT_TXN_MAX;
	*newest_stop_tsp = WT_TS_NONE;
	*newest_stop_txnp = WT_TXN_NONE;
	if (!__wt_process.page_version_ts || r->page->type == WT_PAGE_COL_FIX) {
		*newest_durable_ts = WT_TS_NONE;
		*oldest_start_tsp = WT_TS_NONE;
		*oldest_start_txnp = WT_TXN_NONE;
		*newest_stop_tsp = WT_TS_MAX;
		*newest_stop_txnp = WT_TXN_MAX;
	}
}

/*
 * __wt_rec_addr_ts_update --
 *	Update the chunk's timestamp information.
 */
static inline void
__wt_rec_addr_ts_update(WT_RECONCILE *r, wt_timestamp_t newest_durable_ts,
    wt_timestamp_t oldest_start_ts, uint64_t oldest_start_txn,
    wt_timestamp_t newest_stop_ts, uint64_t newest_stop_txn)
{
	r->cur_ptr->newest_durable_ts =
	    WT_MAX(newest_durable_ts, r->cur_ptr->newest_durable_ts);
	r->cur_ptr->oldest_start_ts =
	    WT_MIN(oldest_start_ts, r->cur_ptr->oldest_start_ts);
	r->cur_ptr->oldest_start_txn =
	    WT_MIN(oldest_start_txn, r->cur_ptr->oldest_start_txn);
	r->cur_ptr->newest_stop_ts =
	    WT_MAX(newest_stop_ts, r->cur_ptr->newest_stop_ts);
	r->cur_ptr->newest_stop_txn =
	    WT_MAX(newest_stop_txn, r->cur_ptr->newest_stop_txn);
}

/*
 * __wt_rec_incr --
 *	Update the memory tracking structure for a set of new entries.
 */
static inline void
__wt_rec_incr(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v, size_t size)
{
	/*
	 * The buffer code is fragile and prone to off-by-one errors -- check
	 * for overflow in diagnostic mode.
	 */
	WT_ASSERT(session, r->space_avail >= size);
	WT_ASSERT(session, WT_BLOCK_FITS(r->first_free, size,
	    r->cur_ptr->image.mem, r->cur_ptr->image.memsize));

	r->entries += v;
	r->space_avail -= size;
	r->first_free += size;

	/*
	 * If offset for the minimum split size boundary is not set, we have not
	 * yet reached the minimum boundary, reduce the space available for it.
	 */
	if (r->cur_ptr->min_offset == 0) {
		if (r->min_space_avail >= size)
			r->min_space_avail -= size;
		else
			r->min_space_avail = 0;
	}
}

/*
 * __wt_rec_image_copy --
 *	Copy a key/value cell and buffer pair into the new image.
 */
static inline void
__wt_rec_image_copy(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REC_KV *kv)
{
	size_t len;
	uint8_t *p, *t;

	/*
	 * If there's only one chunk of data to copy (because the cell and data
	 * are being copied from the original disk page), the cell length won't
	 * be set, the WT_ITEM data/length will reference the data to be copied.
	 *
	 * WT_CELLs are typically small, 1 or 2 bytes -- don't call memcpy, do
	 * the copy in-line.
	 */
	for (p = r->first_free,
	    t = (uint8_t *)&kv->cell, len = kv->cell_len; len > 0; --len)
		*p++ = *t++;

	/* The data can be quite large -- call memcpy. */
	if (kv->buf.size != 0)
		memcpy(p, kv->buf.data, kv->buf.size);

	WT_ASSERT(session, kv->len == kv->cell_len + kv->buf.size);
	__wt_rec_incr(session, r, 1, kv->len);
}

/*
 * __wt_rec_cell_build_addr --
 *	Process an address reference and return a cell structure to be stored
 *	on the page.
 */
static inline void
__wt_rec_cell_build_addr(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_ADDR *addr, bool proxy_cell, uint64_t recno)
{
	WT_REC_KV *val;
	u_int cell_type;

	val = &r->v;

	/*
	 * Our caller optionally specifies a cell type (deleted proxy cells),
	 * otherwise go with what we know.
	 */
	if (proxy_cell)
		cell_type = WT_CELL_ADDR_DEL;
	else {
		switch (addr->type) {
		case WT_ADDR_INT:
			cell_type = WT_CELL_ADDR_INT;
			break;
		case WT_ADDR_LEAF:
			cell_type = WT_CELL_ADDR_LEAF;
			break;
		case WT_ADDR_LEAF_NO:
		default:
			cell_type = WT_CELL_ADDR_LEAF_NO;
			break;
		}
		WT_ASSERT(session, addr->size != 0);
	}

	/*
	 * We don't check the address size because we can't store an address on
	 * an overflow page: if the address won't fit, the overflow page's
	 * address won't fit either.  This possibility must be handled by Btree
	 * configuration, we have to disallow internal page sizes that are too
	 * small with respect to the largest address cookie the underlying block
	 * manager might return.
	 */

	/*
	 * We don't copy the data into the buffer, it's not necessary; just
	 * re-point the buffer's data/length fields.
	 */
	val->buf.data = addr->addr;
	val->buf.size = addr->size;
	val->cell_len = __wt_cell_pack_addr(
	    session, &val->cell, cell_type, recno, addr->newest_durable_ts,
	    addr->oldest_start_ts, addr->oldest_start_txn,
	    addr->newest_stop_ts, addr->newest_stop_txn, val->buf.size);
	val->len = val->cell_len + val->buf.size;
}

/*
 * __wt_rec_cell_build_val --
 *	Process a data item and return a WT_CELL structure and byte string to
 *	be stored on the page.
 */
static inline int
__wt_rec_cell_build_val(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    const void *data, size_t size,
    wt_timestamp_t start_ts, uint64_t start_txn,
    wt_timestamp_t stop_ts, uint64_t stop_txn, uint64_t rle)
{
	WT_BTREE *btree;
	WT_REC_KV *val;

	btree = S2BT(session);

	val = &r->v;

	/*
	 * We don't copy the data into the buffer, it's not necessary; just
	 * re-point the buffer's data/length fields.
	 */
	val->buf.data = data;
	val->buf.size = size;

	/* Handle zero-length cells quickly. */
	if (size != 0) {
		/* Optionally compress the data using the Huffman engine. */
		if (btree->huffman_value != NULL)
			WT_RET(__wt_huffman_encode(
			    session, btree->huffman_value,
			    val->buf.data, (uint32_t)val->buf.size, &val->buf));

		/* Create an overflow object if the data won't fit. */
		if (val->buf.size > btree->maxleafvalue) {
			WT_STAT_DATA_INCR(session, rec_overflow_value);

			return (__wt_rec_cell_build_ovfl(session, r, val,
			    WT_CELL_VALUE_OVFL,
			    start_ts, start_txn, stop_ts, stop_txn, rle));
		}
	}
	val->cell_len = __wt_cell_pack_value(session, &val->cell,
	    start_ts, start_txn, stop_ts, stop_txn, rle, val->buf.size);
	val->len = val->cell_len + val->buf.size;

	return (0);
}

/*
 * __wt_rec_dict_replace --
 *	Check for a dictionary match.
 */
static inline int
__wt_rec_dict_replace(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    wt_timestamp_t start_ts, uint64_t start_txn,
    wt_timestamp_t stop_ts, uint64_t stop_txn, uint64_t rle, WT_REC_KV *val)
{
	WT_REC_DICTIONARY *dp;
	uint64_t offset;

	/*
	 * We optionally create a dictionary of values and only write a unique
	 * value once per page, using a special "copy" cell for all subsequent
	 * copies of the value.  We have to do the cell build and resolution at
	 * this low level because we need physical cell offsets for the page.
	 *
	 * Sanity check: short-data cells can be smaller than dictionary-copy
	 * cells.  If the data is already small, don't bother doing the work.
	 * This isn't just work avoidance: on-page cells can't grow as a result
	 * of writing a dictionary-copy cell, the reconciliation functions do a
	 * split-boundary test based on the size required by the value's cell;
	 * if we grow the cell after that test we'll potentially write off the
	 * end of the buffer's memory.
	 */
	if (val->buf.size <= WT_INTPACK32_MAXSIZE)
		return (0);
	WT_RET(__wt_rec_dictionary_lookup(session, r, val, &dp));
	if (dp == NULL)
		return (0);

	/*
	 * If the dictionary offset isn't set, we're creating a new entry in the
	 * dictionary, set its location.
	 *
	 * If the dictionary offset is set, we have a matching value. Create a
	 * copy cell instead.
	 */
	if (dp->offset == 0)
		dp->offset = WT_PTRDIFF32(r->first_free, r->cur_ptr->image.mem);
	else {
		/*
		 * The offset is the byte offset from this cell to the previous,
		 * matching cell, NOT the byte offset from the beginning of the
		 * page.
		 */
		offset = (uint64_t)WT_PTRDIFF(r->first_free,
		    (uint8_t *)r->cur_ptr->image.mem + dp->offset);
		val->len = val->cell_len = __wt_cell_pack_copy(session,
		    &val->cell,
		    start_ts, start_txn, stop_ts, stop_txn, rle, offset);
		val->buf.data = NULL;
		val->buf.size = 0;
	}
	return (0);
}
