/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __rec_split_fixup(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_split_row_promote(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_ITEM *, uint8_t);
static int __rec_split_row_promote_cell(
		WT_SESSION_IMPL *, WT_PAGE_HEADER *, WT_ITEM *);
static int  __rec_split_write(WT_SESSION_IMPL *,
		WT_RECONCILE *, WT_BOUNDARY *, WT_ITEM *, bool);

/*
 * __rec_split_bnd_init --
 *	Initialize a single boundary structure.
 */
static void
__rec_split_bnd_init(WT_SESSION_IMPL *session, WT_BOUNDARY *bnd)
{
	bnd->offset = 0;
	bnd->recno = WT_RECNO_OOB;
	bnd->entries = 0;

	__wt_free(session, bnd->addr.addr);
	WT_CLEAR(bnd->addr);
	bnd->size = 0;
	bnd->cksum = 0;
	__wt_free(session, bnd->dsk);

	__wt_free(session, bnd->supd);
	bnd->supd_next = 0;
	bnd->supd_allocated = 0;

	/*
	 * Don't touch the key, we re-use that memory in each new
	 * reconciliation.
	 */

	bnd->already_compressed = false;
}

/*
 * __rec_split_bnd_grow --
 *	Grow the boundary array as necessary.
 */
static int
__rec_split_bnd_grow(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	/*
	 * Make sure there's enough room for another boundary.  The calculation
	 * is +2, because when filling in the current boundary's information,
	 * we save start information for the next boundary (a byte offset and a
	 * record number or key), in the (current + 1) slot.
	 *
	 * For the same reason, we're always initializing one ahead.
	 */
	WT_RET(__wt_realloc_def(
	    session, &r->bnd_allocated, r->bnd_next + 2, &r->bnd));
	r->bnd_entries = r->bnd_allocated / sizeof(r->bnd[0]);

	__rec_split_bnd_init(session, &r->bnd[r->bnd_next + 1]);

	return (0);
}

/*
 * __wt_rec_split_page_size --
 *	Split page size calculation: we don't want to repeatedly split every
 * time a new entry is added, so we split to a smaller-than-maximum page size.
 */
uint32_t
__wt_rec_split_page_size(WT_BTREE *btree, uint32_t maxpagesize)
{
	uintmax_t a;
	uint32_t split_size;

	/*
	 * Ideally, the split page size is some percentage of the maximum page
	 * size rounded to an allocation unit (round to an allocation unit so
	 * we don't waste space when we write).
	 */
	a = maxpagesize;			/* Don't overflow. */
	split_size = (uint32_t)
	    WT_ALIGN((a * (u_int)btree->split_pct) / 100, btree->allocsize);

	/*
	 * If the result of that calculation is the same as the allocation unit
	 * (that happens if the maximum size is the same size as an allocation
	 * unit, use a percentage of the maximum page size).
	 */
	if (split_size == btree->allocsize)
		split_size = (uint32_t)((a * (u_int)btree->split_pct) / 100);

	return (split_size);
}

/*
 * __wt_rec_split_init --
 *	Initialization for the reconciliation split functions.
 */
int
__wt_rec_split_init(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, uint64_t recno, uint32_t max)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_PAGE_HEADER *dsk;
	size_t corrected_page_size;

	btree = S2BT(session);
	bm = btree->bm;

	/*
	 * The maximum leaf page size governs when an in-memory leaf page splits
	 * into multiple on-disk pages; however, salvage can't be allowed to
	 * split, there's no parent page yet.  If we're doing salvage, override
	 * the caller's selection of a maximum page size, choosing a page size
	 * that ensures we won't split.
	 */
	if (r->salvage != NULL)
		max = __wt_rec_leaf_page_max(session, r);

	/*
	 * Set the page sizes.  If we're doing the page layout, the maximum page
	 * size is the same as the page size.  If the application is doing page
	 * layout (raw compression is configured), we accumulate some amount of
	 * additional data because we don't know how well it will compress, and
	 * we don't want to increment our way up to the amount of data needed by
	 * the application to successfully compress to the target page size.
	 */
	r->page_size = r->page_size_orig = max;
	if (r->raw_compression)
		r->page_size *= 10;

	/*
	 * Ensure the disk image buffer is large enough for the max object, as
	 * corrected by the underlying block manager.
	 */
	corrected_page_size = r->page_size;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_init(session, &r->dsk, corrected_page_size));

	/*
	 * Clear the disk page's header and block-manager space, set the page
	 * type (the type doesn't change, and setting it later would require
	 * additional code in a few different places).
	 */
	dsk = r->dsk.mem;
	memset(dsk, 0, WT_PAGE_HEADER_BYTE_SIZE(btree));
	dsk->type = page->type;

	/*
	 * If we have to split, we want to choose a smaller page size for the
	 * split pages, because otherwise we could end up splitting one large
	 * packed page over and over. We don't want to pick the minimum size
	 * either, because that penalizes an application that did a bulk load
	 * and subsequently inserted a few items into packed pages.  Currently
	 * defaulted to 75%, but I have no empirical evidence that's "correct".
	 *
	 * The maximum page size may be a multiple of the split page size (for
	 * example, there's a maximum page size of 128KB, but because the table
	 * is active and we don't want to split a lot, the split size is 20KB).
	 * The maximum page size may NOT be an exact multiple of the split page
	 * size.
	 *
	 * It's lots of work to build these pages and don't want to start over
	 * when we reach the maximum page size (it's painful to restart after
	 * creating overflow items and compacted data, for example, as those
	 * items have already been written to disk).  So, the loop calls the
	 * helper functions when approaching a split boundary, and we save the
	 * information at that point.  That allows us to go back and split the
	 * page at the boundary points if we eventually overflow the maximum
	 * page size.
	 *
	 * Finally, all this doesn't matter for fixed-size column-store pages,
	 * raw compression, and salvage.  Fixed-size column store pages can
	 * split under (very) rare circumstances, but they're allocated at a
	 * fixed page size, never anything smaller.  In raw compression, the
	 * underlying compression routine decides when we split, so it's not
	 * our problem.  In salvage, as noted above, we can't split at all.
	 */
	if (r->raw_compression || r->salvage != NULL) {
		r->split_size = 0;
		r->space_avail = r->page_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}
	else if (page->type == WT_PAGE_COL_FIX) {
		r->split_size = r->page_size;
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	} else {
		r->split_size = __wt_rec_split_page_size(btree, r->page_size);
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}
	r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);

	/* Initialize the first boundary. */
	r->bnd_next = 0;
	WT_RET(__rec_split_bnd_grow(session, r));
	__rec_split_bnd_init(session, &r->bnd[0]);
	r->bnd[0].recno = recno;
	r->bnd[0].offset = WT_PAGE_HEADER_BYTE_SIZE(btree);

	/*
	 * If the maximum page size is the same as the split page size, either
	 * because of the object type or application configuration, there isn't
	 * any need to maintain split boundaries within a larger page.
	 *
	 * No configuration for salvage here, because salvage can't split.
	 */
	if (r->raw_compression)
		r->bnd_state = SPLIT_TRACKING_RAW;
	else if (max == r->split_size)
		r->bnd_state = SPLIT_TRACKING_OFF;
	else
		r->bnd_state = SPLIT_BOUNDARY;

	/* Initialize the entry counters. */
	r->entries = r->total_entries = 0;

	/* Initialize the starting record number. */
	r->recno = recno;

	/* New page, compression off. */
	r->key_pfx_compress = r->key_sfx_compress = false;

	return (0);
}

/*
 * __rec_split_grow --
 *	Grow the split buffer.
 */
static int
__rec_split_grow(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t add_len)
{
	WT_BM *bm;
	WT_BTREE *btree;
	size_t corrected_page_size, len;

	btree = S2BT(session);
	bm = btree->bm;

	len = WT_PTRDIFF(r->first_free, r->dsk.mem);
	corrected_page_size = len + add_len;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_grow(session, &r->dsk, corrected_page_size));
	r->first_free = (uint8_t *)r->dsk.mem + len;
	WT_ASSERT(session, corrected_page_size >= len);
	r->space_avail = corrected_page_size - len;
	WT_ASSERT(session, r->space_avail >= add_len);
	return (0);
}

/*
 * __wt_rec_split --
 *	Handle the page reconciliation bookkeeping.  (Did you know "bookkeeper"
 * has 3 doubled letters in a row?  Sweet-tooth does, too.)
 */
int
__wt_rec_split(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
{
	WT_BOUNDARY *last, *next;
	WT_BTREE *btree;
	WT_PAGE_HEADER *dsk;
	size_t inuse;

	btree = S2BT(session);
	dsk = r->dsk.mem;

	/*
	 * We should never split during salvage, and we're about to drop core
	 * because there's no parent page.
	 */
	if (r->salvage != NULL)
		WT_PANIC_RET(session, WT_PANIC,
		    "%s page too large, attempted split during salvage",
		    __wt_page_type_string(r->page->type));

	/* Hitting a page boundary resets the dictionary, in all cases. */
	__wt_rec_dictionary_reset(r);

	inuse = WT_PTRDIFF32(r->first_free, dsk);
	switch (r->bnd_state) {
	case SPLIT_BOUNDARY:
		/*
		 * We can get here if the first key/value pair won't fit.
		 * Additionally, grow the buffer to contain the current item if
		 * we haven't already consumed a reasonable portion of a split
		 * chunk.
		 */
		if (inuse < r->split_size / 2)
			break;

		/*
		 * About to cross a split boundary but not yet forced to split
		 * into multiple pages. If we have to split, this is one of the
		 * split points, save information about where we are when the
		 * split would have happened.
		 */
		WT_RET(__rec_split_bnd_grow(session, r));
		last = &r->bnd[r->bnd_next++];
		next = last + 1;

		/* Set the number of entries for the just finished chunk. */
		last->entries = r->entries - r->total_entries;
		r->total_entries = r->entries;

		/* Set the key for the next chunk. */
		next->recno = r->recno;
		if (dsk->type == WT_PAGE_ROW_INT ||
		    dsk->type == WT_PAGE_ROW_LEAF)
			WT_RET(__rec_split_row_promote(
			    session, r, &next->key, dsk->type));

		/*
		 * Set the starting buffer offset and clear the entries (the
		 * latter not required, but cleaner).
		 */
		next->offset = WT_PTRDIFF(r->first_free, dsk);
		next->entries = 0;

		/* Set the space available to another split-size chunk. */
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);

		/*
		 * Adjust the space available to handle two cases:
		 *  - We don't have enough room for another full split-size
		 *    chunk on the page.
		 *  - We chose to fill past a page boundary because of a
		 *    large item.
		 */
		if (inuse + r->space_avail > r->page_size) {
			r->space_avail =
			    r->page_size > inuse ? (r->page_size - inuse) : 0;

			/* There are no further boundary points. */
			r->bnd_state = SPLIT_MAX;
		}

		/*
		 * Return if the next object fits into this page, else we have
		 * to split the page.
		 */
		if (r->space_avail >= next_len)
			return (0);

		/* FALLTHROUGH */
	case SPLIT_MAX:
		/*
		 * We're going to have to split and create multiple pages.
		 *
		 * Cycle through the saved split-point information, writing the
		 * split chunks we have tracked.  The underlying fixup function
		 * sets the space available and other information, and copied
		 * any unwritten chunk of data to the beginning of the buffer.
		 */
		WT_RET(__rec_split_fixup(session, r));

		/* We're done saving split chunks. */
		r->bnd_state = SPLIT_TRACKING_OFF;
		break;
	case SPLIT_TRACKING_OFF:
		/*
		 * We can get here if the first key/value pair won't fit.
		 * Additionally, grow the buffer to contain the current item if
		 * we haven't already consumed a reasonable portion of a split
		 * chunk.
		 */
		if (inuse < r->split_size / 2)
			break;

		/*
		 * The key/value pairs didn't fit into a single page, but either
		 * we've already noticed that and are now processing the rest of
		 * the pairs at split size boundaries, or the split size was the
		 * same as the page size, and we never bothered with split point
		 * information at all.
		 */
		WT_RET(__rec_split_bnd_grow(session, r));
		last = &r->bnd[r->bnd_next++];
		next = last + 1;

		/*
		 * Set the key for the next chunk (before writing the block, a
		 * key range is needed in that code).
		 */
		next->recno = r->recno;
		if (dsk->type == WT_PAGE_ROW_INT ||
		    dsk->type == WT_PAGE_ROW_LEAF)
			WT_RET(__rec_split_row_promote(
			    session, r, &next->key, dsk->type));

		/* Clear the entries (not required, but cleaner). */
		next->entries = 0;

		/* Finalize the header information and write the page. */
		dsk->recno = last->recno;
		dsk->u.entries = r->entries;
		dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);
		WT_RET(__rec_split_write(session, r, last, &r->dsk, false));

		/*
		 * Set the caller's entry count and buffer information for the
		 * next chunk.  We only get here if we're not splitting or have
		 * already split, so it's split-size chunks from here on out.
		 */
		r->entries = 0;
		r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
		break;
	case SPLIT_TRACKING_RAW:
	WT_ILLEGAL_VALUE(session);
	}

	/*
	 * Overflow values can be larger than the maximum page size but still be
	 * "on-page". If the next key/value pair is larger than space available
	 * after a split has happened (in other words, larger than the maximum
	 * page size), create a page sized to hold that one key/value pair. This
	 * generally splits the page into key/value pairs before a large object,
	 * the object, and key/value pairs after the object. It's possible other
	 * key/value pairs will also be aggregated onto the bigger page before
	 * or after, if the page happens to hold them, but it won't necessarily
	 * happen that way.
	 */
	if (r->space_avail < next_len)
		WT_RET(__rec_split_grow(session, r, next_len));

	return (0);
}

/*
 * __rec_split_raw_worker --
 *	Handle the raw compression page reconciliation bookkeeping.
 */
static int
__rec_split_raw_worker(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, size_t next_len, bool no_more_rows)
{
	WT_BM *bm;
	WT_BOUNDARY *last, *next;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_COMPRESSOR *compressor;
	WT_DECL_RET;
	WT_ITEM *dst, *write_ref;
	WT_PAGE_HEADER *dsk, *dsk_dst;
	WT_SESSION *wt_session;
	size_t corrected_page_size, extra_skip, len, result_len;
	uint64_t recno;
	uint32_t entry, i, result_slots, slots;
	bool last_block;
	uint8_t *dsk_start;

	wt_session = (WT_SESSION *)session;
	btree = S2BT(session);
	bm = btree->bm;

	unpack = &_unpack;
	compressor = btree->compressor;
	dst = &r->raw_destination;
	dsk = r->dsk.mem;

	WT_RET(__rec_split_bnd_grow(session, r));
	last = &r->bnd[r->bnd_next];
	next = last + 1;

	/*
	 * We can get here if the first key/value pair won't fit.
	 */
	if (r->entries == 0)
		goto split_grow;

	/*
	 * Build arrays of offsets and cumulative counts of cells and rows in
	 * the page: the offset is the byte offset to the possible split-point
	 * (adjusted for an initial chunk that cannot be compressed), entries
	 * is the cumulative page entries covered by the byte offset, recnos is
	 * the cumulative rows covered by the byte offset.
	 */
	if (r->entries >= r->raw_max_slots) {
		__wt_free(session, r->raw_entries);
		__wt_free(session, r->raw_offsets);
		__wt_free(session, r->raw_recnos);
		r->raw_max_slots = 0;

		i = r->entries + 100;
		WT_RET(__wt_calloc_def(session, i, &r->raw_entries));
		WT_RET(__wt_calloc_def(session, i, &r->raw_offsets));
		if (dsk->type == WT_PAGE_COL_INT ||
		    dsk->type == WT_PAGE_COL_VAR)
			WT_RET(__wt_calloc_def(session, i, &r->raw_recnos));
		r->raw_max_slots = i;
	}

	/*
	 * We're going to walk the disk image, which requires setting the
	 * number of entries.
	 */
	dsk->u.entries = r->entries;

	/*
	 * We track the record number at each column-store split point, set an
	 * initial value.
	 */
	recno = WT_RECNO_OOB;
	if (dsk->type == WT_PAGE_COL_VAR)
		recno = last->recno;

	entry = slots = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++entry;

		/*
		 * Row-store pages can split at keys, but not at values,
		 * column-store pages can split at values.
		 */
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
		case WT_CELL_KEY_SHORT:
			break;
		case WT_CELL_ADDR_DEL:
		case WT_CELL_ADDR_INT:
		case WT_CELL_ADDR_LEAF:
		case WT_CELL_ADDR_LEAF_NO:
		case WT_CELL_DEL:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
		case WT_CELL_VALUE_SHORT:
			if (dsk->type == WT_PAGE_COL_INT) {
				recno = unpack->v;
				break;
			}
			if (dsk->type == WT_PAGE_COL_VAR) {
				recno += __wt_cell_rle(unpack);
				break;
			}
			r->raw_entries[slots] = entry;
			continue;
		WT_ILLEGAL_VALUE(session);
		}

		/*
		 * We can't compress the first 64B of the block (it must be
		 * written without compression), and a possible split point
		 * may appear in that 64B; keep it simple, ignore the first
		 * allocation size of data, anybody splitting smaller than
		 * that (as calculated before compression), is doing it wrong.
		 */
		if ((len = WT_PTRDIFF(cell, dsk)) > btree->allocsize)
			r->raw_offsets[++slots] =
			    WT_STORE_SIZE(len - WT_BLOCK_COMPRESS_SKIP);

		if (dsk->type == WT_PAGE_COL_INT ||
		    dsk->type == WT_PAGE_COL_VAR)
			r->raw_recnos[slots] = recno;
		r->raw_entries[slots] = entry;
	}

	/*
	 * If we haven't managed to find at least one split point, we're done,
	 * don't bother calling the underlying compression function.
	 */
	if (slots == 0) {
		result_len = 0;
		result_slots = 0;
		goto no_slots;
	}

	/* The slot at array's end is the total length of the data. */
	r->raw_offsets[++slots] =
	    WT_STORE_SIZE(WT_PTRDIFF(cell, dsk) - WT_BLOCK_COMPRESS_SKIP);

	/*
	 * Allocate a destination buffer. If there's a pre-size function, call
	 * it to determine the destination buffer's size, else the destination
	 * buffer is documented to be at least the source size. (We can't use
	 * the target page size, any single key/value could be larger than the
	 * page size. Don't bother figuring out a minimum, just use the source
	 * size.)
	 *
	 * The destination buffer needs to be large enough for the final block
	 * size, corrected for the requirements of the underlying block manager.
	 * If the final block size is 8KB, that's a multiple of 512B and so the
	 * underlying block manager is fine with it.  But... we don't control
	 * what the pre_size method returns us as a required size, and we don't
	 * want to document the compress_raw method has to skip bytes in the
	 * buffer because that's confusing, so do something more complicated.
	 * First, find out how much space the compress_raw function might need,
	 * either the value returned from pre_size, or the initial source size.
	 * Add the compress-skip bytes, and then correct that value for the
	 * underlying block manager. As a result, we have a destination buffer
	 * that's large enough when calling the compress_raw method, and there
	 * are bytes in the header just for us.
	 */
	if (compressor->pre_size == NULL)
		result_len = (size_t)r->raw_offsets[slots];
	else
		WT_RET(compressor->pre_size(compressor, wt_session,
		    (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP,
		    (size_t)r->raw_offsets[slots], &result_len));
	extra_skip = btree->kencryptor == NULL ? 0 :
	    btree->kencryptor->size_const + WT_ENCRYPT_LEN_SIZE;

	corrected_page_size = result_len + WT_BLOCK_COMPRESS_SKIP;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_init(session, dst, corrected_page_size));

	/*
	 * Copy the header bytes into the destination buffer, then call the
	 * compression function.
	 */
	memcpy(dst->mem, dsk, WT_BLOCK_COMPRESS_SKIP);
	ret = compressor->compress_raw(compressor, wt_session,
	    r->page_size_orig, btree->split_pct,
	    WT_BLOCK_COMPRESS_SKIP + extra_skip,
	    (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP,
	    r->raw_offsets, slots,
	    (uint8_t *)dst->mem + WT_BLOCK_COMPRESS_SKIP,
	    result_len, no_more_rows, &result_len, &result_slots);
	switch (ret) {
	case EAGAIN:
		/*
		 * The compression function wants more rows; accumulate and
		 * retry.
		 *
		 * Reset the resulting slots count, just in case the compression
		 * function modified it before giving up.
		 */
		result_slots = 0;
		break;
	case 0:
		/*
		 * If the compression function returned zero result slots, it's
		 * giving up and we write the original data.  (This is a pretty
		 * bad result: we've not done compression on a block much larger
		 * than the maximum page size, but once compression gives up,
		 * there's not much else we can do.)
		 *
		 * If the compression function returned non-zero result slots,
		 * we were successful and have a block to write.
		 */
		if (result_slots == 0) {
			WT_STAT_FAST_DATA_INCR(session, compress_raw_fail);

			/*
			 * If there are no more rows, we can write the original
			 * data from the original buffer.
			 */
			if (no_more_rows)
				break;

			/*
			 * Copy the original data to the destination buffer, as
			 * if the compression function simply copied it.  Take
			 * all but the last row of the original data (the last
			 * row has to be set as the key for the next block).
			 */
			result_slots = slots - 1;
			result_len = r->raw_offsets[result_slots];
			WT_RET(__wt_buf_grow(
			    session, dst, result_len + WT_BLOCK_COMPRESS_SKIP));
			memcpy((uint8_t *)dst->mem + WT_BLOCK_COMPRESS_SKIP,
			    (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP,
			    result_len);

			/*
			 * Mark it as uncompressed so the standard compression
			 * function is called before the buffer is written.
			 */
			last->already_compressed = false;
		} else {
			WT_STAT_FAST_DATA_INCR(session, compress_raw_ok);

			/*
			 * If there are more rows and the compression function
			 * consumed all of the current data, there are problems:
			 * First, with row-store objects, we're potentially
			 * skipping updates, we must have a key for the next
			 * block so we know with what block a skipped update is
			 * associated.  Second, if the compression function
			 * compressed all of the data, we're not pushing it
			 * hard enough (unless we got lucky and gave it exactly
			 * the right amount to work with, which is unlikely).
			 * Handle both problems by accumulating more data any
			 * time we're not writing the last block and compression
			 * ate all of the rows.
			 */
			if (result_slots == slots && !no_more_rows)
				result_slots = 0;
			else
				last->already_compressed = true;
		}
		break;
	default:
		return (ret);
	}

no_slots:
	/*
	 * Check for the last block we're going to write: if no more rows and
	 * we failed to compress anything, or we compressed everything, it's
	 * the last block.
	 */
	last_block = no_more_rows &&
	    (result_slots == 0 || result_slots == slots);

	if (result_slots != 0) {
		/*
		 * We have a block, finalize the header information.
		 */
		dst->size = result_len + WT_BLOCK_COMPRESS_SKIP;
		dsk_dst = dst->mem;
		dsk_dst->recno = last->recno;
		dsk_dst->mem_size =
		    r->raw_offsets[result_slots] + WT_BLOCK_COMPRESS_SKIP;
		dsk_dst->u.entries = r->raw_entries[result_slots - 1];

		/*
		 * There is likely a remnant in the working buffer that didn't
		 * get compressed; copy it down to the start of the buffer and
		 * update the starting record number, free space and so on.
		 * !!!
		 * Note use of memmove, the source and destination buffers can
		 * overlap.
		 */
		len = WT_PTRDIFF(
		    r->first_free, (uint8_t *)dsk + dsk_dst->mem_size);
		dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
		(void)memmove(dsk_start, (uint8_t *)r->first_free - len, len);

		r->entries -= r->raw_entries[result_slots - 1];
		r->first_free = dsk_start + len;
		r->space_avail += r->raw_offsets[result_slots];
		WT_ASSERT(session, r->first_free + r->space_avail <=
		    (uint8_t *)r->dsk.mem + r->dsk.memsize);

		/*
		 * Set the key for the next block (before writing the block, a
		 * key range is needed in that code).
		 */
		switch (dsk->type) {
		case WT_PAGE_COL_INT:
			next->recno = r->raw_recnos[result_slots];
			break;
		case WT_PAGE_COL_VAR:
			next->recno = r->raw_recnos[result_slots - 1];
			break;
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			next->recno = WT_RECNO_OOB;
			if (!last_block) {
				/*
				 * Confirm there was uncompressed data remaining
				 * in the buffer, we're about to read it for the
				 * next chunk's initial key.
				 */
				WT_ASSERT(session, len > 0);
				WT_RET(__rec_split_row_promote_cell(
				    session, dsk, &next->key));
			}
			break;
		}
		write_ref = dst;
	} else if (no_more_rows) {
		/*
		 * Compression failed and there are no more rows to accumulate,
		 * write the original buffer instead.
		 */
		WT_STAT_FAST_DATA_INCR(session, compress_raw_fail);

		dsk->recno = last->recno;
		dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);
		dsk->u.entries = r->entries;

		r->entries = 0;
		r->first_free = WT_PAGE_HEADER_BYTE(btree, dsk);
		r->space_avail = r->page_size - WT_PAGE_HEADER_BYTE_SIZE(btree);

		write_ref = &r->dsk;
		last->already_compressed = false;
	} else {
		/*
		 * Compression failed, there are more rows to accumulate and the
		 * compression function wants to try again; increase the size of
		 * the "page" and try again after we accumulate some more rows.
		 */
		WT_STAT_FAST_DATA_INCR(session, compress_raw_fail_temporary);
		goto split_grow;
	}

	/* We have a block, update the boundary counter. */
	++r->bnd_next;

	/*
	 * If we are writing the whole page in our first/only attempt, it might
	 * be a checkpoint (checkpoints are only a single page, by definition).
	 * Further, checkpoints aren't written here, the wrapup functions do the
	 * write, and they do the write from the original buffer location.  If
	 * it's a checkpoint and the block isn't in the right buffer, copy it.
	 *
	 * If it's not a checkpoint, write the block.
	 */
	if (r->bnd_next == 1 &&
	    last_block && __wt_rec_is_checkpoint(session, r, last)) {
		if (write_ref == dst)
			WT_RET(__wt_buf_set(
			    session, &r->dsk, dst->mem, dst->size));
	} else
		WT_RET(
		    __rec_split_write(session, r, last, write_ref, last_block));

	/*
	 * We got called because there wasn't enough room in the buffer for the
	 * next key and we might or might not have written a block. In any case,
	 * make sure the next key fits into the buffer.
	 */
	if (r->space_avail < next_len) {
split_grow:	/*
		 * Double the page size and make sure we accommodate at least
		 * one more record. The reason for the latter is that we may
		 * be here because there's a large key/value pair that won't
		 * fit in our initial page buffer, even at its expanded size.
		 */
		r->page_size *= 2;
		return (__rec_split_grow(session, r, r->page_size + next_len));
	}
	return (0);
}

/*
 * __wt_rec_split_raw --
 *	Raw compression split routine.
 */
int
__wt_rec_split_raw(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
{
	return (__rec_split_raw_worker(session, r, next_len, false));
}

/*
 * __rec_split_finish_std --
 *	Finish processing a page, standard version.
 */
static int
__rec_split_finish_std(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BOUNDARY *bnd;
	WT_PAGE_HEADER *dsk;

	/* Adjust the boundary information based on our split status. */
	switch (r->bnd_state) {
	case SPLIT_BOUNDARY:
	case SPLIT_MAX:
		/*
		 * We never split, the reconciled page fit into a maximum page
		 * size.  Change the first boundary slot to represent the full
		 * page (the first boundary slot is largely correct, just update
		 * the number of entries).
		 */
		r->bnd_next = 0;
		break;
	case SPLIT_TRACKING_OFF:
		/*
		 * If we have already split, or aren't tracking boundaries, put
		 * the remaining data in the next boundary slot.
		 */
		WT_RET(__rec_split_bnd_grow(session, r));
		break;
	case SPLIT_TRACKING_RAW:
		/*
		 * We were configured for raw compression, but never actually
		 * wrote anything.
		 */
		break;
	WT_ILLEGAL_VALUE(session);
	}

	/*
	 * We may arrive here with no entries to write if the page was entirely
	 * empty or if nothing on the page was visible to us.
	 */
	if (r->entries == 0) {
		/*
		 * Pages with skipped or not-yet-globally visible updates aren't
		 * really empty; otherwise, the page is truly empty and we will
		 * merge it into its parent during the parent's reconciliation.
		 */
		if (r->supd_next == 0)
			return (0);

		/*
		 * If using the save/restore eviction path, continue with the
		 * write, the page will be restored after we finish.
		 *
		 * If using the lookaside table eviction path, we can't continue
		 * (we need a page to be written, otherwise we won't ever find
		 * the updates for future reads).
		 */
		if (F_ISSET(r, WT_EVICT_LOOKASIDE))
			return (EBUSY);
	}

	/* Set the boundary reference and increment the count. */
	bnd = &r->bnd[r->bnd_next++];
	bnd->entries = r->entries;

	/* Finalize the header information. */
	dsk = r->dsk.mem;
	dsk->recno = bnd->recno;
	dsk->u.entries = r->entries;
	dsk->mem_size = r->dsk.size = WT_PTRDIFF32(r->first_free, dsk);

	/* If this is a checkpoint, we're done, otherwise write the page. */
	return (__wt_rec_is_checkpoint(session, r, bnd) ?
	    0 : __rec_split_write(session, r, bnd, &r->dsk, true));
}

/*
 * __wt_rec_split_finish --
 *	Finish processing a page.
 */
int
__wt_rec_split_finish(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	/* We're done reconciling - write the final page */
	if (r->raw_compression && r->entries != 0) {
		while (r->entries != 0)
			WT_RET(__rec_split_raw_worker(session, r, 0, true));
	} else
		WT_RET(__rec_split_finish_std(session, r));

	return (0);
}

/*
 * __rec_split_fixup --
 *	Fix up after crossing the maximum page boundary.
 */
static int
__rec_split_fixup(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BOUNDARY *bnd;
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE_HEADER *dsk;
	size_t i, len;
	uint8_t *dsk_start, *p;

	/*
	 * When we overflow physical limits of the page, we walk the list of
	 * split chunks we've created and write those pages out, then update
	 * the caller's information.
	 */
	btree = S2BT(session);

	/*
	 * The data isn't laid out on a page boundary or nul padded; copy it to
	 * a clean, aligned, padded buffer before writing it.
	 *
	 * Allocate a scratch buffer to hold the new disk image.  Copy the
	 * WT_PAGE_HEADER header onto the scratch buffer, most of the header
	 * information remains unchanged between the pages.
	 */
	WT_RET(__wt_scr_alloc(session, r->dsk.memsize, &tmp));
	dsk = tmp->mem;
	memcpy(dsk, r->dsk.mem, WT_PAGE_HEADER_SIZE);

	/*
	 * For each split chunk we've created, update the disk image and copy
	 * it into place.
	 */
	dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
	for (i = 0, bnd = r->bnd; i < r->bnd_next; ++i, ++bnd) {
		/* Copy the page contents to the temporary buffer. */
		len = (bnd + 1)->offset - bnd->offset;
		memcpy(dsk_start, (uint8_t *)r->dsk.mem + bnd->offset, len);

		/* Finalize the header information and write the page. */
		dsk->recno = bnd->recno;
		dsk->u.entries = bnd->entries;
		tmp->size = WT_PAGE_HEADER_BYTE_SIZE(btree) + len;
		dsk->mem_size = WT_STORE_SIZE(tmp->size);
		WT_ERR(__rec_split_write(session, r, bnd, tmp, false));
	}

	/*
	 * There is probably a remnant in the working buffer that didn't get
	 * written, copy it down to the beginning of the working buffer.
	 *
	 * Confirm the remnant is no larger than a split-sized chunk, including
	 * header. We know that's the maximum sized remnant because we only have
	 * remnants if split switches from accumulating to a split boundary to
	 * accumulating to the end of the page (the other path here is when we
	 * hit a split boundary, there was room for another split chunk in the
	 * page, and the next item still wouldn't fit, in which case there is no
	 * remnant). So: we were accumulating to the end of the page and created
	 * a remnant. We know the remnant cannot be as large as a split-sized
	 * chunk, including header, because if there was room for that large a
	 * remnant, we wouldn't have switched from accumulating to a page end.
	 */
	p = (uint8_t *)r->dsk.mem + bnd->offset;
	len = WT_PTRDIFF(r->first_free, p);
	if (len >= r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree))
		WT_PANIC_ERR(session, EINVAL,
		    "Reconciliation remnant too large for the split buffer");
	dsk = r->dsk.mem;
	dsk_start = WT_PAGE_HEADER_BYTE(btree, dsk);
	(void)memmove(dsk_start, p, len);

	/*
	 * Fix up our caller's information, including updating the starting
	 * record number.
	 */
	r->entries -= r->total_entries;
	r->first_free = dsk_start + len;
	WT_ASSERT(session,
	    r->page_size >= (WT_PAGE_HEADER_BYTE_SIZE(btree) + len));
	r->space_avail =
	    r->split_size - (WT_PAGE_HEADER_BYTE_SIZE(btree) + len);

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

/*
 * __rec_split_write --
 *	Write a disk block out for the split helper functions.
 */
static int
__rec_split_write(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_BOUNDARY *bnd, WT_ITEM *buf, bool last_block)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_MULTI *multi;
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	WT_PAGE_MODIFY *mod;
	WT_SAVE_UPD *supd;
	size_t addr_size;
	uint32_t bnd_slot, i, j;
	int cmp;
	uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];

	btree = S2BT(session);
	dsk = buf->mem;
	page = r->page;
	mod = page->modify;

	WT_RET(__wt_scr_alloc(session, 0, &key));

	/* Set the zero-length value flag in the page header. */
	if (dsk->type == WT_PAGE_ROW_LEAF) {
		F_CLR(dsk, WT_PAGE_EMPTY_V_ALL | WT_PAGE_EMPTY_V_NONE);

		if (r->entries != 0 && r->all_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_ALL);
		if (r->entries != 0 && !r->any_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_NONE);
	}

	/* Initialize the address (set the page type for the parent). */
	switch (dsk->type) {
	case WT_PAGE_COL_FIX:
		bnd->addr.type = WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_VAR:
	case WT_PAGE_ROW_LEAF:
		bnd->addr.type = r->ovfl_items ? WT_ADDR_LEAF : WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		bnd->addr.type = WT_ADDR_INT;
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}

	bnd->size = (uint32_t)buf->size;
	bnd->cksum = 0;

	/*
	 * Check if we've saved updates that belong to this block, and move
	 * any to the per-block structure.  Quit as soon as we find a saved
	 * update that doesn't belong to the block, they're in sorted order.
	 *
	 * This code requires a key be filled in for the next block (or the
	 * last block flag be set, if there's no next block).
	 */
	for (i = 0, supd = r->supd; i < r->supd_next; ++i, ++supd) {
		/* The last block gets all remaining saved updates. */
		if (last_block) {
			WT_ERR(__wt_rec_update_move(session, bnd, supd));
			continue;
		}

		/*
		 * Get the saved update's key and compare it with this block's
		 * key range.  If the saved update list belongs with the block
		 * we're about to write, move it to the per-block memory.  Check
		 * only to the first update that doesn't go with the block, they
		 * must be in sorted order.
		 */
		switch (page->type) {
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			if (WT_INSERT_RECNO(supd->ins) >= (bnd + 1)->recno)
				goto supd_check_complete;
			break;
		case WT_PAGE_ROW_LEAF:
			if (supd->ins == NULL)
				WT_ERR(__wt_row_leaf_key(
				    session, page, supd->rip, key, false));
			else {
				key->data = WT_INSERT_KEY(supd->ins);
				key->size = WT_INSERT_KEY_SIZE(supd->ins);
			}
			WT_ERR(__wt_compare(session,
			    btree->collator, key, &(bnd + 1)->key, &cmp));
			if (cmp >= 0)
				goto supd_check_complete;
			break;
		WT_ILLEGAL_VALUE_ERR(session);
		}
		WT_ERR(__wt_rec_update_move(session, bnd, supd));
	}

supd_check_complete:
	/*
	 * If there are updates that weren't moved to the block, shuffle them to
	 * the beginning of the cached list (we maintain the saved updates in
	 * sorted order, new saved updates must be appended to the list).
	 */
	for (j = 0; i < r->supd_next; ++j, ++i)
		r->supd[j] = r->supd[i];
	r->supd_next = j;

	/*
	 * If using the lookaside table eviction path and we found updates that
	 * weren't globally visible when reconciling this page, note that in the
	 * page header.
	 */
	if (F_ISSET(r, WT_EVICT_LOOKASIDE) && bnd->supd != NULL) {
		F_SET(dsk, WT_PAGE_LAS_UPDATE);
		r->cache_write_lookaside = true;
	}

	/*
	 * If using the save/restore eviction path and we had to skip updates in
	 * order to build this disk image, we can't actually write it. Instead,
	 * we will re-instantiate the page using the disk image and the list of
	 * updates we skipped.
	 */
	if (F_ISSET(r, WT_EVICT_UPDATE_RESTORE) && bnd->supd != NULL) {
		r->cache_write_restore = true;

		/*
		 * If the buffer is compressed (raw compression was configured),
		 * we have to decompress it so we can instantiate it later. It's
		 * a slow and convoluted path, but it's also a rare one and it's
		 * not worth making it faster. Else, the disk image is ready,
		 * copy it into place for later. It's possible the disk image
		 * has no items; we have to flag that for verification, it's a
		 * special case since read/writing empty pages isn't generally
		 * allowed.
		 */
		if (bnd->already_compressed)
			WT_ERR(__wt_rec_raw_decompress(
			    session, buf->data, buf->size, &bnd->dsk));
		else {
			WT_ERR(__wt_strndup(
			    session, buf->data, buf->size, &bnd->dsk));
			WT_ASSERT(session, __wt_verify_dsk_image(session,
			    "[evict split]", buf->data, buf->size, true) == 0);
		}
		goto done;
	}

	/*
	 * If we wrote this block before, re-use it.  Pages get written in the
	 * same block order every time, only check the appropriate slot.  The
	 * expensive part of this test is the checksum, only do that work when
	 * there has been or will be a reconciliation of this page involving
	 * split pages.  This test isn't perfect: we're doing a checksum if a
	 * previous reconciliation of the page split or if we will split this
	 * time, but that test won't calculate a checksum on the first block
	 * the first time the page splits.
	 */
	bnd_slot = (uint32_t)(bnd - r->bnd);
	if (bnd_slot > 1 ||
	    (mod->rec_result == WT_PM_REC_MULTIBLOCK &&
	    mod->mod_multi != NULL)) {
		/*
		 * There are page header fields which need to be cleared to get
		 * consistent checksums: specifically, the write generation and
		 * the memory owned by the block manager.  We are reusing the
		 * same buffer space each time, clear it before calculating the
		 * checksum.
		 */
		dsk->write_gen = 0;
		memset(WT_BLOCK_HEADER_REF(dsk), 0, btree->block_header);
		bnd->cksum = __wt_cksum(buf->data, buf->size);

		if (mod->rec_result == WT_PM_REC_MULTIBLOCK &&
		    mod->mod_multi_entries > bnd_slot) {
			multi = &mod->mod_multi[bnd_slot];
			if (multi->size == bnd->size &&
			    multi->cksum == bnd->cksum) {
				multi->addr.reuse = 1;
				bnd->addr = multi->addr;

				WT_STAT_FAST_DATA_INCR(session, rec_page_match);
				goto done;
			}
		}
	}

	WT_ERR(__wt_bt_write(session,
	    buf, addr, &addr_size, false, bnd->already_compressed));
	WT_ERR(__wt_strndup(session, addr, addr_size, &bnd->addr.addr));
	bnd->addr.size = (uint8_t)addr_size;

	/*
	 * If using the lookaside table eviction path and we found updates that
	 * weren't globally visible when reconciling this page, copy them into
	 * the database's lookaside store.
	 */
	if (F_ISSET(r, WT_EVICT_LOOKASIDE) && bnd->supd != NULL)
		ret = __wt_rec_update_las(session, r, btree->id, bnd);

done:
err:	__wt_scr_free(session, &key);
	return (ret);
}

/*
 * __wt_rec_split_discard --
 *	Discard the pages resulting from a previous split.
 */
int
__wt_rec_split_discard(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;
	WT_MULTI *multi;
	uint32_t i;

	mod = page->modify;

	/*
	 * A page that split is being reconciled for the second, or subsequent
	 * time; discard underlying block space used in the last reconciliation
	 * that is not being reused for this reconciliation.
	 */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		switch (page->type) {
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			__wt_free(session, multi->key.ikey);
			break;
		}
		if (multi->supd == NULL) {
			if (multi->addr.reuse)
				multi->addr.addr = NULL;
			else {
				WT_RET(__wt_rec_block_free(session,
				    multi->addr.addr, multi->addr.size));
				__wt_free(session, multi->addr.addr);
			}
		} else {
			__wt_free(session, multi->supd);
			__wt_free(session, multi->supd_dsk);
		}
	}
	__wt_free(session, mod->mod_multi);
	mod->mod_multi_entries = 0;

	/*
	 * This routine would be trivial, and only walk a single page freeing
	 * any blocks written to support the split, except for root splits.
	 * In the case of root splits, we have to cope with multiple pages in
	 * a linked list, and we also have to discard overflow items written
	 * for the page.
	 */
	switch (page->type) {
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		if (mod->mod_root_split == NULL)
			break;
		WT_RET(__wt_rec_split_discard(session, mod->mod_root_split));
		WT_RET(__wt_ovfl_track_wrapup(session, mod->mod_root_split));
		__wt_page_out(session, &mod->mod_root_split);
		break;
	}

	return (ret);
}

/*
 * __rec_split_row_promote_cell --
 *	Get a key from a cell for the purposes of promotion.
 */
static int
__rec_split_row_promote_cell(
    WT_SESSION_IMPL *session, WT_PAGE_HEADER *dsk, WT_ITEM *key)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack;

	btree = S2BT(session);
	kpack = &_kpack;

	/*
	 * The cell had better have a zero-length prefix and not be a copy cell;
	 * the first cell on a page cannot refer an earlier cell on the page.
	 */
	cell = WT_PAGE_HEADER_BYTE(btree, dsk);
	__wt_cell_unpack(cell, kpack);
	WT_ASSERT(session,
	    kpack->prefix == 0 && kpack->raw != WT_CELL_VALUE_COPY);

	WT_RET(__wt_cell_data_copy(session, dsk->type, kpack, key));
	return (0);
}

/*
 * __rec_split_row_promote --
 *	Key promotion for a row-store.
 */
static int
__rec_split_row_promote(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_ITEM *key, uint8_t type)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(update);
	WT_DECL_RET;
	WT_ITEM *max;
	WT_SAVE_UPD *supd;
	size_t cnt, len, size;
	uint32_t i;
	const uint8_t *pa, *pb;
	int cmp;

	/*
	 * For a column-store, the promoted key is the recno and we already have
	 * a copy.  For a row-store, it's the first key on the page, a variable-
	 * length byte string, get a copy.
	 *
	 * This function is called from the split code at each split boundary,
	 * but that means we're not called before the first boundary, and we
	 * will eventually have to get the first key explicitly when splitting
	 * a page.
	 *
	 * For the current slot, take the last key we built, after doing suffix
	 * compression.  The "last key we built" describes some process: before
	 * calling the split code, we must place the last key on the page before
	 * the boundary into the "last" key structure, and the first key on the
	 * page after the boundary into the "current" key structure, we're going
	 * to compare them for suffix compression.
	 *
	 * Suffix compression is a hack to shorten keys on internal pages.  We
	 * only need enough bytes in the promoted key to ensure searches go to
	 * the correct page: the promoted key has to be larger than the last key
	 * on the leaf page preceding it, but we don't need any more bytes than
	 * that. In other words, we can discard any suffix bytes not required
	 * to distinguish between the key being promoted and the last key on the
	 * leaf page preceding it.  This can only be done for the first level of
	 * internal pages, you cannot repeat suffix truncation as you split up
	 * the tree, it loses too much information.
	 *
	 * Note #1: if the last key on the previous page was an overflow key,
	 * we don't have the in-memory key against which to compare, and don't
	 * try to do suffix compression.  The code for that case turns suffix
	 * compression off for the next key, we don't have to deal with it here.
	 */
	if (type != WT_PAGE_ROW_LEAF || !r->key_sfx_compress)
		return (__wt_buf_set(session, key, r->cur->data, r->cur->size));

	btree = S2BT(session);
	WT_RET(__wt_scr_alloc(session, 0, &update));

	/*
	 * Note #2: if we skipped updates, an update key may be larger than the
	 * last key stored in the previous block (probable for append-centric
	 * workloads).  If there are skipped updates, check for one larger than
	 * the last key and smaller than the current key.
	 */
	max = r->last;
	if (F_ISSET(r, WT_EVICT_UPDATE_RESTORE))
		for (i = r->supd_next; i > 0; --i) {
			supd = &r->supd[i - 1];
			if (supd->ins == NULL)
				WT_ERR(__wt_row_leaf_key(session,
				    r->page, supd->rip, update, false));
			else {
				update->data = WT_INSERT_KEY(supd->ins);
				update->size = WT_INSERT_KEY_SIZE(supd->ins);
			}

			/* Compare against the current key, it must be less. */
			WT_ERR(__wt_compare(
			    session, btree->collator, update, r->cur, &cmp));
			if (cmp >= 0)
				continue;

			/* Compare against the last key, it must be greater. */
			WT_ERR(__wt_compare(
			    session, btree->collator, update, r->last, &cmp));
			if (cmp >= 0)
				max = update;

			/*
			 * The saved updates are in key-sort order so the entry
			 * we're looking for is either the last or the next-to-
			 * last one in the list.  Once we've compared an entry
			 * against the last key on the page, we're done.
			 */
			break;
		}

	/*
	 * The largest key on the last block must sort before the current key,
	 * so we'll either find a larger byte value in the current key, or the
	 * current key will be a longer key, and the interesting byte is one
	 * past the length of the shorter key.
	 */
	pa = max->data;
	pb = r->cur->data;
	len = WT_MIN(max->size, r->cur->size);
	size = len + 1;
	for (cnt = 1; len > 0; ++cnt, --len, ++pa, ++pb)
		if (*pa != *pb) {
			if (size != cnt) {
				WT_STAT_FAST_DATA_INCRV(session,
				    rec_suffix_compression, size - cnt);
				size = cnt;
			}
			break;
		}
	ret = __wt_buf_set(session, key, r->cur->data, size);

err:	__wt_scr_free(session, &update);
	return (ret);
}

