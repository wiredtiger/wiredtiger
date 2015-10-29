/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int  __rec_col_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_col_var_helper(WT_SESSION_IMPL *, WT_RECONCILE *,
		WT_SALVAGE_COOKIE *, WT_ITEM *, bool, uint8_t, uint64_t);

/*
 * __wt_rec_col_int --
 *	Reconcile a column-store internal page.
 */
int
__wt_rec_col_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_CHILD_STATE state;
	WT_DECL_RET;
	WT_KV *val;
	WT_PAGE *child;
	WT_REF *ref;
	bool hazard;

	btree = S2BT(session);
	child = NULL;
	hazard = false;

	val = &r->v;
	vpack = &_vpack;

	WT_RET(__wt_rec_split_init(
	    session, r, page, page->pg_intl_recno, btree->maxintlpage));

	/* For each entry in the in-memory page... */
	WT_INTL_FOREACH_BEGIN(session, page, ref) {
		/* Update the starting record number in case we split. */
		r->recno = ref->key.recno;

		/*
		 * Modified child.
		 * The page may be emptied or internally created during a split.
		 * Deleted/split pages are merged into the parent and discarded.
		 */
		WT_ERR(__wt_rec_child_modify(session, r, ref, &hazard, &state));
		addr = NULL;
		child = ref->page;

		switch (state) {
		case WT_CHILD_IGNORE:
			/* Deleted child we don't have to write. */
			WT_CHILD_RELEASE_ERR(session, hazard, ref);
			continue;

		case WT_CHILD_MODIFIED:
			/*
			 * Modified child. Empty pages are merged into the
			 * parent and discarded.
			 */
			switch (child->modify->rec_result) {
			case WT_PM_REC_EMPTY:
				/*
				 * Column-store pages are almost never empty, as
				 * discarding a page would remove a chunk of the
				 * name space.  The exceptions are pages created
				 * when the tree is created, and never filled.
				 */
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_MULTIBLOCK:
				WT_ERR(__rec_col_merge(session, r, child));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_REPLACE:
				addr = &child->modify->mod_replace;
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			break;
		case WT_CHILD_ORIGINAL:
			/* Original child. */
			break;
		case WT_CHILD_PROXY:
			/*
			 * Deleted child where we write a proxy cell, not
			 * yet supported for column-store.
			 */
			ret = __wt_illegal_value(session, NULL);
			goto err;
		}

		/*
		 * Build the value cell.  The child page address is in one of 3
		 * places: if the page was replaced, the page's modify structure
		 * references it and we built the value cell just above in the
		 * switch statement.  Else, the WT_REF->addr reference points to
		 * an on-page cell or an off-page WT_ADDR structure: if it's an
		 * on-page cell and we copy it from the page, else build a new
		 * cell.
		 */
		if (addr == NULL && __wt_off_page(page, ref->addr))
			addr = ref->addr;
		if (addr == NULL) {
			__wt_cell_unpack(ref->addr, vpack);
			val->buf.data = ref->addr;
			val->buf.size = __wt_cell_total_len(vpack);
			val->cell_len = 0;
			val->len = val->buf.size;
		} else
			__wt_rec_cell_build_addr(session, r,
			    addr->addr, addr->size,
			    __wt_rec_vtype(addr), ref->key.recno);
		WT_CHILD_RELEASE_ERR(session, hazard, ref);

		/* Boundary: split or write the page. */
		if (val->len > r->space_avail)
			WT_ERR(r->raw_compression ?
			    __wt_rec_split_raw(session, r, val->len) :
			    __wt_rec_split(session, r, val->len));

		/* Copy the value onto the page. */
		__wt_rec_copy_incr(session, r, val);
	} WT_INTL_FOREACH_END;

	/* Write the remnant page. */
	return (__wt_rec_split_finish(session, r));

err:	WT_CHILD_RELEASE(session, hazard, ref);
	return (ret);
}

/*
 * __rec_col_merge --
 *	Merge in a split page.
 */
static int
__rec_col_merge(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_KV *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	val = &r->v;

	/* For each entry in the split array... */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		/* Update the starting record number in case we split. */
		r->recno = multi->key.recno;

		/* Build the value cell. */
		addr = &multi->addr;
		__wt_rec_cell_build_addr(session, r,
		    addr->addr, addr->size, __wt_rec_vtype(addr), r->recno);

		/* Boundary: split or write the page. */
		if (val->len > r->space_avail)
			WT_RET(r->raw_compression ?
			    __wt_rec_split_raw(session, r, val->len) :
			    __wt_rec_split(session, r, val->len));

		/* Copy the value onto the page. */
		__wt_rec_copy_incr(session, r, val);
	}
	return (0);
}

/*
 * __wt_rec_col_fix --
 *	Reconcile a fixed-width, column-store leaf page.
 */
int
__wt_rec_col_fix(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BTREE *btree;
	WT_INSERT *ins;
	WT_UPDATE *upd;
	uint64_t recno;
	uint32_t entry, nrecs;

	btree = S2BT(session);

	WT_RET(__wt_rec_split_init(
	    session, r, page, page->pg_fix_recno, btree->maxleafpage));

	/* Update any changes to the original on-page data items. */
	WT_SKIP_FOREACH(ins, WT_COL_UPDATE_SINGLE(page)) {
		WT_RET(__wt_rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd != NULL)
			__bit_setv_recno(page, WT_INSERT_RECNO(ins),
			    btree->bitcnt, ((uint8_t *)WT_UPDATE_DATA(upd))[0]);
	}

	/* Copy the updated, disk-image bytes into place. */
	memcpy(r->first_free, page->pg_fix_bitf,
	    __bitstr_size((size_t)page->pg_fix_entries * btree->bitcnt));

	/* Calculate the number of entries per page remainder. */
	entry = page->pg_fix_entries;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(
	    btree, r->space_avail) - page->pg_fix_entries;
	r->recno += entry;

	/* Walk any append list. */
	WT_SKIP_FOREACH(ins, WT_COL_APPEND(page)) {
		WT_RET(__wt_rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL)
			continue;
		for (;;) {
			/*
			 * The application may have inserted records which left
			 * gaps in the name space.
			 */
			for (recno = WT_INSERT_RECNO(ins);
			    nrecs > 0 && r->recno < recno;
			    --nrecs, ++entry, ++r->recno)
				__bit_setv(
				    r->first_free, entry, btree->bitcnt, 0);

			if (nrecs > 0) {
				__bit_setv(r->first_free, entry, btree->bitcnt,
				    ((uint8_t *)WT_UPDATE_DATA(upd))[0]);
				--nrecs;
				++entry;
				++r->recno;
				break;
			}

			/*
			 * If everything didn't fit, update the counters and
			 * split.
			 *
			 * Boundary: split or write the page.
			 */
			__wt_rec_incr(session, r, entry,
			    __bitstr_size((size_t)entry * btree->bitcnt));
			WT_RET(__wt_rec_split(session, r, 0));

			/* Calculate the number of entries per page. */
			entry = 0;
			nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
		}
	}

	/* Update the counters. */
	__wt_rec_incr(
	    session, r, entry, __bitstr_size((size_t)entry * btree->bitcnt));

	/* Write the remnant page. */
	return (__wt_rec_split_finish(session, r));
}

/*
 * __wt_rec_col_fix_slvg --
 *	Reconcile a fixed-width, column-store leaf page created during salvage.
 */
int
__wt_rec_col_fix_slvg(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;
	uint64_t page_start, page_take;
	uint32_t entry, nrecs;

	btree = S2BT(session);

	/*
	 * !!!
	 * It's vanishingly unlikely and probably impossible for fixed-length
	 * column-store files to have overlapping key ranges.  It's possible
	 * for an entire key range to go missing (if a page is corrupted and
	 * lost), but because pages can't split, it shouldn't be possible to
	 * find pages where the key ranges overlap.  That said, we check for
	 * it during salvage and clean up after it here because it doesn't
	 * cost much and future column-store formats or operations might allow
	 * for fixed-length format ranges to overlap during salvage, and I
	 * don't want to have to retrofit the code later.
	 */
	WT_RET(__wt_rec_split_init(
	    session, r, page, page->pg_fix_recno, btree->maxleafpage));

	/* We may not be taking all of the entries on the original page. */
	page_take = salvage->take == 0 ? page->pg_fix_entries : salvage->take;
	page_start = salvage->skip == 0 ? 0 : salvage->skip;

	/* Calculate the number of entries per page. */
	entry = 0;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);

	for (; nrecs > 0 && salvage->missing > 0;
	    --nrecs, --salvage->missing, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt, 0);

	for (; nrecs > 0 && page_take > 0;
	    --nrecs, --page_take, ++page_start, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt,
		    __bit_getv(page->pg_fix_bitf,
			(uint32_t)page_start, btree->bitcnt));

	r->recno += entry;
	__wt_rec_incr(session, r, entry,
	    __bitstr_size((size_t)entry * btree->bitcnt));

	/*
	 * We can't split during salvage -- if everything didn't fit, it's
	 * all gone wrong.
	 */
	if (salvage->missing != 0 || page_take != 0)
		WT_PANIC_RET(session, WT_PANIC,
		    "%s page too large, attempted split during salvage",
		    __wt_page_type_string(page->type));

	/* Write the page. */
	return (__wt_rec_split_finish(session, r));
}

/*
 * __rec_col_var_helper --
 *	Create a column-store variable length record cell and write it onto a
 * page.
 */
static int
__rec_col_var_helper(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_SALVAGE_COOKIE *salvage,
    WT_ITEM *value, bool deleted, uint8_t overflow_type, uint64_t rle)
{
	WT_BTREE *btree;
	WT_KV *val;

	btree = S2BT(session);

	val = &r->v;

	/*
	 * Occasionally, salvage needs to discard records from the beginning or
	 * end of the page, and because the items may be part of a RLE cell, do
	 * the adjustments here. It's not a mistake we don't bother telling
	 * our caller we've handled all the records from the page we care about,
	 * and can quit processing the page: salvage is a rare operation and I
	 * don't want to complicate our caller's loop.
	 */
	if (salvage != NULL) {
		if (salvage->done)
			return (0);
		if (salvage->skip != 0) {
			if (rle <= salvage->skip) {
				salvage->skip -= rle;
				return (0);
			}
			rle -= salvage->skip;
			salvage->skip = 0;
		}
		if (salvage->take != 0) {
			if (rle <= salvage->take)
				salvage->take -= rle;
			else {
				rle = salvage->take;
				salvage->take = 0;
			}
			if (salvage->take == 0)
				salvage->done = true;
		}
	}

	if (deleted) {
		val->cell_len = __wt_cell_pack_del(&val->cell, rle);
		val->buf.data = NULL;
		val->buf.size = 0;
		val->len = val->cell_len;
	} else if (overflow_type) {
		val->cell_len = __wt_cell_pack_ovfl(
		    &val->cell, overflow_type, rle, value->size);
		val->buf.data = value->data;
		val->buf.size = value->size;
		val->len = val->cell_len + value->size;
	} else
		WT_RET(__wt_rec_cell_build_val(
		    session, r, value->data, value->size, rle));

	/* Boundary: split or write the page. */
	if (val->len > r->space_avail)
		WT_RET(r->raw_compression ?
		    __wt_rec_split_raw(session, r, val->len) :
		    __wt_rec_split(session, r, val->len));

	/* Copy the value onto the page. */
	if (!deleted && !overflow_type && btree->dictionary)
		WT_RET(__wt_rec_dictionary_replace(session, r, rle, val));
	__wt_rec_copy_incr(session, r, val);

	/* Update the starting record number in case we split. */
	r->recno += rle;

	return (0);
}

/*
 * __wt_rec_col_var --
 *	Reconcile a variable-width column-store leaf page.
 */
int
__wt_rec_col_var(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	enum { OVFL_IGNORE, OVFL_UNUSED, OVFL_USED } ovfl_state;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_COL *cip;
	WT_DECL_ITEM(orig);
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_ITEM *last;
	WT_UPDATE *upd;
	uint64_t n, nrepeat, repeat_count, rle, skip, src_recno;
	uint32_t i, size;
	bool deleted, last_deleted, orig_deleted, update_no_copy;
	const void *data;

	btree = S2BT(session);
	last = r->last;
	vpack = &_vpack;

	WT_RET(__wt_scr_alloc(session, 0, &orig));
	data = NULL;
	size = 0;
	upd = NULL;

	WT_RET(__wt_rec_split_init(
	    session, r, page, page->pg_var_recno, btree->maxleafpage));

	/*
	 * The salvage code may be calling us to reconcile a page where there
	 * were missing records in the column-store name space.  If taking the
	 * first record from on the page, it might be a deleted record, so we
	 * have to give the RLE code a chance to figure that out.  Else, if
	 * not taking the first record from the page, write a single element
	 * representing the missing records onto a new page.  (Don't pass the
	 * salvage cookie to our helper function in this case, we're handling
	 * one of the salvage cookie fields on our own, and we don't need the
	 * helper function's assistance.)
	 */
	rle = 0;
	last_deleted = false;
	if (salvage != NULL && salvage->missing != 0) {
		if (salvage->skip == 0) {
			rle = salvage->missing;
			last_deleted = true;

			/*
			 * Correct the number of records we're going to "take",
			 * pretending the missing records were on the page.
			 */
			salvage->take += salvage->missing;
		} else
			WT_ERR(__rec_col_var_helper(session,
			    r, NULL, NULL, true, false, salvage->missing));
	}

	/*
	 * We track two data items through this loop: the previous (last) item
	 * and the current item: if the last item is the same as the current
	 * item, we increment the RLE count for the last item; if the last item
	 * is different from the current item, we write the last item onto the
	 * page, and replace it with the current item.  The r->recno counter
	 * tracks records written to the page, and is incremented by the helper
	 * function immediately after writing records to the page.  The record
	 * number of our source record, that is, the current item, is maintained
	 * in src_recno.
	 */
	src_recno = r->recno + rle;

	/* For each entry in the in-memory page... */
	WT_COL_FOREACH(page, cip, i) {
		ovfl_state = OVFL_IGNORE;
		if ((cell = WT_COL_PTR(page, cip)) == NULL) {
			nrepeat = 1;
			ins = NULL;
			orig_deleted = true;
		} else {
			__wt_cell_unpack(cell, vpack);
			nrepeat = __wt_cell_rle(vpack);
			ins = WT_SKIP_FIRST(WT_COL_UPDATE(page, cip));

			/*
			 * If the original value is "deleted", there's no value
			 * to compare, we're done.
			 */
			orig_deleted = vpack->type == WT_CELL_DEL;
			if (orig_deleted)
				goto record_loop;

			/*
			 * Overflow items are tricky: we don't know until we're
			 * finished processing the set of values if we need the
			 * overflow value or not.  If we don't use the overflow
			 * item at all, we have to discard it from the backing
			 * file, otherwise we'll leak blocks on the checkpoint.
			 * That's safe because if the backing overflow value is
			 * still needed by any running transaction, we'll cache
			 * a copy in the reconciliation tracking structures.
			 *
			 * Regardless, we avoid copying in overflow records: if
			 * there's a WT_INSERT entry that modifies a reference
			 * counted overflow record, we may have to write copies
			 * of the overflow record, and in that case we'll do the
			 * comparisons, but we don't read overflow items just to
			 * see if they match records on either side.
			 */
			if (vpack->ovfl) {
				ovfl_state = OVFL_UNUSED;
				goto record_loop;
			}

			/*
			 * If data is Huffman encoded, we have to decode it in
			 * order to compare it with the last item we saw, which
			 * may have been an update string.  This guarantees we
			 * find every single pair of objects we can RLE encode,
			 * including applications updating an existing record
			 * where the new value happens (?) to match a Huffman-
			 * encoded value in a previous or next record.
			 */
			WT_ERR(__wt_dsk_cell_data_ref(
			    session, WT_PAGE_COL_VAR, vpack, orig));
		}

record_loop:	/*
		 * Generate on-page entries: loop repeat records, looking for
		 * WT_INSERT entries matching the record number.  The WT_INSERT
		 * lists are in sorted order, so only need check the next one.
		 */
		for (n = 0;
		    n < nrepeat; n += repeat_count, src_recno += repeat_count) {
			upd = NULL;
			if (ins != NULL && WT_INSERT_RECNO(ins) == src_recno) {
				WT_ERR(__wt_rec_txn_read(
				    session, r, ins, NULL, vpack, &upd));
				ins = WT_SKIP_NEXT(ins);
			}
			if (upd != NULL) {
				update_no_copy = true;	/* No data copy */
				repeat_count = 1;	/* Single record */

				deleted = WT_UPDATE_DELETED_ISSET(upd);
				if (!deleted) {
					data = WT_UPDATE_DATA(upd);
					size = upd->size;
				}
			} else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				update_no_copy = true;	/* No data copy */
				repeat_count = 1;	/* Single record */

				deleted = false;

				/*
				 * If doing update save and restore, there's an
				 * update that's not globally visible, and the
				 * underlying value is a removed overflow value,
				 * we end up here.
				 *
				 * When the update save/restore code noticed the
				 * removed overflow value, it appended a copy of
				 * the cached, original overflow value to the
				 * update list being saved (ensuring the on-page
				 * item will never be accessed after the page is
				 * re-instantiated), then returned a NULL update
				 * to us.
				 *
				 * Assert the case: if we remove an underlying
				 * overflow object, checkpoint reconciliation
				 * should never see it again, there should be a
				 * visible update in the way.
				 *
				 * Write a placeholder.
				 */
				 WT_ASSERT(session,
				     F_ISSET(r, WT_EVICT_UPDATE_RESTORE));

				data = "@";
				size = 1;
			} else {
				update_no_copy = false;	/* Maybe data copy */

				/*
				 * The repeat count is the number of records up
				 * to the next WT_INSERT record, or up to the
				 * end of the entry if we have no more WT_INSERT
				 * records.
				 */
				if (ins == NULL)
					repeat_count = nrepeat - n;
				else
					repeat_count =
					    WT_INSERT_RECNO(ins) - src_recno;

				deleted = orig_deleted;
				if (deleted)
					goto compare;

				/*
				 * If we are handling overflow items, use the
				 * overflow item itself exactly once, after
				 * which we have to copy it into a buffer and
				 * from then on use a complete copy because we
				 * are re-creating a new overflow record each
				 * time.
				 */
				switch (ovfl_state) {
				case OVFL_UNUSED:
					/*
					 * An as-yet-unused overflow item.
					 *
					 * We're going to copy the on-page cell,
					 * write out any record we're tracking.
					 */
					if (rle != 0) {
						WT_ERR(__rec_col_var_helper(
						    session, r, salvage, last,
						    last_deleted, 0, rle));
						rle = 0;
					}

					last->data = vpack->data;
					last->size = vpack->size;
					WT_ERR(__rec_col_var_helper(
					    session, r, salvage, last, false,
					    WT_CELL_VALUE_OVFL, repeat_count));

					/* Track if page has overflow items. */
					r->ovfl_items = true;

					ovfl_state = OVFL_USED;
					continue;
				case OVFL_USED:
					/*
					 * Original is an overflow item; we used
					 * it for a key and now we need another
					 * copy; read it into memory.
					 */
					WT_ERR(__wt_dsk_cell_data_ref(session,
					    WT_PAGE_COL_VAR, vpack, orig));

					ovfl_state = OVFL_IGNORE;
					/* FALLTHROUGH */
				case OVFL_IGNORE:
					/*
					 * Original is an overflow item and we
					 * were forced to copy it into memory,
					 * or the original wasn't an overflow
					 * item; use the data copied into orig.
					 */
					data = orig->data;
					size = (uint32_t)orig->size;
					break;
				}
			}

compare:		/*
			 * If we have a record against which to compare, and
			 * the records compare equal, increment the rle counter
			 * and continue.  If the records don't compare equal,
			 * output the last record and swap the last and current
			 * buffers: do NOT update the starting record number,
			 * we've been doing that all along.
			 */
			if (rle != 0) {
				if ((deleted && last_deleted) ||
				    (!last_deleted && !deleted &&
				    last->size == size &&
				    memcmp(last->data, data, size) == 0)) {
					rle += repeat_count;
					continue;
				}
				WT_ERR(__rec_col_var_helper(session, r,
				    salvage, last, last_deleted, 0, rle));
			}

			/*
			 * Swap the current/last state.
			 *
			 * Reset RLE counter and turn on comparisons.
			 */
			if (!deleted) {
				/*
				 * We can't simply assign the data values into
				 * the last buffer because they may have come
				 * from a copy built from an encoded/overflow
				 * cell and creating the next record is going
				 * to overwrite that memory.  Check, because
				 * encoded/overflow cells aren't that common
				 * and we'd like to avoid the copy.  If data
				 * was taken from the current unpack structure
				 * (which points into the page), or was taken
				 * from an update structure, we can just use
				 * the pointers, they're not moving.
				 */
				if (data == vpack->data || update_no_copy) {
					last->data = data;
					last->size = size;
				} else
					WT_ERR(__wt_buf_set(
					    session, last, data, size));
			}
			last_deleted = deleted;
			rle = repeat_count;
		}

		/*
		 * If we had a reference to an overflow record we never used,
		 * discard the underlying blocks, they're no longer useful.
		 *
		 * One complication: we must cache a copy before discarding the
		 * on-disk version if there's a transaction in the system that
		 * might read the original value.
		 */
		if (ovfl_state == OVFL_UNUSED &&
		    vpack->raw != WT_CELL_VALUE_OVFL_RM)
			WT_ERR(__wt_ovfl_cache(session, page, upd, vpack));
	}

	/* Walk any append list. */
	WT_SKIP_FOREACH(ins, WT_COL_APPEND(page)) {
		WT_ERR(__wt_rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL)
			continue;
		for (n = WT_INSERT_RECNO(ins); src_recno <= n; ++src_recno) {
			/*
			 * The application may have inserted records which left
			 * gaps in the name space, and these gaps can be huge.
			 * If we're in a set of deleted records, skip the boring
			 * part.
			 */
			if (src_recno < n) {
				deleted = true;
				if (last_deleted) {
					/*
					 * The record adjustment is decremented
					 * by one so we can naturally fall into
					 * the RLE accounting below, where we
					 * increment rle by one, then continue
					 * in the outer loop, where we increment
					 * src_recno by one.
					 */
					skip = (n - src_recno) - 1;
					rle += skip;
					src_recno += skip;
				}
			} else {
				deleted = WT_UPDATE_DELETED_ISSET(upd);
				if (!deleted) {
					data = WT_UPDATE_DATA(upd);
					size = upd->size;
				}
			}

			/*
			 * Handle RLE accounting and comparisons -- see comment
			 * above, this code fragment does the same thing.
			 */
			if (rle != 0) {
				if ((deleted && last_deleted) ||
				    (!last_deleted && !deleted &&
				    last->size == size &&
				    memcmp(last->data, data, size) == 0)) {
					++rle;
					continue;
				}
				WT_ERR(__rec_col_var_helper(session, r,
				    salvage, last, last_deleted, 0, rle));
			}

			/*
			 * Swap the current/last state.  We always assign the
			 * data values to the buffer because they can only be
			 * the data from a WT_UPDATE structure.
			 *
			 * Reset RLE counter and turn on comparisons.
			 */
			if (!deleted) {
				last->data = data;
				last->size = size;
			}
			last_deleted = deleted;
			rle = 1;
		}
	}

	/* If we were tracking a record, write it. */
	if (rle != 0)
		WT_ERR(__rec_col_var_helper(
		    session, r, salvage, last, last_deleted, 0, rle));

	/* Write the remnant page. */
	ret = __wt_rec_split_finish(session, r);

err:	__wt_scr_free(session, &orig);
	return (ret);
}

/*
 * __wt_rec_col_split --
 *	Split a column-store page into a set of replacement blocks.
 */
int
__wt_rec_col_split(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BOUNDARY *bnd;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	/* Allocate, then initialize the array of replacement blocks. */
	WT_RET(__wt_calloc_def(session, r->bnd_next, &mod->mod_multi));

	for (multi = mod->mod_multi,
	    bnd = r->bnd, i = 0; i < r->bnd_next; ++multi, ++bnd, ++i) {
		multi->key.recno = bnd->recno;

		if (F_ISSET(r, WT_EVICT_UPDATE_RESTORE) && bnd->supd != NULL) {
			multi->supd = bnd->supd;
			multi->supd_entries = bnd->supd_next;
			bnd->supd = NULL;
			multi->supd_dsk = bnd->dsk;
			bnd->dsk = NULL;
		} else {
			multi->addr = bnd->addr;
			multi->addr.reuse = 0;
			multi->size = bnd->size;
			multi->cksum = bnd->cksum;
			bnd->addr.addr = NULL;
		}
	}
	mod->mod_multi_entries = r->bnd_next;

	return (0);
}

