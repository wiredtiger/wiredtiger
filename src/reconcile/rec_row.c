/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int  __rec_row_leaf_insert(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_INSERT *);
static int  __rec_row_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);

/*
 * __wt_rec_row_key_state_update --
 *	Update prefix and suffix compression based on the last key.
 */
inline void
__wt_rec_row_key_state_update(WT_RECONCILE *r, bool ovfl_key)
{
	WT_ITEM *a;

	/*
	 * If writing an overflow key onto the page, don't update the "last key"
	 * value, and leave the state of prefix compression alone.  (If we are
	 * currently doing prefix compression, we have a key state which will
	 * continue to work, we're just skipping the key just created because
	 * it's an overflow key and doesn't participate in prefix compression.
	 * If we are not currently doing prefix compression, we can't start, an
	 * overflow key doesn't give us any state.)
	 *
	 * Additionally, if we wrote an overflow key onto the page, turn off the
	 * suffix compression of row-store internal node keys.  (When we split,
	 * "last key" is the largest key on the previous page, and "cur key" is
	 * the first key on the next page, which is being promoted.  In some
	 * cases we can discard bytes from the "cur key" that are not needed to
	 * distinguish between the "last key" and "cur key", compressing the
	 * size of keys on internal nodes.  If we just built an overflow key,
	 * we're not going to update the "last key", making suffix compression
	 * impossible for the next key. Alternatively, we could remember where
	 * the last key was on the page, detect it's an overflow key, read it
	 * from disk and do suffix compression, but that's too much work for an
	 * unlikely event.)
	 *
	 * If we're not writing an overflow key on the page, update the last-key
	 * value and turn on both prefix and suffix compression.
	 */
	if (ovfl_key)
		r->key_sfx_compress = false;
	else {
		a = r->cur;
		r->cur = r->last;
		r->last = a;

		r->key_pfx_compress = r->key_pfx_compress_conf;
		r->key_sfx_compress = r->key_sfx_compress_conf;
	}
}

/*
 * __wt_rec_row_int --
 *	Reconcile a row-store internal page.
 */
int
__wt_rec_row_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_CHILD_STATE state;
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_KV *key, *val;
	WT_PAGE *child;
	WT_REF *ref;
	size_t size;
	u_int vtype;
	bool hazard, key_onpage_ovfl, ovfl_key;
	const void *p;

	btree = S2BT(session);
	child = NULL;
	hazard = false;

	key = &r->k;
	kpack = &_kpack;
	WT_CLEAR(*kpack);	/* -Wuninitialized */
	val = &r->v;
	vpack = &_vpack;
	WT_CLEAR(*vpack);	/* -Wuninitialized */

	ikey = NULL;		/* -Wuninitialized */
	cell = NULL;
	key_onpage_ovfl = false;

	WT_RET(__wt_rec_split_init(session, r, page, 0ULL, btree->maxintlpage));

	/*
	 * Ideally, we'd never store the 0th key on row-store internal pages
	 * because it's never used during tree search and there's no reason
	 * to waste the space.  The problem is how we do splits: when we split,
	 * we've potentially picked out several "split points" in the buffer
	 * which is overflowing the maximum page size, and when the overflow
	 * happens, we go back and physically split the buffer, at those split
	 * points, into new pages.  It would be both difficult and expensive
	 * to re-process the 0th key at each split point to be an empty key,
	 * so we don't do that.  However, we are reconciling an internal page
	 * for whatever reason, and the 0th key is known to be useless.  We
	 * truncate the key to a single byte, instead of removing it entirely,
	 * it simplifies various things in other parts of the code (we don't
	 * have to special case transforming the page from its disk image to
	 * its in-memory version, for example).
	 */
	r->cell_zero = true;

	/* For each entry in the in-memory page... */
	WT_INTL_FOREACH_BEGIN(session, page, ref) {
		/*
		 * There are different paths if the key is an overflow item vs.
		 * a straight-forward on-page value. If an overflow item, we
		 * would have instantiated it, and we can use that fact to set
		 * things up.
		 *
		 * Note the cell reference and unpacked key cell are available
		 * only in the case of an instantiated, off-page key, we don't
		 * bother setting them if that's not possible.
		 */
		if (F_ISSET_ATOMIC(page, WT_PAGE_OVERFLOW_KEYS)) {
			cell = NULL;
			key_onpage_ovfl = false;
			ikey = __wt_ref_key_instantiated(ref);
			if (ikey != NULL && ikey->cell_offset != 0) {
				cell =
				    WT_PAGE_REF_OFFSET(page, ikey->cell_offset);
				__wt_cell_unpack(cell, kpack);
				key_onpage_ovfl = kpack->ovfl &&
				    kpack->raw != WT_CELL_KEY_OVFL_RM;
			}
		}

		WT_ERR(__wt_rec_child_modify(session, r, ref, &hazard, &state));
		addr = ref->addr;
		child = ref->page;

		switch (state) {
		case WT_CHILD_IGNORE:
			/*
			 * Deleted child we don't have to write.
			 *
			 * Overflow keys referencing discarded pages are no
			 * longer useful, schedule them for discard.  Don't
			 * worry about instantiation, internal page keys are
			 * always instantiated.  Don't worry about reuse,
			 * reusing this key in this reconciliation is unlikely.
			 */
			if (key_onpage_ovfl)
				WT_ERR(__wt_ovfl_discard_add(
				    session, page, kpack->cell));
			WT_CHILD_RELEASE_ERR(session, hazard, ref);
			continue;

		case WT_CHILD_MODIFIED:
			/*
			 * Modified child.  Empty pages are merged into the
			 * parent and discarded.
			 */
			switch (child->modify->rec_result) {
			case WT_PM_REC_EMPTY:
				/*
				 * Overflow keys referencing empty pages are no
				 * longer useful, schedule them for discard.
				 * Don't worry about instantiation, internal
				 * page keys are always instantiated.  Don't
				 * worry about reuse, reusing this key in this
				 * reconciliation is unlikely.
				 */
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_MULTIBLOCK:
				/*
				 * Overflow keys referencing split pages are no
				 * longer useful (the split page's key is the
				 * interesting key); schedule them for discard.
				 * Don't worry about instantiation, internal
				 * page keys are always instantiated.  Don't
				 * worry about reuse, reusing this key in this
				 * reconciliation is unlikely.
				 */
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));

				WT_ERR(__rec_row_merge(session, r, child));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_REPLACE:
				/*
				 * If the page is replaced, the page's modify
				 * structure has the page's address.
				 */
				addr = &child->modify->mod_replace;
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			break;
		case WT_CHILD_ORIGINAL:
			/* Original child. */
			break;
		case WT_CHILD_PROXY:
			/* Deleted child where we write a proxy cell. */
			break;
		}

		/*
		 * Build the value cell, the child page's address.  Addr points
		 * to an on-page cell or an off-page WT_ADDR structure. There's
		 * a special cell type in the case of page deletion requiring
		 * a proxy cell, otherwise use the information from the addr or
		 * original cell.
		 */
		if (__wt_off_page(page, addr)) {
			p = addr->addr;
			size = addr->size;
			vtype = state == WT_CHILD_PROXY ?
			    WT_CELL_ADDR_DEL : __wt_rec_vtype(addr);
		} else {
			__wt_cell_unpack(ref->addr, vpack);
			p = vpack->data;
			size = vpack->size;
			vtype = state == WT_CHILD_PROXY ?
			    WT_CELL_ADDR_DEL : (u_int)vpack->raw;
		}
		__wt_rec_cell_build_addr(r, p, size, vtype, WT_RECNO_OOB);
		WT_CHILD_RELEASE_ERR(session, hazard, ref);

		/*
		 * Build key cell.
		 * Truncate any 0th key, internal pages don't need 0th keys.
		 */
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = true;
		} else {
			__wt_ref_key(page, ref, &p, &size);
			WT_ERR(__wt_rec_cell_build_int_key(
			    session, r, p, r->cell_zero ? 1 : size, &ovfl_key));
		}
		r->cell_zero = false;

		/* Boundary: split or write the page. */
		if (key->len + val->len > r->space_avail) {
			if (r->raw_compression)
				WT_ERR(__wt_rec_split_raw(
				    session, r, key->len + val->len));
			else {
				/*
				 * In one path above, we copied address blocks
				 * from the page rather than building the actual
				 * key.  In that case, we have to build the key
				 * now because we are about to promote it.
				 */
				if (key_onpage_ovfl) {
					WT_ERR(__wt_buf_set(session, r->cur,
					    WT_IKEY_DATA(ikey), ikey->size));
					key_onpage_ovfl = false;
				}
				WT_ERR(__wt_rec_split(
				    session, r, key->len + val->len));
			}
		}

		/* Copy the key and value onto the page. */
		__wt_rec_copy_incr(session, r, key);
		__wt_rec_copy_incr(session, r, val);

		/* Update compression state. */
		__wt_rec_row_key_state_update(r, ovfl_key);
	} WT_INTL_FOREACH_END;

	/* Write the remnant page. */
	return (__wt_rec_split_finish(session, r));

err:	WT_CHILD_RELEASE(session, hazard, ref);
	return (ret);
}

/*
 * __rec_row_merge --
 *	Merge in a split page.
 */
static int
__rec_row_merge(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_KV *key, *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;
	bool ovfl_key;

	mod = page->modify;

	key = &r->k;
	val = &r->v;

	/* For each entry in the split array... */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		/* Build the key and value cells. */
		WT_RET(__wt_rec_cell_build_int_key(session, r,
		    WT_IKEY_DATA(multi->key.ikey),
		    r->cell_zero ? 1 : multi->key.ikey->size, &ovfl_key));
		r->cell_zero = false;

		addr = &multi->addr;
		__wt_rec_cell_build_addr(r,
		    addr->addr, addr->size, __wt_rec_vtype(addr), WT_RECNO_OOB);

		/* Boundary: split or write the page. */
		if (key->len + val->len > r->space_avail)
			WT_RET(r->raw_compression ?
			    __wt_rec_split_raw(session,
			    r, key->len + val->len) :
			    __wt_rec_split(session, r, key->len + val->len));

		/* Copy the key and value onto the page. */
		__wt_rec_copy_incr(session, r, key);
		__wt_rec_copy_incr(session, r, val);

		/* Update compression state. */
		__wt_rec_row_key_state_update(r, ovfl_key);
	}
	return (0);
}

/*
 * __rec_row_bnd_split_or_write --
 *     Split or write the page if we are at a boundary
 */
static int
__rec_row_bnd_split_or_write(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_KV *key, WT_KV *val, WT_CELL_UNPACK *kpack,
    bool key_onpage_ovfl, bool ovfl_key)
{
	/* Nothing to do if this isn't a boundary */
	if (key->len + val->len <= r->space_avail)
		return (0);

	if (r->raw_compression)
		WT_RET(__wt_rec_split_raw(
		    session, r, key->len + val->len));
	else {
		/*
		 * If we copied address blocks from the page rather than
		 * building the actual key  we have to build the key now
		 * because we are about to promote it.
		 */
		if (key_onpage_ovfl) {
			WT_ASSERT(session, kpack != NULL);
			WT_RET(__wt_dsk_cell_data_ref(session,
			    WT_PAGE_ROW_LEAF, kpack, r->cur));
			key_onpage_ovfl = false;
		}

		/*
		 * Turn off prefix compression until a full key written to the
		 * new page, and (unless already working with an overflow key),
		 * rebuild the key without compression.
		 */
		if (r->key_pfx_compress_conf) {
			r->key_pfx_compress = false;
			if (!ovfl_key)
				WT_RET(
				    __wt_rec_cell_build_leaf_key(
				    session,
				    r, NULL, 0, &ovfl_key));
		}

		WT_RET(__wt_rec_split(
		    session, r, key->len + val->len));
	}
	return (0);
}

/*
 * __wt_rec_row_leaf --
 *	Reconcile a row-store leaf page.
 */
int
__wt_rec_row_leaf(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;
	WT_CELL *cell, *val_cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_DECL_ITEM(tmpkey);
	WT_DECL_ITEM(tmpval);
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_INSERT *ins;
	WT_KV *key, *val;
	WT_ROW *rip;
	WT_UPDATE *upd;
	size_t size;
	uint64_t slvg_skip;
	uint32_t i;
	bool dictionary, key_onpage_ovfl, ovfl_key;
	const void *p;
	void *copy;

	btree = S2BT(session);
	slvg_skip = salvage == NULL ? 0 : salvage->skip;

	key = &r->k;
	val = &r->v;

	WT_RET(__wt_rec_split_init(session, r, page, 0ULL, btree->maxleafpage));

	/*
	 * Write any K/V pairs inserted into the page before the first from-disk
	 * key on the page.
	 */
	if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SMALLEST(page))) != NULL)
		WT_RET(__rec_row_leaf_insert(session, r, ins));

	/*
	 * Temporary buffers in which to instantiate any uninstantiated keys
	 * or value items we need.
	 */
	WT_RET(__wt_scr_alloc(session, 0, &tmpkey));
	WT_RET(__wt_scr_alloc(session, 0, &tmpval));

	/* For each entry in the page... */
	WT_ROW_FOREACH(page, rip, i) {
		/*
		 * The salvage code, on some rare occasions, wants to reconcile
		 * a page but skip some leading records on the page.  Because
		 * the row-store leaf reconciliation function copies keys from
		 * the original disk page, this is non-trivial -- just changing
		 * the in-memory pointers isn't sufficient, we have to change
		 * the WT_CELL structures on the disk page, too.  It's ugly, but
		 * we pass in a value that tells us how many records to skip in
		 * this case.
		 */
		if (slvg_skip != 0) {
			--slvg_skip;
			continue;
		}

		/*
		 * Figure out the key: set any cell reference (and unpack it),
		 * set any instantiated key reference.
		 */
		copy = WT_ROW_KEY_COPY(rip);
		(void)__wt_row_leaf_key_info(
		    page, copy, &ikey, &cell, NULL, NULL);
		if (cell == NULL)
			kpack = NULL;
		else {
			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
		}

		/* Unpack the on-page value cell, and look for an update. */
		if ((val_cell =
		    __wt_row_leaf_value_cell(page, rip, NULL)) == NULL)
			vpack = NULL;
		else {
			vpack = &_vpack;
			__wt_cell_unpack(val_cell, vpack);
		}
		WT_ERR(__wt_rec_txn_read(session, r, NULL, rip, vpack, &upd));

		/* Build value cell. */
		dictionary = false;
		if (upd == NULL) {
			/*
			 * When the page was read into memory, there may not
			 * have been a value item.
			 *
			 * If there was a value item, check if it's a dictionary
			 * cell (a copy of another item on the page).  If it's a
			 * copy, we have to create a new value item as the old
			 * item might have been discarded from the page.
			 */
			if (vpack == NULL) {
				val->buf.data = NULL;
				val->cell_len = val->len = val->buf.size = 0;
			} else if (vpack->raw == WT_CELL_VALUE_COPY) {
				/* If the item is Huffman encoded, decode it. */
				if (btree->huffman_value == NULL) {
					p = vpack->data;
					size = vpack->size;
				} else {
					WT_ERR(__wt_huffman_decode(session,
					    btree->huffman_value,
					    vpack->data, vpack->size,
					    tmpval));
					p = tmpval->data;
					size = tmpval->size;
				}
				WT_ERR(__wt_rec_cell_build_val(
				    session, r, p, size, (uint64_t)0));
				dictionary = true;
			} else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				/*
				 * If doing update save and restore in service
				 * of eviction, there's an update that's not
				 * globally visible, and the underlying value
				 * is a removed overflow value, we end up here.
				 *
				 * When the update save/restore code noticed the
				 * removed overflow value, it appended a copy of
				 * the cached, original overflow value to the
				 * update list being saved (ensuring any on-page
				 * item will never be accessed after the page is
				 * re-instantiated), then returned a NULL update
				 * to us.
				 *
				 * Assert the case.
				 */
				WT_ASSERT(session,
				    F_ISSET(r, WT_EVICT_UPDATE_RESTORE));

				/*
				 * If the key is also a removed overflow item,
				 * don't write anything at all.
				 *
				 * We don't have to write anything because the
				 * code re-instantiating the page gets the key
				 * to match the saved list of updates from the
				 * original page.  By not putting the key on
				 * the page, we'll move the key/value set from
				 * a row-store leaf page slot to an insert list,
				 * but that shouldn't matter.
				 *
				 * The reason we bother with the test is because
				 * overflows are expensive to write.  It's hard
				 * to imagine a real workload where this test is
				 * worth the effort, but it's a simple test.
				 */
				if (kpack != NULL &&
				    kpack->raw == WT_CELL_KEY_OVFL_RM)
					goto leaf_insert;

				/*
				 * The on-page value will never be accessed,
				 * write a placeholder record.
				 */
				WT_ERR(__wt_rec_cell_build_val(
				    session, r, "@", 1, (uint64_t)0));
			} else {
				val->buf.data = val_cell;
				val->buf.size = __wt_cell_total_len(vpack);
				val->cell_len = 0;
				val->len = val->buf.size;

				/* Track if page has overflow items. */
				if (vpack->ovfl)
					r->ovfl_items = true;
			}
		} else {
			/*
			 * If the original value was an overflow and we've not
			 * already done so, discard it.  One complication: we
			 * must cache a copy before discarding the on-disk
			 * version if there's a transaction in the system that
			 * might read the original value.
			 */
			if (vpack != NULL &&
			    vpack->ovfl && vpack->raw != WT_CELL_VALUE_OVFL_RM)
				WT_ERR(
				    __wt_ovfl_cache(session, page, rip, vpack));

			/* If this key/value pair was deleted, we're done. */
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				/*
				 * Overflow keys referencing discarded values
				 * are no longer useful, discard the backing
				 * blocks.  Don't worry about reuse, reusing
				 * keys from a row-store page reconciliation
				 * seems unlikely enough to ignore.
				 */
				if (kpack != NULL && kpack->ovfl &&
				    kpack->raw != WT_CELL_KEY_OVFL_RM) {
					/*
					 * Keys are part of the name-space, we
					 * can't remove them from the in-memory
					 * tree; if an overflow key was deleted
					 * without being instantiated (for
					 * example, cursor-based truncation, do
					 * it now.
					 */
					if (ikey == NULL)
						WT_ERR(__wt_row_leaf_key(
						    session,
						    page, rip, tmpkey, true));

					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));
				}

				/*
				 * We aren't actually creating the key so we
				 * can't use bytes from this key to provide
				 * prefix information for a subsequent key.
				 */
				tmpkey->size = 0;

				/* Proceed with appended key/value pairs. */
				goto leaf_insert;
			}

			/*
			 * If no value, nothing needs to be copied.  Otherwise,
			 * build the value's WT_CELL chunk from the most recent
			 * update value.
			 */
			if (upd->size == 0) {
				val->buf.data = NULL;
				val->cell_len = val->len = val->buf.size = 0;
			} else {
				WT_ERR(__wt_rec_cell_build_val(session, r,
				    WT_UPDATE_DATA(upd), upd->size,
				    (uint64_t)0));
				dictionary = true;
			}
		}

		/*
		 * Build key cell.
		 *
		 * If the key is an overflow key that hasn't been removed, use
		 * the original backing blocks.
		 */
		key_onpage_ovfl = kpack != NULL &&
		    kpack->ovfl && kpack->raw != WT_CELL_KEY_OVFL_RM;
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = true;

			/*
			 * We aren't creating a key so we can't use this key as
			 * a prefix for a subsequent key.
			 */
			tmpkey->size = 0;

			/* Track if page has overflow items. */
			r->ovfl_items = true;
		} else {
			/*
			 * Get the key from the page or an instantiated key, or
			 * inline building the key from a previous key (it's a
			 * fast path for simple, prefix-compressed keys), or by
			 * by building the key from scratch.
			 */
			if (__wt_row_leaf_key_info(page, copy,
			    NULL, &cell, &tmpkey->data, &tmpkey->size))
				goto build;

			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
			if (btree->huffman_key == NULL &&
			    kpack->type == WT_CELL_KEY &&
			    tmpkey->size >= kpack->prefix) {
				/*
				 * The previous clause checked for a prefix of
				 * zero, which means the temporary buffer must
				 * have a non-zero size, and it references a
				 * valid key.
				 */
				WT_ASSERT(session, tmpkey->size != 0);

				/*
				 * Grow the buffer as necessary, ensuring data
				 * data has been copied into local buffer space,
				 * then append the suffix to the prefix already
				 * in the buffer.
				 *
				 * Don't grow the buffer unnecessarily or copy
				 * data we don't need, truncate the item's data
				 * length to the prefix bytes.
				 */
				tmpkey->size = kpack->prefix;
				WT_ERR(__wt_buf_grow(session,
				    tmpkey, tmpkey->size + kpack->size));
				memcpy((uint8_t *)tmpkey->mem + tmpkey->size,
				    kpack->data, kpack->size);
				tmpkey->size += kpack->size;
			} else
				WT_ERR(__wt_row_leaf_key_copy(
				    session, page, rip, tmpkey));
build:
			WT_ERR(__wt_rec_cell_build_leaf_key(session, r,
			    tmpkey->data, tmpkey->size, &ovfl_key));
		}

		/* Boundary: split or write the page. */
		WT_ERR(__rec_row_bnd_split_or_write(session,
		    r, key, val, kpack, key_onpage_ovfl, ovfl_key));

		/* Copy the key/value pair onto the page. */
		__wt_rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = true;
		else {
			r->all_empty_value = false;
			if (dictionary && btree->dictionary)
				WT_ERR(__wt_rec_dictionary_replace(
				    session, r, 0, val));
			__wt_rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__wt_rec_row_key_state_update(r, ovfl_key);

leaf_insert:	/* Write any K/V pairs inserted into the page after this key. */
		if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT(page, rip))) != NULL)
		    WT_ERR(__rec_row_leaf_insert(session, r, ins));
	}

	/* Write the remnant page. */
	ret = __wt_rec_split_finish(session, r);

err:	__wt_scr_free(session, &tmpkey);
	__wt_scr_free(session, &tmpval);
	return (ret);
}

/*
 * __rec_row_leaf_insert --
 *	Walk an insert chain, writing K/V pairs.
 */
static int
__rec_row_leaf_insert(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_INSERT *ins)
{
	WT_BTREE *btree;
	WT_KV *key, *val;
	WT_UPDATE *upd;
	bool ovfl_key;

	btree = S2BT(session);

	key = &r->k;
	val = &r->v;

	for (; ins != NULL; ins = WT_SKIP_NEXT(ins)) {
		/* Look for an update. */
		WT_RET(__wt_rec_txn_read(session, r, ins, NULL, NULL, &upd));
		if (upd == NULL || WT_UPDATE_DELETED_ISSET(upd))
			continue;

		if (upd->size == 0)			/* Build value cell. */
			val->len = 0;
		else
			WT_RET(__wt_rec_cell_build_val(session, r,
			    WT_UPDATE_DATA(upd), upd->size, (uint64_t)0));

							/* Build key cell. */
		WT_RET(__wt_rec_cell_build_leaf_key(session, r,
		    WT_INSERT_KEY(ins), WT_INSERT_KEY_SIZE(ins), &ovfl_key));

		/* Boundary: split or write the page. */
		WT_RET(__rec_row_bnd_split_or_write(session,
		    r, key, val, NULL, false, ovfl_key));

		/* Copy the key/value pair onto the page. */
		__wt_rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = true;
		else {
			r->all_empty_value = false;
			if (btree->dictionary)
				WT_RET(__wt_rec_dictionary_replace(
				    session, r, 0, val));
			__wt_rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__wt_rec_row_key_state_update(r, ovfl_key);
	}

	return (0);
}

/*
 * __wt_rec_row_split --
 *	Split a row-store page into a set of replacement blocks.
 */
int
__wt_rec_row_split(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BOUNDARY *bnd;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	WT_REF *ref;
	uint32_t i;
	size_t size;
	void *p;

	mod = page->modify;

	/* We never set the first page's key, grab it from the original page. */
	ref = r->ref;
	if (__wt_ref_is_root(ref))
		WT_RET(__wt_buf_set(session, &r->bnd[0].key, "", 1));
	else {
		__wt_ref_key(ref->home, ref, &p, &size);
		WT_RET(__wt_buf_set(session, &r->bnd[0].key, p, size));
	}

	/* Allocate, then initialize the array of replacement blocks. */
	WT_RET(__wt_calloc_def(session, r->bnd_next, &mod->mod_multi));

	for (multi = mod->mod_multi,
	    bnd = r->bnd, i = 0; i < r->bnd_next; ++multi, ++bnd, ++i) {
		WT_RET(__wt_row_ikey_alloc(session, 0,
		    bnd->key.data, bnd->key.size, &multi->key.ikey));

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
