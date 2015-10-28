/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static void __rec_bnd_cleanup(WT_SESSION_IMPL *, WT_RECONCILE *, bool);
static int  __rec_destroy_session(WT_SESSION_IMPL *);
static int  __rec_root_write(WT_SESSION_IMPL *, WT_PAGE *, uint32_t);
static int  __rec_write_wrapup_err(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);

/*
 * __wt_reconcile --
 *	Reconcile an in-memory page into its on-disk format, and write it.
 */
int
__wt_reconcile(WT_SESSION_IMPL *session,
    WT_REF *ref, WT_SALVAGE_COOKIE *salvage, uint32_t flags)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	WT_RECONCILE *r;

	page = ref->page;
	mod = page->modify;

	WT_RET(__wt_verbose(session,
	    WT_VERB_RECONCILE, "%s", __wt_page_type_string(page->type)));

	/* We shouldn't get called with a clean page, that's an error. */
	WT_ASSERT(session, __wt_page_is_modified(page));

#ifdef HAVE_DIAGNOSTIC
	{
	/*
	 * Check that transaction time always moves forward for a given page.
	 * If this check fails, reconciliation can free something that a future
	 * reconciliation will need.
	 */
	uint64_t oldest_id = __wt_txn_oldest_id(session);
	WT_ASSERT(session, WT_TXNID_LE(mod->last_oldest_id, oldest_id));
	mod->last_oldest_id = oldest_id;
	}
#endif

	/* Record the most recent transaction ID we will *not* write. */
	mod->disk_snap_min = session->txn.snap_min;

	/* Initialize the reconciliation structure for each new run. */
	WT_RET(__wt_rec_write_init(
	    session, ref, flags, salvage, &session->reconcile));
	r = session->reconcile;

	/*
	 * Reconciliation locks the page for three reasons:
	 *    Reconciliation reads the lists of page updates, obsolete updates
	 * cannot be discarded while reconciliation is in progress;
	 *    The compaction process reads page modification information, which
	 * reconciliation modifies;
	 *    In-memory splits: reconciliation of an internal page cannot handle
	 * a child page splitting during the reconciliation.
	 */
	WT_RET(__wt_fair_lock(session, &page->page_lock));

	/* Reconcile the page. */
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		if (salvage != NULL)
			ret = __wt_rec_col_fix_slvg(session, r, page, salvage);
		else
			ret = __wt_rec_col_fix(session, r, page);
		break;
	case WT_PAGE_COL_INT:
		WT_WITH_PAGE_INDEX(session,
		    ret = __wt_rec_col_int(session, r, page));
		break;
	case WT_PAGE_COL_VAR:
		ret = __wt_rec_col_var(session, r, page, salvage);
		break;
	case WT_PAGE_ROW_INT:
		WT_WITH_PAGE_INDEX(session,
		    ret = __wt_rec_row_int(session, r, page));
		break;
	case WT_PAGE_ROW_LEAF:
		ret = __wt_rec_row_leaf(session, r, page, salvage);
		break;
	WT_ILLEGAL_VALUE_SET(session);
	}

	/* Get the final status for the reconciliation. */
	if (ret == 0)
		ret = __wt_rec_write_status(session, r, page);

	/* Wrap up the page reconciliation. */
	if (ret == 0)
		ret = __wt_rec_write_wrapup(session, r, page);
	else
		WT_TRET(__rec_write_wrapup_err(session, r, page));

	/* Release the reconciliation lock. */
	WT_TRET(__wt_fair_unlock(session, &page->page_lock));

	/* Update statistics. */
	WT_STAT_FAST_CONN_INCR(session, rec_pages);
	WT_STAT_FAST_DATA_INCR(session, rec_pages);
	if (LF_ISSET(WT_EVICTING)) {
		WT_STAT_FAST_CONN_INCR(session, rec_pages_eviction);
		WT_STAT_FAST_DATA_INCR(session, rec_pages_eviction);
	}
	if (r->cache_write_lookaside) {
		WT_STAT_FAST_CONN_INCR(session, cache_write_lookaside);
		WT_STAT_FAST_DATA_INCR(session, cache_write_lookaside);
	}
	if (r->cache_write_restore) {
		WT_STAT_FAST_CONN_INCR(session, cache_write_restore);
		WT_STAT_FAST_DATA_INCR(session, cache_write_restore);
	}

	/*
	 * Clean up reconciliation resources: some workloads have millions of
	 * boundary structures, and if associated with an application session
	 * pulled into doing forced eviction, they won't be discarded for the
	 * life of the session (or until session.reset is called). Discard all
	 * of the reconciliation resources if an application thread, not doing
	 * a checkpoint.
	 */
	__rec_bnd_cleanup(session, r,
	    F_ISSET(session, WT_SESSION_INTERNAL) ||
	    WT_SESSION_IS_CHECKPOINT(session) ? false : true);

	WT_RET(ret);

	/*
	 * Root pages are special, splits have to be done, we can't put it off
	 * as the parent's problem any more.
	 */
	if (__wt_ref_is_root(ref)) {
		WT_WITH_PAGE_INDEX(session,
		    ret = __rec_root_write(session, page, flags));
		return (ret);
	}

	/*
	 * Otherwise, mark the page's parent dirty.
	 * Don't mark the tree dirty: if this reconciliation is in service of a
	 * checkpoint, it's cleared the tree's dirty flag, and we don't want to
	 * set it again as part of that walk.
	 */
	return (__wt_page_parent_modify_set(session, ref, true));
}

/*
 * __rec_las_checkpoint_test --
 *	Return if the lookaside table is going to collide with a checkpoint.
 */
static inline bool
__rec_las_checkpoint_test(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_CONNECTION_IMPL *conn;
	WT_BTREE *btree;

	conn = S2C(session);
	btree = S2BT(session);

	/*
	 * Running checkpoints can collide with the lookaside table because
	 * reconciliation using the lookaside table writes the key's last
	 * committed value, which might not be the value checkpoint would write.
	 * If reconciliation was configured for lookaside table eviction, this
	 * file participates in checkpoints, and any of the tree or system
	 * transactional generation numbers don't match, there's a possible
	 * collision.
	 *
	 * It's a complicated test, but the alternative is to have checkpoint
	 * drain lookaside table reconciliations, and this isn't a problem for
	 * most workloads.
	 */
	if (!F_ISSET(r, WT_EVICT_LOOKASIDE))
		return (false);
	if (F_ISSET(btree, WT_BTREE_NO_CHECKPOINT))
		return (false);
	if (r->orig_btree_checkpoint_gen == btree->checkpoint_gen &&
	    r->orig_txn_checkpoint_gen == conn->txn_global.checkpoint_gen &&
	    r->orig_btree_checkpoint_gen == r->orig_txn_checkpoint_gen)
		return (false);
	return (true);
}

/*
 * __wt_rec_write_status --
 *	Return the final status for reconciliation.
 */
int
__wt_rec_write_status(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BTREE *btree;
	WT_PAGE_MODIFY *mod;

	btree = S2BT(session);
	mod = page->modify;

	/* Check for a lookaside table and checkpoint collision. */
	if (__rec_las_checkpoint_test(session, r))
		return (EBUSY);

	/*
	 * Set the page's status based on whether or not we cleaned the page.
	 */
	if (r->leave_dirty) {
		/*
		 * The page remains dirty.
		 *
		 * Any checkpoint call cleared the tree's modified flag before
		 * writing pages, so we must explicitly reset it.  We insert a
		 * barrier after the change for clarity (the requirement is the
		 * flag be set before a subsequent checkpoint reads it, and
		 * as the current checkpoint is waiting on this reconciliation
		 * to complete, there's no risk of that happening)
		 */
		btree->modified = 1;
		WT_FULL_BARRIER();

		/*
		 * Eviction should only be here if following the save/restore
		 * eviction path.
		 */
		WT_ASSERT(session,
		    !F_ISSET(r, WT_EVICTING) ||
		    F_ISSET(r, WT_EVICT_UPDATE_RESTORE));
	} else {
		/*
		 * Track the page's maximum transaction ID (used to decide if
		 * we're likely to be able to evict this page in the future).
		 */
		mod->rec_max_txn = r->max_txn;

		/*
		 * Track the tree's maximum transaction ID (used to decide if
		 * it's safe to discard the tree). Reconciliation for eviction
		 * is multi-threaded, only update the tree's maximum transaction
		 * ID when doing a checkpoint. That's sufficient, we only care
		 * about the maximum transaction ID of current updates in the
		 * tree, and checkpoint visits every dirty page in the tree.
		 */
		if (!F_ISSET(r, WT_EVICTING) &&
		    WT_TXNID_LT(btree->rec_max_txn, r->max_txn))
			btree->rec_max_txn = r->max_txn;

		/*
		 * The page only might be clean; if the write generation is
		 * unchanged since reconciliation started, it's clean.
		 *
		 * If the write generation changed, the page has been written
		 * since reconciliation started and remains dirty (that can't
		 * happen when evicting, the page is exclusively locked).
		 */
		if (__wt_atomic_cas32(&mod->write_gen, r->orig_write_gen, 0))
			__wt_cache_dirty_decr(session, page);
		else
			WT_ASSERT(session, !F_ISSET(r, WT_EVICTING));
	}

	return (0);
}

/*
 * __rec_root_write --
 *	Handle the write of a root page.
 */
static int
__rec_root_write(WT_SESSION_IMPL *session, WT_PAGE *page, uint32_t flags)
{
	WT_DECL_RET;
	WT_PAGE *next;
	WT_PAGE_INDEX *pindex;
	WT_PAGE_MODIFY *mod;
	WT_REF fake_ref;
	uint32_t i;

	mod = page->modify;

	/*
	 * If a single root page was written (either an empty page or there was
	 * a 1-for-1 page swap), we've written root and checkpoint, we're done.
	 * If the root page split, write the resulting WT_REF array.  We already
	 * have an infrastructure for writing pages, create a fake root page and
	 * write it instead of adding code to write blocks based on the list of
	 * blocks resulting from a multiblock reconciliation.
	 */
	switch (mod->rec_result) {
	case WT_PM_REC_EMPTY:				/* Page is empty */
	case WT_PM_REC_REPLACE:				/* 1-for-1 page swap */
		return (0);
	case WT_PM_REC_MULTIBLOCK:			/* Multiple blocks */
		break;
	WT_ILLEGAL_VALUE(session);
	}

	WT_RET(__wt_verbose(session, WT_VERB_SPLIT,
	    "root page split -> %" PRIu32 " pages", mod->mod_multi_entries));

	/*
	 * Create a new root page, initialize the array of child references,
	 * mark it dirty, then write it.
	 */
	switch (page->type) {
	case WT_PAGE_COL_INT:
		WT_RET(__wt_page_alloc(session,
		    WT_PAGE_COL_INT, 1, mod->mod_multi_entries, false, &next));
		break;
	case WT_PAGE_ROW_INT:
		WT_RET(__wt_page_alloc(session,
		    WT_PAGE_ROW_INT, 0, mod->mod_multi_entries, false, &next));
		break;
	WT_ILLEGAL_VALUE(session);
	}

	WT_INTL_INDEX_GET(session, next, pindex);
	for (i = 0; i < mod->mod_multi_entries; ++i) {
		/*
		 * There's special error handling required when re-instantiating
		 * pages in memory; it's not needed here, asserted for safety.
		 */
		WT_ASSERT(session, mod->mod_multi[i].supd == NULL);

		WT_ERR(__wt_multi_to_ref(session,
		    next, &mod->mod_multi[i], &pindex->index[i], NULL));
		pindex->index[i]->home = next;
	}

	/*
	 * We maintain a list of pages written for the root in order to free the
	 * backing blocks the next time the root is written.
	 */
	mod->mod_root_split = next;

	/*
	 * Mark the page dirty.
	 * Don't mark the tree dirty: if this reconciliation is in service of a
	 * checkpoint, it's cleared the tree's dirty flag, and we don't want to
	 * set it again as part of that walk.
	 */
	WT_ERR(__wt_page_modify_init(session, next));
	__wt_page_only_modify_set(session, next);

	/*
	 * Fake up a reference structure, and write the next root page.
	 */
	__wt_root_ref_init(&fake_ref, next, page->type == WT_PAGE_COL_INT);
	return (__wt_reconcile(session, &fake_ref, NULL, flags));

err:	__wt_page_out(session, &next);
	return (ret);
}

/*
 * __rec_raw_compression_config --
 *	Configure raw compression.
 */
static inline bool
__rec_raw_compression_config(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;

	btree = S2BT(session);

	/* Check if raw compression configured. */
	if (btree->compressor == NULL ||
	    btree->compressor->compress_raw == NULL)
		return (false);

	/* Only for row-store and variable-length column-store objects. */
	if (page->type == WT_PAGE_COL_FIX)
		return (false);

	/*
	 * Raw compression cannot support dictionary compression. (Technically,
	 * we could still use the raw callback on column-store variable length
	 * internal pages with dictionary compression configured, because
	 * dictionary compression only applies to column-store leaf pages, but
	 * that seems an unlikely use case.)
	 */
	if (btree->dictionary != 0)
		return (false);

	/* Raw compression cannot support prefix compression. */
	if (btree->prefix_compression)
		return (false);

	/*
	 * Raw compression is also turned off during salvage: we can't allow
	 * pages to split during salvage, raw compression has no point if it
	 * can't manipulate the page size.
	 */
	if (salvage != NULL)
		return (false);

	return (true);
}

/*
 * __wt_rec_write_init --
 *	Initialize the reconciliation structure.
 */
int
__wt_rec_write_init(WT_SESSION_IMPL *session,
    WT_REF *ref, uint32_t flags, WT_SALVAGE_COOKIE *salvage, void *reconcilep)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_PAGE *page;
	WT_RECONCILE *r;

	btree = S2BT(session);
	conn = S2C(session);
	page = ref->page;

	if ((r = *(WT_RECONCILE **)reconcilep) == NULL) {
		WT_RET(__wt_calloc_one(session, &r));

		*(WT_RECONCILE **)reconcilep = r;
		session->reconcile_cleanup = __rec_destroy_session;

		/* Connect pointers/buffers. */
		r->cur = &r->_cur;
		r->last = &r->_last;

		/* Disk buffers need to be aligned for writing. */
		F_SET(&r->dsk, WT_ITEM_ALIGNED);
	}

	/* Reconciliation is not re-entrant, make sure that doesn't happen. */
	WT_ASSERT(session, r->ref == NULL);

	/* Remember the configuration. */
	r->ref = ref;
	r->page = page;

	/*
	 * Save the page's write generation before reading the page.
	 * Save the transaction generations before reading the page.
	 * These are all ordered reads, but we only need one.
	 */
	r->orig_btree_checkpoint_gen = btree->checkpoint_gen;
	r->orig_txn_checkpoint_gen = conn->txn_global.checkpoint_gen;
	WT_ORDERED_READ(r->orig_write_gen, page->modify->write_gen);

	/*
	 * Lookaside table eviction is configured when eviction gets aggressive,
	 * adjust the flags for cases we don't support.
	 */
	if (LF_ISSET(WT_EVICT_LOOKASIDE)) {
		/*
		 * Saving lookaside table updates into the lookaside table won't
		 * work.
		 */
		if (F_ISSET(btree, WT_BTREE_LOOKASIDE))
			LF_CLR(WT_EVICT_LOOKASIDE);

		/*
		 * We don't yet support fixed-length column-store combined with
		 * the lookaside table. It's not hard to do, but the underlying
		 * function that reviews which updates can be written to the
		 * evicted page and which updates need to be written to the
		 * lookaside table needs access to the original value from the
		 * page being evicted, and there's no code path for that in the
		 * case of fixed-length column-store objects. (Row-store and
		 * variable-width column-store objects provide a reference to
		 * the unpacked on-page cell for this purpose, but there isn't
		 * an on-page cell for fixed-length column-store objects.) For
		 * now, turn it off.
		 */
		if (page->type == WT_PAGE_COL_FIX)
			LF_CLR(WT_EVICT_LOOKASIDE);

		/*
		 * Check for a lookaside table and checkpoint collision, and if
		 * we find one, turn off the lookaside file (we've gone to all
		 * the effort of getting exclusive access to the page, might as
		 * well try and evict it).
		 */
		if (__rec_las_checkpoint_test(session, r))
			LF_CLR(WT_EVICT_LOOKASIDE);
	}
	r->flags = flags;

	/* Track if the page can be marked clean. */
	r->leave_dirty = false;

	/* Raw compression. */
	r->raw_compression =
	    __rec_raw_compression_config(session, page, salvage);
	r->raw_destination.flags = WT_ITEM_ALIGNED;

	/* Track overflow items. */
	r->ovfl_items = false;

	/* Track empty values. */
	r->all_empty_value = true;
	r->any_empty_value = false;

	/* The list of saved updates. */
	r->supd_next = 0;

	/*
	 * Dictionary compression only writes repeated values once.  We grow
	 * the dictionary as necessary, always using the largest size we've
	 * seen.
	 *
	 * Reset the dictionary.
	 *
	 * Sanity check the size: 100 slots is the smallest dictionary we use.
	 */
	if (btree->dictionary != 0 && btree->dictionary > r->dictionary_slots)
		WT_RET(__wt_rec_dictionary_init(session,
		    r, btree->dictionary < 100 ? 100 : btree->dictionary));
	__wt_rec_dictionary_reset(r);

	/*
	 * Prefix compression discards repeated prefix bytes from row-store leaf
	 * page keys.
	 */
	r->key_pfx_compress_conf = false;
	if (btree->prefix_compression && page->type == WT_PAGE_ROW_LEAF)
		r->key_pfx_compress_conf = true;

	/*
	 * Suffix compression shortens internal page keys by discarding trailing
	 * bytes that aren't necessary for tree navigation.  We don't do suffix
	 * compression if there is a custom collator because we don't know what
	 * bytes a custom collator might use.  Some custom collators (for
	 * example, a collator implementing reverse ordering of strings), won't
	 * have any problem with suffix compression: if there's ever a reason to
	 * implement suffix compression for custom collators, we can add a
	 * setting to the collator, configured when the collator is added, that
	 * turns on suffix compression.
	 *
	 * The raw compression routines don't even consider suffix compression,
	 * but it doesn't hurt to confirm that.
	 */
	r->key_sfx_compress_conf = false;
	if (btree->collator == NULL &&
	    btree->internal_key_truncate && !r->raw_compression)
		r->key_sfx_compress_conf = true;

	r->is_bulk_load = false;

	r->salvage = salvage;

	r->cache_write_lookaside = r->cache_write_restore = false;

	return (0);
}

/*
 * __wt_rec_destroy --
 *	Clean up the reconciliation structure.
 */
void
__wt_rec_destroy(WT_SESSION_IMPL *session, void *reconcilep)
{
	WT_RECONCILE *r;

	if ((r = *(WT_RECONCILE **)reconcilep) == NULL)
		return;
	*(WT_RECONCILE **)reconcilep = NULL;

	__wt_buf_free(session, &r->dsk);

	__wt_free(session, r->raw_entries);
	__wt_free(session, r->raw_offsets);
	__wt_free(session, r->raw_recnos);
	__wt_buf_free(session, &r->raw_destination);

	__rec_bnd_cleanup(session, r, true);

	__wt_free(session, r->supd);

	__wt_buf_free(session, &r->k.buf);
	__wt_buf_free(session, &r->v.buf);
	__wt_buf_free(session, &r->_cur);
	__wt_buf_free(session, &r->_last);

	__wt_rec_dictionary_free(session, r);

	__wt_free(session, r);
}

/*
 * __rec_destroy_session --
 *	Clean up the reconciliation structure, session version.
 */
static int
__rec_destroy_session(WT_SESSION_IMPL *session)
{
	__wt_rec_destroy(session, &session->reconcile);
	return (0);
}

/*
 * __rec_bnd_cleanup --
 *	Cleanup the boundary structure information.
 */
static void
__rec_bnd_cleanup(WT_SESSION_IMPL *session, WT_RECONCILE *r, bool destroy)
{
	WT_BOUNDARY *bnd;
	uint32_t i, last_used;

	if (r->bnd == NULL)
		return;

	/* Reconciliation is not re-entrant, make sure that doesn't happen. */
	r->ref = NULL;

	/*
	 * Free the boundary structures' memory.  In the case of normal cleanup,
	 * discard any memory we won't reuse in the next reconciliation; in the
	 * case of destruction, discard everything.
	 *
	 * During some big-page evictions we have seen boundary arrays that have
	 * millions of elements.  That should not be a normal event, but if the
	 * memory is associated with a random application session, it won't be
	 * discarded until the session is closed or reset. If there are more
	 * than 10,000 boundary structure elements, discard the boundary array
	 * entirely and start over next time.
	 */
	if (destroy || r->bnd_entries > 10 * 1000) {
		for (bnd = r->bnd, i = 0; i < r->bnd_entries; ++bnd, ++i) {
			__wt_free(session, bnd->addr.addr);
			__wt_free(session, bnd->dsk);
			__wt_free(session, bnd->supd);
			__wt_buf_free(session, &bnd->key);
		}
		__wt_free(session, r->bnd);
		r->bnd_next = 0;
		r->bnd_entries = r->bnd_allocated = 0;
	} else {
		/*
		 * The boundary-next field points to the next boundary structure
		 * we were going to use, but there's no requirement that value
		 * be incremented before reconciliation updates the structure it
		 * points to, that is, there's no guarantee elements of the next
		 * boundary structure are still unchanged. Be defensive, clean
		 * up the "next" structure as well as the ones we know we used.
		 */
		last_used = r->bnd_next;
		if (last_used < r->bnd_entries)
			++last_used;
		for (bnd = r->bnd, i = 0; i < last_used; ++bnd, ++i) {
			__wt_free(session, bnd->addr.addr);
			__wt_free(session, bnd->dsk);
			__wt_free(session, bnd->supd);
		}
	}
}

/*
 * __wt_rec_block_free --
 *	Helper function to free a block.
 */
int
__wt_rec_block_free(
    WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	WT_BM *bm;
	WT_BTREE *btree;

	btree = S2BT(session);
	bm = btree->bm;

	return (bm->free(bm, session, addr, addr_size));
}

/*
 * __rec_update_save --
 *	Save a WT_UPDATE list for later restoration.
 */
static int
__rec_update_save(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_INSERT *ins, WT_ROW *rip, uint64_t txnid)
{
	WT_RET(__wt_realloc_def(
	    session, &r->supd_allocated, r->supd_next + 1, &r->supd));
	r->supd[r->supd_next].ins = ins;
	r->supd[r->supd_next].rip = rip;
	r->supd[r->supd_next].onpage_txn = txnid;
	++r->supd_next;
	return (0);
}

/*
 * __wt_rec_update_move --
 *	Move a saved WT_UPDATE list from the per-page cache to a specific
 * block's list.
 */
int
__wt_rec_update_move(
    WT_SESSION_IMPL *session, WT_BOUNDARY *bnd, WT_SAVE_UPD *supd)
{
	WT_RET(__wt_realloc_def(
	    session, &bnd->supd_allocated, bnd->supd_next + 1, &bnd->supd));
	bnd->supd[bnd->supd_next] = *supd;
	++bnd->supd_next;

	supd->ins = NULL;
	supd->rip = NULL;
	return (0);
}

/*
 * __wt_rec_txn_read --
 *	Return the update in a list that should be written (or NULL if none can
 * be written).
 */
int
__wt_rec_txn_read(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_INSERT *ins, WT_ROW *rip, WT_CELL_UNPACK *vpack, WT_UPDATE **updp)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_DECL_ITEM(tmp);
	WT_PAGE *page;
	WT_UPDATE *append, *upd, *upd_list;
	size_t notused;
	uint64_t max_txn, min_txn, txnid;
	bool append_origv, skipped;

	*updp = NULL;

	btree = S2BT(session);
	page = r->page;

	/*
	 * If called with a WT_INSERT item, use its WT_UPDATE list (which must
	 * exist), otherwise check for an on-page row-store WT_UPDATE list
	 * (which may not exist). Return immediately if the item has no updates.
	 */
	if (ins == NULL) {
		if ((upd_list = WT_ROW_UPDATE(page, rip)) == NULL)
			return (0);
	} else
		upd_list = ins->upd;

	for (skipped = false,
	    max_txn = WT_TXN_NONE, min_txn = UINT64_MAX,
	    upd = upd_list; upd != NULL; upd = upd->next) {
		if ((txnid = upd->txnid) == WT_TXN_ABORTED)
			continue;

		/* Track the largest/smallest transaction IDs on the list. */
		if (WT_TXNID_LT(max_txn, txnid))
			max_txn = txnid;
		if (WT_TXNID_LT(txnid, min_txn))
			min_txn = txnid;

		/*
		 * Find the first update we can use.
		 */
		if (F_ISSET(r, WT_EVICTING)) {
			/*
			 * Eviction can write any committed update.
			 *
			 * When reconciling for eviction, track whether any
			 * uncommitted updates are found.
			 */
			if (__wt_txn_committed(session, txnid)) {
				if (*updp == NULL)
					*updp = upd;
			} else
				skipped = true;
		} else {
			/*
			 * Checkpoint can only write updates visible as of its
			 * snapshot.
			 *
			 * When reconciling for a checkpoint, track whether any
			 * updates were skipped on the way to finding the first
			 * visible update.
			 */
			if (*updp == NULL) {
				if (__wt_txn_visible(session, txnid))
					*updp = upd;
				else
					skipped = true;
			}
		}
	}

	/*
	 * If all of the updates were aborted, quit. This test is not strictly
	 * necessary because the above loop exits with skipped not set and the
	 * maximum transaction left at its initial value of WT_TXN_NONE, so
	 * the test below will be branch true and return, but it's cheap and a
	 * little more explicit, and makes Coverity happy.
	 */
	if (max_txn == WT_TXN_NONE)
		return (0);

	/*
	 * Track the maximum transaction ID in the page.  We store this in the
	 * tree at the end of reconciliation in the service of checkpoints, it
	 * is used to avoid discarding trees from memory when they have changes
	 * required to satisfy a snapshot read.
	 */
	if (WT_TXNID_LT(r->max_txn, max_txn))
		r->max_txn = max_txn;

	/*
	 * If there are no skipped updates and all updates are globally visible,
	 * the page can be marked clean and we're done, regardless if evicting
	 * or checkpointing.
	 *
	 * We have to check both: the oldest transaction ID may have moved while
	 * we were scanning the update list, so it is possible to find a skipped
	 * update, but then find all updates are stable at the end of the scan.
	 *
	 * Skip the visibility check for the lookaside table as a special-case,
	 * we know there are no older readers of that table.
	 */
	if (!skipped &&
	    (F_ISSET(btree, WT_BTREE_LOOKASIDE) ||
	    __wt_txn_visible_all(session, max_txn)))
		return (0);

	/*
	 * In some cases, there had better not be skipped updates or updates not
	 * yet globally visible.
	 */
	if (F_ISSET(r, WT_VISIBILITY_ERR))
		WT_PANIC_RET(session, EINVAL,
		    "reconciliation error, uncommitted update or update not "
		    "globally visible");

	/*
	 * If not trying to evict the page, we know what we'll write and we're
	 * done. Because some updates were skipped or are not globally visible,
	 * the page can't be marked clean.
	 */
	if (!F_ISSET(r, WT_EVICTING)) {
		r->leave_dirty = true;
		return (0);
	}

	/*
	 * Evicting with either uncommitted changes or not-yet-globally-visible
	 * changes. There are two ways to continue, the save/restore eviction
	 * path or the lookaside table eviction path. Both cannot be configured
	 * because the paths track different information. The save/restore path
	 * can handle both uncommitted and not-yet-globally-visible changes, by
	 * evicting most of the page and then creating a new, smaller page into
	 * which we re-instantiate those changes. The lookaside table path can
	 * only handle not-yet-globally-visible changes by writing those changes
	 * into the lookaside table and restoring them on demand if and when the
	 * page is read back into memory.
	 *
	 * Both paths are configured outside of reconciliation: the save/restore
	 * path is the WT_EVICT_UPDATE_RESTORE flag, the lookaside table path is
	 * the WT_EVICT_LOOKASIDE flag.
	 */
	if (!F_ISSET(r, WT_EVICT_LOOKASIDE | WT_EVICT_UPDATE_RESTORE))
		return (EBUSY);
	if (skipped && !F_ISSET(r, WT_EVICT_UPDATE_RESTORE))
		return (EBUSY);

	append_origv = false;
	if (F_ISSET(r, WT_EVICT_UPDATE_RESTORE)) {
		/*
		 * The save/restore eviction path.
		 *
		 * Clear the returned update so our caller ignores the key/value
		 * pair in the case of an insert/append list entry (everything
		 * we need is in the update list), and otherwise writes the
		 * original on-page key/value pair to which the update list
		 * applies.
		 */
		*updp = NULL;

		/* The page can't be marked clean. */
		r->leave_dirty = true;

		/*
		 * A special-case for overflow values, where we can't write the
		 * original on-page value item to disk because it's been updated
		 * or removed.
		 *
		 * What happens is that an overflow value is updated or removed
		 * and its backing blocks freed.  If any reader in the system
		 * might still want the value, a copy was cached in the page
		 * reconciliation tracking memory, and the page cell set to
		 * WT_CELL_VALUE_OVFL_RM.  Eviction then chose the page and
		 * we're splitting it up in order to push parts of it out of
		 * memory.
		 *
		 * We could write the original on-page value item to disk... if
		 * we had a copy.  The cache may not have a copy (a globally
		 * visible update would have kept a value from being cached), or
		 * an update that subsequently became globally visible could
		 * cause a cached value to be discarded.  Either way, once there
		 * is a globally visible update, we may not have the original
		 * value.
		 *
		 * Fortunately, if there's a globally visible update we don't
		 * care about the original version, so we simply ignore it, no
		 * transaction can ever try and read it.  If there isn't a
		 * globally visible update, there had better be a cached value.
		 *
		 * In the latter case, we could write the value out to disk, but
		 * (1) we are planning on re-instantiating this page in memory,
		 * it isn't going to disk, and (2) the value item is eventually
		 * going to be discarded, that seems like a waste of a write.
		 * Instead, find the cached value and append it to the update
		 * list we're saving for later restoration.
		 */
		if (vpack != NULL &&
		    vpack->raw == WT_CELL_VALUE_OVFL_RM &&
		    !__wt_txn_visible_all(session, min_txn))
			append_origv = true;
	} else {
		/*
		 * The lookaside table eviction path.
		 *
		 * If at least one update is globally visible, copy the update
		 * list and ignore the current on-page value. If no update is
		 * globally visible, readers require the page's original value.
		 */
		if (!__wt_txn_visible_all(session, min_txn))
			append_origv = true;
	}

	/*
	 * We need the original on-page value for some reason: get a copy and
	 * append it to the end of the update list with a transaction ID that
	 * guarantees its visibility.
	 */
	if (append_origv) {
		/*
		 * If we don't have a value cell, it's an insert/append list
		 * key/value pair which simply doesn't exist for some reader;
		 * place a deleted record at the end of the update list.
		 */
		if (vpack == NULL || vpack->type == WT_CELL_DEL)
			WT_RET(__wt_update_alloc(
			    session, NULL, &append, &notused));
		else {
			WT_RET(__wt_scr_alloc(session, 0, &tmp));
			if ((ret = __wt_page_cell_data_ref(
			    session, page, vpack, tmp)) == 0)
				ret = __wt_update_alloc(
				    session, tmp, &append, &notused);
			__wt_scr_free(session, &tmp);
			WT_RET(ret);
		}

		/*
		 * Give the entry an impossibly low transaction ID to ensure its
		 * global visibility, append it to the update list.
		 *
		 * Note the change to the actual reader-accessible update list:
		 * from now on, the original on-page value appears at the end
		 * of the update list, even if this reconciliation subsequently
		 * fails.
		 */
		append->txnid = WT_TXN_NONE;
		for (upd = upd_list; upd->next != NULL; upd = upd->next)
			;
		upd->next = append;
	}

	/*
	 * The order of the updates on the list matters, we can't move only the
	 * unresolved updates, move the entire update list.
	 *
	 * If we skipped updates, the transaction value is never used.  If we
	 * didn't skip updates, the list of updates are eventually written to
	 * the lookaside table, and associated with each update record is the
	 * transaction ID of the update we wrote in the reconciled page; once
	 * that transaction ID is globally visible, we know we no longer need
	 * the lookaside table records, allowing them to be discarded.
	 */
	return (__rec_update_save(session,
	    r, ins, rip, (*updp == NULL) ? WT_TXN_NONE : (*updp)->txnid));
}

/*
 * __rec_child_deleted --
 *	Handle pages with leaf pages in the WT_REF_DELETED state.
 */
static int
__rec_child_deleted(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_REF *ref, WT_CHILD_STATE *statep)
{
	WT_PAGE_DELETED *page_del;
	size_t addr_size;
	const uint8_t *addr;

	page_del = ref->page_del;

	/*
	 * Internal pages with child leaf pages in the WT_REF_DELETED state are
	 * a special case during reconciliation.  First, if the deletion was a
	 * result of a session truncate call, the deletion may not be visible to
	 * us. In that case, we proceed as with any change not visible during
	 * reconciliation by ignoring the change for the purposes of writing the
	 * internal page.
	 *
	 * In this case, there must be an associated page-deleted structure, and
	 * it holds the transaction ID we care about.
	 *
	 * In some cases, there had better not be any updates we can't see.
	 */
	if (F_ISSET(r, WT_VISIBILITY_ERR) &&
	    page_del != NULL && !__wt_txn_visible(session, page_del->txnid))
		WT_PANIC_RET(session, EINVAL,
		    "reconciliation illegally skipped an update");

	/*
	 * Deal with any underlying disk blocks.
	 *
	 * First, check to see if there is an address associated with this leaf:
	 * if there isn't, we're done, the underlying page is already gone.  If
	 * the page still exists, check for any transactions in the system that
	 * might want to see the page's state before it's deleted.
	 *
	 * If any such transactions exist, we cannot discard the underlying leaf
	 * page to the block manager because the transaction may eventually read
	 * it.  However, this write might be part of a checkpoint, and should we
	 * recover to that checkpoint, we'll need to delete the leaf page, else
	 * we'd leak it.  The solution is to write a proxy cell on the internal
	 * page ensuring the leaf page is eventually discarded.
	 *
	 * If no such transactions exist, we can discard the leaf page to the
	 * block manager and no cell needs to be written at all.  We do this
	 * outside of the underlying tracking routines because this action is
	 * permanent and irrevocable.  (Clearing the address means we've lost
	 * track of the disk address in a permanent way.  This is safe because
	 * there's no path to reading the leaf page again: if there's ever a
	 * read into this part of the name space again, the cache read function
	 * instantiates an entirely new page.)
	 */
	if (ref->addr != NULL &&
	    (page_del == NULL ||
	    __wt_txn_visible_all(session, page_del->txnid))) {
		WT_RET(__wt_ref_info(session, ref, &addr, &addr_size, NULL));
		WT_RET(__wt_rec_block_free(session, addr, addr_size));

		if (__wt_off_page(ref->home, ref->addr)) {
			__wt_free(session, ((WT_ADDR *)ref->addr)->addr);
			__wt_free(session, ref->addr);
		}
		ref->addr = NULL;
	}

	/*
	 * If the original page is gone, we can skip the slot on the internal
	 * page.
	 */
	if (ref->addr == NULL) {
		*statep = WT_CHILD_IGNORE;

		/*
		 * Minor memory cleanup: if a truncate call deleted this page
		 * and we were ever forced to instantiate the page in memory,
		 * we would have built a list of updates in the page reference
		 * in order to be able to abort the truncate.  It's a cheap
		 * test to make that memory go away, we do it here because
		 * there's really nowhere else we do the checks.  In short, if
		 * we have such a list, and the backing address blocks are
		 * gone, there can't be any transaction that can abort.
		 */
		if (page_del != NULL) {
			__wt_free(session, ref->page_del->update_list);
			__wt_free(session, ref->page_del);
		}

		return (0);
	}

	/*
	 * Internal pages with deletes that aren't stable cannot be evicted, we
	 * don't have sufficient information to restore the page's information
	 * if subsequently read (we wouldn't know which transactions should see
	 * the original page and which should see the deleted page).
	 */
	if (F_ISSET(r, WT_EVICTING))
		return (EBUSY);

	/*
	 * If there are deleted child pages we can't discard immediately, keep
	 * the page dirty so they are eventually freed.
	 */
	r->leave_dirty = 1;

	/*
	 * If the original page cannot be freed, we need to keep a slot on the
	 * page to reference it from the parent page.
	 *
	 * If the delete is not visible in this checkpoint, write the original
	 * address normally.  Otherwise, we have to write a proxy record.
	 */
	if (__wt_txn_visible(session, page_del->txnid))
		*statep = WT_CHILD_PROXY;

	return (0);
}

/*
 * __wt_rec_child_modify --
 *	Return if the internal page's child references any modifications.
 */
int
__wt_rec_child_modify(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_REF *ref, bool *hazardp, WT_CHILD_STATE *statep)
{
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;

	/* We may acquire a hazard pointer our caller must release. */
	*hazardp = false;

	/* Default to using the original child address. */
	*statep = WT_CHILD_ORIGINAL;

	/*
	 * This function is called when walking an internal page to decide how
	 * to handle child pages referenced by the internal page, specifically
	 * if the child page is to be merged into its parent.
	 *
	 * Internal pages are reconciled for two reasons: first, when evicting
	 * an internal page, second by the checkpoint code when writing internal
	 * pages.  During eviction, the subtree is locked down so all pages
	 * should be in the WT_REF_DISK or WT_REF_LOCKED state. During
	 * checkpoint, any eviction that might affect our review of an internal
	 * page is prohibited, however, as the subtree is not reserved for our
	 * exclusive use, there are other page states that must be considered.
	 */
	for (;; __wt_yield())
		switch (r->tested_ref_state = ref->state) {
		case WT_REF_DISK:
			/* On disk, not modified by definition. */
			goto done;

		case WT_REF_DELETED:
			/*
			 * The child is in a deleted state.
			 *
			 * It's possible the state could change underneath us as
			 * the page is read in, and we can race between checking
			 * for a deleted state and looking at the transaction ID
			 * to see if the delete is visible to us.  Lock down the
			 * structure.
			 */
			if (!__wt_atomic_casv32(
			    &ref->state, WT_REF_DELETED, WT_REF_LOCKED))
				break;
			ret = __rec_child_deleted(session, r, ref, statep);
			WT_PUBLISH(ref->state, WT_REF_DELETED);
			goto done;

		case WT_REF_LOCKED:
			/*
			 * Locked.
			 *
			 * If evicting, the evicted page's subtree, including
			 * this child, was selected for eviction by us and the
			 * state is stable until we reset it, it's an in-memory
			 * state.  This is the expected state for a child being
			 * merged into a page (where the page was selected by
			 * the eviction server for eviction).
			 */
			if (F_ISSET(r, WT_EVICTING))
				goto in_memory;

			/*
			 * If called during checkpoint, the child is being
			 * considered by the eviction server or the child is a
			 * fast-delete page being read.  The eviction may have
			 * started before the checkpoint and so we must wait
			 * for the eviction to be resolved.  I suspect we could
			 * handle fast-delete reads, but we can't distinguish
			 * between the two and fast-delete reads aren't expected
			 * to be common.
			 */
			break;

		case WT_REF_MEM:
			/*
			 * In memory.
			 *
			 * If evicting, the evicted page's subtree, including
			 * this child, was selected for eviction by us and the
			 * state is stable until we reset it, it's an in-memory
			 * state.  This is the expected state for a child being
			 * merged into a page (where the page belongs to a file
			 * being discarded from the cache during close).
			 */
			if (F_ISSET(r, WT_EVICTING))
				goto in_memory;

			/*
			 * If called during checkpoint, acquire a hazard pointer
			 * so the child isn't evicted, it's an in-memory case.
			 *
			 * This call cannot return split/restart, eviction of
			 * pages that split into their parent is shutout during
			 * checkpoint, all splits in process will have completed
			 * before we walk any pages for checkpoint.
			 */
			ret = __wt_page_in(session, ref,
			    WT_READ_CACHE | WT_READ_NO_EVICT |
			    WT_READ_NO_GEN | WT_READ_NO_WAIT);
			if (ret == WT_NOTFOUND) {
				ret = 0;
				break;
			}
			WT_RET(ret);
			*hazardp = true;
			goto in_memory;

		case WT_REF_READING:
			/*
			 * Being read, not modified by definition.
			 *
			 * We should never be here during eviction, a child page
			 * in this state within an evicted page's subtree would
			 * have caused normally eviction to fail, and exclusive
			 * eviction shouldn't ever see pages being read.
			 */
			WT_ASSERT(session, !F_ISSET(r, WT_EVICTING));
			goto done;

		case WT_REF_SPLIT:
			/*
			 * The page was split out from under us.
			 *
			 * We should never be here during eviction, a child page
			 * in this state within an evicted page's subtree would
			 * have caused eviction to fail.
			 *
			 * We should never be here during checkpoint, dirty page
			 * eviction is shutout during checkpoint, all splits in
			 * process will have completed before we walk any pages
			 * for checkpoint.
			 */
			WT_ASSERT(session, ref->state != WT_REF_SPLIT);
			/* FALLTHROUGH */

		WT_ILLEGAL_VALUE(session);
		}

in_memory:
	/*
	 * In-memory states: the child is potentially modified if the page's
	 * modify structure has been instantiated. If the modify structure
	 * exists and the page has actually been modified, set that state.
	 * If that's not the case, we would normally use the original cell's
	 * disk address as our reference, but, if we're forced to instantiate
	 * a deleted child page and it's never modified, we end up here with
	 * a page that has a modify structure, no modifications, and no disk
	 * address.  Ignore those pages, they're not modified and there is no
	 * reason to write the cell.
	 */
	mod = ref->page->modify;
	if (mod != NULL && mod->rec_result != 0)
		*statep = WT_CHILD_MODIFIED;
	else if (ref->addr == NULL) {
		*statep = WT_CHILD_IGNORE;
		WT_CHILD_RELEASE(session, *hazardp, ref);
	}

done:	WT_DIAGNOSTIC_YIELD;
	return (ret);
}

/*
 * __wt_rec_incr --
 *	Update the memory tracking structure for a set of new entries.
 */
void
__wt_rec_incr(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v, size_t size)
{
	/*
	 * The buffer code is fragile and prone to off-by-one errors -- check
	 * for overflow in diagnostic mode.
	 */
	WT_ASSERT(session, r->space_avail >= size);
	WT_ASSERT(session,
	    WT_BLOCK_FITS(r->first_free, size, r->dsk.mem, r->dsk.memsize));

	r->entries += v;
	r->space_avail -= size;
	r->first_free += size;
}

/*
 * __wt_rec_copy_incr --
 *	Copy a key/value cell and buffer pair into the new image.
 */
void
__wt_rec_copy_incr(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_KV *kv)
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
	for (p = (uint8_t *)r->first_free,
	    t = (uint8_t *)&kv->cell, len = kv->cell_len; len > 0; --len)
		*p++ = *t++;

	/* The data can be quite large -- call memcpy. */
	if (kv->buf.size != 0)
		memcpy(p, kv->buf.data, kv->buf.size);

	WT_ASSERT(session, kv->len == kv->cell_len + kv->buf.size);
	__wt_rec_incr(session, r, 1, kv->len);
}

/*
 * __wt_rec_leaf_page_max --
 *	Figure out the maximum leaf page size for the reconciliation.
 */
uint32_t
__wt_rec_leaf_page_max(WT_SESSION_IMPL *session,  WT_RECONCILE *r)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	uint32_t page_size;

	btree = S2BT(session);
	page = r->page;

	page_size = 0;
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		/*
		 * Column-store pages can grow if there are missing records
		 * (that is, we lost a chunk of the range, and have to write
		 * deleted records).  Fixed-length objects are a problem, if
		 * there's a big missing range, we could theoretically have to
		 * write large numbers of missing objects.
		 */
		page_size = (uint32_t)WT_ALIGN(WT_FIX_ENTRIES_TO_BYTES(btree,
		    r->salvage->take + r->salvage->missing), btree->allocsize);
		break;
	case WT_PAGE_COL_VAR:
		/*
		 * Column-store pages can grow if there are missing records
		 * (that is, we lost a chunk of the range, and have to write
		 * deleted records).  Variable-length objects aren't usually a
		 * problem because we can write any number of deleted records
		 * in a single page entry because of the RLE, we just need to
		 * ensure that additional entry fits.
		 */
		break;
	case WT_PAGE_ROW_LEAF:
	default:
		/*
		 * Row-store pages can't grow, salvage never does anything
		 * other than reduce the size of a page read from disk.
		 */
		break;
	}

	/*
	 * Default size for variable-length column-store and row-store pages
	 * during salvage is the maximum leaf page size.
	 */
	if (page_size < btree->maxleafpage)
		page_size = btree->maxleafpage;

	/*
	 * The page we read from the disk should be smaller than the page size
	 * we just calculated, check out of paranoia.
	 */
	if (page_size < page->dsk->mem_size)
		page_size = page->dsk->mem_size;

	/*
	 * Salvage is the backup plan: don't let this fail.
	 */
	return (page_size * 2);
}

/*
 * __wt_rec_is_checkpoint --
 *	Return if we're writing a checkpoint.
 */
bool
__wt_rec_is_checkpoint(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_BOUNDARY *bnd)
{
	WT_BTREE *btree;

	btree = S2BT(session);

	/*
	 * Check to see if we're going to create a checkpoint.
	 *
	 * This function exists as a place to hang this comment.
	 *
	 * Any time we write the root page of the tree without splitting we are
	 * creating a checkpoint (and have to tell the underlying block manager
	 * so it creates and writes the additional information checkpoints
	 * require).  However, checkpoints are completely consistent, and so we
	 * have to resolve information about the blocks we're expecting to free
	 * as part of the checkpoint, before writing the checkpoint.  In short,
	 * we don't do checkpoint writes here; clear the boundary information as
	 * a reminder and create the checkpoint during wrapup.
	 */
	if (!F_ISSET(btree, WT_BTREE_NO_CHECKPOINT) &&
	    bnd == &r->bnd[0] && __wt_ref_is_root(r->ref)) {
		bnd->addr.addr = NULL;
		bnd->addr.size = 0;
		bnd->addr.type = 0;
		return (true);
	}
	return (false);
}

/*
 * __wt_rec_raw_decompress --
 *	Decompress a raw-compressed image.
 */
int
__wt_rec_raw_decompress(
    WT_SESSION_IMPL *session, const void *image, size_t size, void *retp)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE_HEADER const *dsk;
	size_t result_len;

	btree = S2BT(session);
	dsk = image;

	/*
	 * We skipped an update and we can't write a block, but unfortunately,
	 * the block has already been compressed. Decompress the block so we
	 * can subsequently re-instantiate it in memory.
	 */
	WT_RET(__wt_scr_alloc(session, dsk->mem_size, &tmp));
	memcpy(tmp->mem, image, WT_BLOCK_COMPRESS_SKIP);
	WT_ERR(btree->compressor->decompress(btree->compressor,
	    &session->iface,
	    (uint8_t *)image + WT_BLOCK_COMPRESS_SKIP,
	    size - WT_BLOCK_COMPRESS_SKIP,
	    (uint8_t *)tmp->mem + WT_BLOCK_COMPRESS_SKIP,
	    dsk->mem_size - WT_BLOCK_COMPRESS_SKIP,
	    &result_len));
	if (result_len != dsk->mem_size - WT_BLOCK_COMPRESS_SKIP)
		WT_ERR(__wt_illegal_value(session, btree->dhandle->name));

	WT_ERR(__wt_strndup(session, tmp->data, dsk->mem_size, retp));
	WT_ASSERT(session, __wt_verify_dsk_image(session,
	    "[raw evict split]", tmp->data, dsk->mem_size, false) == 0);

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

/*
 * __wt_rec_update_las --
 *	Copy a set of updates into the database's lookaside buffer.
 */
int
__wt_rec_update_las(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, uint32_t btree_id, WT_BOUNDARY *bnd)
{
	WT_CURSOR *cursor;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_ITEM las_addr, las_value;
	WT_PAGE *page;
	WT_SAVE_UPD *list;
	WT_UPDATE *upd;
	uint64_t las_counter;
	uint32_t i, session_flags, slot;
	uint8_t *p;

	cursor = NULL;
	WT_CLEAR(las_addr);
	WT_CLEAR(las_value);
	page = r->page;

	/*
	 * We're writing lookaside records: start instantiating them on pages
	 * we read (with the right flag set), and start sweeping the file.
	 */
	__wt_las_set_written(session);

	WT_ERR(__wt_las_cursor(session, &cursor, &session_flags));

	/* Ensure enough room for a column-store key without checking. */
	WT_ERR(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

	/*
	 * Each key in the lookaside table is associated with a block, and those
	 * blocks are freed and reallocated to other pages as pages in the tree
	 * are modified and reconciled. We want to be sure we don't add records
	 * to the lookaside table, then discard the block to which they apply,
	 * then write a new block to the same address, and then apply the old
	 * records to the new block when it's read. We don't want to clean old
	 * records out of the lookaside table every time we free a block because
	 * that happens a lot and would be costly; instead, we clean out the old
	 * records when adding new records into the lookaside table. This works
	 * because we only read from the lookaside table for pages marked with
	 * the WT_PAGE_LAS_UPDATE flag: that flag won't be set if we rewrite a
	 * block with no lookaside records, so the lookaside table won't be
	 * checked when the block is read, even if there are lookaside table
	 * records matching that block. If we rewrite a block that has lookaside
	 * records, we'll run this code, discarding any old records that might
	 * exist.
	 */
	WT_ERR(__wt_las_remove_block(
	    session, cursor, btree_id, bnd->addr.addr, bnd->addr.size));

	/* Lookaside table key component: block address. */
	las_addr.data = bnd->addr.addr;
	las_addr.size = bnd->addr.size;

	/* Enter each update in the boundary's list into the lookaside store. */
	for (las_counter = 0, i = 0,
	    list = bnd->supd; i < bnd->supd_next; ++i, ++list) {
		/* Lookaside table key component: source key. */
		switch (page->type) {
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			p = key->mem;
			WT_ERR(
			    __wt_vpack_uint(&p, 0, WT_INSERT_RECNO(list->ins)));
			key->size = WT_PTRDIFF(p, key->data);

			break;
		case WT_PAGE_ROW_LEAF:
			if (list->ins == NULL)
				WT_ERR(__wt_row_leaf_key(
				    session, page, list->rip, key, false));
			else {
				key->data = WT_INSERT_KEY(list->ins);
				key->size = WT_INSERT_KEY_SIZE(list->ins);
			}
			break;
		WT_ILLEGAL_VALUE_ERR(session);
		}

		/* Lookaside table value component: update reference. */
		switch (page->type) {
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			upd = list->ins->upd;
			break;
		case WT_PAGE_ROW_LEAF:
			if (list->ins == NULL) {
				slot = WT_ROW_SLOT(page, list->rip);
				upd = page->pg_row_upd[slot];
			} else
				upd = list->ins->upd;
			break;
		WT_ILLEGAL_VALUE_ERR(session);
		}

		/*
		 * Walk the list of updates, storing each key/value pair into
		 * the lookaside table.
		 */
		do {
			cursor->set_key(cursor, btree_id,
			    &las_addr, ++las_counter, list->onpage_txn, key);

			if (WT_UPDATE_DELETED_ISSET(upd))
				las_value.size = 0;
			else {
				las_value.data = WT_UPDATE_DATA(upd);
				las_value.size = upd->size;
			}
			cursor->set_value(
			    cursor, upd->txnid, upd->size, &las_value);

			WT_ERR(cursor->insert(cursor));
		} while ((upd = upd->next) != NULL);
	}

err:	WT_TRET(__wt_las_cursor_close(session, &cursor, session_flags));

	__wt_scr_free(session, &key);
	return (ret);
}

/*
 * __wt_rec_vtype --
 *	Return a value cell's address type.
 *	TODO: inline?
 */
u_int
__wt_rec_vtype(WT_ADDR *addr)
{
	if (addr->type == WT_ADDR_INT)
		return (WT_CELL_ADDR_INT);
	if (addr->type == WT_ADDR_LEAF)
		return (WT_CELL_ADDR_LEAF);
	return (WT_CELL_ADDR_LEAF_NO);
}


/*
 * __rec_split_dump_keys --
 *     Dump out the split keys in verbose mode.
 */
static int
__rec_split_dump_keys(WT_SESSION_IMPL *session, WT_PAGE *page, WT_RECONCILE *r)
{
       WT_BOUNDARY *bnd;
       WT_DECL_ITEM(tkey);
       WT_DECL_RET;
       uint32_t i;

       if (page->type == WT_PAGE_ROW_INT || page->type == WT_PAGE_ROW_LEAF)
	       WT_RET(__wt_scr_alloc(session, 0, &tkey));
       WT_ERR(__wt_verbose(
	   session, WT_VERB_SPLIT, "split: %" PRIu32 " pages", r->bnd_next));
       for (bnd = r->bnd, i = 0; i < r->bnd_next; ++bnd, ++i)
	       switch (page->type) {
	       case WT_PAGE_ROW_INT:
	       case WT_PAGE_ROW_LEAF:
		       WT_ERR(__wt_buf_set_printable(
			   session, tkey, bnd->key.data, bnd->key.size));
		       WT_ERR(__wt_verbose(session, WT_VERB_SPLIT,
			   "starting key %.*s",
			   (int)tkey->size, (const char *)tkey->data));
		       break;
	       case WT_PAGE_COL_FIX:
	       case WT_PAGE_COL_INT:
	       case WT_PAGE_COL_VAR:
		       WT_ERR(__wt_verbose(session, WT_VERB_SPLIT,
			   "starting recno %" PRIu64, bnd->recno));
		       break;
	       WT_ILLEGAL_VALUE_ERR(session);
	       }
err:   __wt_scr_free(session, &tkey);
       return (ret);
}

/*
 * __wt_rec_write_wrapup --
 *	Finish the reconciliation.
 */
int
__wt_rec_write_wrapup(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BM *bm;
	WT_BOUNDARY *bnd;
	WT_BTREE *btree;
	WT_PAGE_MODIFY *mod;
	WT_REF *ref;
	size_t addr_size;
	const uint8_t *addr;

	btree = S2BT(session);
	bm = btree->bm;
	mod = page->modify;
	ref = r->ref;

	/*
	 * This page may have previously been reconciled, and that information
	 * is now about to be replaced.  Make sure it's discarded at some point,
	 * and clear the underlying modification information, we're creating a
	 * new reality.
	 */
	switch (mod->rec_result) {
	case 0:	/*
		 * The page has never been reconciled before, free the original
		 * address blocks (if any).  The "if any" is for empty trees
		 * created when a new tree is opened or previously deleted pages
		 * instantiated in memory.
		 *
		 * The exception is root pages are never tracked or free'd, they
		 * are checkpoints, and must be explicitly dropped.
		 */
		if (__wt_ref_is_root(ref))
			break;
		if (ref->addr != NULL) {
			/*
			 * Free the page and clear the address (so we don't free
			 * it twice).
			 */
			WT_RET(__wt_ref_info(
			    session, ref, &addr, &addr_size, NULL));
			WT_RET(__wt_rec_block_free(session, addr, addr_size));
			if (__wt_off_page(ref->home, ref->addr)) {
				__wt_free(
				    session, ((WT_ADDR *)ref->addr)->addr);
				__wt_free(session, ref->addr);
			}
			ref->addr = NULL;
		}
		break;
	case WT_PM_REC_EMPTY:				/* Page deleted */
		break;
	case WT_PM_REC_MULTIBLOCK:			/* Multiple blocks */
		/*
		 * Discard the multiple replacement blocks.
		 */
		WT_RET(__wt_rec_split_discard(session, page));
		break;
	case WT_PM_REC_REPLACE:				/* 1-for-1 page swap */
		/*
		 * Discard the replacement leaf page's blocks.
		 *
		 * The exception is root pages are never tracked or free'd, they
		 * are checkpoints, and must be explicitly dropped.
		 */
		if (!__wt_ref_is_root(ref))
			WT_RET(__wt_rec_block_free(session,
			    mod->mod_replace.addr, mod->mod_replace.size));

		/* Discard the replacement page's address. */
		__wt_free(session, mod->mod_replace.addr);
		mod->mod_replace.size = 0;
		break;
	WT_ILLEGAL_VALUE(session);
	}

	/* Reset the reconciliation state. */
	mod->rec_result = 0;

	/*
	 * Wrap up overflow tracking.  If we are about to create a checkpoint,
	 * the system must be entirely consistent at that point (the underlying
	 * block manager is presumably going to do some action to resolve the
	 * list of allocated/free/whatever blocks that are associated with the
	 * checkpoint).
	 */
	WT_RET(__wt_ovfl_track_wrapup(session, page));

	switch (r->bnd_next) {
	case 0:						/* Page delete */
		WT_RET(__wt_verbose(
		    session, WT_VERB_RECONCILE, "page %p empty", page));
		WT_STAT_FAST_DATA_INCR(session, rec_page_delete);

		/* If this is the root page, we need to create a sync point. */
		ref = r->ref;
		if (__wt_ref_is_root(ref))
			WT_RET(bm->checkpoint(
			    bm, session, NULL, btree->ckpt, false));

		/*
		 * If the page was empty, we want to discard it from the tree
		 * by discarding the parent's key when evicting the parent.
		 * Mark the page as deleted, then return success, leaving the
		 * page in memory.  If the page is subsequently modified, that
		 * is OK, we'll just reconcile it again.
		 */
		mod->rec_result = WT_PM_REC_EMPTY;
		break;
	case 1:						/* 1-for-1 page swap */
		/*
		 * Because WiredTiger's pages grow without splitting, we're
		 * replacing a single page with another single page most of
		 * the time.
		 */
		bnd = &r->bnd[0];

		/*
		 * If saving/restoring changes for this page and there's only
		 * one block, there's nothing to write. This is a special case
		 * of forced eviction: set up a single block as if to split,
		 * then use that block to rewrite the page in memory.
		 */
		if (F_ISSET(r, WT_EVICT_UPDATE_RESTORE) && bnd->supd != NULL)
			goto split;

		/*
		 * If this is a root page, then we don't have an address and we
		 * have to create a sync point.  The address was cleared when
		 * we were about to write the buffer so we know what to do here.
		 */
		if (bnd->addr.addr == NULL)
			WT_RET(__wt_bt_write(session, &r->dsk,
			    NULL, NULL, true, bnd->already_compressed));
		else {
			mod->mod_replace = bnd->addr;
			bnd->addr.addr = NULL;
		}

		mod->rec_result = WT_PM_REC_REPLACE;
		break;
	default:					/* Page split */
		WT_RET(__wt_verbose(session, WT_VERB_RECONCILE,
		    "page %p reconciled into %" PRIu32 " pages",
		    page, r->bnd_next));

		switch (page->type) {
		case WT_PAGE_COL_INT:
		case WT_PAGE_ROW_INT:
			WT_STAT_FAST_DATA_INCR(
			    session, rec_multiblock_internal);
			break;
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
		case WT_PAGE_ROW_LEAF:
			WT_STAT_FAST_DATA_INCR(session, rec_multiblock_leaf);
			break;
		WT_ILLEGAL_VALUE(session);
		}

		/* Optionally display the actual split keys in verbose mode. */
		if (WT_VERBOSE_ISSET(session, WT_VERB_SPLIT))
			WT_RET(__rec_split_dump_keys(session, page, r));

		/* Track the largest set of page-splits. */
		if (r->bnd_next > r->bnd_next_max) {
			r->bnd_next_max = r->bnd_next;
			WT_STAT_FAST_DATA_SET(
			    session, rec_multiblock_max, r->bnd_next_max);
		}

split:		switch (page->type) {
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			WT_RET(__wt_rec_row_split(session, r, page));
			break;
		case WT_PAGE_COL_INT:
		case WT_PAGE_COL_FIX:
		case WT_PAGE_COL_VAR:
			WT_RET(__wt_rec_col_split(session, r, page));
			break;
		WT_ILLEGAL_VALUE(session);
		}
		mod->rec_result = WT_PM_REC_MULTIBLOCK;
		break;
	}
	return (0);
}

/*
 * __rec_write_wrapup_err --
 *	Finish the reconciliation on error.
 */
static int
__rec_write_wrapup_err(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BOUNDARY *bnd;
	WT_DECL_RET;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	/*
	 * Clear the address-reused flag from the multiblock reconciliation
	 * information (otherwise we might think the backing block is being
	 * reused on a subsequent reconciliation where we want to free it).
	 */
	 if (mod->rec_result == WT_PM_REC_MULTIBLOCK)
		for (multi = mod->mod_multi,
		    i = 0; i < mod->mod_multi_entries; ++multi, ++i)
			multi->addr.reuse = 0;

	/*
	 * On error, discard blocks we've written, they're unreferenced by the
	 * tree.  This is not a question of correctness, we're avoiding block
	 * leaks.
	 *
	 * Don't discard backing blocks marked for reuse, they remain part of
	 * a previous reconciliation.
	 */
	WT_TRET(__wt_ovfl_track_wrapup_err(session, page));
	for (bnd = r->bnd, i = 0; i < r->bnd_next; ++bnd, ++i)
		if (bnd->addr.addr != NULL) {
			if (bnd->addr.reuse)
				bnd->addr.addr = NULL;
			else {
				WT_TRET(__wt_rec_block_free(session,
				    bnd->addr.addr, bnd->addr.size));
				__wt_free(session, bnd->addr.addr);
			}
		}

	return (ret);
}
