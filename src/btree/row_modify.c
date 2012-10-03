/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_row_modify --
 *	Row-store insert, update and delete.
 */
int
__wt_row_modify(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, int is_remove)
{
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_INSERT_HEAD **inshead, *new_inshead, **new_inslist;
	WT_ITEM *key, *value;
	WT_PAGE *page;
	WT_UPDATE **new_upd, *upd, **upd_entry;
	size_t ins_size, upd_size;
	size_t new_inshead_size, new_inslist_size, new_upd_size;
	uint32_t ins_slot;
	u_int skipdepth;
	int i, logged;

	key = &cbt->iface.key;
	value = is_remove ? NULL : &cbt->iface.value;

	page = cbt->page;

	ins = NULL;
	new_inshead = NULL;
	new_inslist = NULL;
	new_upd = NULL;
	upd = NULL;
	logged = 0;

	/*
	 * Modify: allocate an update array as necessary, build a WT_UPDATE
	 * structure, and call a serialized function to insert the WT_UPDATE
	 * structure.
	 *
	 * Insert: allocate an insert array as necessary, build a WT_INSERT
	 * and WT_UPDATE structure pair, and call a serialized function to
	 * insert the WT_INSERT structure.
	 */
	if (cbt->compare == 0) {
		new_upd_size = 0;
		if (cbt->ins == NULL) {
			/*
			 * Allocate an update array as necessary.
			 *
			 * Set the WT_UPDATE array reference.
			 */
			if (page->u.row.upd == NULL) {
				WT_ERR(__wt_calloc_def(
				    session, page->entries, &new_upd));
				new_upd_size =
				    page->entries * sizeof(WT_UPDATE *);
				upd_entry = &new_upd[cbt->slot];
			} else
				upd_entry = &page->u.row.upd[cbt->slot];
		} else
			upd_entry = &cbt->ins->upd;

		/* Make sure the update can proceed. */
		WT_ERR(__wt_update_check(session, page, *upd_entry));

		/* Allocate and insert a WT_UPDATE structure. */
		WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
		logged = 1;
		WT_ERR(__wt_update_serial(session, page, cbt->write_gen,
		    upd_entry, &new_upd, new_upd_size, &upd, upd_size));
	} else {
		/*
		 * Allocate insert array if necessary, and set the array
		 * reference.
		 *
		 * We allocate an additional insert array slot for insert keys
		 * sorting less than any key on the page.  The test to select
		 * that slot is baroque: if the search returned the first page
		 * slot, we didn't end up processing an insert list, and the
		 * comparison value indicates the search key was smaller than
		 * the returned slot, then we're using the smallest-key insert
		 * slot.  That's hard, so we set a flag.
		 */
		ins_slot = F_ISSET(
		    cbt, WT_CBT_SEARCH_SMALLEST) ? page->entries : cbt->slot;

		WT_ERR(__wt_update_check(session, page, NULL));

		new_inshead_size = new_inslist_size = 0;
		if (page->u.row.ins == NULL) {
			WT_ERR(__wt_calloc_def(
			    session, page->entries + 1, &new_inslist));
			new_inslist_size =
			    (page->entries + 1) * sizeof(WT_INSERT_HEAD *);
			inshead = &new_inslist[ins_slot];
		} else
			inshead = &page->u.row.ins[ins_slot];

		/*
		 * Allocate a new insert list head as necessary.
		 *
		 * If allocating a new insert list head, we have to initialize
		 * the cursor's insert list stack and insert head reference as
		 * well, search couldn't have.
		 */
		if (*inshead == NULL) {
			new_inshead_size = sizeof(WT_INSERT_HEAD);
			WT_ERR(__wt_calloc_def(session, 1, &new_inshead));
			for (i = 0; i < WT_SKIP_MAXDEPTH; i++)
				cbt->ins_stack[i] = &new_inshead->head[i];
			cbt->ins_head = new_inshead;
		}

		/* Choose a skiplist depth for this insert. */
		skipdepth = __wt_skip_choose_depth();

		/*
		 * Allocate a WT_INSERT/WT_UPDATE pair, and update the cursor
		 * to reference it.
		 */
		WT_ERR(__wt_row_insert_alloc(
		    session, key, skipdepth, &ins, &ins_size));
		WT_ERR(__wt_update_alloc(session, value, &upd, &upd_size));
		logged = 1;
		ins->upd = upd;
		ins_size += upd_size;
		cbt->ins = ins;

		/* Insert the WT_INSERT structure. */
		WT_ERR(__wt_insert_serial(session, page, cbt->write_gen,
		    inshead, cbt->ins_stack,
		    &new_inslist, new_inslist_size,
		    &new_inshead, new_inshead_size,
		    &ins, ins_size, skipdepth));
	}

	if (0) {
err:		/*
		 * Remove the update from the current transaction, so we don't
		 * try to modify it on rollback.
		 */
		if (logged)
			__wt_txn_unmodify(session);
		__wt_free(session, ins);
		__wt_free(session, upd);
	}

	/* Free any insert, update arrays. */
	__wt_free(session, new_inslist);
	__wt_free(session, new_inshead);
	__wt_free(session, new_upd);

	return (ret);
}

/*
 * __wt_row_insert_alloc --
 *	Row-store insert: allocate a WT_INSERT structure and fill it in.
 */
int
__wt_row_insert_alloc(WT_SESSION_IMPL *session,
    WT_ITEM *key, u_int skipdepth, WT_INSERT **insp, size_t *ins_sizep)
{
	WT_INSERT *ins;
	size_t ins_size;

	/*
	 * Allocate the WT_INSERT structure, next pointers for the skip list,
	 * and room for the key.  Then copy the key into place.
	 */
	ins_size = sizeof(WT_INSERT) +
	    skipdepth * sizeof(WT_INSERT *) + key->size;
	WT_RET(__wt_calloc(session, 1, ins_size, &ins));

	ins->u.key.offset = WT_STORE_SIZE(ins_size - key->size);
	WT_INSERT_KEY_SIZE(ins) = key->size;
	memcpy(WT_INSERT_KEY(ins), key->data, key->size);

	*insp = ins;
	if (ins_sizep != NULL)
		*ins_sizep = ins_size;
	return (0);
}

/*
 * __wt_insert_serial_func --
 *	Server function to add an WT_INSERT entry to the page.
 */
int
__wt_insert_serial_func(WT_SESSION_IMPL *session, void *args)
{
	WT_INSERT *new_ins, ***ins_stack;
	WT_INSERT_HEAD *inshead, **insheadp, **new_inslist, *new_inshead;
	WT_PAGE *page;
	uint32_t write_gen;
	u_int i, skipdepth;

	__wt_insert_unpack(args, &page, &write_gen, &insheadp,
	    &ins_stack, &new_inslist, &new_inshead, &new_ins, &skipdepth);

	/* Check the page's write-generation. */
	WT_RET(__wt_page_write_gen_check(session, page, write_gen));

	/*
	 * Publish: First, point the new WT_INSERT item's skiplist references
	 * to the next elements in the insert list, then flush memory.  Second,
	 * update the skiplist elements that reference the new WT_INSERT item,
	 * this ensures the list is never inconsistent.
	 */
	if ((inshead = *insheadp) == NULL)
		inshead = new_inshead;
	for (i = 0; i < skipdepth; i++)
		new_ins->next[i] = *ins_stack[i];
	WT_WRITE_BARRIER();
	for (i = 0; i < skipdepth; i++) {
		if (inshead->tail[i] == NULL ||
		    ins_stack[i] == &inshead->tail[i]->next[i])
			inshead->tail[i] = new_ins;
		*ins_stack[i] = new_ins;
	}

	__wt_insert_new_ins_taken(session, args, page);

	/*
	 * If the insert head does not yet have an insert list, our caller
	 * passed us one.
	 *
	 * NOTE: it is important to do this after the item has been added to
	 * the list.  Code can assume that if the list is set, it is non-empty.
	 */
	if (*insheadp == NULL) {
		WT_PUBLISH(*insheadp, new_inshead);
		__wt_insert_new_inshead_taken(session, args, page);
	}

	/*
	 * If the page does not yet have an insert array, our caller passed
	 * us one.
	 *
	 * NOTE: it is important to do this after publishing the list entry.
	 * Code can assume that if the array is set, it is non-empty.
	 */
	if (page->type == WT_PAGE_ROW_LEAF) {
		if (page->u.row.ins == NULL) {
			page->u.row.ins = new_inslist;
			__wt_insert_new_inslist_taken(session, args, page);
		}
	} else
		if (page->modify->update == NULL) {
			page->modify->update = new_inslist;
			__wt_insert_new_inslist_taken(session, args, page);
		}
	__wt_page_and_tree_modify_set(session, page);
	return (0);
}

/*
 * __wt_update_check --
 *	Check whether an update can proceed, and maintain the first txnid in
 *	the page->modify structure.
 */
int
__wt_update_check(WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *next)
{
	WT_DECL_RET;
	WT_TXN *txn;
	int lockout, wake = 1;

	/* Discard obsolete WT_UPDATE structures. */
	if (next != NULL)
		__wt_update_obsolete(session, page, next);

	/* Before allocating anything, make sure this update is permitted. */
	WT_RET(__wt_txn_update_check(session, next));

	/*
	 * Pause if the cache is full.
	 * This matches the logic in __wt_page_in_func.
	 */
	for (;;) {
		__wt_eviction_check(session, &lockout, wake);
		wake = 0;
		if (!lockout ||
		    F_ISSET(session, WT_SESSION_SCHEMA_LOCKED))
			break;
		if ((ret = __wt_evict_lru_page(session, 1)) == EBUSY)
			__wt_yield();
		else
			WT_RET_NOTFOUND_OK(ret);
	}

	/*
	 * Record the transaction ID for the first update to a page.
	 * We don't care if this races: there is a buffer built into the
	 * check for ancient updates.
	 */
	txn = &session->txn;
	if (page->modify->first_id == WT_TXN_NONE && txn->id != WT_TXN_NONE)
		page->modify->first_id = txn->id;

	return (0);
}

/*
 * __wt_update_alloc --
 *	Allocate a WT_UPDATE structure and associated value and fill it in.
 */
int
__wt_update_alloc(WT_SESSION_IMPL *session,
    WT_ITEM *value, WT_UPDATE **updp, size_t *sizep)
{
	WT_DECL_RET;
	WT_UPDATE *upd;
	size_t size;

	/*
	 * Allocate the WT_UPDATE structure and room for the value, then copy
	 * the value into place.
	 */
	size = value == NULL ? 0 : value->size;
	WT_RET(__wt_calloc(session, 1, sizeof(WT_UPDATE) + size, &upd));
	if (value == NULL)
		WT_UPDATE_DELETED_SET(upd);
	else {
		upd->size = WT_STORE_SIZE(size);
		memcpy(WT_UPDATE_DATA(upd), value->data, size);
	}

	/*
	 * This must come last: after __wt_txn_modify succeeds, we must return
	 * a non-NULL upd so our callers can call __wt_txn_unmodify on any
	 * subsequent failure.
	 */
	if ((ret = __wt_txn_modify(session, &upd->txnid)) != 0) {
		__wt_free(session, upd);
		return (ret);
	}

	*updp = upd;
	if (sizep != NULL)
		*sizep = sizeof(WT_UPDATE) + size;
	return (0);
}

/*
 * __wt_update_obsolete --
 *	Discard obsolete updates.
 */
void
__wt_update_obsolete(WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd)
{
	WT_TXN *txn;
	WT_UPDATE *next;
	size_t size;

	txn = &session->txn;

	if (txn->isolation != TXN_ISO_SNAPSHOT &&
	    txn->isolation != TXN_ISO_READ_COMMITTED)
		return;

	/*
	 * Walk the list of updates, looking for obsolete updates.  If we find
	 * an update no session will ever move past, we can discard any updates
	 * that appear after it.
	 */
	for (; upd != NULL; upd = upd->next)
		if (__wt_txn_visible_all(session, upd->txnid)) {
			/*
			 * We cannot discard this WT_UPDATE structure, we can
			 * only discard WT_UPDATE structures subsequent to it,
			 * other threads of control will terminate their walk
			 * in this element.  Save a reference to the list we
			 * will discard, and NULL terminate the list.
			 */
			if ((next = upd->next) == NULL)
				return;
			if (!WT_ATOMIC_CAS(upd->next, next, NULL))
				return;
			upd = next;
			break;
		}

	/*
	 * No update after upd in the list will ever be visible to any session,
	 * discard them all.
	 */
	for (size = 0; upd != NULL; upd = next) {
		/* Deleted items have a dummy size: don't include that. */
		size += sizeof(WT_UPDATE) +
		    (WT_UPDATE_DELETED_ISSET(upd) ? 0 : upd->size);
		next = upd->next;
		__wt_free(session, upd);
	}
	if (size != 0)
		__wt_cache_page_inmem_decr(session, page, size);
}

/*
 * __wt_page_obsolete --
 *	Discard all obsolete updates on a row-store leaf page.
 */
void
__wt_row_leaf_obsolete(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_INSERT *ins;
	WT_ROW *rip;
	uint32_t i;

	/* For entries before the first on-page record... */
	WT_SKIP_FOREACH(ins, WT_ROW_INSERT_SMALLEST(page))
		__wt_update_obsolete(session, page, ins->upd);

	/* For each entry on the page... */
	WT_ROW_FOREACH(page, rip, i) {
		__wt_update_obsolete(session, page, WT_ROW_UPDATE(page, rip));

		WT_SKIP_FOREACH(ins, WT_ROW_INSERT(page, rip))
			__wt_update_obsolete(session, page, ins->upd);
	}
}

/*
 * __wt_update_serial_func --
 *	Server function to add an WT_UPDATE entry in the page array.
 */
int
__wt_update_serial_func(WT_SESSION_IMPL *session, void *args)
{
	WT_PAGE *page;
	WT_UPDATE **new_upd, *upd, **upd_entry;
	uint32_t write_gen;

	__wt_update_unpack(args, &page, &write_gen, &upd_entry, &new_upd, &upd);

	/* Check the page's write-generation. */
	WT_RET(__wt_page_write_gen_check(session, page, write_gen));

	upd->next = *upd_entry;
	/*
	 * Publish: there must be a barrier to ensure the new entry's next
	 * pointer is set before we update the linked list.
	 */
	WT_PUBLISH(*upd_entry, upd);
	__wt_update_upd_taken(session, args, page);

	/*
	 * If the page needs an update array (column-store pages and inserts on
	 * row-store pages do not use the update array), our caller passed us
	 * one of the correct size.   Check the page still needs one (the write
	 * generation test should have caught that, though).
	 *
	 * NOTE: it is important to do this after publishing that the update is
	 * set.  Code can assume that if the array is set, it is non-empty.
	 */
	if (new_upd != NULL && page->u.row.upd == NULL) {
		page->u.row.upd = new_upd;
		__wt_update_new_upd_taken(session, args, page);
	}

	__wt_page_and_tree_modify_set(session, page);
	return (0);
}
