/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_page_modify_alloc --
 *	Allocate a page's modification structure.
 */
int
__wt_page_modify_alloc(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_DECL_RET;
	WT_PAGE_MODIFY *modify;

	WT_RET(__wt_calloc_one(session, &modify));

	/* Initialize the spinlock for the page. */
	WT_ERR(__wt_spin_init(session, &modify->page_lock, "btree page"));

	/*
	 * Multiple threads of control may be searching and deciding to modify
	 * a page.  If our modify structure is used, update the page's memory
	 * footprint, else discard the modify structure, another thread did the
	 * work.
	 */
	if (__wt_atomic_cas_ptr(&page->modify, NULL, modify))
		__wt_cache_page_inmem_incr(session, page, sizeof(*modify));
	else
err:		__wt_free(session, modify);
	return (ret);
}

/*
 * __wt_row_modify --
 *	Row-store insert, update and delete.
 */
int
__wt_row_modify(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
    const WT_ITEM *key, const WT_ITEM *value,
    WT_UPDATE *upd_arg, u_int modify_type, bool exclusive)
{
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_INSERT_HEAD *ins_head, **ins_headp;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	WT_UPDATE *old_upd, *upd, **upd_entry;
	size_t ins_size, upd_size;
	uint32_t ins_slot;
	u_int i, skipdepth;
	bool logged;

	ins = NULL;
	page = cbt->ref->page;
	upd = upd_arg;
	logged = false;

	/* If we don't yet have a modify structure, we'll need one. */
	WT_RET(__wt_page_modify_init(session, page));
	mod = page->modify;

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
		if (cbt->ins == NULL) {
			/* Allocate an update array as necessary. */
			WT_PAGE_ALLOC_AND_SWAP(session, page,
			    mod->mod_row_update, upd_entry, page->entries);

			/* Set the WT_UPDATE array reference. */
			upd_entry = &mod->mod_row_update[cbt->slot];
		} else
			upd_entry = &cbt->ins->upd;

		if (upd_arg == NULL) {
			/* Make sure the update can proceed. */
			WT_ERR(__wt_txn_update_check(
			    session, old_upd = *upd_entry));

			/* Allocate a WT_UPDATE structure and transaction ID. */
			WT_ERR(__wt_update_alloc(session,
			    value, &upd, &upd_size, modify_type));
			WT_ERR(__wt_txn_modify(session, upd));
			logged = true;

			/* Avoid WT_CURSOR.update data copy. */
			cbt->modify_update = upd;
		} else {
			upd_size = __wt_update_list_memsize(upd);

			/*
			 * We are restoring updates that couldn't be evicted,
			 * there should only be one update list per key.
			 */
			WT_ASSERT(session, *upd_entry == NULL);

			/*
			 * Set the "old" entry to the second update in the list
			 * so that the serialization function succeeds in
			 * swapping the first update into place.
			 */
			old_upd = *upd_entry = upd->next;
		}

		/*
		 * Point the new WT_UPDATE item to the next element in the list.
		 * If we get it right, the serialization function lock acts as
		 * our memory barrier to flush this write.
		 */
		upd->next = old_upd;

		/* Serialize the update. */
		WT_ERR(__wt_update_serial(
		    session, page, upd_entry, &upd, upd_size, exclusive));
	} else {
		/*
		 * Allocate the insert array as necessary.
		 *
		 * We allocate an additional insert array slot for insert keys
		 * sorting less than any key on the page.  The test to select
		 * that slot is baroque: if the search returned the first page
		 * slot, we didn't end up processing an insert list, and the
		 * comparison value indicates the search key was smaller than
		 * the returned slot, then we're using the smallest-key insert
		 * slot.  That's hard, so we set a flag.
		 */
		WT_PAGE_ALLOC_AND_SWAP(session, page,
		    mod->mod_row_insert, ins_headp, page->entries + 1);

		ins_slot = F_ISSET(cbt, WT_CBT_SEARCH_SMALLEST) ?
		    page->entries: cbt->slot;
		ins_headp = &mod->mod_row_insert[ins_slot];

		/* Allocate the WT_INSERT_HEAD structure as necessary. */
		WT_PAGE_ALLOC_AND_SWAP(session, page, *ins_headp, ins_head, 1);
		ins_head = *ins_headp;

		/* Choose a skiplist depth for this insert. */
		skipdepth = __wt_skip_choose_depth(session);

		/*
		 * Allocate a WT_INSERT/WT_UPDATE pair and transaction ID, and
		 * update the cursor to reference it (the WT_INSERT_HEAD might
		 * be allocated, the WT_INSERT was allocated).
		 */
		WT_ERR(__wt_row_insert_alloc(
		    session, key, skipdepth, &ins, &ins_size));
		cbt->ins_head = ins_head;
		cbt->ins = ins;

		if (upd_arg == NULL) {
			WT_ERR(__wt_update_alloc(session,
			    value, &upd, &upd_size, modify_type));
			WT_ERR(__wt_txn_modify(session, upd));
			logged = true;

			/* Avoid WT_CURSOR.update data copy. */
			cbt->modify_update = upd;
		} else
			upd_size = __wt_update_list_memsize(upd);

		ins->upd = upd;
		ins_size += upd_size;

		/*
		 * If there was no insert list during the search, the cursor's
		 * information cannot be correct, search couldn't have
		 * initialized it.
		 *
		 * Otherwise, point the new WT_INSERT item's skiplist to the
		 * next elements in the insert list (which we will check are
		 * still valid inside the serialization function).
		 *
		 * The serial mutex acts as our memory barrier to flush these
		 * writes before inserting them into the list.
		 */
		if (cbt->ins_stack[0] == NULL)
			for (i = 0; i < skipdepth; i++) {
				cbt->ins_stack[i] = &ins_head->head[i];
				ins->next[i] = cbt->next_stack[i] = NULL;
			}
		else
			for (i = 0; i < skipdepth; i++)
				ins->next[i] = cbt->next_stack[i];

		/* Insert the WT_INSERT structure. */
		WT_ERR(__wt_insert_serial(
		    session, page, cbt->ins_head, cbt->ins_stack,
		    &ins, ins_size, skipdepth, exclusive));
	}

	if (logged && modify_type != WT_UPDATE_RESERVED)
		WT_ERR(__wt_txn_log_op(session, cbt));

	if (0) {
err:		/*
		 * Remove the update from the current transaction, so we don't
		 * try to modify it on rollback.
		 */
		if (logged)
			__wt_txn_unmodify(session);
		__wt_free(session, ins);
		cbt->ins = NULL;
		if (upd_arg == NULL)
			__wt_free(session, upd);
	}

	return (ret);
}

/*
 * __wt_row_insert_alloc --
 *	Row-store insert: allocate a WT_INSERT structure and fill it in.
 */
int
__wt_row_insert_alloc(WT_SESSION_IMPL *session,
    const WT_ITEM *key, u_int skipdepth, WT_INSERT **insp, size_t *ins_sizep)
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
	WT_INSERT_KEY_SIZE(ins) = WT_STORE_SIZE(key->size);
	memcpy(WT_INSERT_KEY(ins), key->data, key->size);

	*insp = ins;
	if (ins_sizep != NULL)
		*ins_sizep = ins_size;
	return (0);
}

/*
 * __wt_update_alloc --
 *	Allocate a WT_UPDATE structure and associated value and fill it in.
 */
int
__wt_update_alloc(WT_SESSION_IMPL *session, const WT_ITEM *value,
    WT_UPDATE **updp, size_t *sizep, u_int modify_type)
{
	WT_UPDATE *upd;

	*updp = NULL;

	/*
	 * The code paths leading here are convoluted: assert we never attempt
	 * to allocate an update structure if only intending to insert one we
	 * already have.
	 */
	WT_ASSERT(session, modify_type != WT_UPDATE_INVALID);

	/*
	 * Allocate the WT_UPDATE structure and room for the value, then copy
	 * the value into place.
	 */
	if (modify_type == WT_UPDATE_DELETED ||
	    modify_type == WT_UPDATE_RESERVED)
		WT_RET(__wt_calloc(session, 1, WT_UPDATE_SIZE, &upd));
	else {
		WT_RET(__wt_calloc(
		    session, 1, WT_UPDATE_SIZE + value->size, &upd));
		if (value->size != 0) {
			upd->size = WT_STORE_SIZE(value->size);
			memcpy(upd->data, value->data, value->size);
		}
	}
	upd->type = (uint8_t)modify_type;

	*updp = upd;
	*sizep = WT_UPDATE_MEMSIZE(upd);
	return (0);
}

/*
 * __wt_update_obsolete_check --
 *	Check for obsolete updates.
 */
WT_UPDATE *
__wt_update_obsolete_check(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd)
{
	WT_UPDATE *first, *next;
	u_int count;

	/*
	 * This function identifies obsolete updates, and truncates them from
	 * the rest of the chain; because this routine is called from inside
	 * a serialization function, the caller has responsibility for actually
	 * freeing the memory.
	 *
	 * Walk the list of updates, looking for obsolete updates at the end.
	 *
	 * Only updates with globally visible, self-contained data can terminate
	 * update chains.
	 */
	for (first = NULL, count = 0; upd != NULL; upd = upd->next, count++)
		if (WT_UPDATE_DATA_VALUE(upd) &&
		    __wt_txn_upd_visible_all(session, upd)) {
			if (first == NULL)
				first = upd;
		} else if (upd->txnid != WT_TXN_ABORTED)
			first = NULL;

	/*
	 * We cannot discard this WT_UPDATE structure, we can only discard
	 * WT_UPDATE structures subsequent to it, other threads of control will
	 * terminate their walk in this element.  Save a reference to the list
	 * we will discard, and terminate the list.
	 */
	if (first != NULL &&
	    (next = first->next) != NULL &&
	    __wt_atomic_cas_ptr(&first->next, next, NULL))
		return (next);

	/*
	 * If the list is long, don't retry checks on this page until the
	 * transaction state has moved forwards. This function is used to
	 * trim update lists independently of the page state, ensure there
	 * is a modify structure.
	 */
	if (count > 20 && page->modify != NULL)
		page->modify->obsolete_check_txn =
		    S2C(session)->txn_global.last_running;

	return (NULL);
}

/*
 * __wt_update_obsolete_free --
 *	Free an obsolete update list.
 */
void
__wt_update_obsolete_free(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd)
{
	WT_UPDATE *next;
	size_t size;

	/* Free a WT_UPDATE list. */
	for (size = 0; upd != NULL; upd = next) {
		next = upd->next;
		size += WT_UPDATE_MEMSIZE(upd);
		__wt_free(session, upd);
	}
	if (size != 0)
		__wt_cache_page_inmem_decr(session, page, size);
}
