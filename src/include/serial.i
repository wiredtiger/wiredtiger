/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __page_write_gen_wrapped_check --
 *	Confirm the page's write generation number won't wrap.
 */
static inline int
__page_write_gen_wrapped_check(WT_PAGE *page)
{
	return (page->modify->write_gen >
	    UINT32_MAX - WT_MILLION ? WT_RESTART : 0);
}

/*
 * __insert_serial_func --
 *	Worker function to add a WT_INSERT entry to a skiplist.
 */
static inline int
__insert_serial_func(WT_SESSION_IMPL *session,
    WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT *new_ins,
    u_int skipdepth)
{
	u_int i;

	WT_UNUSED(session);

	/*
	 * Confirm we are still in the expected position, and no item has been
	 * added where our insert belongs.  Take extra care at the beginning
	 * and end of the list (at each level): retry if we race there.
	 *
	 * !!!
	 * Note the test for ins_stack[0] == NULL: that's the test for an
	 * uninitialized cursor, ins_stack[0] is cleared as part of
	 * initializing a cursor for a search.
	 */
	for (i = 0; i < skipdepth; i++) {
		if (ins_stack[i] == NULL ||
		    *ins_stack[i] != new_ins->next[i])
			return (WT_RESTART);
		if (new_ins->next[i] == NULL &&
		    ins_head->tail[i] != NULL &&
		    ins_stack[i] != &ins_head->tail[i]->next[i])
			return (WT_RESTART);
	}

	/* Update the skiplist elements referencing the new WT_INSERT item. */
	for (i = 0; i < skipdepth; i++) {
		if (ins_head->tail[i] == NULL ||
		    ins_stack[i] == &ins_head->tail[i]->next[i])
			ins_head->tail[i] = new_ins;
		*ins_stack[i] = new_ins;
	}

	return (0);
}

/*
 * __col_append_serial_func --
 *	Worker function to allocate a record number as necessary, then add a
 * WT_INSERT entry to a skiplist.
 */
static inline int
__col_append_serial_func(WT_SESSION_IMPL *session,
    WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT *new_ins,
    uint64_t *recnop, u_int skipdepth)
{
	WT_BTREE *btree;
	uint64_t recno;
	u_int i;

	btree = S2BT(session);

	/*
	 * If the application didn't specify a record number, allocate a new one
	 * and set up for an append.
	 */
	if ((recno = WT_INSERT_RECNO(new_ins)) == 0) {
		recno = WT_INSERT_RECNO(new_ins) = btree->last_recno + 1;
		WT_ASSERT(session, WT_SKIP_LAST(ins_head) == NULL ||
		    recno > WT_INSERT_RECNO(WT_SKIP_LAST(ins_head)));
		for (i = 0; i < skipdepth; i++)
			ins_stack[i] = ins_head->tail[i] == NULL ?
			    &ins_head->head[i] : &ins_head->tail[i]->next[i];
	}

	/* Confirm position and insert the new WT_INSERT item. */
	WT_RET(__insert_serial_func(
	    session, ins_head, ins_stack, new_ins, skipdepth));

	/*
	 * Set the calling cursor's record number.
	 * If we extended the file, update the last record number.
	 */
	*recnop = recno;
	if (recno > btree->last_recno)
		btree->last_recno = recno;

	return (0);
}

/*
 * __update_serial_func --
 *	Worker function to add an WT_UPDATE entry in the page array.
 */
static inline int
__update_serial_func(WT_SESSION_IMPL *session,
    WT_PAGE *page, WT_UPDATE **upd_entry, WT_UPDATE *upd)
{
	WT_UPDATE *obsolete;
	WT_DECL_SPINLOCK_ID(id);			/* Must appear last */

	/*
	 * Swap the update into place.  If that fails, a new update was added
	 * after our search, we raced.  Check if our update is still permitted,
	 * and if it is, do a full-barrier to ensure the update's next pointer
	 * is set before we update the linked list and try again.
	 */
	while (!WT_ATOMIC_CAS(*upd_entry, upd->next, upd)) {
		WT_RET(__wt_txn_update_check(session, upd->next = *upd_entry));
		WT_WRITE_BARRIER();
	}

	/*
	 * If there are WT_UPDATE structures to review, we're evicting pages,
	 * and no other thread holds the page's spinlock, discard obsolete
	 * WT_UPDATE structures.  Serialization is needed so only one thread
	 * does the obsolete check at a time.
	 */
	if (upd->next != NULL &&
	    F_ISSET(S2C(session)->cache, WT_EVICT_ACTIVE) &&
	    WT_PAGE_TRYLOCK(session, page, &id) == 0) {
		obsolete = __wt_update_obsolete_check(session, upd->next);
		WT_PAGE_UNLOCK(session, page);
		if (obsolete != NULL)
			__wt_update_obsolete_free(session, page, obsolete);
	}
	return (0);
}

/*
 * DO NOT EDIT: automatically built by dist/serial.py.
 * Serialization function section: BEGIN
 */

static inline int
__wt_col_append_serial(
	WT_SESSION_IMPL *session, WT_PAGE *page, WT_INSERT_HEAD *ins_head,
	WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
	uint64_t *recnop, u_int skipdepth)
{
	WT_INSERT *new_ins = *new_insp;
	WT_DECL_RET;
	size_t incr_mem;

	/* Clear references to memory we now own. */
	*new_insp = NULL;

	/*
	 * Check to see if the page's write generation is about to wrap (wildly
	 * unlikely as it implies 4B updates between clean page reconciliations,
	 * but technically possible), and fail the update.
	 *
	 * The check is outside of the serialization mutex because the page's
	 * write generation is going to be a hot cache line, so technically it's
	 * possible for the page's write generation to wrap between the test and
	 * our subsequent modification of it.  However, the test is (4B-1M), and
	 * there cannot be a million threads that have done the test but not yet
	 * completed their modification.
	 */
	 WT_RET(__page_write_gen_wrapped_check(page));

	/* Acquire the page's spinlock, call the worker function. */
	WT_PAGE_LOCK(session, page);
	ret = __col_append_serial_func(
	    session, ins_head, ins_stack, new_ins, recnop, skipdepth);
	WT_PAGE_UNLOCK(session, page);

	/* Free unused memory on error. */
	if (ret != 0) {
		__wt_free(session, new_ins);

		return (ret);
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	incr_mem = 0;
	WT_ASSERT(session, new_ins_size != 0);
	incr_mem += new_ins_size;
	if (incr_mem != 0)
		__wt_cache_page_inmem_incr(session, page, incr_mem);

	/* Mark the page dirty after updating the footprint. */
	__wt_page_modify_set(session, page);

	return (0);
}

static inline int
__wt_insert_serial(
	WT_SESSION_IMPL *session, WT_PAGE *page, WT_INSERT_HEAD *ins_head,
	WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
	u_int skipdepth)
{
	WT_INSERT *new_ins = *new_insp;
	WT_DECL_RET;
	size_t incr_mem;

	/* Clear references to memory we now own. */
	*new_insp = NULL;

	/*
	 * Check to see if the page's write generation is about to wrap (wildly
	 * unlikely as it implies 4B updates between clean page reconciliations,
	 * but technically possible), and fail the update.
	 *
	 * The check is outside of the serialization mutex because the page's
	 * write generation is going to be a hot cache line, so technically it's
	 * possible for the page's write generation to wrap between the test and
	 * our subsequent modification of it.  However, the test is (4B-1M), and
	 * there cannot be a million threads that have done the test but not yet
	 * completed their modification.
	 */
	 WT_RET(__page_write_gen_wrapped_check(page));

	/* Acquire the page's spinlock, call the worker function. */
	WT_PAGE_LOCK(session, page);
	ret = __insert_serial_func(
	    session, ins_head, ins_stack, new_ins, skipdepth);
	WT_PAGE_UNLOCK(session, page);

	/* Free unused memory on error. */
	if (ret != 0) {
		__wt_free(session, new_ins);

		return (ret);
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	incr_mem = 0;
	WT_ASSERT(session, new_ins_size != 0);
	incr_mem += new_ins_size;
	if (incr_mem != 0)
		__wt_cache_page_inmem_incr(session, page, incr_mem);

	/* Mark the page dirty after updating the footprint. */
	__wt_page_modify_set(session, page);

	return (0);
}

static inline int
__wt_update_serial(
	WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE **srch_upd,
	WT_UPDATE **updp, size_t upd_size)
{
	WT_UPDATE *upd = *updp;
	WT_DECL_RET;
	size_t incr_mem;

	/* Clear references to memory we now own. */
	*updp = NULL;

	/*
	 * Check to see if the page's write generation is about to wrap (wildly
	 * unlikely as it implies 4B updates between clean page reconciliations,
	 * but technically possible), and fail the update.
	 *
	 * The check is outside of the serialization mutex because the page's
	 * write generation is going to be a hot cache line, so technically it's
	 * possible for the page's write generation to wrap between the test and
	 * our subsequent modification of it.  However, the test is (4B-1M), and
	 * there cannot be a million threads that have done the test but not yet
	 * completed their modification.
	 */
	 WT_RET(__page_write_gen_wrapped_check(page));

	ret = __update_serial_func(
	    session, page, srch_upd, upd);

	/* Free unused memory on error. */
	if (ret != 0) {
		__wt_free(session, upd);

		return (ret);
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	incr_mem = 0;
	WT_ASSERT(session, upd_size != 0);
	incr_mem += upd_size;
	if (incr_mem != 0)
		__wt_cache_page_inmem_incr(session, page, incr_mem);

	/* Mark the page dirty after updating the footprint. */
	__wt_page_modify_set(session, page);

	return (0);
}

/*
 * Serialization function section: END
 * DO NOT EDIT: automatically built by dist/serial.py.
 */
