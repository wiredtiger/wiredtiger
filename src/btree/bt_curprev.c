/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Walking backwards through skip lists.
 *
 * The skip list stack is an array of pointers set up by a search.  It points
 * to the position a node should go in the skip list.  In other words, the skip
 * list search stack always points *after* the search item (that is, into the
 * search item's next array).
 *
 * Helper macros to go from a stack pointer at level i, pointing into a next
 * array, back to the insert node containing that next array.
 */
#undef	PREV_ITEM
#define	PREV_ITEM(ins_head, insp, i)					\
	(((insp) == &(ins_head)->head[i] || (insp) == NULL) ? NULL :	\
	    (WT_INSERT *)((char *)((insp) - (i)) - offsetof(WT_INSERT, next)))

#undef	PREV_INS
#define	PREV_INS(cbt, i)						\
	PREV_ITEM((cbt)->ins_head, (cbt)->ins_stack[(i)], (i))

/*
 * __cursor_skip_prev --
 *	Move back one position in a skip list stack (aka "finger").
 */
static inline int
__cursor_skip_prev(WT_CURSOR_BTREE *cbt)
{
	WT_INSERT *current, *ins;
	WT_ITEM key;
	WT_SESSION_IMPL *session;
	int i;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

restart:
	/*
	 * If the search stack does not point at the current item, fill it in
	 * with a search.
	 */
	while ((current = cbt->ins) != PREV_INS(cbt, 0)) {
		if (cbt->btree->type == BTREE_ROW) {
			key.data = WT_INSERT_KEY(current);
			key.size = WT_INSERT_KEY_SIZE(current);
			WT_RET(__wt_search_insert(
			    session, cbt, cbt->ins_head, &key));
		} else
			cbt->ins = __col_insert_search(cbt->ins_head,
			    cbt->ins_stack, WT_INSERT_RECNO(current));
	}

	/*
	 * Find the first node up the search stack that does not move.
	 *
	 * The depth of the current item must be at least this level, since we
	 * see it in that many levels of the stack.
	 *
	 * !!! Watch these loops carefully: they all rely on the value of i,
	 * and the exit conditions to end up with the right values are
	 * non-trivial.
	 */
	for (i = 0; i < WT_SKIP_MAXDEPTH - 1; i++)
		if ((ins = PREV_INS(cbt, i + 1)) != current)
			break;

	/*
	 * Find a starting point for the new search.  That is either at the
	 * non-moving node if we found a valid node, or the beginning of the
	 * next list down that is not the current node.
	 *
	 * Since it is the beginning of a list, and we know the current node is
	 * has a skip depth at least this high, any node we find must sort
	 * before the current node.
	 */
	if (ins == NULL || ins == current)
		for (; i >= 0; i--) {
			cbt->ins_stack[i] = NULL;
			ins = cbt->ins_head->head[i];
			if (ins != NULL && ins != current)
				break;
		}

	/* Walk any remaining levels until just before the current node. */
	while (i >= 0) {
		/*
		 * If we get to the end of a list without finding the current
		 * item, we must have raced with an insert.  Restart the search.
		 */
		if (ins == NULL) {
			cbt->ins_stack[0] = NULL;
			goto restart;
		}
		if (ins->next[i] != current)		/* Stay at this level */
			ins = ins->next[i];
		else {					/* Drop down a level */
			cbt->ins_stack[i] = &ins->next[i];
			--i;
		}
	}

	/* If we found a previous node, the next one must be current. */
	if (cbt->ins_stack[0] != NULL && *cbt->ins_stack[0] != current)
		goto restart;

	cbt->ins = PREV_INS(cbt, 0);
	return (0);
}

/*
 * __cursor_fix_append_prev --
 *	Return the previous fixed-length entry on the append list.
 */
static inline int
__cursor_fix_append_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		if ((cbt->ins = WT_SKIP_LAST(cbt->ins_head)) == NULL)
			return (WT_NOTFOUND);
	} else {
		/*
		 * Handle the special case of leading implicit records, that is,
		 * there aren't any records in the tree not on the append list,
		 * and the first record on the append list isn't record 1.
		 *
		 * The "right" place to handle this is probably in our caller.
		 * The high-level cursor-previous routine would:
		 *    -- call this routine to walk the append list
		 *    -- call the routine to walk the standard page items
		 *    -- call the tree walk routine looking for a previous page
		 * Each of them returns WT_NOTFOUND, at which point our caller
		 * checks the cursor record number, and if it's larger than 1,
		 * returns the implicit records.  Instead, I'm trying to detect
		 * the case here, mostly because I don't want to put that code
		 * into our caller.  Anyway, if this code breaks for any reason,
		 * that's the way I'd go.
		 *
		 * If we're not pointing to a WT_INSERT entry, or we can't find
		 * a WT_INSERT record that precedes our record name-space, check
		 * if there are any records on the page.  If there aren't, then
		 * we're in the magic zone, keep going until we get to a record
		 * number of 1.
		 */
		if (cbt->ins != NULL &&
		    cbt->recno <= WT_INSERT_RECNO(cbt->ins))
			WT_RET(__cursor_skip_prev(cbt));
		if (cbt->ins == NULL &&
		    (cbt->recno == 1 || __col_last_recno(cbt->page) != 0))
			return (WT_NOTFOUND);
	}

	/*
	 * This code looks different from the cursor-next code.  The append
	 * list appears on the last page of the tree and contains the last
	 * records in the tree.  If we're iterating through the tree, starting
	 * at the last record in the tree, by definition we're starting a new
	 * iteration and we set the record number to the last record found in
	 * the tree.  Otherwise, decrement the record.
	 */
	if (newpage)
		__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
	else
		__cursor_set_recno(cbt, cbt->recno - 1);

	/*
	 * Fixed-width column store appends are inherently non-transactional.
	 * Even a non-visible update by a concurrent or aborted transaction
	 * changes the effective end of the data.  The effect is subtle because
	 * of the blurring between deleted and empty values, but ideally we
	 * would skip all uncommitted changes at the end of the data.  This
	 * doesn't apply to variable-width column stores because the implicitly
	 * created records written by reconciliation are deleted and so can be
	 * never seen by a read.
	 */
	if (cbt->ins == NULL ||
	    cbt->recno > WT_INSERT_RECNO(cbt->ins) ||
	    (upd = __wt_txn_read(session, cbt->ins->upd)) == NULL) {
		cbt->v = 0;
		val->data = &cbt->v;
	} else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;
	return (0);
}

/*
 * __cursor_fix_prev --
 *	Move to the previous, fixed-length column-store item.
 */
static inline int
__cursor_fix_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_BTREE *btree;
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = session->btree;
	val = &cbt->iface.value;

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_last_recno(cbt->page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	/* Move to the previous entry and return the item. */
	for (;;) {
		if (cbt->recno == cbt->page->u.col_fix.recno)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->recno - 1);

new_page:	/* Check any insert list for a matching record. */
		cbt->ins_head = WT_COL_UPDATE_SINGLE(cbt->page);
		cbt->ins = __col_insert_search(
		    cbt->ins_head, cbt->ins_stack, cbt->recno);
		if (cbt->ins != NULL &&
		    cbt->recno != WT_INSERT_RECNO(cbt->ins))
			cbt->ins = NULL;
		upd = cbt->ins == NULL ?
		    NULL : __wt_txn_read(session, cbt->ins->upd);
		if (upd != NULL) {
			val->data = WT_UPDATE_DATA(upd);
			val->size = 1;
			return (0);
		}

		cbt->v = __bit_getv_recno(cbt->page, cbt->recno, btree->bitcnt);
		val->data = &cbt->v;
		val->size = 1;
		return (0);
	}
	/* NOTREACHED */
}

/*
 * __cursor_var_append_prev --
 *	Return the previous variable-length entry on the append list.
 */
static inline int
__cursor_var_append_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		cbt->ins = WT_SKIP_LAST(cbt->ins_head);
		goto new_page;
	}

	for (;;) {
		WT_RET(__cursor_skip_prev(cbt));
new_page:	if (cbt->ins == NULL)
			return (WT_NOTFOUND);

		__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
		if ((upd = __wt_txn_read(session, cbt->ins->upd)) == NULL ||
		    WT_UPDATE_DELETED_ISSET(upd))
			continue;
		val->data = WT_UPDATE_DATA(upd);
		val->size = upd->size;
		break;
	}
	return (0);
}

/*
 * __cursor_var_prev --
 *	Move to the previous, variable-length column-store item.
 */
static inline int
__cursor_var_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_COL *cip;
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_last_recno(cbt->page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	/* Move to the previous entry and return the item. */
	for (;;) {
		__cursor_set_recno(cbt, cbt->recno - 1);

new_page:	if (cbt->recno < cbt->page->u.col_var.recno)
			return (WT_NOTFOUND);

		/* Find the matching WT_COL slot. */
		if ((cip = __col_var_search(cbt->page, cbt->recno)) == NULL)
			return (WT_NOTFOUND);
		cbt->slot = WT_COL_SLOT(cbt->page, cip);

		/* Check any insert list for a matching record. */
		cbt->ins_head = WT_COL_UPDATE_SLOT(cbt->page, cbt->slot);
		cbt->ins = __col_insert_search_match(cbt->ins_head, cbt->recno);
		upd = cbt->ins == NULL ?
		    NULL : __wt_txn_read(session, cbt->ins->upd);
		if (upd != NULL) {
			if (WT_UPDATE_DELETED_ISSET(upd))
				continue;

			val->data = WT_UPDATE_DATA(upd);
			val->size = upd->size;
			return (0);
		}

		/*
		 * If we're at the same slot as the last reference and there's
		 * no matching insert list item, re-use the return information
		 * (so encoded items with large repeat counts aren't repeatedly
		 * decoded).  Otherwise, unpack the cell and build the return
		 * information.
		 */
		if (cbt->cip_saved != cip) {
			if ((cell = WT_COL_PTR(cbt->page, cip)) == NULL)
				continue;
			__wt_cell_unpack(cell, &unpack);
			switch (unpack.type) {
			case WT_CELL_DEL:
				continue;
			case WT_CELL_VALUE:
				if (session->btree->huffman_value == NULL) {
					cbt->tmp.data = unpack.data;
					cbt->tmp.size = unpack.size;
					break;
				}
				/* FALLTHROUGH */
			default:
				WT_RET(__wt_cell_unpack_copy(
				    session, &unpack, &cbt->tmp));
			}
			cbt->cip_saved = cip;
		}
		val->data = cbt->tmp.data;
		val->size = cbt->tmp.size;
		return (0);
	}
	/* NOTREACHED */
}

/*
 * __cursor_row_prev --
 *	Move to the previous row-store item.
 */
static inline int
__cursor_row_prev(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_INSERT *ins;
	WT_ITEM *key, *val;
	WT_ROW *rip;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	key = &cbt->iface.key;
	val = &cbt->iface.value;

	/*
	 * For row-store pages, we need a single item that tells us the part
	 * of the page we're walking (otherwise switching from next to prev
	 * and vice-versa is just too complicated), so we map the WT_ROW and
	 * WT_INSERT_HEAD insert array slots into a single name space: slot 1
	 * is the "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is
	 * WT_INSERT_HEAD[0], and so on.  This means WT_INSERT lists are
	 * odd-numbered slots, and WT_ROW array slots are even-numbered slots.
	 *
	 * New page configuration.
	 */
	if (newpage) {
		/*
		 * If we haven't instantiated keys on this page, do so, else it
		 * is a very, very slow traversal.
		 */
		if (!F_ISSET_ATOMIC(cbt->page, WT_PAGE_BUILD_KEYS))
			WT_RET(__wt_row_leaf_keys(session, cbt->page));

		if (cbt->page->entries == 0)
			cbt->ins_head = WT_ROW_INSERT_SMALLEST(cbt->page);
		else
			cbt->ins_head = WT_ROW_INSERT_SLOT(
			    cbt->page, cbt->page->entries - 1);
		cbt->ins = WT_SKIP_LAST(cbt->ins_head);
		cbt->row_iteration_slot = cbt->page->entries * 2 + 1;
		goto new_insert;
	}

	/* Move to the previous entry and return the item. */
	for (;;) {
		/*
		 * Continue traversing any insert list.  Maintain the reference
		 * to the current insert element in case we switch to a cursor
		 * next movement.
		 */
		if (cbt->ins != NULL)
			WT_RET(__cursor_skip_prev(cbt));

new_insert:	if ((ins = cbt->ins) != NULL) {
			if ((upd = __wt_txn_read(session, ins->upd)) == NULL ||
			    WT_UPDATE_DELETED_ISSET(upd))
				continue;
			key->data = WT_INSERT_KEY(ins);
			key->size = WT_INSERT_KEY_SIZE(ins);
			val->data = WT_UPDATE_DATA(upd);
			val->size = upd->size;
			return (0);
		}

		/* Check for the beginning of the page. */
		if (cbt->row_iteration_slot == 1)
			return (WT_NOTFOUND);
		--cbt->row_iteration_slot;

		/*
		 * Odd-numbered slots configure as WT_INSERT_HEAD entries,
		 * even-numbered slots configure as WT_ROW entries.
		 */
		if (cbt->row_iteration_slot & 0x01) {
			cbt->ins_head = cbt->row_iteration_slot == 1 ?
			    WT_ROW_INSERT_SMALLEST(cbt->page) :
			    WT_ROW_INSERT_SLOT(
			    cbt->page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = WT_SKIP_LAST(cbt->ins_head);
			goto new_insert;
		}
		cbt->ins_head = NULL;
		cbt->ins = NULL;

		cbt->slot = cbt->row_iteration_slot / 2 - 1;
		rip = &cbt->page->u.row.d[cbt->slot];
		upd = __wt_txn_read(session, WT_ROW_UPDATE(cbt->page, rip));
		if (upd != NULL && WT_UPDATE_DELETED_ISSET(upd))
			continue;

		return (__cursor_row_slot_return(cbt, rip, upd));
	}
	/* NOTREACHED */
}

/*
 * __wt_btcur_prev --
 *	Move to the previous record in the tree.
 */
int
__wt_btcur_prev(WT_CURSOR_BTREE *cbt, int discard)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	uint32_t flags;
	int newpage;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	WT_BSTAT_INCR(session, cursor_read_prev);

	flags = WT_TREE_PREV;				/* Tree walk flags. */
	if (discard)
		LF_SET(WT_TREE_DISCARD);

	__cursor_func_init(cbt, 0);
	__cursor_position_clear(cbt);

	/*
	 * If we aren't already iterating in the right direction, there's
	 * some setup to do.
	 */
	if (!F_ISSET(cbt, WT_CBT_ITERATE_PREV))
		__wt_btcur_iterate_setup(cbt, 0);

	/*
	 * If this is a modification, we're about to read information from the
	 * page, save the write generation.
	 */
	page = cbt->page;
	if (discard && page != NULL) {
		WT_ERR(__wt_page_modify_init(session, page));
		WT_ORDERED_READ(cbt->write_gen, page->modify->write_gen);
	}

	/*
	 * Walk any page we're holding until the underlying call returns not-
	 * found.  Then, move to the previous page, until we reach the start
	 * of the file.
	 */
	for (newpage = 0;; newpage = 1) {
		if (F_ISSET(cbt, WT_CBT_ITERATE_APPEND)) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_append_prev(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_append_prev(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			if (ret == 0)
				break;
			F_CLR(cbt, WT_CBT_ITERATE_APPEND);
			if (ret != WT_NOTFOUND)
				break;
			newpage = 1;
		}
		if (page != NULL) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_prev(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_prev(cbt, newpage);
				break;
			case WT_PAGE_ROW_LEAF:
				ret = __cursor_row_prev(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			if (ret != WT_NOTFOUND)
				break;
		}

		cbt->page = NULL;
		do {
			WT_ERR(__wt_tree_walk(session, &page, flags));
			WT_ERR_TEST(page == NULL, WT_NOTFOUND);
		} while (
		    page->type == WT_PAGE_COL_INT ||
		    page->type == WT_PAGE_ROW_INT);
		cbt->page = page;

		/* Initialize the page's modification information */
		if (discard) {
			WT_ERR(__wt_page_modify_init(session, page));
			WT_ORDERED_READ(
			    cbt->write_gen, page->modify->write_gen);
		}

		/*
		 * The last page in a column-store has appended entries.
		 * We handle it separately from the usual cursor code:
		 * it's only that one page and it's in a simple format.
		 */
		if (page->type != WT_PAGE_ROW_LEAF &&
		    (cbt->ins_head = WT_COL_APPEND(page)) != NULL)
			F_SET(cbt, WT_CBT_ITERATE_APPEND);
	}

err:	__cursor_func_resolve(cbt, ret);
	return (ret);
}
