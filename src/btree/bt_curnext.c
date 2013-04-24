/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __cursor_fix_append_next --
 *	Return the next entry on the append list.
 */
static inline int
__cursor_fix_append_next(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		if ((cbt->ins = WT_SKIP_FIRST(cbt->ins_head)) == NULL)
			return (WT_NOTFOUND);
	} else
		if (cbt->recno >= WT_INSERT_RECNO(cbt->ins) &&
		    (cbt->ins = WT_SKIP_NEXT(cbt->ins)) == NULL)
			return (WT_NOTFOUND);

	/*
	 * This code looks different from the cursor-previous code.  The append
	 * list appears on the last page of the tree, but it may be preceded by
	 * other rows, which means the cursor's recno will be set to a value and
	 * we simply want to increment it.  If the cursor's recno is NOT set,
	 * we're starting our iteration in a tree that has only appended items.
	 * In that case, recno will be 0 and happily enough the increment will
	 * set it to 1, which is correct.
	 */
	__cursor_set_recno(cbt, cbt->recno + 1);

	/*
	 * Fixed-width column store appends are inherently non-transactional.
	 * Even a non-visible update by a concurrent or aborted transaction
	 * changes the effective end of the data.  The effect is subtle because
	 * of the blurring between deleted and empty values, but ideally we
	 * would skip all uncommitted changes at the end of the data.  This
	 * doesn't apply to variable-width column stores because the implicitly
	 * created records written by reconciliation are deleted and so can be
	 * never seen by a read.
	 *
	 * The problem is that we don't know at this point whether there may be
	 * multiple uncommitted changes at the end of the data, and it would be
	 * expensive to check every time we hit an aborted update.  If an
	 * insert is aborted, we simply return zero (empty), regardless of
	 * whether we are at the end of the data.
	 */
	if (cbt->recno < WT_INSERT_RECNO(cbt->ins) ||
	    (upd = __wt_txn_read(session, cbt->ins->upd)) == NULL) {
		cbt->v = 0;
		val->data = &cbt->v;
	} else
		val->data = WT_UPDATE_DATA(upd);
	val->size = 1;
	return (0);
}

/*
 * __cursor_fix_next --
 *	Move to the next, fixed-length column-store item.
 */
static inline int
__cursor_fix_next(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_BTREE *btree;
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = S2BT(session);
	val = &cbt->iface.value;

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_last_recno(cbt->page);
		if (cbt->last_standard_recno == 0)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->page->u.col_fix.recno);
		goto new_page;
	}

	/* Move to the next entry and return the item. */
	for (;;) {
		if (cbt->recno >= cbt->last_standard_recno)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->recno + 1);

new_page:	/* Check any insert list for a matching record. */
		cbt->ins_head = WT_COL_UPDATE_SINGLE(cbt->page);
		cbt->ins = __col_insert_search(
		    cbt->ins_head, cbt->ins_stack, cbt->next_stack, cbt->recno);
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
 * __cursor_var_append_next --
 *	Return the next variable-length entry on the append list.
 */
static inline int
__cursor_var_append_next(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_ITEM *val;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
		WT_ASSERT(session, cbt->ins != NULL);
		goto new_page;
	}

	for (;;) {
		if ((cbt->ins = WT_SKIP_NEXT(cbt->ins)) == NULL)
			return (WT_NOTFOUND);

new_page:	__cursor_set_recno(cbt, WT_INSERT_RECNO(cbt->ins));
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
 * __cursor_var_next --
 *	Move to the next, variable-length column-store item.
 */
static inline int
__cursor_var_next(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;
	WT_COL *cip;
	WT_DECL_RET;
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
		__cursor_set_recno(cbt, cbt->page->u.col_var.recno);
		goto new_page;
	}

	/* Move to the next entry and return the item. */
	for (;;) {
		if (cbt->recno >= cbt->last_standard_recno)
			return (WT_NOTFOUND);
		__cursor_set_recno(cbt, cbt->recno + 1);

new_page:	/* Find the matching WT_COL slot. */
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
			if (unpack.type == WT_CELL_DEL)
				continue;

			/*
			 * Restart for a variable-length column-store.  We could
			 * catch restart higher up the call-stack but there's no
			 * point to it: unlike row-store (where a normal search
			 * path finds cached overflow values), we have to access
			 * the page's reconciliation structures, and that's as
			 * easy here as higher up the stack.
			 */
			if ((ret = __wt_cell_unpack_ref(
			    session, &unpack, &cbt->tmp)) == WT_RESTART)
				ret = __wt_ovfl_cache_col_restart(
				    session, cbt->page, &unpack, &cbt->tmp);
			WT_RET(ret);

			cbt->cip_saved = cip;
		}
		val->data = cbt->tmp.data;
		val->size = cbt->tmp.size;
		return (0);
	}
	/* NOTREACHED */
}

/*
 * __cursor_row_next --
 *	Move to the next row-store item.
 */
static inline int
__cursor_row_next(WT_CURSOR_BTREE *cbt, int newpage)
{
	WT_INSERT *ins;
	WT_ITEM *key, *val;
	WT_ROW *rip;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;

	key = &cbt->iface.key;
	val = &cbt->iface.value;
	session = (WT_SESSION_IMPL *)cbt->iface.session;

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
		cbt->ins_head = WT_ROW_INSERT_SMALLEST(cbt->page);
		cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
		cbt->row_iteration_slot = 1;
		goto new_insert;
	}

	/* Move to the next entry and return the item. */
	for (;;) {
		/*
		 * Continue traversing any insert list; maintain the insert list
		 * head reference and entry count in case we switch to a cursor
		 * previous movement.
		 */
		if (cbt->ins != NULL)
			cbt->ins = WT_SKIP_NEXT(cbt->ins);

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

		/* Check for the end of the page. */
		if (cbt->row_iteration_slot >= cbt->page->entries * 2 + 1)
			return (WT_NOTFOUND);
		++cbt->row_iteration_slot;

		/*
		 * Odd-numbered slots configure as WT_INSERT_HEAD entries,
		 * even-numbered slots configure as WT_ROW entries.
		 */
		if (cbt->row_iteration_slot & 0x01) {
			cbt->ins_head = WT_ROW_INSERT_SLOT(
			    cbt->page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
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
 * __wt_btcur_iterate_setup --
 *	Initialize a cursor for iteration, usually based on a search.
 */
void
__wt_btcur_iterate_setup(WT_CURSOR_BTREE *cbt, int next)
{
	WT_PAGE *page;

	WT_UNUSED(next);

	/*
	 * We don't currently have to do any setup when we switch between next
	 * and prev calls, but I'm sure we will someday -- I'm leaving support
	 * here for both flags for that reason.
	 */
	F_SET(cbt, WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV);

	/*
	 * If we don't have a search page, then we're done, we're starting at
	 * the beginning or end of the tree, not as a result of a search.
	 */
	if ((page = cbt->page) == NULL)
		return;

	if (page->type == WT_PAGE_ROW_LEAF) {
		/*
		 * For row-store pages, we need a single item that tells us the
		 * part of the page we're walking (otherwise switching from next
		 * to prev and vice-versa is just too complicated), so we map
		 * the WT_ROW and WT_INSERT_HEAD insert array slots into a
		 * single name space: slot 1 is the "smallest key insert list",
		 * slot 2 is WT_ROW[0], slot 3 is WT_INSERT_HEAD[0], and so on.
		 * This means WT_INSERT lists are odd-numbered slots, and WT_ROW
		 * array slots are even-numbered slots.
		 */
		cbt->row_iteration_slot = (cbt->slot + 1) * 2;
		if (cbt->ins_head != NULL) {
			if (cbt->ins_head == WT_ROW_INSERT_SMALLEST(page))
				cbt->row_iteration_slot = 1;
			else
				cbt->row_iteration_slot += 1;
		}
	} else {
		/*
		 * For column-store pages, calculate the largest record on the
		 * page.
		 */
		cbt->last_standard_recno = __col_last_recno(page);

		/* If we're traversing the append list, set the reference. */
		if (cbt->ins_head != NULL &&
		    cbt->ins_head == WT_COL_APPEND(page))
			F_SET(cbt, WT_CBT_ITERATE_APPEND);
	}
}

/*
 * __wt_btcur_next --
 *	Move to the next record in the tree.
 */
int
__wt_btcur_next(WT_CURSOR_BTREE *cbt, int discard)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SESSION_IMPL *session;
	uint32_t flags;
	int newpage;

	session = (WT_SESSION_IMPL *)cbt->iface.session;

	WT_CSTAT_INCR(session, cursor_next);
	WT_DSTAT_INCR(session, cursor_next);

	flags = WT_TREE_SKIP_INTL;			/* Tree walk flags. */
	if (discard)
		LF_SET(WT_TREE_DISCARD);

retry:	WT_RET(__cursor_func_init(cbt, 0));
	__cursor_position_clear(cbt);

	/*
	 * If we aren't already iterating in the right direction, there's
	 * some setup to do.
	 */
	if (!F_ISSET(cbt, WT_CBT_ITERATE_NEXT))
		__wt_btcur_iterate_setup(cbt, 1);

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
	 * found.  Then, move to the next page, until we reach the end of the
	 * file.
	 */
	for (newpage = 0;; newpage = 1) {
		if (F_ISSET(cbt, WT_CBT_ITERATE_APPEND)) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_append_next(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_append_next(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			if (ret == 0)
				break;
			F_CLR(cbt, WT_CBT_ITERATE_APPEND);
			if (ret != WT_NOTFOUND)
				break;
		} else if (page != NULL) {
			switch (page->type) {
			case WT_PAGE_COL_FIX:
				ret = __cursor_fix_next(cbt, newpage);
				break;
			case WT_PAGE_COL_VAR:
				ret = __cursor_var_next(cbt, newpage);
				break;
			case WT_PAGE_ROW_LEAF:
				ret = __cursor_row_next(cbt, newpage);
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			if (ret != WT_NOTFOUND)
				break;

			/*
			 * The last page in a column-store has appended entries.
			 * We handle it separately from the usual cursor code:
			 * it's only that one page and it's in a simple format.
			 */
			if (page->type != WT_PAGE_ROW_LEAF &&
			    (cbt->ins_head = WT_COL_APPEND(page)) != NULL) {
				F_SET(cbt, WT_CBT_ITERATE_APPEND);
				continue;
			}
		}

		cbt->page = NULL;
		WT_ERR(__wt_tree_walk(session, &page, flags));
		WT_ERR_TEST(page == NULL, WT_NOTFOUND);
		WT_ASSERT(session,
		    page->type != WT_PAGE_COL_INT &&
		    page->type != WT_PAGE_ROW_INT);
		cbt->page = page;

		/* Initialize the page's modification information */
		if (discard) {
			WT_ERR(__wt_page_modify_init(session, page));
			WT_ORDERED_READ(
			    cbt->write_gen, page->modify->write_gen);
		}
	}

err:	if (ret == WT_RESTART)
		goto retry;
	WT_TRET(__cursor_func_resolve(cbt, ret));
	return (ret);
}

/*
 * __wt_btcur_next_random --
 *	Move to a random record in the tree.
 */
int
__wt_btcur_next_random(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = cbt->btree;

	WT_CSTAT_INCR(session, cursor_next);
	WT_DSTAT_INCR(session, cursor_next);

retry:	WT_RET(__cursor_func_init(cbt, 1));
	__cursor_position_clear(cbt);

	/*
	 * Only supports row-store: applications can trivially select a random
	 * value from a column-store, if there were any reason to do so.
	 */
	WT_ERR(btree->type == BTREE_ROW ?
	    __wt_row_random(session, cbt) : ENOTSUP);
	ret = cbt->compare == 0 ?
	    __wt_kv_return(session, cbt) : WT_NOTFOUND;

err:	if (ret == WT_RESTART)
		goto retry;
	WT_TRET(__cursor_func_resolve(cbt, ret));
	return (ret);
}
