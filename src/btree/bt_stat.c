/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int  __stat_page(WT_SESSION_IMPL *, WT_PAGE *, WT_DSRC_STATS *);
static int  __stat_page_col_var(WT_SESSION_IMPL *, WT_PAGE *, WT_DSRC_STATS *);
static int  __stat_page_row_leaf(WT_SESSION_IMPL *, WT_PAGE *, WT_DSRC_STATS *);

/*
 * __wt_btree_stat_init --
 *	Initialize the Btree statistics.
 */
int
__wt_btree_stat_init(WT_SESSION_IMPL *session, uint32_t flags)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_DSRC_STATS *stats;
	WT_PAGE *page;

	btree = S2BT(session);
	bm = btree->bm;
	stats = &btree->dhandle->stats;

	WT_RET(bm->stat(bm, session, stats));

	WT_STAT_SET(session, stats, btree_fixed_len, btree->bitcnt);
	WT_STAT_SET(session, stats, btree_maximum_depth, btree->maximum_depth);
	WT_STAT_SET(session, stats, btree_maxintlitem, btree->maxintlitem);
	WT_STAT_SET(session, stats, btree_maxintlpage, btree->maxintlpage);
	WT_STAT_SET(session, stats, btree_maxleafitem, btree->maxleafitem);
	WT_STAT_SET(session, stats, btree_maxleafpage, btree->maxleafpage);

	page = NULL;
	if (LF_ISSET(WT_STATISTICS_FAST))
		return (0);

	while ((ret = __wt_tree_walk(session, &page, 0)) == 0 && page != NULL)
		WT_RET(__stat_page(session, page, stats));
	return (ret == WT_NOTFOUND ? 0 : ret);
}

/*
 * __stat_page --
 *	Stat any Btree page.
 */
static int
__stat_page(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	/*
	 * All internal pages and overflow pages are trivial, all we track is
	 * a count of the page type.
	 */
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		WT_STAT_INCR(session, stats, btree_column_fix);
		WT_STAT_INCRV(session, stats, btree_entries, page->entries);
		break;
	case WT_PAGE_COL_INT:
		WT_STAT_INCR(session, stats, btree_column_internal);
		WT_STAT_INCRV(session, stats, btree_entries, page->entries);
		break;
	case WT_PAGE_COL_VAR:
		WT_RET(__stat_page_col_var(session, page, stats));
		break;
	case WT_PAGE_OVFL:
		WT_STAT_INCR(session, stats, btree_overflow);
		break;
	case WT_PAGE_ROW_INT:
		WT_STAT_INCR(session, stats, btree_row_internal);
		WT_STAT_INCRV(session, stats, btree_entries, page->entries);
		break;
	case WT_PAGE_ROW_LEAF:
		WT_RET(__stat_page_row_leaf(session, page, stats));
		break;
	WT_ILLEGAL_VALUE(session);
	}
	return (0);
}

/*
 * __stat_page_col_var --
 *	Stat a WT_PAGE_COL_VAR page.
 */
static int
__stat_page_col_var(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_COL *cip;
	WT_INSERT *ins;
	WT_UPDATE *upd;
	uint32_t i;
	int orig_deleted;

	unpack = &_unpack;

	WT_STAT_INCR(session, stats, btree_column_variable);

	/*
	 * Walk the page, counting regular and overflow data items, and checking
	 * to be sure any updates weren't deletions.  If the item was updated,
	 * assume it was updated by an item of the same size (it's expensive to
	 * figure out if it will require the same space or not, especially if
	 * there's Huffman encoding).
	 */
	WT_COL_FOREACH(page, cip, i) {
		if ((cell = WT_COL_PTR(page, cip)) == NULL) {
			orig_deleted = 1;
			WT_STAT_INCR(session, stats, btree_column_deleted);
		} else {
			orig_deleted = 0;
			__wt_cell_unpack(cell, WT_PAGE_COL_VAR, unpack);
			WT_STAT_INCRV(session,
			    stats, btree_entries, __wt_cell_rle(unpack));
		}

		/*
		 * Walk the insert list, checking for changes.  For each insert
		 * we find, correct the original count based on its state.
		 */
		WT_SKIP_FOREACH(ins, WT_COL_UPDATE(page, cip)) {
			upd = ins->upd;
			if (WT_UPDATE_DELETED_ISSET(upd)) {
				if (orig_deleted)
					continue;
				WT_STAT_INCR(
				    session, stats, btree_column_deleted);
				WT_STAT_DECR(session, stats, btree_entries);
			} else {
				if (!orig_deleted)
					continue;
				WT_STAT_DECR(
				    session, stats, btree_column_deleted);
				WT_STAT_INCR(session, stats, btree_entries);
			}
		}
	}
	return (0);
}

/*
 * __stat_page_row_leaf --
 *	Stat a WT_PAGE_ROW_LEAF page.
 */
static int
__stat_page_row_leaf(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSRC_STATS *stats)
{
	WT_INSERT *ins;
	WT_ROW *rip;
	WT_UPDATE *upd;
	uint32_t cnt, i;

	WT_STAT_INCR(session, stats, btree_row_leaf);

	/*
	 * Stat any K/V pairs inserted into the page before the first from-disk
	 * key on the page.
	 */
	cnt = 0;
	WT_SKIP_FOREACH(ins, WT_ROW_INSERT_SMALLEST(page))
		if (!WT_UPDATE_DELETED_ISSET(ins->upd))
			++cnt;

	/* Stat the page's K/V pairs. */
	WT_ROW_FOREACH(page, rip, i) {
		upd = WT_ROW_UPDATE(page, rip);
		if (upd == NULL || !WT_UPDATE_DELETED_ISSET(upd))
			++cnt;

		/* Stat inserted K/V pairs. */
		WT_SKIP_FOREACH(ins, WT_ROW_INSERT(page, rip))
			if (!WT_UPDATE_DELETED_ISSET(ins->upd))
				++cnt;
	}

	WT_STAT_INCRV(session, stats, btree_entries, cnt);

	return (0);
}
