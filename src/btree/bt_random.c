/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_row_random_leaf --
 *	Return a random key from a row-store leaf page.
 */
int
__wt_row_random_leaf(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
{
	WT_INSERT *ins, **start, **stop;
	WT_INSERT_HEAD *ins_head;
	WT_PAGE *page;
	uint64_t samples;
	uint32_t choice, entries, i;
	int level;

	page = cbt->ref->page;
	start = stop = NULL;		/* [-Wconditional-uninitialized] */
	entries = 0;			/* [-Wconditional-uninitialized] */

	__cursor_pos_clear(cbt);

	/* If the page has disk-based entries, select from them. */
	if (page->entries != 0) {
		cbt->compare = 0;
		cbt->slot = __wt_random(&session->rnd) % page->entries;

		/*
		 * The real row-store search function builds the key, so we
		 * have to as well.
		 */
		return (__wt_row_leaf_key(session,
		    page, page->pg_row + cbt->slot, cbt->tmp, false));
	}

	/*
	 * If the tree is new (and not empty), it might have a large insert
	 * list.
	 *
	 * Walk down the list until we find a level with at least 50 entries,
	 * that's where we'll start rolling random numbers. The value 50 is
	 * used to ignore levels with only a few entries, that is, levels which
	 * are potentially badly skewed.
	 */
	F_SET(cbt, WT_CBT_SEARCH_SMALLEST);
	if ((ins_head = WT_ROW_INSERT_SMALLEST(page)) == NULL)
		return (WT_NOTFOUND);
	for (level = WT_SKIP_MAXDEPTH - 1; level >= 0; --level) {
		start = &ins_head->head[level];
		for (entries = 0, stop = start;
		    *stop != NULL; stop = &(*stop)->next[level])
			++entries;

		if (entries > 50)
			break;
	}

	/*
	 * If it's a tiny list and we went all the way to level 0, correct the
	 * level; entries is correctly set.
	 */
	if (level < 0)
		level = 0;

	/*
	 * Step down the skip list levels, selecting a random chunk of the name
	 * space at each level.
	 */
	for (samples = entries; level > 0; samples += entries) {
		/*
		 * There are (entries) or (entries + 1) chunks of the name space
		 * considered at each level. They are: between start and the 1st
		 * element, between the 1st and 2nd elements, and so on to the
		 * last chunk which is the name space after the stop element on
		 * the current level. This last chunk of name space may or may
		 * not be there: as we descend the levels of the skip list, this
		 * chunk may appear, depending if the next level down has
		 * entries logically after the stop point in the current level.
		 * We can't ignore those entries: because of the algorithm used
		 * to determine the depth of a skiplist, there may be a large
		 * number of entries "revealed" by descending a level.
		 *
		 * If the next level down has more items after the current stop
		 * point, there are (entries + 1) chunks to consider, else there
		 * are (entries) chunks.
		 */
		if (*(stop - 1) == NULL)
			choice = __wt_random(&session->rnd) % entries;
		else
			choice = __wt_random(&session->rnd) % (entries + 1);

		if (choice == entries) {
			/*
			 * We selected the name space after the stop element on
			 * this level. Set the start point to the current stop
			 * point, descend a level and move the stop element to
			 * the end of the list, that is, the end of the newly
			 * discovered name space, counting entries as we go.
			 */
			start = stop;
			--start;
			--level;
			for (entries = 0, stop = start;
			    *stop != NULL; stop = &(*stop)->next[level])
				++entries;
		} else {
			/*
			 * We selected another name space on the level. Move the
			 * start pointer the selected number of entries forward
			 * to the start of the selected chunk (if the selected
			 * number is 0, start won't move). Set the stop pointer
			 * to the next element in the list and drop both start
			 * and stop down a level.
			 */
			for (i = 0; i < choice; ++i)
				start = &(*start)->next[level];
			stop = &(*start)->next[level];

			--start;
			--stop;
			--level;

			/* Count the entries in the selected name space. */
			for (entries = 0,
			    ins = *start; ins != *stop; ins = ins->next[level])
				++entries;
		}
	}

	/*
	 * When we reach the bottom level, entries will already be set. Select
	 * a random entry from the name space and return it.
	 *
	 * It should be impossible for the entries count to be 0 at this point,
	 * but check for it out of paranoia and to quiet static testing tools.
	 */
	if (entries > 0)
		entries = __wt_random(&session->rnd) % entries;
	for (ins = *start; entries > 0; --entries)
		ins = ins->next[0];

	cbt->ins = ins;
	cbt->ins_head = ins_head;
	cbt->compare = 0;

	/*
	 * Random lookups in newly created collections can be slow if a page
	 * consists of a large skiplist. Schedule the page for eviction if we
	 * encounter a large skiplist. This worthwhile because applications
	 * that take a sample often take many samples, so the overhead of
	 * traversing the skip list each time accumulates to real time.
	 */
	if (samples > 5000)
		__wt_page_evict_soon(session, cbt->ref);

	return (0);
}

/*
 * __wt_random_descent --
 *	Find a random leaf page in a tree.
 */
int
__wt_random_descent(WT_SESSION_IMPL *session, WT_REF **refp, bool eviction)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_INDEX *pindex;
	WT_REF *current, *descent;
	uint32_t flags, i, entries, retry;

	btree = S2BT(session);
	current = NULL;
	retry = 100;

	/* Eviction should not be tapped to do eviction. */
	flags = WT_READ_RESTART_OK;
	if (eviction)
		LF_SET(WT_READ_NO_EVICT);

	if (0) {
restart:	/*
		 * Discard the currently held page and restart the search from
		 * the root.
		 */
		WT_RET(__wt_page_release(session, current, 0));
	}

	/* Search the internal pages of the tree. */
	current = &btree->root;
	for (;;) {
		page = current->page;
		if (!WT_PAGE_IS_INTERNAL(page))
			break;

		WT_INTL_INDEX_GET(session, page, pindex);
		entries = pindex->entries;

		/*
		 * There may be empty pages in the tree, and they're useless to
		 * us. If we don't find a non-empty page in "entries" random
		 * guesses, take the first non-empty page in the tree. If the
		 * search page contains nothing other than empty pages, restart
		 * from the root some number of times before giving up.
		 *
		 * Eviction is only looking for a place in the cache and so only
		 * wants in-memory pages (but a deleted page is fine); currently
		 * our other caller is looking for a key/value pair on a random
		 * leave page, and so will accept any page that contains a valid
		 * key/value pair, so on-disk is fine, but deleted is not.
		 */
		descent = NULL;
		for (i = 0; i < entries; ++i) {
			descent =
			    pindex->index[__wt_random(&session->rnd) % entries];
			if (descent->state == WT_REF_MEM ||
			    (!eviction && descent->state == WT_REF_DISK))
				break;
		}
		if (i == entries)
			for (i = 0; i < entries; ++i) {
				descent = pindex->index[i];
				if (descent->state == WT_REF_MEM ||
				    (!eviction &&
				    descent->state == WT_REF_DISK))
					break;
			}
		if (i == entries || descent == NULL) {
			if (--retry > 0)
				goto restart;

			WT_RET(__wt_page_release(session, current, flags));
			return (WT_NOTFOUND);
		}

		/*
		 * Swap the current page for the child page. If the page splits
		 * while we're retrieving it, restart the search at the root.
		 *
		 * On other error, simply return, the swap call ensures we're
		 * holding nothing on failure.
		 */
		if ((ret =
		    __wt_page_swap(session, current, descent, flags)) == 0) {
			current = descent;
			continue;
		}
		if (ret == WT_RESTART)
			goto restart;
		return (ret);
	}

	*refp = current;
	return (0);
}

/*
 * __wt_btcur_next_random --
 *	Move to a random record in the tree. There are two algorithms, one
 *	where we select a record at random from the whole tree on each
 *	retrieval and one where we first select a record at random from the
 *	whole tree, and then subsequently sample forward from that location.
 *	The sampling approach allows us to select reasonably uniform random
 *	points from unbalanced trees.
 */
int
__wt_btcur_next_random(WT_CURSOR_BTREE *cbt)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_UPDATE *upd;
	wt_off_t size;
	uint64_t skip;

	session = (WT_SESSION_IMPL *)cbt->iface.session;
	btree = cbt->btree;

	/*
	 * Only supports row-store: applications can trivially select a random
	 * value from a column-store, if there were any reason to do so.
	 */
	if (btree->type != BTREE_ROW)
		WT_RET_MSG(session, ENOTSUP,
		    "WT_CURSOR.next_random only supported by row-store tables");

	WT_STAT_CONN_INCR(session, cursor_next);
	WT_STAT_DATA_INCR(session, cursor_next);

	/*
	 * If retrieving random values without sampling, or we don't have a
	 * page reference, pick a roughly random leaf page in the tree.
	 */
	if (cbt->ref == NULL || cbt->next_random_sample_size == 0) {
		/*
		 * Skip past the sample size of the leaf pages in the tree
		 * between each random key return to compensate for unbalanced
		 * trees.
		 *
		 * Use the underlying file size divided by its block allocation
		 * size as our guess of leaf pages in the file (this can be
		 * entirely wrong, as it depends on how many pages are in this
		 * particular checkpoint, how large the leaf and internal pages
		 * really are, and other factors). Then, divide that value by
		 * the configured sample size and increment the final result to
		 * make sure tiny files don't leave us with a skip value of 0.
		 *
		 * !!!
		 * Ideally, the number would be prime to avoid restart issues.
		 */
		if (cbt->next_random_sample_size != 0) {
			WT_ERR(btree->bm->size(btree->bm, session, &size));
			cbt->next_random_leaf_skip = (uint64_t)
			    ((size / btree->allocsize) /
			    cbt->next_random_sample_size) + 1;
		}

		/*
		 * Choose a leaf page from the tree.
		 */
		WT_ERR(__cursor_func_init(cbt, true));
		WT_WITH_PAGE_INDEX(session,
		    ret = __wt_random_descent(session, &cbt->ref, false));
		WT_ERR(ret);
	} else {
		/*
		 * Read through the tree, skipping leaf pages. Be cautious about
		 * the skip count: if the last leaf page skipped was also the
		 * last leaf page in the tree, it may be set to zero on return
		 * with the end-of-walk condition.
		 *
		 * Pages read for data sampling aren't "useful"; don't update
		 * the read generation of pages already in memory, and if a page
		 * is read, set its generation to a low value so it is evicted
		 * quickly.
		 */
		for (skip =
		    cbt->next_random_leaf_skip; cbt->ref == NULL || skip > 0;)
			WT_ERR(__wt_tree_walk_skip(session, &cbt->ref, &skip,
			    WT_READ_NO_GEN |
			    WT_READ_SKIP_INTL | WT_READ_WONT_NEED));
	}

	/*
	 * Select a random entry from the leaf page. If it's not valid, move to
	 * the next entry, if that doesn't work, move to the previous entry.
	 */
	WT_ERR(__wt_row_random_leaf(session, cbt));
	if (__wt_cursor_valid(cbt, &upd))
		WT_ERR(__wt_kv_return(session, cbt, upd));
	else {
		if ((ret = __wt_btcur_next(cbt, false)) == WT_NOTFOUND)
			ret = __wt_btcur_prev(cbt, false);
		WT_ERR(ret);
	}
	return (0);

err:	WT_TRET(__cursor_reset(cbt));
	return (ret);
}
