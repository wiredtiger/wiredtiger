/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef HAVE_TIMESTAMPS

/*
 * __txn_rollback_nondurable_commits_check --
 *	Ensure the rollback request is reasonable.
 */
static int
__txn_rollback_nondurable_commits_check(
    WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
	WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;
	WT_TXN_STATE *s;
	uint32_t i, session_cnt;

	if (__wt_timestamp_cmp(
	    rollback_timestamp, txn_global->pinned_timestamp) < 0)
		WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
		    "requires a timestamp greater than the pinned timestamp");

	/*
	 * Help the user - see if they have any active transactions. I'd
	 * like to check the transaction running flag, but that would
	 * require peeking into all open sessions, which isn't really
	 * kosher.
	 */
	WT_ORDERED_READ(session_cnt, S2C(session)->session_cnt);
	for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		if (s->id != WT_TXN_NONE || s->pinned_id != WT_TXN_NONE)
			WT_RET_MSG(session, EINVAL,
			    "rollback_nondurable_commits not supported with "
			    "active transactions");
	}

	return (0);
}

/*
 * __txn_abort_newer_update --
 *	Review an update chain
 */
static void
__txn_abort_newer_update(WT_SESSION_IMPL *session,
    WT_UPDATE *upd, wt_timestamp_t rollback_timestamp)
{
	bool aborted_one = false;
	WT_UPDATE *next_upd;

	for (next_upd = upd; next_upd != NULL; next_upd = next_upd->next) {
		/* Only updates with timestamps will be aborted */
		if (!__wt_timestamp_iszero(next_upd->timestamp) &&
		    __wt_timestamp_cmp(
		    rollback_timestamp, next_upd->timestamp) < 0) {
			next_upd->txnid = WT_TXN_ABORTED;
			__wt_timestamp_set_zero(next_upd->timestamp);

			/*
			* If any updates are aborted, all newer updates
			* better be aborted as well.
			*/
			if (!aborted_one)
				WT_ASSERT(session,
				    !aborted_one || upd == next_upd);
			aborted_one = true;
		}
	}
}

/*
 * __txn_abort_newer_row_skip --
 *	Apply the update abort check to each entry in an insert skip list
 */
static void
__txn_abort_newer_row_skip(WT_SESSION_IMPL *session,
    WT_INSERT_HEAD *head, wt_timestamp_t rollback_timestamp)
{
	WT_INSERT *ins;

	WT_SKIP_FOREACH(ins, head)
		__txn_abort_newer_update(session, ins->upd, rollback_timestamp);
}

/*
 * __txn_abort_newer_row_leaf --
 *	Abort updates on a row leaf page with timestamps too new.
 */
static void
__txn_abort_newer_row_leaf(
    WT_SESSION_IMPL *session, WT_PAGE *page, wt_timestamp_t rollback_timestamp)
{
	WT_INSERT_HEAD *insert;
	WT_ROW *rip;
	WT_UPDATE *upd;
	uint32_t i;

	/*
	 * Review the insert list for keys before the first entry on the disk
	 * page.
	 */
	if ((insert = WT_ROW_INSERT_SMALLEST(page)) != NULL)
		__txn_abort_newer_row_skip(
		    session, insert, rollback_timestamp);

	/*
	 * Review updates that belong to keys that are on the disk image,
	 * as well as for keys inserted since the page was read from disk.
	 */
	WT_ROW_FOREACH(page, rip, i) {
		if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
			__txn_abort_newer_update(
			    session, upd, rollback_timestamp);

		if ((insert = WT_ROW_INSERT(page, rip)) != NULL)
			__txn_abort_newer_row_skip(
			    session, insert, rollback_timestamp);
	}
}

/*
 * __txn_abort_newer_updates --
 *	Abort updates on this page newer than the timestamp.
 */
static int
__txn_abort_newer_updates(
    WT_SESSION_IMPL *session, WT_REF *ref, wt_timestamp_t rollback_timestamp)
{
	WT_PAGE *page;

	page = ref->page;

	switch (page->type) {
	case WT_PAGE_ROW_INT:
		/*
		 * There is nothing to do for internal pages, since we aren't
		 * rolling back far enough to potentially include reconciled
		 * changes - and thus won't need to roll back structure
		 * changes on internal pages.
		 */
		/* That is a lie: I think we need to check for fast deleted pages */
		break;
	case WT_PAGE_ROW_LEAF:
		__txn_abort_newer_row_leaf(session, page, rollback_timestamp);
		break;
	default:
		WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
		    "is only supported for row store btrees");
	}

	return (0);
}

/*
 * __txn_rollback_nondurable_updates_custom_skip --
 *	Return if custom rollback requires we read this page.
 */
static int
__txn_rollback_nondurable_updates_custom_skip(
    WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool *skipp)
{
	WT_UNUSED(session);
	WT_UNUSED(context);

	/* Review all pages that are in memory. */
	if (ref->state == WT_REF_MEM || ref->state == WT_REF_DELETED)
		*skipp = false;
	else
		*skipp = true;
	return (0);
}

/*
 * __txn_rollback_nondurable_commits_btree_walk --
 *	Called for each open handle - choose to either skip or wipe the commits
 */
static int
__txn_rollback_nondurable_commits_btree_walk(
    WT_SESSION_IMPL *session, wt_timestamp_t rollback_timestamp)
{
	WT_DECL_RET;
	WT_PAGE *page;
	WT_REF *ref;

	/* Walk the tree, marking commits aborted where appropriate. */
	ref = NULL;
	while ((ret = __wt_tree_walk_custom_skip(session, &ref,
	    __txn_rollback_nondurable_updates_custom_skip,
	    NULL, WT_READ_NO_EVICT)) == 0 && ref != NULL) {
		page = ref->page;

		/* Review deleted page saved to the ref */
		if (ref->page_del != NULL && __wt_timestamp_cmp(
		    rollback_timestamp, ref->page_del->timestamp) < 0)
			__wt_delete_page_rollback(session, ref);

		if (!__wt_page_is_modified(page))
			continue;

		WT_RET(__txn_abort_newer_updates(
		    session, ref, rollback_timestamp));
	}
	return (ret);
}

/*
 * __txn_rollback_nondurable_commits_btree --
 *	Called for each open handle - choose to either skip or wipe the commits
 */
static int
__txn_rollback_nondurable_commits_btree(
    WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_DECL_TIMESTAMP(rollback_timestamp)
	WT_BTREE *btree;

	btree = S2BT(session);

	/* Logged files don't get their commits wiped - that wouldn't be safe */
	if (FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_ENABLED) &&
	    !F_ISSET(btree, WT_BTREE_NO_LOGGING))
		return (0);

	/* There is never anything to do for checkpoint handles */
	if (session->dhandle->checkpoint != NULL)
		return (0);

	/* It is possible to have empty trees - there is nothing */
	if (btree->root.page == NULL)
		return (0);

	if (btree->type != BTREE_ROW)
		WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
		    "is only supported for row store btrees");

	ret = __wt_config_gets(session, cfg, "timestamp", &cval);

	/*
	 * This check isn't strictly necessary, since we've already done a
	 * check of this configuration between ourselves and the API, but
	 * it's better safe than sorry, and otherwise difficult to structure
	 * the code in a way that makes static checkers happy.
	 */
	if (ret != 0 || cval.len == 0)
		WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
		    "requires a timestamp in the configuration string");
	WT_RET(__wt_txn_parse_timestamp(session,
	    "rollback_nondurable_commits", rollback_timestamp, &cval));

	WT_RET(__txn_rollback_nondurable_commits_btree_walk(
	    session, rollback_timestamp));
	return (0);
}
#endif

/*
 * __wt_txn_rollback_nondurable_commits --
 *	Rollback all in-memory state related to timestamps more recent than
 *	the passed in timestamp.
 */
int
__wt_txn_rollback_nondurable_commits(
    WT_SESSION_IMPL *session, const char *cfg[])
{
#ifndef HAVE_TIMESTAMPS
	WT_UNUSED(cfg);

	WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
	    "requires a version of WiredTiger built with timestamp "
	    "support");
#else
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_DECL_TIMESTAMP(rollback_timestamp)

	/*
	 * Get the timestamp.
	 */
	ret = __wt_config_gets(session, cfg, "timestamp", &cval);
	if (ret != 0 || cval.len == 0)
		WT_RET_MSG(session, EINVAL, "rollback_nondurable_commits "
		    "requires a timestamp in the configuration string");

	WT_RET(__wt_txn_parse_timestamp(session,
	    "rollback_nondurable_commits", rollback_timestamp, &cval));
	WT_RET(__txn_rollback_nondurable_commits_check(
	    session, rollback_timestamp));

	WT_RET(__wt_conn_btree_apply(session,
	    NULL, __txn_rollback_nondurable_commits_btree, NULL, cfg));
	return (0);
#endif
}
