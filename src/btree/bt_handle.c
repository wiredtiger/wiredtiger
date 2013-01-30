/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __btree_conf(WT_SESSION_IMPL *, WT_CKPT *ckpt, const char *[]);
static int __btree_get_last_recno(WT_SESSION_IMPL *);
static int __btree_page_sizes(WT_SESSION_IMPL *, const char *);
static int __btree_tree_open_empty(WT_SESSION_IMPL *, int);

static int pse1(WT_SESSION_IMPL *, const char *, uint32_t, uint32_t);
static int pse2(WT_SESSION_IMPL *, const char *, uint32_t, uint32_t, uint32_t);

/*
 * __wt_btree_create --
 *	Create a Btree.
 */
int
__wt_btree_create(WT_SESSION_IMPL *session, const char *filename)
{
	return (__wt_block_manager_create(session, filename));
}

/*
 * __wt_btree_truncate --
 *	Truncate a Btree.
 */
int
__wt_btree_truncate(WT_SESSION_IMPL *session, const char *filename)
{
	return (__wt_block_manager_truncate(session, filename));
}

/*
 * __wt_btree_open --
 *	Open a Btree.
 */
int
__wt_btree_open(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CKPT ckpt;
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	uint32_t root_addr_size;
	uint8_t root_addr[WT_BTREE_MAX_ADDR_COOKIE];
	int creation, forced_salvage, readonly;
	const char *filename;

	btree = session->btree;
	bm = NULL;

	/* Checkpoint files and no-cache files are readonly. */
	readonly = btree->checkpoint == NULL ? 0 : 1;
	if (!readonly && cfg != NULL) {
		ret = __wt_config_gets(session, cfg, "no_cache", &cval);
		if (ret != 0 && ret != WT_NOTFOUND)
			WT_RET(ret);
		if (ret == 0 && cval.val != 0)
			readonly = 1;
	}

	/* Get the checkpoint information for this name/checkpoint pair. */
	WT_CLEAR(ckpt);
	WT_RET(__wt_meta_checkpoint(
	    session, btree->name, btree->checkpoint, &ckpt));

	/*
	 * Bulk-load is only permitted on newly created files, not any empty
	 * file -- see the checkpoint code for a discussion.
	 */
	creation = ckpt.raw.size == 0;
	if (!creation && F_ISSET(btree, WT_BTREE_BULK))
		WT_ERR_MSG(session, EINVAL,
		    "bulk-load is only supported on newly created objects");

	/* Handle salvage configuration. */
	forced_salvage = 0;
	if (F_ISSET(btree, WT_BTREE_SALVAGE) && cfg != NULL) {
		ret = __wt_config_gets(session, cfg, "force", &cval);
		if (ret != 0 && ret != WT_NOTFOUND)
			WT_ERR(ret);
		if (ret == 0 && cval.val != 0)
			forced_salvage = 1;
	}

	/* Initialize and configure the WT_BTREE structure. */
	WT_ERR(__btree_conf(session, &ckpt, cfg));

	/* Connect to the underlying block manager. */
	filename = btree->name;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		WT_ERR_MSG(session, EINVAL, "expected a 'file:' URI");
	WT_ERR(__wt_block_manager_open(
	    session, filename, btree->config, cfg, forced_salvage, &btree->bm));
	bm = btree->bm;

	/*
	 * !!!
	 * As part of block-manager configuration, we need to return the maximum
	 * sized address cookie that a block manager will ever return.  There's
	 * a limit of WT_BTREE_MAX_ADDR_COOKIE, but at 255B, it's too large for
	 * a Btree with 512B internal pages.  The default block manager packs
	 * an off_t and 2 uint32_t's into its cookie, so there's no problem now,
	 * but when we create a block manager extension API, we need some way to
	 * consider the block manager's maximum cookie size versus the minimum
	 * Btree internal node size.
	 */
	btree->block_header = bm->block_header(bm);

	/*
	 * Open the specified checkpoint unless it's a special command (special
	 * commands are responsible for loading their own checkpoints, if any).
	 */
	if (!F_ISSET(btree,
	    WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY)) {
		/*
		 * There are two reasons to load an empty tree rather than a
		 * checkpoint: either there is no checkpoint (the file is
		 * being created), or the load call returns no root page (the
		 * checkpoint is for an empty file).
		 */
		WT_ERR(bm->checkpoint_load(bm, session,
		    ckpt.raw.data, ckpt.raw.size,
		    root_addr, &root_addr_size, readonly));
		if (creation || root_addr_size == 0)
			WT_ERR(__btree_tree_open_empty(session, creation));
		else {
			WT_ERR(__wt_btree_tree_open(
			    session, root_addr, root_addr_size));

			/* Get the last record number in a column-store file. */
			if (btree->type != BTREE_ROW)
				WT_ERR(__btree_get_last_recno(session));
		}
	}

	if (0) {
err:		WT_TRET(__wt_btree_close(session));
	}
	__wt_meta_checkpoint_free(session, &ckpt);

	return (ret);
}

/*
 * __wt_btree_close --
 *	Close a Btree.
 */
int
__wt_btree_close(WT_SESSION_IMPL *session)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_RET;

	btree = session->btree;

	if ((bm = btree->bm) != NULL) {
		/* Unload the checkpoint, unless it's a special command. */
		if (F_ISSET(btree, WT_BTREE_OPEN) &&
		    !F_ISSET(btree,
		    WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY))
			WT_TRET(bm->checkpoint_unload(bm, session));

		/* Close the underlying block manager reference. */
		WT_TRET(bm->close(bm, session));

		btree->bm = NULL;
	}

	/* Close the Huffman tree. */
	__wt_btree_huffman_close(session);

	/* Free allocated memory. */
	__wt_free(session, btree->key_format);
	__wt_free(session, btree->value_format);
	if (btree->val_ovfl_lock != NULL)
		WT_TRET(__wt_rwlock_destroy(session, &btree->val_ovfl_lock));
	__wt_free(session, btree->stats);

	btree->bulk_load_ok = 0;

	return (ret);
}

/*
 * __btree_conf --
 *	Configure a WT_BTREE structure.
 */
static int
__btree_conf(WT_SESSION_IMPL *session, WT_CKPT *ckpt, const char *cfg[])
{
	WT_BTREE *btree;
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_COLLATOR *ncoll;
	WT_NAMED_COMPRESSOR *ncomp;
	uint32_t bitcnt;
	int fixed;
	const char *config;

	btree = session->btree;
	conn = S2C(session);
	config = btree->config;

	/* Validate file types and check the data format plan. */
	WT_RET(__wt_config_getones(session, config, "key_format", &cval));
	WT_RET(__wt_struct_check(session, cval.str, cval.len, NULL, NULL));
	if (WT_STRING_MATCH("r", cval.str, cval.len))
		btree->type = BTREE_COL_VAR;
	else
		btree->type = BTREE_ROW;
	WT_RET(__wt_strndup(session, cval.str, cval.len, &btree->key_format));

	WT_RET(__wt_config_getones(session, config, "value_format", &cval));
	WT_RET(__wt_strndup(session, cval.str, cval.len, &btree->value_format));

	/* Row-store key comparison and key gap for prefix compression. */
	if (btree->type == BTREE_ROW) {
		WT_RET(__wt_config_getones(session, config, "collator", &cval));
		if (cval.len > 0) {
			TAILQ_FOREACH(ncoll, &conn->collqh, q) {
				if (WT_STRING_MATCH(
				    ncoll->name, cval.str, cval.len)) {
					btree->collator = ncoll->collator;
					break;
				}
			}
			if (btree->collator == NULL)
				WT_RET_MSG(session, EINVAL,
				    "unknown collator '%.*s'",
				    (int)cval.len, cval.str);
		}
		WT_RET(__wt_config_getones(session, config, "key_gap", &cval));
		btree->key_gap = (uint32_t)cval.val;
	}
	/* Check for fixed-size data. */
	if (btree->type == BTREE_COL_VAR) {
		WT_RET(__wt_struct_check(
		    session, cval.str, cval.len, &fixed, &bitcnt));
		if (fixed) {
			if (bitcnt == 0 || bitcnt > 8)
				WT_RET_MSG(session, EINVAL,
				    "fixed-width field sizes must be greater "
				    "than 0 and less than or equal to 8");
			btree->bitcnt = (uint8_t)bitcnt;
			btree->type = BTREE_COL_FIX;
		}
	}

	/* Page sizes */
	WT_RET(__btree_page_sizes(session, config));

	/* Eviction; the metadata file is never evicted. */
	if (strcmp(btree->name, WT_METADATA_URI) == 0)
		F_SET(btree, WT_BTREE_NO_EVICTION | WT_BTREE_NO_HAZARD);
	else {
		WT_RET(__wt_config_getones(
		    session, config, "cache_resident", &cval));
		if (cval.val)
			F_SET(btree, WT_BTREE_NO_EVICTION | WT_BTREE_NO_HAZARD);
		else
			F_CLR(btree, WT_BTREE_NO_EVICTION);
	}

	/* No-cache files are never evicted or cached. */
	if (cfg != NULL) {
		ret = __wt_config_gets(session, cfg, "no_cache", &cval);
		if (ret != 0 && ret != WT_NOTFOUND)
			WT_RET(ret);
		if (ret == 0 && cval.val != 0)
			F_SET(session->btree, WT_BTREE_NO_CACHE |
			    WT_BTREE_NO_EVICTION | WT_BTREE_NO_HAZARD);
	}

	/* Checksums */
	WT_RET(__wt_config_getones(session, config, "checksum", &cval));
	if (WT_STRING_MATCH("on", cval.str, cval.len))
		btree->checksum = CKSUM_ON;
	else if (WT_STRING_MATCH("off", cval.str, cval.len))
		btree->checksum = CKSUM_OFF;
	else
		btree->checksum = CKSUM_UNCOMPRESSED;

	/* Huffman encoding */
	WT_RET(__wt_btree_huffman_open(session, config));

	/*
	 * Reconciliation configuration:
	 *	Block compression (all)
	 *	Dictionary compression (variable-length column-store, row-store)
	 *	Page-split percentage
	 *	Prefix compression (row-store)
	 *	Suffix compression (row-store)
	 */
	switch (btree->type) {
	case BTREE_COL_FIX:
		break;
	case BTREE_ROW:
		WT_RET(__wt_config_getones(
		    session, config, "internal_key_truncate", &cval));
		btree->internal_key_truncate = cval.val == 0 ? 0 : 1;

		WT_RET(__wt_config_getones(
		    session, config, "prefix_compression", &cval));
		btree->prefix_compression = cval.val == 0 ? 0 : 1;
		/* FALLTHROUGH */
	case BTREE_COL_VAR:
		WT_RET(
		    __wt_config_getones(session, config, "dictionary", &cval));
		btree->dictionary = (u_int)cval.val;
		break;
	}

	WT_RET(__wt_config_getones(session, config, "split_pct", &cval));
	btree->split_pct = (u_int)cval.val;

	WT_RET(__wt_config_getones(session, config, "block_compressor", &cval));
	if (cval.len > 0) {
		TAILQ_FOREACH(ncomp, &conn->compqh, q)
			if (WT_STRING_MATCH(ncomp->name, cval.str, cval.len)) {
				btree->compressor = ncomp->compressor;
				break;
			}
		if (btree->compressor == NULL)
			WT_RET_MSG(session, EINVAL,
			    "unknown block compressor '%.*s'",
			    (int)cval.len, cval.str);
	}

	/* Overflow lock. */
	WT_RET(__wt_rwlock_alloc(
	    session, "btree overflow lock", &btree->val_ovfl_lock));

	WT_RET(__wt_stat_alloc_dsrc_stats(session, &btree->stats));

	btree->write_gen = ckpt->write_gen;		/* Write generation */
	btree->modified = 0;				/* Clean */

	return (0);
}

/*
 * __wt_btree_tree_open --
 *	Read in a tree from disk.
 */
int
__wt_btree_tree_open(
    WT_SESSION_IMPL *session, const uint8_t *addr, uint32_t addr_size)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_ITEM dsk;
	WT_PAGE *page;

	btree = session->btree;

	/*
	 * A buffer into which we read a root page; don't use a scratch buffer,
	 * the buffer's allocated memory becomes the persistent in-memory page.
	 */
	WT_CLEAR(dsk);

	/* Read the page, then build the in-memory version of the page. */
	WT_ERR(__wt_bt_read(session, &dsk, addr, addr_size));
	WT_ERR(__wt_page_inmem(session,
	    NULL, NULL, dsk.mem, F_ISSET(&dsk, WT_ITEM_MAPPED) ? 1 : 0, &page));
	btree->root_page = page;

	if (0) {
err:		__wt_buf_free(session, &dsk);
	}
	return (ret);
}

/*
 * __btree_tree_open_empty --
 *	Create an empty in-memory tree.
 */
static int
__btree_tree_open_empty(WT_SESSION_IMPL *session, int creation)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *root, *leaf;
	WT_REF *ref;

	btree = session->btree;
	root = leaf = NULL;

	/*
	 * Newly created objects can be used for cursor inserts or for bulk
	 * loads; set a flag that's cleared when a row is inserted into the
	 * tree.
	 */
	if (creation)
		btree->bulk_load_ok = 1;

	/*
	 * A note about empty trees: the initial tree is a root page and a leaf
	 * page.  We need a pair of pages instead of just a single page because
	 * we can reconcile the leaf page while the root stays pinned in memory.
	 * If the pair is evicted without being modified, that's OK, nothing is
	 * ever written.
	 *
	 * Create the root and leaf pages.
	 *
	 * !!!
	 * Be cautious about changing the order of updates in this code: to call
	 * __wt_page_out on error, we require a correct page setup at each point
	 * where we might fail.
	 */
	WT_ERR(__wt_calloc_def(session, 1, &root));
	switch (btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		root->type = WT_PAGE_COL_INT;
		root->u.intl.recno = 1;
		WT_ERR(__wt_calloc_def(session, 1, &root->u.intl.t));
		ref = root->u.intl.t;
		WT_ERR(__wt_btree_leaf_create(session, root, ref, &leaf));
		ref->page = leaf;
		ref->addr = NULL;
		ref->state = WT_REF_MEM;
		ref->u.recno = 1;
		break;
	case BTREE_ROW:
		root->type = WT_PAGE_ROW_INT;
		WT_ERR(__wt_calloc_def(session, 1, &root->u.intl.t));
		ref = root->u.intl.t;
		WT_ERR(__wt_btree_leaf_create(session, root, ref, &leaf));
		ref->page = leaf;
		ref->addr = NULL;
		ref->state = WT_REF_MEM;
		WT_ERR(__wt_row_ikey_alloc(session, 0, "", 1, &ref->u.key));
		break;
	WT_ILLEGAL_VALUE_ERR(session);
	}
	root->entries = 1;
	root->parent = NULL;
	root->ref = NULL;

	/*
	 * Mark the leaf page dirty: we didn't create an entirely valid root
	 * page (specifically, the root page's disk address isn't set, and it's
	 * the act of reconciling the leaf page that makes it work, we don't
	 * try and use the original disk address of modified pages).  We could
	 * get around that by leaving the leaf page clean and building a better
	 * root page, but then we get into trouble because a checkpoint marks
	 * the root page dirty to force a write, and without reconciling the
	 * leaf page we won't realize there's no records to write, we'll write
	 * a root page, which isn't correct for an empty tree.
	 *    Earlier versions of this code kept the leaf page clean, but with
	 * the "empty" flag set in the leaf page's modification structure; in
	 * that case, checkpoints works (forced reconciliation of a root with
	 * a single "empty" page wouldn't write any blocks). That version had
	 * memory leaks because the eviction code didn't correctly handle pages
	 * that were "clean" (and so never reconciled), yet "modified" with an
	 * "empty" flag.  The goal of this code is to mimic a real tree that
	 * simply has no records, for whatever reason, and trust reconciliation
	 * to figure out it's empty and not write any blocks.
	 *    We do not set the tree's modified flag because the checkpoint code
	 * skips unmodified files in closing checkpoints (checkpoints that don't
	 * require a write unless the file is actually dirty).  There's no need
	 * to reconcile this file unless the application does a real checkpoint
	 * or it's actually modified.
	 */
	WT_ERR(__wt_page_modify_init(session, leaf));
	__wt_page_modify_set(session, leaf);

	btree->root_page = root;

	return (0);

err:	if (leaf != NULL)
		__wt_page_out(session, &leaf);
	if (root != NULL)
		__wt_page_out(session, &root);
	return (ret);
}

/*
 * __wt_btree_leaf_create --
 *	Create an empty leaf page.
 */
int
__wt_btree_leaf_create(
    WT_SESSION_IMPL *session, WT_PAGE *parent, WT_REF *ref, WT_PAGE **pagep)
{
	WT_BTREE *btree;
	WT_PAGE *leaf;

	btree = session->btree;

	WT_RET(__wt_calloc_def(session, 1, &leaf));
	switch (btree->type) {
	case BTREE_COL_FIX:
		leaf->u.col_fix.recno = 1;
		leaf->type = WT_PAGE_COL_FIX;
		break;
	case BTREE_COL_VAR:
		leaf->u.col_var.recno = 1;
		leaf->type = WT_PAGE_COL_VAR;
		break;
	case BTREE_ROW:
		leaf->type = WT_PAGE_ROW_LEAF;
		break;
	}
	leaf->entries = 0;
	leaf->ref = ref;
	leaf->parent = parent;

	*pagep = leaf;
	return (0);
}

/*
 * __wt_btree_get_memsize --
 *      Access the size of an in-memory tree with a single leaf page.
 */
int
__wt_btree_get_memsize(
    WT_SESSION_IMPL *session, WT_BTREE *btree, uint32_t **memsizep)
{
	WT_PAGE *root, *child;

	WT_UNUSED(session);
	root = btree->root_page;
	child = root->u.intl.t->page;

	if (root->entries != 1 || child == NULL) {
		*memsizep = NULL;
		return (WT_ERROR);
	}

	*memsizep = &child->memory_footprint;
	F_SET(btree, WT_BTREE_NO_EVICTION);
	return (0);
}

/*
 * __wt_btree_release_memsize --
 *      Release a cache-resident tree.
 */
int
__wt_btree_release_memsize(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
	WT_UNUSED(session);
	F_CLR(btree, WT_BTREE_NO_EVICTION);
	return (0);
}

/*
 * __btree_get_last_recno --
 *	Set the last record number for a column-store.
 */
static int
__btree_get_last_recno(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;
	WT_PAGE *page;

	btree = session->btree;

	page = NULL;
	WT_RET(__wt_tree_walk(session, &page, WT_TREE_PREV));
	if (page == NULL)
		return (WT_NOTFOUND);

	btree->last_recno = __col_last_recno(page);
	return (__wt_stack_release(session, page));
}

/*
 * __btree_page_sizes --
 *	Verify the page sizes.
 */
static int
__btree_page_sizes(WT_SESSION_IMPL *session, const char *config)
{
	WT_BTREE *btree;
	WT_CONFIG_ITEM cval;
	uint32_t intl_split_size, leaf_split_size, split_pct;

	btree = session->btree;

	WT_RET(__wt_config_getones(session, config, "allocation_size", &cval));
	btree->allocsize = (uint32_t)cval.val;
	WT_RET(
	    __wt_config_getones(session, config, "internal_page_max", &cval));
	btree->maxintlpage = (uint32_t)cval.val;
	WT_RET(__wt_config_getones(
	    session, config, "internal_item_max", &cval));
	btree->maxintlitem = (uint32_t)cval.val;
	WT_RET(__wt_config_getones(session, config, "leaf_page_max", &cval));
	btree->maxleafpage = (uint32_t)cval.val;
	WT_RET(__wt_config_getones(
	    session, config, "leaf_item_max", &cval));
	btree->maxleafitem = (uint32_t)cval.val;

	/*
	 * When a page is forced to split, we want at least 50 entries on its
	 * parent.
	 */
	WT_RET(__wt_config_getones(session, config, "memory_page_max", &cval));
	btree->maxmempage = WT_MAX((uint64_t)cval.val, 50 * btree->maxleafpage);

	/* Allocation sizes must be a power-of-two, nothing else makes sense. */
	if (!__wt_ispo2(btree->allocsize))
		WT_RET_MSG(session,
		    EINVAL, "the allocation size must be a power of two");

	/* All page sizes must be in units of the allocation size. */
	if (btree->maxintlpage < btree->allocsize ||
	    btree->maxintlpage % btree->allocsize != 0 ||
	    btree->maxleafpage < btree->allocsize ||
	    btree->maxleafpage % btree->allocsize != 0)
		WT_RET_MSG(session, EINVAL,
		    "page sizes must be a multiple of the page allocation "
		    "size (%" PRIu32 "B)", btree->allocsize);

	/*
	 * Set the split percentage: reconciliation splits to a
	 * smaller-than-maximum page size so we don't split every time a new
	 * entry is added.
	 */
	WT_RET(__wt_config_getones(session, config, "split_pct", &cval));
	split_pct = (uint32_t)cval.val;
	intl_split_size = WT_SPLIT_PAGE_SIZE(
	    btree->maxintlpage, btree->allocsize, split_pct);
	leaf_split_size = WT_SPLIT_PAGE_SIZE(
	    btree->maxleafpage, btree->allocsize, split_pct);

	/*
	 * Default values for internal and leaf page items: make sure at least
	 * 8 items fit on split pages.
	 */
	if (btree->maxintlitem == 0)
		    btree->maxintlitem = intl_split_size / 8;
	if (btree->maxleafitem == 0)
		    btree->maxleafitem = leaf_split_size / 8;
	/* Check we can fit at least 2 items on a page. */
	if (btree->maxintlitem > btree->maxintlpage / 2)
		return (pse1(session, "internal",
		    btree->maxintlpage, btree->maxintlitem));
	if (btree->maxleafitem > btree->maxleafpage / 2)
		return (pse1(session, "leaf",
		    btree->maxleafpage, btree->maxleafitem));

	/*
	 * Take into account the size of a split page:
	 *
	 * Make it a separate error message so it's clear what went wrong.
	 */
	if (btree->maxintlitem > intl_split_size / 2)
		return (pse2(session, "internal",
		    btree->maxintlpage, btree->maxintlitem, split_pct));
	if (btree->maxleafitem > leaf_split_size / 2)
		return (pse2(session, "leaf",
		    btree->maxleafpage, btree->maxleafitem, split_pct));

	/*
	 * Limit allocation units to 128MB, and page sizes to 512MB.  There's
	 * no reason we couldn't support larger sizes (any sizes up to the
	 * smaller of an off_t and a size_t should work), but an application
	 * specifying larger allocation or page sizes would likely be making
	 * as mistake.  The API checked this, but we assert it anyway.
	 */
	WT_ASSERT(session, btree->allocsize >= WT_BTREE_ALLOCATION_SIZE_MIN);
	WT_ASSERT(session, btree->allocsize <= WT_BTREE_ALLOCATION_SIZE_MAX);
	WT_ASSERT(session, btree->maxintlpage <= WT_BTREE_PAGE_SIZE_MAX);
	WT_ASSERT(session, btree->maxleafpage <= WT_BTREE_PAGE_SIZE_MAX);

	return (0);
}

static int
pse1(WT_SESSION_IMPL *session, const char *type, uint32_t max, uint32_t ovfl)
{
	WT_RET_MSG(session, EINVAL,
	    "%s page size (%" PRIu32 "B) too small for the maximum item size "
	    "(%" PRIu32 "B); the page must be able to hold at least 2 items",
	    type, max, ovfl);
}

static int
pse2(WT_SESSION_IMPL *session,
    const char *type, uint32_t max, uint32_t ovfl, uint32_t pct)
{
	WT_RET_MSG(session, EINVAL,
	    "%s page size (%" PRIu32 "B) too small for the maximum item size "
	    "(%" PRIu32 "B), because of the split percentage (%" PRIu32
	    "%%); a split page must be able to hold at least 2 items",
	    type, max, ovfl, pct);
}
