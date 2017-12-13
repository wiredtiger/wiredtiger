/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

struct __rec_chunk;		typedef struct __rec_chunk WT_CHUNK;
struct __rec_dictionary;	typedef struct __rec_dictionary WT_DICTIONARY;
struct __rec_kv;		typedef struct __rec_kv WT_KV;

/*
 * Reconciliation is the process of taking an in-memory page, walking each entry
 * in the page, building a backing disk image in a temporary buffer representing
 * that information, and writing that buffer to disk.  What could be simpler?
 *
 * WT_RECONCILE --
 *	Information tracking a single page reconciliation.
 */
typedef struct {
	WT_REF  *ref;			/* Page being reconciled */
	WT_PAGE *page;
	uint32_t flags;			/* Caller's configuration */

	/*
	 * Track start/stop write generation to decide if all changes to the
	 * page are written.
	 */
	uint32_t orig_write_gen;

	/*
	 * Track start/stop checkpoint generations to decide if lookaside table
	 * records are correct.
	 */
	uint64_t orig_btree_checkpoint_gen;
	uint64_t orig_txn_checkpoint_gen;

	/*
	 * Track the oldest running transaction and whether to skew lookaside
	 * to the newest update.
	 */
	bool las_skew_newest;
	uint64_t last_running;

	/* Track the page's min/maximum transactions. */
	uint64_t max_txn;
	WT_DECL_TIMESTAMP(max_timestamp)
	WT_DECL_TIMESTAMP(max_onpage_timestamp)
	WT_DECL_TIMESTAMP(min_saved_timestamp)

	u_int updates_seen;		/* Count of updates seen. */
	u_int updates_unstable;		/* Count of updates not visible_all. */

	bool update_uncommitted;	/* An update was uncommitted */
	bool update_used;		/* An update could be used */

	/*
	 * When we can't mark the page clean (for example, checkpoint found some
	 * uncommitted updates), there's a leave-dirty flag.
	 */
	bool leave_dirty;

	/*
	 * Raw compression (don't get me started, as if normal reconciliation
	 * wasn't bad enough).  If an application wants absolute control over
	 * what gets written to disk, we give it a list of byte strings and it
	 * gives us back an image that becomes a file block.  Because we don't
	 * know the number of items we're storing in a block until we've done
	 * a lot of work, we turn off most compression: dictionary, copy-cell,
	 * prefix and row-store internal page suffix compression are all off.
	 */
	bool	  raw_compression;
	uint32_t  raw_max_slots;	/* Raw compression array sizes */
	uint32_t *raw_entries;		/* Raw compression slot entries */
	uint32_t *raw_offsets;		/* Raw compression slot offsets */
	uint64_t *raw_recnos;		/* Raw compression recno count */
	WT_ITEM	  raw_destination;	/* Raw compression destination buffer */

	/*
	 * Track if reconciliation has seen any overflow items.  If a leaf page
	 * with no overflow items is written, the parent page's address cell is
	 * set to the leaf-no-overflow type.  This means we can delete the leaf
	 * page without reading it because we don't have to discard any overflow
	 * items it might reference.
	 *
	 * The test test is per-page reconciliation, that is, once we see an
	 * overflow item on the page, all subsequent leaf pages written for the
	 * page will not be leaf-no-overflow type, regardless of whether or not
	 * they contain overflow items.  In other words, leaf-no-overflow is not
	 * guaranteed to be set on every page that doesn't contain an overflow
	 * item, only that if it is set, the page contains no overflow items.
	 *
	 * The reason is because of raw compression: there's no easy/fast way to
	 * figure out if the rows selected by raw compression included overflow
	 * items, and the optimization isn't worth another pass over the data.
	 */
	bool	ovfl_items;

	/*
	 * Track if reconciliation of a row-store leaf page has seen empty (zero
	 * length) values.  We don't write out anything for empty values, so if
	 * there are empty values on a page, we have to make two passes over the
	 * page when it's read to figure out how many keys it has, expensive in
	 * the common case of no empty values and (entries / 2) keys.  Likewise,
	 * a page with only empty values is another common data set, and keys on
	 * that page will be equal to the number of entries.  In both cases, set
	 * a flag in the page's on-disk header.
	 *
	 * The test is per-page reconciliation as described above for the
	 * overflow-item test.
	 */
	bool	all_empty_value, any_empty_value;

	/*
	 * Reconciliation gets tricky if we have to split a page, which happens
	 * when the disk image we create exceeds the page type's maximum disk
	 * image size.
	 *
	 * First, the sizes of the page we're building.  If WiredTiger is doing
	 * page layout, page_size is the same as page_size_orig. We accumulate
	 * a "page size" of raw data and when we reach that size, we split the
	 * page into multiple chunks, eventually compressing those chunks.  When
	 * the application is doing page layout (raw compression is configured),
	 * page_size can continue to grow past page_size_orig, and we keep
	 * accumulating raw data until the raw compression callback accepts it.
	 */
	uint32_t page_size;		/* Set page size */
	uint32_t page_size_orig;	/* Saved set page size */
	uint32_t max_raw_page_size;	/* Max page size with raw compression */

	/*
	 * Second, the split size: if we're doing the page layout, split to a
	 * smaller-than-maximum page size when a split is required so we don't
	 * repeatedly split a packed page.
	 */
	uint32_t split_size;		/* Split page size */
	uint32_t min_split_size;	/* Minimum split page size */

	/*
	 * We maintain two split chunks in the memory during reconciliation to
	 * be written out as pages. As we get to the end of the data, if the
	 * last one turns out to be smaller than the minimum split size, we go
	 * back into the penultimate chunk and split at this minimum split size
	 * boundary. This moves some data from the penultimate chunk to the last
	 * chunk, hence increasing the size of the last page written without
	 * decreasing the penultimate page size beyond the minimum split size.
	 * For this reason, we maintain an expected split percentage boundary
	 * and a minimum split percentage boundary.
	 *
	 * Chunks are referenced by current and previous pointers. In case of a
	 * split, previous references the first chunk and current switches to
	 * the second chunk. If reconciliation generates more split chunks, the
	 * the previous chunk is written to the disk and current and previous
	 * swap.
	 */
	struct __rec_chunk {
		/*
		 * The recno and entries fields are the starting record number
		 * of the split chunk (for column-store splits), and the number
		 * of entries in the split chunk.
		 *
		 * The key for a row-store page; no column-store key is needed
		 * because the page's recno, stored in the recno field, is the
		 * column-store key.
		 */
		uint32_t entries;
		uint64_t recno;
		WT_ITEM  key;

		uint32_t min_entries;
		uint64_t min_recno;
		WT_ITEM  min_key;

		/* Minimum split-size boundary buffer offset. */
		size_t   min_offset;

		WT_ITEM image;				/* disk-image */
	} chunkA, chunkB, *cur_ptr, *prev_ptr;

	/*
	 * We track current information about the current record number, the
	 * number of entries copied into the disk image buffer, where we are
	 * in the buffer, and how much memory remains. Those values are
	 * packaged here rather than passing pointers to stack locations
	 * around the code.
	 */
	uint64_t recno;			/* Current record number */
	uint32_t entries;		/* Current number of entries */
	uint8_t *first_free;		/* Current first free byte */
	size_t	 space_avail;		/* Remaining space in this chunk */
	/* Remaining space in this chunk to put a minimum size boundary */
	size_t	 min_space_avail;

	/*
	 * Saved update list, supporting the WT_REC_UPDATE_RESTORE and
	 * WT_REC_LOOKASIDE configurations. While reviewing updates for each
	 * page, we save WT_UPDATE lists here, and then move them to per-block
	 * areas as the blocks are defined.
	 */
	WT_SAVE_UPD *supd;		/* Saved updates */
	uint32_t     supd_next;
	size_t	     supd_allocated;
	size_t       supd_memsize;	/* Size of saved update structures */

	/* List of pages we've written so far. */
	WT_MULTI *multi;
	uint32_t  multi_next;
	size_t	  multi_allocated;

	/*
	 * Root pages are written when wrapping up the reconciliation, remember
	 * the image we're going to write.
	 */
	WT_ITEM *wrapup_checkpoint;
	bool	 wrapup_checkpoint_compressed;

	/*
	 * We don't need to keep the 0th key around on internal pages, the
	 * search code ignores them as nothing can sort less by definition.
	 * There's some trickiness here, see the code for comments on how
	 * these fields work.
	 */
	bool	cell_zero;		/* Row-store internal page 0th key */

	/*
	 * We calculate checksums to find previously written identical blocks,
	 * but once a match fails during an eviction, there's no point trying
	 * again.
	 */
	bool	evict_matching_checksum_failed;

	/*
	 * WT_DICTIONARY --
	 *	We optionally build a dictionary of values for leaf pages. Where
	 * two value cells are identical, only write the value once, the second
	 * and subsequent copies point to the original cell. The dictionary is
	 * fixed size, but organized in a skip-list to make searches faster.
	 */
	struct __rec_dictionary {
		uint64_t hash;				/* Hash value */
		uint32_t offset;			/* Matching cell */

		u_int depth;				/* Skiplist */
		WT_DICTIONARY *next[0];
	} **dictionary;					/* Dictionary */
	u_int dictionary_next, dictionary_slots;	/* Next, max entries */
							/* Skiplist head. */
	WT_DICTIONARY *dictionary_head[WT_SKIP_MAXDEPTH];

	/*
	 * WT_KV--
	 *	An on-page key/value item we're building.
	 */
	struct __rec_kv {
		WT_ITEM	 buf;		/* Data */
		WT_CELL	 cell;		/* Cell and cell's length */
		size_t cell_len;
		size_t len;		/* Total length of cell + data */
	} k, v;				/* Key/Value being built */

	WT_ITEM *cur, _cur;		/* Key/Value being built */
	WT_ITEM *last, _last;		/* Last key/value built */

	bool key_pfx_compress;		/* If can prefix-compress next key */
	bool key_pfx_compress_conf;	/* If prefix compression configured */
	bool key_sfx_compress;		/* If can suffix-compress next key */
	bool key_sfx_compress_conf;	/* If suffix compression configured */

	bool is_bulk_load;		/* If it's a bulk load */

	WT_SALVAGE_COOKIE *salvage;	/* If it's a salvage operation */

	bool cache_write_lookaside;	/* Used the lookaside table */
	bool cache_write_restore;	/* Used update/restoration */

	uint32_t tested_ref_state;	/* Debugging information */

	/*
	 * XXX
	 * In the case of a modified update, we may need a copy of the current
	 * value as a set of bytes. We call back into the btree code using a
	 * fake cursor to do that work. This a layering violation and fragile,
	 * we need a better solution.
	 */
	WT_CURSOR_BTREE update_modify_cbt;
} WT_RECONCILE;

#define	WT_CROSSING_MIN_BND(r, next_len)				\
	((r)->cur_ptr->min_offset == 0 &&				\
	    (next_len) > (r)->min_space_avail)
#define	WT_CROSSING_SPLIT_BND(r, next_len) ((next_len) > (r)->space_avail)
#define	WT_CHECK_CROSSING_BND(r, next_len)				\
	(WT_CROSSING_MIN_BND(r, next_len) || WT_CROSSING_SPLIT_BND(r, next_len))

static void __rec_cell_build_addr(WT_SESSION_IMPL *,
		WT_RECONCILE *, const void *, size_t, u_int, uint64_t);
static int  __rec_cell_build_int_key(WT_SESSION_IMPL *,
		WT_RECONCILE *, const void *, size_t, bool *);
static int  __rec_cell_build_leaf_key(WT_SESSION_IMPL *,
		WT_RECONCILE *, const void *, size_t, bool *);
static int  __rec_cell_build_ovfl(WT_SESSION_IMPL *,
		WT_RECONCILE *, WT_KV *, uint8_t, uint64_t);
static int  __rec_cell_build_val(WT_SESSION_IMPL *,
		WT_RECONCILE *, const void *, size_t, uint64_t);
static void __rec_cleanup(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_col_fix(WT_SESSION_IMPL *, WT_RECONCILE *, WT_REF *);
static int  __rec_col_fix_slvg(WT_SESSION_IMPL *,
		WT_RECONCILE *, WT_REF *, WT_SALVAGE_COOKIE *);
static int  __rec_col_int(WT_SESSION_IMPL *, WT_RECONCILE *, WT_REF *);
static int  __rec_col_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_col_var(WT_SESSION_IMPL *,
		WT_RECONCILE *, WT_REF *, WT_SALVAGE_COOKIE *);
static int  __rec_col_var_helper(WT_SESSION_IMPL *, WT_RECONCILE *,
		WT_SALVAGE_COOKIE *, WT_ITEM *, bool, uint8_t, uint64_t);
static int  __rec_destroy_session(WT_SESSION_IMPL *);
static int  __rec_init(WT_SESSION_IMPL *,
		WT_REF *, uint32_t, WT_SALVAGE_COOKIE *, void *);
static int  __rec_las_wrapup(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_las_wrapup_err(WT_SESSION_IMPL *, WT_RECONCILE *);
static uint32_t __rec_min_split_page_size(WT_BTREE *, uint32_t);
static int  __rec_root_write(WT_SESSION_IMPL *, WT_PAGE *, uint32_t);
static int  __rec_row_int(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_row_leaf(WT_SESSION_IMPL *,
		WT_RECONCILE *, WT_PAGE *, WT_SALVAGE_COOKIE *);
static int  __rec_row_leaf_insert(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_INSERT *);
static int  __rec_row_merge(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_split_discard(WT_SESSION_IMPL *, WT_PAGE *);
static int  __rec_split_row_promote(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_ITEM *, uint8_t);
static int  __rec_split_write(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_CHUNK *, WT_ITEM *, bool);
static int  __rec_write_check_complete(
		WT_SESSION_IMPL *, WT_RECONCILE *, int, bool *);
static void __rec_write_page_status(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_write_wrapup(WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);
static int  __rec_write_wrapup_err(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_PAGE *);

static void __rec_dictionary_free(WT_SESSION_IMPL *, WT_RECONCILE *);
static int  __rec_dictionary_init(WT_SESSION_IMPL *, WT_RECONCILE *, u_int);
static int  __rec_dictionary_lookup(
		WT_SESSION_IMPL *, WT_RECONCILE *, WT_KV *, WT_DICTIONARY **);
static void __rec_dictionary_reset(WT_RECONCILE *);

/*
 * __wt_reconcile --
 *	Reconcile an in-memory page into its on-disk format, and write it.
 */
int
__wt_reconcile(WT_SESSION_IMPL *session, WT_REF *ref,
    WT_SALVAGE_COOKIE *salvage, uint32_t flags, bool *lookaside_retryp)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	WT_RECONCILE *r;
	uint64_t oldest_id;

	btree = S2BT(session);
	page = ref->page;
	mod = page->modify;
	if (lookaside_retryp != NULL)
		*lookaside_retryp = false;

	__wt_verbose(session, WT_VERB_RECONCILE,
	    "%p reconcile %s (%s%s%s)",
	    (void *)ref, __wt_page_type_string(page->type),
	    LF_ISSET(WT_REC_EVICT) ? "evict" : "checkpoint",
	    LF_ISSET(WT_REC_LOOKASIDE) ? ", lookaside" : "",
	    LF_ISSET(WT_REC_UPDATE_RESTORE) ? ", update/restore" : "");

	/*
	 * Sanity check flags.
	 *
	 * We can only do update/restore eviction when the version that ends up
	 * in the page image is the oldest one any reader could need.
	 * Otherwise we would need to keep updates in memory that go back older
	 * than the version in the disk image, and since modify operations
	 * aren't idempotent, that is problematic.
	 *
	 * If we try to do eviction using transaction visibility, we had better
	 * have a snapshot.  This doesn't apply to checkpoints: there are
	 * (rare) cases where we write data at read-uncommitted isolation.
	 */
	WT_ASSERT(session, !LF_ISSET(WT_REC_UPDATE_RESTORE) ||
	    LF_ISSET(WT_REC_VISIBLE_ALL));
	WT_ASSERT(session, !LF_ISSET(WT_REC_EVICT) ||
	    LF_ISSET(WT_REC_VISIBLE_ALL) ||
	    F_ISSET(&session->txn, WT_TXN_HAS_SNAPSHOT));

	/* We shouldn't get called with a clean page, that's an error. */
	WT_ASSERT(session, __wt_page_is_modified(page));

	/*
	 * Reconciliation locks the page for three reasons:
	 *    Reconciliation reads the lists of page updates, obsolete updates
	 * cannot be discarded while reconciliation is in progress;
	 *    The compaction process reads page modification information, which
	 * reconciliation modifies;
	 *    In-memory splits: reconciliation of an internal page cannot handle
	 * a child page splitting during the reconciliation.
	 */
	WT_PAGE_LOCK(session, page);

	/*
	 * Now that the page is locked, if attempting to evict it, check again
	 * whether eviction is permitted. The page's state could have changed
	 * while we were waiting to acquire the lock (e.g., the page could have
	 * split).
	 */
	if (LF_ISSET(WT_REC_EVICT) &&
	    !__wt_page_can_evict(session, ref, NULL)) {
		WT_PAGE_UNLOCK(session, page);
		return (EBUSY);
	}

	oldest_id = __wt_txn_oldest_id(session);
	if (LF_ISSET(WT_REC_EVICT)) {
		mod->last_eviction_id = oldest_id;
#ifdef HAVE_TIMESTAMPS
		WT_WITH_TIMESTAMP_READLOCK(session,
		    &S2C(session)->txn_global.rwlock,
		    __wt_timestamp_set(&mod->last_eviction_timestamp,
		    &S2C(session)->txn_global.pinned_timestamp));
#endif
		mod->last_evict_pass_gen = S2C(session)->cache->evict_pass_gen;
	}

#ifdef HAVE_DIAGNOSTIC
	/*
	 * Check that transaction time always moves forward for a given page.
	 * If this check fails, reconciliation can free something that a future
	 * reconciliation will need.
	 */
	WT_ASSERT(session, WT_TXNID_LE(mod->last_oldest_id, oldest_id));
	mod->last_oldest_id = oldest_id;
#endif

	/* Initialize the reconciliation structure for each new run. */
	if ((ret = __rec_init(
	    session, ref, flags, salvage, &session->reconcile)) != 0) {
		WT_PAGE_UNLOCK(session, page);
		return (ret);
	}
	r = session->reconcile;

	/* Reconcile the page. */
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		if (salvage != NULL)
			ret = __rec_col_fix_slvg(session, r, ref, salvage);
		else
			ret = __rec_col_fix(session, r, ref);
		break;
	case WT_PAGE_COL_INT:
		WT_WITH_PAGE_INDEX(session,
		    ret = __rec_col_int(session, r, ref));
		break;
	case WT_PAGE_COL_VAR:
		ret = __rec_col_var(session, r, ref, salvage);
		break;
	case WT_PAGE_ROW_INT:
		WT_WITH_PAGE_INDEX(session,
		    ret = __rec_row_int(session, r, page));
		break;
	case WT_PAGE_ROW_LEAF:
		ret = __rec_row_leaf(session, r, page, salvage);
		break;
	WT_ILLEGAL_VALUE_SET(session);
	}

	/*
	 * Update the global lookaside score.  Only use observations during
	 * eviction, not checkpoints and don't count eviction of the lookaside
	 * table itself.
	 */
	if (F_ISSET(r, WT_REC_EVICT) && !F_ISSET(btree, WT_BTREE_LOOKASIDE))
		__wt_cache_update_lookaside_score(
		    session, r->updates_seen, r->updates_unstable);

	/* Check for a successful reconciliation. */
	WT_TRET(__rec_write_check_complete(session, r, ret, lookaside_retryp));

	/* Wrap up the page reconciliation. */
	if (ret == 0 && (ret = __rec_write_wrapup(session, r, page)) == 0)
		__rec_write_page_status(session, r);
	else
		WT_TRET(__rec_write_wrapup_err(session, r, page));

	/* Release the reconciliation lock. */
	WT_PAGE_UNLOCK(session, page);

	/* Update statistics. */
	WT_STAT_CONN_INCR(session, rec_pages);
	WT_STAT_DATA_INCR(session, rec_pages);
	if (LF_ISSET(WT_REC_EVICT)) {
		WT_STAT_CONN_INCR(session, rec_pages_eviction);
		WT_STAT_DATA_INCR(session, rec_pages_eviction);
	}
	if (r->cache_write_lookaside) {
		WT_STAT_CONN_INCR(session, cache_write_lookaside);
		WT_STAT_DATA_INCR(session, cache_write_lookaside);
	}
	if (r->cache_write_restore) {
		WT_STAT_CONN_INCR(session, cache_write_restore);
		WT_STAT_DATA_INCR(session, cache_write_restore);
	}
	if (r->multi_next > btree->rec_multiblock_max)
		btree->rec_multiblock_max = r->multi_next;

	/* Clean up the reconciliation structure. */
	__rec_cleanup(session, r);

	/*
	 * When threads perform eviction, don't cache block manager structures
	 * (even across calls), we can have a significant number of threads
	 * doing eviction at the same time with large items. Ignore checkpoints,
	 * once the checkpoint completes, all unnecessary session resources will
	 * be discarded.
	 */
	if (!WT_SESSION_IS_CHECKPOINT(session)) {
		/*
		 * Clean up the underlying block manager memory too: it's not
		 * reconciliation, but threads discarding reconciliation
		 * structures want to clean up the block manager's structures
		 * as well, and there's no obvious place to do that.
		 */
		if (session->block_manager_cleanup != NULL)
			WT_TRET(session->block_manager_cleanup(session));

		WT_TRET(__rec_destroy_session(session));
	}

	/*
	 * We track removed overflow objects in case there's a reader in
	 * transit when they're removed. Any form of eviction locks out
	 * readers, we can discard them all.
	 */
	if (LF_ISSET(WT_REC_EVICT))
		__wt_ovfl_discard_remove(session, page);

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
	WT_BTREE *btree;

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
	if (!F_ISSET(r, WT_REC_LOOKASIDE))
		return (false);
	if (F_ISSET(btree, WT_BTREE_NO_CHECKPOINT))
		return (false);
	if (r->orig_btree_checkpoint_gen == btree->checkpoint_gen &&
	    r->orig_txn_checkpoint_gen ==
	    __wt_gen(session, WT_GEN_CHECKPOINT) &&
	    r->orig_btree_checkpoint_gen == r->orig_txn_checkpoint_gen)
		return (false);
	return (true);
}

/*
 * __rec_write_check_complete --
 *	Check that reconciliation should complete.
 */
static int
__rec_write_check_complete(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, int tret, bool *lookaside_retryp)
{
	/*
	 * Tests in this function are lookaside tests and tests to decide if
	 * rewriting a page in memory is worth doing. In-memory configurations
	 * can't use a lookaside table, and we ignore page rewrite desirability
	 * checks for in-memory eviction because a small cache can force us to
	 * rewrite every possible page.
	 */
	if (F_ISSET(r, WT_REC_IN_MEMORY))
		return (0);

	/*
	 * If we have used the lookaside table, check for a lookaside table and
	 * checkpoint collision.
	 */
	if (r->cache_write_lookaside && __rec_las_checkpoint_test(session, r))
		return (EBUSY);

	/*
	 * Fall back to lookaside eviction during checkpoints if a page can't
	 * be evicted.
	 */
	if (tret == EBUSY && lookaside_retryp != NULL &&
	    !F_ISSET(r, WT_REC_UPDATE_RESTORE) && !r->update_uncommitted)
		*lookaside_retryp = true;

	/* Don't continue if we have already given up. */
	WT_RET(tret);

	/*
	 * Check if this reconciliation attempt is making progress.  If there's
	 * any sign of progress, don't fall back to the lookaside table.
	 *
	 * Check if the current reconciliation split, in which case we'll
	 * likely get to write at least one of the blocks.  If we've created a
	 * page image for a page that previously didn't have one, or we had a
	 * page image and it is now empty, that's also progress.
	 */
	if (r->multi_next > 1)
		return (0);

	/*
	 * We only suggest lookaside if currently in an evict/restore attempt
	 * and some updates were saved.  Our caller sets the evict/restore flag
	 * based on various conditions (like if this is a leaf page), which is
	 * why we're testing that flag instead of a set of other conditions.
	 * If no updates were saved, eviction will succeed without needing to
	 * restore anything.
	 */
	if (!F_ISSET(r, WT_REC_UPDATE_RESTORE) || lookaside_retryp == NULL ||
	    (r->multi_next == 1 && r->multi->supd_entries == 0))
		return (0);

	/*
	 * Check if the current reconciliation applied some updates, in which
	 * case evict/restore should gain us some space.
	 *
	 * Check if lookaside eviction is possible.  If any of the updates we
	 * saw were uncommitted, the lookaside table cannot be used.
	 */
	if (r->update_uncommitted || r->update_used)
		return (0);

	*lookaside_retryp = true;
	return (EBUSY);
}

/*
 * __rec_write_page_status --
 *	Set the page status after reconciliation.
 */
static void
__rec_write_page_status(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;

	btree = S2BT(session);
	page = r->page;
	mod = page->modify;

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
		 * to complete, there's no risk of that happening).
		 */
		btree->modified = true;
		WT_FULL_BARRIER();
		if (!S2C(session)->modified)
			S2C(session)->modified = true;

		/*
		 * Eviction should only be here if following the save/restore
		 * eviction path.
		 */
		WT_ASSERT(session,
		    !F_ISSET(r, WT_REC_EVICT) ||
		    F_ISSET(r, WT_REC_LOOKASIDE | WT_REC_UPDATE_RESTORE));
	} else {
		/*
		 * Track the page's maximum transaction ID (used to decide if
		 * we're likely to be able to evict this page in the future).
		 */
		mod->rec_max_txn = r->max_txn;
		__wt_timestamp_set(&mod->rec_max_timestamp, &r->max_timestamp);

		/*
		 * Track the tree's maximum transaction ID (used to decide if
		 * it's safe to discard the tree). Reconciliation for eviction
		 * is multi-threaded, only update the tree's maximum transaction
		 * ID when doing a checkpoint. That's sufficient, we only care
		 * about the maximum transaction ID of current updates in the
		 * tree, and checkpoint visits every dirty page in the tree.
		 */
		if (!F_ISSET(r, WT_REC_EVICT)) {
			if (WT_TXNID_LT(btree->rec_max_txn, r->max_txn))
				btree->rec_max_txn = r->max_txn;
#ifdef HAVE_TIMESTAMPS
			if (__wt_timestamp_cmp(
			    &btree->rec_max_timestamp, &r->max_timestamp) < 0)
				__wt_timestamp_set(&btree->rec_max_timestamp,
				    &r->max_timestamp);
#endif
		}

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
			WT_ASSERT(session, !F_ISSET(r, WT_REC_EVICT));
	}
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

	__wt_verbose(session, WT_VERB_SPLIT,
	    "root page split -> %" PRIu32 " pages", mod->mod_multi_entries);

	/*
	 * Create a new root page, initialize the array of child references,
	 * mark it dirty, then write it.
	 */
	WT_RET(__wt_page_alloc(session,
	    page->type, mod->mod_multi_entries, false, &next));

	WT_INTL_INDEX_GET(session, next, pindex);
	for (i = 0; i < mod->mod_multi_entries; ++i) {
		/*
		 * There's special error handling required when re-instantiating
		 * pages in memory; it's not needed here, asserted for safety.
		 */
		WT_ASSERT(session, mod->mod_multi[i].supd == NULL);
		WT_ASSERT(session, mod->mod_multi[i].disk_image == NULL);

		WT_ERR(__wt_multi_to_ref(session,
		    next, &mod->mod_multi[i], &pindex->index[i], NULL, false));
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
	return (__wt_reconcile(session, &fake_ref, NULL, flags, NULL));

err:	__wt_page_out(session, &next);
	return (ret);
}

/*
 * __rec_raw_compression_config --
 *	Configure raw compression.
 */
static inline bool
__rec_raw_compression_config(WT_SESSION_IMPL *session,
    uint32_t flags, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
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
	 * XXX
	 * Turn off if lookaside or update/restore are configured: those modes
	 * potentially write blocks without entries and raw compression isn't
	 * ready for that.
	 */
	if (LF_ISSET(WT_REC_LOOKASIDE | WT_REC_UPDATE_RESTORE))
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
 * __rec_init --
 *	Initialize the reconciliation structure.
 */
static int
__rec_init(WT_SESSION_IMPL *session,
    WT_REF *ref, uint32_t flags, WT_SALVAGE_COOKIE *salvage, void *reconcilep)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	WT_RECONCILE *r;
	WT_TXN_GLOBAL *txn_global;
	bool las_skew_oldest;

	btree = S2BT(session);
	page = ref->page;

	if ((r = *(WT_RECONCILE **)reconcilep) == NULL) {
		WT_RET(__wt_calloc_one(session, &r));

		*(WT_RECONCILE **)reconcilep = r;
		session->reconcile_cleanup = __rec_destroy_session;

		/* Connect pointers/buffers. */
		r->cur = &r->_cur;
		r->last = &r->_last;

		/* Disk buffers need to be aligned for writing. */
		F_SET(&r->chunkA.image, WT_ITEM_ALIGNED);
		F_SET(&r->chunkB.image, WT_ITEM_ALIGNED);
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
	r->orig_txn_checkpoint_gen = __wt_gen(session, WT_GEN_CHECKPOINT);
	WT_ORDERED_READ(r->orig_write_gen, page->modify->write_gen);

	/*
	 * Cache the oldest running transaction ID.  This is used to check
	 * whether updates seen by reconciliation have committed.  We keep a
	 * cached copy to avoid races where a concurrent transaction could
	 * abort while reconciliation is examining its updates.  This way, any
	 * transaction running when reconciliation starts is considered
	 * uncommitted.
	 */
	txn_global = &S2C(session)->txn_global;
	if (__wt_btree_immediately_durable(session))
		las_skew_oldest = false;
	else
		WT_ORDERED_READ(las_skew_oldest,
		    txn_global->has_stable_timestamp);
	r->las_skew_newest = LF_ISSET(WT_REC_LOOKASIDE) &&
	    LF_ISSET(WT_REC_VISIBLE_ALL) && !las_skew_oldest;

	WT_ORDERED_READ(r->last_running, txn_global->last_running);

	/*
	 * When operating on the lookaside table, we should never try
	 * update/restore or lookaside eviction.
	 */
	WT_ASSERT(session, !F_ISSET(btree, WT_BTREE_LOOKASIDE) ||
	    !LF_ISSET(WT_REC_LOOKASIDE | WT_REC_UPDATE_RESTORE));

	/*
	 * Lookaside table eviction is configured when eviction gets aggressive,
	 * adjust the flags for cases we don't support.
	 *
	 * We don't yet support fixed-length column-store combined with the
	 * lookaside table. It's not hard to do, but the underlying function
	 * that reviews which updates can be written to the evicted page and
	 * which updates need to be written to the lookaside table needs access
	 * to the original value from the page being evicted, and there's no
	 * code path for that in the case of fixed-length column-store objects.
	 * (Row-store and variable-width column-store objects provide a
	 * reference to the unpacked on-page cell for this purpose, but there
	 * isn't an on-page cell for fixed-length column-store objects.) For
	 * now, turn it off.
	 */
	if (page->type == WT_PAGE_COL_FIX)
		LF_CLR(WT_REC_LOOKASIDE);

	/*
	 * Check for a lookaside table and checkpoint collision, and if we find
	 * one, turn off the lookaside file (we've gone to all the effort of
	 * getting exclusive access to the page, might as well try and evict
	 * it).
	 */
	if (LF_ISSET(WT_REC_LOOKASIDE) && __rec_las_checkpoint_test(session, r))
		LF_CLR(WT_REC_LOOKASIDE);

	r->flags = flags;

	/* Track the page's min/maximum transaction */
	r->max_txn = WT_TXN_NONE;
	__wt_timestamp_set_zero(&r->max_timestamp);
	__wt_timestamp_set_zero(&r->max_onpage_timestamp);
	__wt_timestamp_set_inf(&r->min_saved_timestamp);

	/* Track if updates were used and/or uncommitted. */
	r->updates_seen = r->updates_unstable = 0;
	r->update_uncommitted = r->update_used = false;

	/* Track if the page can be marked clean. */
	r->leave_dirty = false;

	/* Raw compression. */
	r->raw_compression =
	    __rec_raw_compression_config(session, flags, page, salvage);
	r->raw_destination.flags = WT_ITEM_ALIGNED;

	/* Track overflow items. */
	r->ovfl_items = false;

	/* Track empty values. */
	r->all_empty_value = true;
	r->any_empty_value = false;

	/* The list of saved updates is reused. */
	r->supd_next = 0;
	r->supd_memsize = 0;

	/* The list of pages we've written. */
	r->multi = NULL;
	r->multi_next = 0;
	r->multi_allocated = 0;

	r->wrapup_checkpoint = NULL;
	r->wrapup_checkpoint_compressed = false;

	r->evict_matching_checksum_failed = false;

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
		WT_RET(__rec_dictionary_init(session,
		    r, btree->dictionary < 100 ? 100 : btree->dictionary));
	__rec_dictionary_reset(r);

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

	/*
	 * The fake cursor used to figure out modified update values points to
	 * the enclosing WT_REF as a way to access the page.
	 */
	r->update_modify_cbt.ref = ref;

	return (0);
}

/*
 * __rec_cleanup --
 *	Clean up after a reconciliation run, except for structures cached
 *	across runs.
 */
static void
__rec_cleanup(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BTREE *btree;
	WT_MULTI *multi;
	uint32_t i;

	btree = S2BT(session);

	if (btree->type == BTREE_ROW)
		for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
			__wt_free(session, multi->key.ikey);
	for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i) {
		__wt_free(session, multi->disk_image);
		__wt_free(session, multi->supd);
		__wt_free(session, multi->addr.addr);
	}
	__wt_free(session, r->multi);

	/* Reconciliation is not re-entrant, make sure that doesn't happen. */
	r->ref = NULL;
}

/*
 * __rec_destroy --
 *	Clean up the reconciliation structure.
 */
static void
__rec_destroy(WT_SESSION_IMPL *session, void *reconcilep)
{
	WT_RECONCILE *r;

	if ((r = *(WT_RECONCILE **)reconcilep) == NULL)
		return;
	*(WT_RECONCILE **)reconcilep = NULL;

	__wt_free(session, r->raw_entries);
	__wt_free(session, r->raw_offsets);
	__wt_free(session, r->raw_recnos);
	__wt_buf_free(session, &r->raw_destination);

	__wt_buf_free(session, &r->chunkA.key);
	__wt_buf_free(session, &r->chunkA.min_key);
	__wt_buf_free(session, &r->chunkA.image);
	__wt_buf_free(session, &r->chunkB.key);
	__wt_buf_free(session, &r->chunkB.min_key);
	__wt_buf_free(session, &r->chunkB.image);

	__wt_free(session, r->supd);

	__rec_dictionary_free(session, r);

	__wt_buf_free(session, &r->k.buf);
	__wt_buf_free(session, &r->v.buf);
	__wt_buf_free(session, &r->_cur);
	__wt_buf_free(session, &r->_last);

	__wt_buf_free(session, &r->update_modify_cbt.iface.value);

	__wt_free(session, r);
}

/*
 * __rec_destroy_session --
 *	Clean up the reconciliation structure, session version.
 */
static int
__rec_destroy_session(WT_SESSION_IMPL *session)
{
	__rec_destroy(session, &session->reconcile);
	return (0);
}

/*
 * __rec_update_save --
 *	Save a WT_UPDATE list for later restoration.
 */
static int
__rec_update_save(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_INSERT *ins, void *ripcip, WT_UPDATE *onpage_upd, size_t upd_memsize)
{
	WT_RET(__wt_realloc_def(
	    session, &r->supd_allocated, r->supd_next + 1, &r->supd));
	r->supd[r->supd_next].ins = ins;
	r->supd[r->supd_next].ripcip = ripcip;
	r->supd[r->supd_next].onpage_upd = onpage_upd;
	++r->supd_next;
	r->supd_memsize += upd_memsize;
	return (0);
}

/*
 * __rec_append_orig_value --
 *	Append the key's original value to its update list.
 */
static int
__rec_append_orig_value(WT_SESSION_IMPL *session,
    WT_PAGE *page, WT_UPDATE *upd, WT_CELL_UNPACK *unpack)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_UPDATE *append;
	size_t size;

	/* Done if at least one self-contained update is globally visible. */
	for (;; upd = upd->next) {
		if (WT_UPDATE_DATA_VALUE(upd) &&
		    __wt_txn_upd_visible_all(session, upd))
			return (0);

		/* Leave reference at the last item in the chain. */
		if (upd->next == NULL)
			break;
	}

	/*
	 * We need the original on-page value for some reader: get a copy and
	 * append it to the end of the update list with a transaction ID that
	 * guarantees its visibility.
	 *
	 * If we don't have a value cell, it's an insert/append list key/value
	 * pair which simply doesn't exist for some reader; place a deleted
	 * record at the end of the update list.
	 */
	append = NULL;			/* -Wconditional-uninitialized */
	size = 0;			/* -Wconditional-uninitialized */
	if (unpack == NULL || unpack->type == WT_CELL_DEL)
		WT_RET(__wt_update_alloc(session,
		    NULL, &append, &size, WT_UPDATE_DELETED));
	else {
		WT_RET(__wt_scr_alloc(session, 0, &tmp));
		WT_ERR(__wt_page_cell_data_ref(session, page, unpack, tmp));
		WT_ERR(__wt_update_alloc(
		    session, tmp, &append, &size, WT_UPDATE_STANDARD));
	}

	/*
	 * Set the entry's transaction information to the lowest possible value.
	 * Since cleared memory matches the lowest possible transaction ID and
	 * timestamp, do nothing.
	 *
	 * Append the new entry to the update list.
	 */
	WT_PUBLISH(upd->next, append);
	__wt_cache_page_inmem_incr(session, page, size);

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

/*
 * __rec_txn_read --
 *	Return the update in a list that should be written (or NULL if none can
 *	be written).
 */
static int
__rec_txn_read(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_INSERT *ins, void *ripcip, WT_CELL_UNPACK *vpack,
    bool *upd_savedp, WT_UPDATE **updp)
{
	WT_PAGE *page;
	WT_UPDATE *first_txn_upd, *first_upd, *upd;
	wt_timestamp_t *timestampp;
	size_t upd_memsize;
	uint64_t max_txn, txnid;
	bool all_visible, uncommitted;

#ifdef HAVE_TIMESTAMPS
	WT_UPDATE *first_ts_upd;
	first_ts_upd = NULL;
#endif

	if (upd_savedp != NULL)
		*upd_savedp = false;
	*updp = NULL;

	page = r->page;
	first_txn_upd = NULL;
	upd_memsize = 0;
	max_txn = WT_TXN_NONE;
	uncommitted = false;

	/*
	 * If called with a WT_INSERT item, use its WT_UPDATE list (which must
	 * exist), otherwise check for an on-page row-store WT_UPDATE list
	 * (which may not exist). Return immediately if the item has no updates.
	 */
	if (ins != NULL)
		first_upd = ins->upd;
	else if ((first_upd = WT_ROW_UPDATE(page, ripcip)) == NULL)
		return (0);

	for (upd = first_upd; upd != NULL; upd = upd->next) {
		if ((txnid = upd->txnid) == WT_TXN_ABORTED)
			continue;

		++r->updates_seen;
		upd_memsize += WT_UPDATE_MEMSIZE(upd);

		/*
		 * Track the first update in the chain that is not aborted and
		 * the maximum transaction ID.
		 */
		if (first_txn_upd == NULL)
			first_txn_upd = upd;

		/* Track the largest transaction ID seen. */
		if (WT_TXNID_LT(max_txn, txnid))
			max_txn = txnid;

		/*
		 * Check whether the update was committed before reconciliation
		 * started.  The global commit point can move forward during
		 * reconciliation so we use a cached copy to avoid races when a
		 * concurrent transaction commits or rolls back while we are
		 * examining its updates.
		 */
		if (F_ISSET(r, WT_REC_EVICT) &&
		    (F_ISSET(r, WT_REC_VISIBLE_ALL) ?
		    WT_TXNID_LE(r->last_running, txnid) :
		    !__txn_visible_id(session, txnid))) {
			uncommitted = r->update_uncommitted = true;
			continue;
		}

#ifdef HAVE_TIMESTAMPS
		/* Track the first update with non-zero timestamp. */
		if (first_ts_upd == NULL &&
		    !__wt_timestamp_iszero(&upd->timestamp))
			first_ts_upd = upd;
#endif

		/*
		 * Find the first update we can use.
		 *
		 * Update/restore eviction can handle any update (including
		 * uncommitted updates).  Lookaside eviction can save any
		 * committed update.  Regular eviction checks that the maximum
		 * transaction ID and timestamp seen are stable.
		 *
		 * Lookaside eviction tries to choose the same version as a
		 * subsequent checkpoint, so that checkpoint can skip over
		 * pages with lookaside entries.  If the application has
		 * supplied a stable timestamp, we assume (a) that it is old,
		 * and (b) that the next checkpoint will use it, so we wait to
		 * see a stable update.  If there is no stable timestamp, we
		 * assume the next checkpoint will write the most recent
		 * version (but we save enough information that checkpoint can
		 * fix things up if we choose an update that is too new).
		 */
		if (*updp == NULL && r->las_skew_newest)
			*updp = upd;

		if (F_ISSET(r, WT_REC_VISIBLE_ALL) ?
		    !__wt_txn_upd_visible_all(session, upd) :
		    !__wt_txn_upd_visible(session, upd)) {
			if (F_ISSET(r, WT_REC_EVICT))
				++r->updates_unstable;

			/*
			 * Rare case: when applications run at low isolation
			 * levels, update/restore eviction may see a stable
			 * update followed by an uncommitted update.  Give up
			 * in that case: we need to discard updates from the
			 * stable update and older for correctness and we can't
			 * discard an uncommitted update.
			 */
			if (F_ISSET(r, WT_REC_UPDATE_RESTORE) &&
			    *updp != NULL && uncommitted) {
				r->leave_dirty = true;
				return (EBUSY);
			}

			continue;
		}

		/*
		 * Lookaside without stable timestamp was taken care of above
		 * (set to the first uncommitted transaction.  Lookaside with
		 * stable timestamp always takes the first stable update.
		 */
		if (*updp == NULL)
			*updp = upd;
	}

	/* Reconciliation should never see an aborted or reserved update. */
	WT_ASSERT(session, *updp == NULL ||
	    ((*updp)->txnid != WT_TXN_ABORTED &&
	    (*updp)->type != WT_UPDATE_RESERVED));

	/* If all of the updates were aborted, quit. */
	if (first_txn_upd == NULL)
		return (0);

	/*
	 * Track the most recent transaction in the page.  We store this in the
	 * tree at the end of reconciliation in the service of checkpoints, it
	 * is used to avoid discarding trees from memory when they have changes
	 * required to satisfy a snapshot read.
	 */
	if (WT_TXNID_LT(r->max_txn, max_txn))
		r->max_txn = max_txn;

#ifdef HAVE_TIMESTAMPS
	if (first_ts_upd != NULL &&
	    __wt_timestamp_cmp(&r->max_timestamp, &first_ts_upd->timestamp) < 0)
		__wt_timestamp_set(&r->max_timestamp, &first_ts_upd->timestamp);
#endif

	/*
	 * The checkpoint transaction is special.  Make sure we never write
	 * metadata updates from a checkpoint in a concurrent session.
	 */
	WT_ASSERT(session, !WT_IS_METADATA(session->dhandle) ||
	    *updp == NULL || (*updp)->txnid == WT_TXN_NONE ||
	    (*updp)->txnid != S2C(session)->txn_global.checkpoint_state.id ||
	    WT_SESSION_IS_CHECKPOINT(session));

	/*
	 * If there are no skipped updates, record that we're making progress.
	 */
	if (*updp == first_txn_upd)
		r->update_used = true;

	/*
	 * Check if all updates on the page are visible.  If not, it must stay
	 * dirty unless we are saving updates to the lookaside table.
	 *
	 * Updates can be out of transaction ID order (but not out of timestamp
	 * order), so we track the maximum transaction ID and the newest update
	 * with a timestamp (if any).
	 */
#ifdef HAVE_TIMESTAMPS
	timestampp = first_ts_upd == NULL ? NULL : &first_ts_upd->timestamp;
#else
	timestampp = NULL;
#endif
	all_visible = *updp == first_txn_upd && !uncommitted &&
	    (F_ISSET(r, WT_REC_VISIBLE_ALL) ?
	    __wt_txn_visible_all(session, max_txn, timestampp) :
	    __wt_txn_visible(session, max_txn, timestampp));

	if (all_visible)
		goto check_original_value;

	if (F_ISSET(r, WT_REC_VISIBILITY_ERR))
		WT_PANIC_RET(session, EINVAL,
		    "reconciliation error, update not visible");

	r->leave_dirty = true;

	/*
	 * If not trying to evict the page, we know what we'll write and we're
	 * done.
	 */
	if (!F_ISSET(r, WT_REC_EVICT))
		goto check_original_value;

	/*
	 * We are attempting eviction with changes that are not yet stable
	 * (i.e. globally visible).  There are two ways to continue, the
	 * save/restore eviction path or the lookaside table eviction path.
	 * Both cannot be configured because the paths track different
	 * information. The update/restore path can handle uncommitted changes,
	 * by evicting most of the page and then creating a new, smaller page
	 * to which we re-attach those changes. Lookaside eviction writes
	 * changes into the lookaside table and restores them on demand if and
	 * when the page is read back into memory.
	 *
	 * Both paths are configured outside of reconciliation: the save/restore
	 * path is the WT_REC_UPDATE_RESTORE flag, the lookaside table path is
	 * the WT_REC_LOOKASIDE flag.
	 */
	if (!F_ISSET(r, WT_REC_LOOKASIDE | WT_REC_UPDATE_RESTORE))
		return (EBUSY);
	if (uncommitted && !F_ISSET(r, WT_REC_UPDATE_RESTORE))
		return (EBUSY);

	WT_ASSERT(session, r->max_txn != WT_TXN_NONE);

	/*
	 * The order of the updates on the list matters, we can't move only the
	 * unresolved updates, move the entire update list.
	 */
	WT_RET(__rec_update_save(session, r, ins, ripcip, *updp, upd_memsize));
	if (upd_savedp != NULL)
		*upd_savedp = true;

#ifdef HAVE_TIMESTAMPS
	/* Track the oldest saved timestamp for lookaside. */
	if (F_ISSET(r, WT_REC_LOOKASIDE)) {
		/* If no updates had timestamps, we're done. */
		if (first_ts_upd == NULL)
			__wt_timestamp_set_zero(&r->min_saved_timestamp);
		for (upd = first_upd; upd != *updp; upd = upd->next) {
			if (upd->txnid != WT_TXN_ABORTED &&
			    __wt_timestamp_cmp(&upd->timestamp,
			    &r->min_saved_timestamp) < 0)
				__wt_timestamp_set(&r->min_saved_timestamp,
				    &upd->timestamp);

			WT_ASSERT(session, upd->txnid == WT_TXN_ABORTED ||
			    WT_TXNID_LE(upd->txnid, r->max_txn));
		}
	}
#endif

check_original_value:
	/*
	 * Paranoia: check that we didn't choose an update that has since been
	 * rolled back.
	 */
	WT_ASSERT(session, *updp == NULL || (*updp)->txnid != WT_TXN_ABORTED);

	/*
	 * Returning an update means the original on-page value might be lost,
	 * and that's a problem if there's a reader that needs it. There are
	 * several cases:
	 * - any update with no backing record (because we will store an empty
	 *   value on page and returning that is wrong).
	 * - any update from a modify operation (because the modify has to be
	 *   applied to a stable update, not the new on-page update),
	 * - any lookaside table eviction (because the backing disk image is
	 *   rewritten),
	 * - or any reconciliation of a backing overflow record that will be
	 *   physically removed once it's no longer needed.
	 */
	if (*updp != NULL && (!WT_UPDATE_DATA_VALUE(*updp) ||
	    F_ISSET(r, WT_REC_LOOKASIDE) || (vpack != NULL &&
	    vpack->ovfl && vpack->raw != WT_CELL_VALUE_OVFL_RM)))
		WT_RET(
		    __rec_append_orig_value(session, page, first_upd, vpack));

#ifdef HAVE_TIMESTAMPS
	if ((upd = *updp) != NULL &&
	    __wt_timestamp_cmp(&upd->timestamp, &r->max_onpage_timestamp) > 0)
		__wt_timestamp_set(&r->max_onpage_timestamp, &upd->timestamp);
#endif

	return (0);
}

/*
 * WT_CHILD_RELEASE, WT_CHILD_RELEASE_ERR --
 *	Macros to clean up during internal-page reconciliation, releasing the
 *	hazard pointer we're holding on child pages.
 */
#define	WT_CHILD_RELEASE(session, hazard, ref) do {			\
	if (hazard) {							\
		(hazard) = false;					\
		WT_TRET(						\
		    __wt_page_release(session, ref, WT_READ_NO_EVICT));	\
	}								\
} while (0)
#define	WT_CHILD_RELEASE_ERR(session, hazard, ref) do {			\
	WT_CHILD_RELEASE(session, hazard, ref);				\
	WT_ERR(ret);							\
} while (0)

typedef enum {
    WT_CHILD_IGNORE,				/* Ignored child */
    WT_CHILD_MODIFIED,				/* Modified child */
    WT_CHILD_ORIGINAL,				/* Original child */
    WT_CHILD_PROXY				/* Deleted child: proxy */
} WT_CHILD_STATE;

/*
 * __rec_child_deleted --
 *	Handle pages with leaf pages in the WT_REF_DELETED state.
 */
static int
__rec_child_deleted(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_REF *ref, WT_CHILD_STATE *statep)
{
	WT_PAGE_DELETED *page_del;

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
	if (F_ISSET(r, WT_REC_VISIBILITY_ERR) && page_del != NULL &&
	    !__wt_txn_visible(session,
	    page_del->txnid, WT_TIMESTAMP_NULL(&page_del->timestamp)))
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
	    (page_del == NULL || __wt_txn_visible_all(
	    session, page_del->txnid, WT_TIMESTAMP_NULL(&page_del->timestamp))))
		WT_RET(__wt_ref_block_free(session, ref));

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
	if (F_ISSET(r, WT_REC_EVICT))
		return (EBUSY);

	/*
	 * If there are deleted child pages we can't discard immediately, keep
	 * the page dirty so they are eventually freed.
	 */
	r->leave_dirty = true;

	/*
	 * If the original page cannot be freed, we need to keep a slot on the
	 * page to reference it from the parent page.
	 *
	 * If the delete is not visible in this checkpoint, write the original
	 * address normally.  Otherwise, we have to write a proxy record.
	 */
	if (__wt_txn_visible(
	    session, page_del->txnid, WT_TIMESTAMP_NULL(&page_del->timestamp)))
		*statep = WT_CHILD_PROXY;

	return (0);
}

/*
 * __rec_child_modify --
 *	Return if the internal page's child references any modifications.
 */
static int
__rec_child_modify(WT_SESSION_IMPL *session,
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
	 * to handle child pages referenced by the internal page.
	 *
	 * Internal pages are reconciled for two reasons: first, when evicting
	 * an internal page, second by the checkpoint code when writing internal
	 * pages.  During eviction, all pages should be in the WT_REF_DISK or
	 * WT_REF_DELETED state. During checkpoint, eviction that might affect
	 * review of an internal page is prohibited, however, as the subtree is
	 * not reserved for our exclusive use, there are other page states that
	 * must be considered.
	 */
	for (;; __wt_yield()) {
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
			 * We should never be here during eviction, active child
			 * pages in an evicted page's subtree fails the eviction
			 * attempt.
			 */
			WT_ASSERT(session, !F_ISSET(r, WT_REC_EVICT));
			if (F_ISSET(r, WT_REC_EVICT))
				return (EBUSY);

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

		case WT_REF_LOOKASIDE:
			/*
			 * On disk, with lookaside updates.
			 *
			 * We should never be here during eviction, active
			 * child pages in an evicted page's subtree fails the
			 * eviction attempt.
			 */
			WT_ASSERT(session, !F_ISSET(r, WT_REC_EVICT));
			if (F_ISSET(r, WT_REC_EVICT))
				return (EBUSY);

			/*
			 * A page evicted with lookaside entries may not have
			 * an address, if no updates were visible to
			 * reconciliation.  Any child pages in that state
			 * should be ignored.
			 */
			if (ref->addr == NULL) {
				*statep = WT_CHILD_IGNORE;
				WT_CHILD_RELEASE(session, *hazardp, ref);
			}

			goto done;

		case WT_REF_MEM:
			/*
			 * In memory.
			 *
			 * We should never be here during eviction, active child
			 * pages in an evicted page's subtree fails the eviction
			 * attempt.
			 */
			WT_ASSERT(session, !F_ISSET(r, WT_REC_EVICT));
			if (F_ISSET(r, WT_REC_EVICT))
				return (EBUSY);

			/*
			 * If called during checkpoint, acquire a hazard pointer
			 * so the child isn't evicted, it's an in-memory case.
			 *
			 * This call cannot return split/restart, we have a lock
			 * on the parent which prevents a child page split.
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
			 * We should never be here during eviction, active child
			 * pages in an evicted page's subtree fails the eviction
			 * attempt.
			 */
			WT_ASSERT(session, !F_ISSET(r, WT_REC_EVICT));
			if (F_ISSET(r, WT_REC_EVICT))
				return (EBUSY);
			goto done;

		case WT_REF_SPLIT:
			/*
			 * The page was split out from under us.
			 *
			 * We should never be here during eviction, active child
			 * pages in an evicted page's subtree fails the eviction
			 * attempt.
			 *
			 * We should never be here during checkpoint, dirty page
			 * eviction is shutout during checkpoint, all splits in
			 * process will have completed before we walk any pages
			 * for checkpoint.
			 */
			WT_ASSERT(session, WT_REF_SPLIT != WT_REF_SPLIT);
			return (EBUSY);

		WT_ILLEGAL_VALUE(session);
		}
		WT_STAT_CONN_INCR(session, child_modify_blocked_page);
	}

in_memory:
	/*
	 * In-memory states: the child is potentially modified if the page's
	 * modify structure has been instantiated. If the modify structure
	 * exists and the page has actually been modified, set that state.
	 * If that's not the case, we would normally use the original cell's
	 * disk address as our reference, however there are two special cases,
	 * both flagged by a missing block address.
	 *
	 * First, if forced to instantiate a deleted child page and it's never
	 * modified, we end up here with a page that has a modify structure, no
	 * modifications, and no disk address. Ignore those pages, they're not
	 * modified and there is no reason to write the cell.
	 *
	 * Second, insert splits are permitted during checkpoint. When doing the
	 * final checkpoint pass, we first walk the internal page's page-index
	 * and write out any dirty pages we find, then we write out the internal
	 * page in post-order traversal. If we found the split page in the first
	 * step, it will have an address; if we didn't find the split page in
	 * the first step, it won't have an address and we ignore it, it's not
	 * part of the checkpoint.
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
 * __rec_incr --
 *	Update the memory tracking structure for a set of new entries.
 */
static inline void
__rec_incr(WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v, size_t size)
{
	/*
	 * The buffer code is fragile and prone to off-by-one errors -- check
	 * for overflow in diagnostic mode.
	 */
	WT_ASSERT(session, r->space_avail >= size);
	WT_ASSERT(session, WT_BLOCK_FITS(r->first_free, size,
	    r->cur_ptr->image.mem, r->cur_ptr->image.memsize));

	r->entries += v;
	r->space_avail -= size;
	r->first_free += size;

	/*
	 * If offset for the minimum split size boundary is not set, we have not
	 * yet reached the minimum boundary, reduce the space available for it.
	 */
	if (r->cur_ptr->min_offset == 0) {
		if (r->min_space_avail >= size)
			r->min_space_avail -= size;
		else
			r->min_space_avail = 0;
	}
}

/*
 * __rec_copy_incr --
 *	Copy a key/value cell and buffer pair into the new image.
 */
static inline void
__rec_copy_incr(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_KV *kv)
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
	for (p = r->first_free,
	    t = (uint8_t *)&kv->cell, len = kv->cell_len; len > 0; --len)
		*p++ = *t++;

	/* The data can be quite large -- call memcpy. */
	if (kv->buf.size != 0)
		memcpy(p, kv->buf.data, kv->buf.size);

	WT_ASSERT(session, kv->len == kv->cell_len + kv->buf.size);
	__rec_incr(session, r, 1, kv->len);
}

/*
 * __rec_dict_replace --
 *	Check for a dictionary match.
 */
static int
__rec_dict_replace(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, uint64_t rle, WT_KV *val)
{
	WT_DICTIONARY *dp;
	uint64_t offset;

	/*
	 * We optionally create a dictionary of values and only write a unique
	 * value once per page, using a special "copy" cell for all subsequent
	 * copies of the value.  We have to do the cell build and resolution at
	 * this low level because we need physical cell offsets for the page.
	 *
	 * Sanity check: short-data cells can be smaller than dictionary-copy
	 * cells.  If the data is already small, don't bother doing the work.
	 * This isn't just work avoidance: on-page cells can't grow as a result
	 * of writing a dictionary-copy cell, the reconciliation functions do a
	 * split-boundary test based on the size required by the value's cell;
	 * if we grow the cell after that test we'll potentially write off the
	 * end of the buffer's memory.
	 */
	if (val->buf.size <= WT_INTPACK32_MAXSIZE)
		return (0);
	WT_RET(__rec_dictionary_lookup(session, r, val, &dp));
	if (dp == NULL)
		return (0);

	/*
	 * If the dictionary offset isn't set, we're creating a new entry in the
	 * dictionary, set its location.
	 *
	 * If the dictionary offset is set, we have a matching value. Create a
	 * copy cell instead.
	 */
	if (dp->offset == 0)
		dp->offset = WT_PTRDIFF32(r->first_free, r->cur_ptr->image.mem);
	else {
		/*
		 * The offset is the byte offset from this cell to the previous,
		 * matching cell, NOT the byte offset from the beginning of the
		 * page.
		 */
		offset = (uint64_t)WT_PTRDIFF(r->first_free,
		    (uint8_t *)r->cur_ptr->image.mem + dp->offset);
		val->len = val->cell_len =
		    __wt_cell_pack_copy(&val->cell, rle, offset);
		val->buf.data = NULL;
		val->buf.size = 0;
	}
	return (0);
}

/*
 * __rec_key_state_update --
 *	Update prefix and suffix compression based on the last key.
 */
static inline void
__rec_key_state_update(WT_RECONCILE *r, bool ovfl_key)
{
	WT_ITEM *a;

	/*
	 * If writing an overflow key onto the page, don't update the "last key"
	 * value, and leave the state of prefix compression alone.  (If we are
	 * currently doing prefix compression, we have a key state which will
	 * continue to work, we're just skipping the key just created because
	 * it's an overflow key and doesn't participate in prefix compression.
	 * If we are not currently doing prefix compression, we can't start, an
	 * overflow key doesn't give us any state.)
	 *
	 * Additionally, if we wrote an overflow key onto the page, turn off the
	 * suffix compression of row-store internal node keys.  (When we split,
	 * "last key" is the largest key on the previous page, and "cur key" is
	 * the first key on the next page, which is being promoted.  In some
	 * cases we can discard bytes from the "cur key" that are not needed to
	 * distinguish between the "last key" and "cur key", compressing the
	 * size of keys on internal nodes.  If we just built an overflow key,
	 * we're not going to update the "last key", making suffix compression
	 * impossible for the next key. Alternatively, we could remember where
	 * the last key was on the page, detect it's an overflow key, read it
	 * from disk and do suffix compression, but that's too much work for an
	 * unlikely event.)
	 *
	 * If we're not writing an overflow key on the page, update the last-key
	 * value and turn on both prefix and suffix compression.
	 */
	if (ovfl_key)
		r->key_sfx_compress = false;
	else {
		a = r->cur;
		r->cur = r->last;
		r->last = a;

		r->key_pfx_compress = r->key_pfx_compress_conf;
		r->key_sfx_compress = r->key_sfx_compress_conf;
	}
}

/*
 * Macros from fixed-length entries to/from bytes.
 */
#define	WT_FIX_BYTES_TO_ENTRIES(btree, bytes)				\
    ((uint32_t)((((bytes) * 8) / (btree)->bitcnt)))
#define	WT_FIX_ENTRIES_TO_BYTES(btree, entries)				\
	((uint32_t)WT_ALIGN((entries) * (btree)->bitcnt, 8))

/*
 * __rec_leaf_page_max --
 *	Figure out the maximum leaf page size for the reconciliation.
 */
static inline uint32_t
__rec_leaf_page_max(WT_SESSION_IMPL *session, WT_RECONCILE *r)
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
 * __rec_need_split --
 *	Check whether adding some bytes to the page requires a split.
 */
static bool
__rec_need_split(WT_RECONCILE *r, size_t len)
{
	/*
	 * In the case of a row-store leaf page, trigger a split if a threshold
	 * number of saved updates is reached. This allows pages to split for
	 * update/restore and lookaside eviction when there is no visible data
	 * causing the disk image to grow.
	 *
	 * In the case of small pages or large keys, we might try to split when
	 * a page has no updates or entries, which isn't possible. To consider
	 * update/restore or lookaside information, require either page entries
	 * or updates that will be attached to the image. The limit is one of
	 * either, but it doesn't make sense to create pages or images with few
	 * entries or updates, even where page sizes are small (especially as
	 * updates that will eventually become overflow items can throw off our
	 * calculations). Bound the combination at something reasonable.
	 */
	if (r->page->type == WT_PAGE_ROW_LEAF && r->entries + r->supd_next > 10)
		len += r->supd_memsize;

	/* Check for the disk image crossing a boundary. */
	return (r->raw_compression ?
	    len > r->space_avail : WT_CHECK_CROSSING_BND(r, len));
}

/*
 * __rec_split_page_size_from_pct --
 *	Given a split percentage, calculate split page size in bytes.
 */
static uint32_t
__rec_split_page_size_from_pct(
    int split_pct, uint32_t maxpagesize, uint32_t allocsize) {
	uintmax_t a;
	uint32_t split_size;

	/*
	 * Ideally, the split page size is some percentage of the maximum page
	 * size rounded to an allocation unit (round to an allocation unit so we
	 * don't waste space when we write).
	 */
	a = maxpagesize;			/* Don't overflow. */
	split_size = (uint32_t)WT_ALIGN_NEAREST(
	    (a * (u_int)split_pct) / 100, allocsize);

	/*
	 * Respect the configured split percentage if the calculated split size
	 * is either zero or a full page. The user has either configured an
	 * allocation size that matches the page size, or a split percentage
	 * that is close to zero or one hundred. Rounding is going to provide a
	 * worse outcome than having a split point that doesn't fall on an
	 * allocation size boundary in those cases.
	 */
	if (split_size == 0 || split_size == maxpagesize)
		split_size = (uint32_t)((a * (u_int)split_pct) / 100);

	return (split_size);
}

/*
 * __wt_split_page_size --
 *	Split page size calculation: we don't want to repeatedly split every
 *	time a new entry is added, so we split to a smaller-than-maximum page
 *	size.
 */
uint32_t
__wt_split_page_size(WT_BTREE *btree, uint32_t maxpagesize)
{
	return (__rec_split_page_size_from_pct(
	    btree->split_pct, maxpagesize, btree->allocsize));
}

/*
 * __rec_min_split_page_size --
 *	Minimum split size boundary calculation: To track a boundary at the
 *	minimum split size that we could have split at instead of splitting at
 *	the split page size.
 */
static uint32_t
__rec_min_split_page_size(WT_BTREE *btree, uint32_t maxpagesize)
{
	return (__rec_split_page_size_from_pct(
	    WT_BTREE_MIN_SPLIT_PCT, maxpagesize, btree->allocsize));
}

/*
 * __rec_split_chunk_init --
 *	Initialize a single chunk structure.
 */
static int
__rec_split_chunk_init(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_CHUNK *chunk, size_t memsize)
{
	chunk->min_recno = WT_RECNO_OOB;
	chunk->min_entries = 0;
	/* Don't touch the key item memory, that memory is reused. */
	chunk->min_key.size = 0;

	chunk->recno = WT_RECNO_OOB;
	chunk->entries = 0;
	/* Don't touch the key item memory, that memory is reused. */
	chunk->key.size = 0;

	chunk->min_offset = 0;

	/*
	 * Allocate and clear the disk image buffer.
	 *
	 * Don't touch the disk image item memory, that memory is reused.
	 *
	 * Clear the disk page header to ensure all of it is initialized, even
	 * the unused fields.
	 *
	 * In the case of fixed-length column-store, clear the entire buffer:
	 * fixed-length column-store sets bits in bytes, where the bytes are
	 * assumed to initially be 0.
	 */
	WT_RET(__wt_buf_init(session, &chunk->image, memsize));
	memset(chunk->image.mem, 0,
	    r->page->type == WT_PAGE_COL_FIX ? memsize : WT_PAGE_HEADER_SIZE);

	return (0);
}

/*
 * __rec_split_init --
 *	Initialization for the reconciliation split functions.
 */
static int
__rec_split_init(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, uint64_t recno, uint32_t max)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CHUNK *chunk;
	WT_REF *ref;
	size_t corrected_page_size, disk_img_buf_size;

	btree = S2BT(session);
	bm = btree->bm;

	/*
	 * The maximum leaf page size governs when an in-memory leaf page splits
	 * into multiple on-disk pages; however, salvage can't be allowed to
	 * split, there's no parent page yet.  If we're doing salvage, override
	 * the caller's selection of a maximum page size, choosing a page size
	 * that ensures we won't split.
	 */
	if (r->salvage != NULL)
		max = __rec_leaf_page_max(session, r);

	/*
	 * Set the page sizes.  If we're doing the page layout, the maximum page
	 * size is the same as the page size.  If the application is doing page
	 * layout (raw compression is configured), we accumulate some amount of
	 * additional data because we don't know how well it will compress, and
	 * we don't want to increment our way up to the amount of data needed by
	 * the application to successfully compress to the target page size.
	 * Ideally accumulate data several times the page size without
	 * approaching the memory page maximum, but at least have data worth
	 * one page.
	 *
	 * There are cases when we grow the page size to accommodate large
	 * records, in those cases we split the pages once they have crossed
	 * the maximum size for a page with raw compression.
	 */
	r->page_size = r->page_size_orig = max;
	if (r->raw_compression)
		r->max_raw_page_size = r->page_size =
		    (uint32_t)WT_MIN((uint64_t)r->page_size * 10,
		    WT_MAX((uint64_t)r->page_size, btree->maxmempage / 2));
	/*
	 * If we have to split, we want to choose a smaller page size for the
	 * split pages, because otherwise we could end up splitting one large
	 * packed page over and over. We don't want to pick the minimum size
	 * either, because that penalizes an application that did a bulk load
	 * and subsequently inserted a few items into packed pages.  Currently
	 * defaulted to 75%, but I have no empirical evidence that's "correct".
	 *
	 * The maximum page size may be a multiple of the split page size (for
	 * example, there's a maximum page size of 128KB, but because the table
	 * is active and we don't want to split a lot, the split size is 20KB).
	 * The maximum page size may NOT be an exact multiple of the split page
	 * size.
	 *
	 * It's lots of work to build these pages and don't want to start over
	 * when we reach the maximum page size (it's painful to restart after
	 * creating overflow items and compacted data, for example, as those
	 * items have already been written to disk).  So, the loop calls the
	 * helper functions when approaching a split boundary, and we save the
	 * information at that point. We also save the boundary information at
	 * the minimum split size. We maintain two chunks (each boundary
	 * represents a chunk that gets written as a page) in the memory,
	 * writing out the older one to the disk as a page when we need to make
	 * space for a new chunk. On reaching the last chunk, if it turns out to
	 * be smaller than the minimum split size, we go back into the
	 * penultimate chunk and split at this minimum split size boundary. This
	 * moves some data from the penultimate chunk to the last chunk, hence
	 * increasing the size of the last page written without decreasing the
	 * penultimate page size beyond the minimum split size.
	 *
	 * Finally, all this doesn't matter for fixed-size column-store pages,
	 * raw compression, and salvage.  Fixed-size column store pages can
	 * split under (very) rare circumstances, but they're allocated at a
	 * fixed page size, never anything smaller.  In raw compression, the
	 * underlying compression routine decides when we split, so it's not our
	 * problem.  In salvage, as noted above, we can't split at all.
	 */
	if (r->raw_compression || r->salvage != NULL) {
		r->split_size = 0;
		r->space_avail = r->page_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	} else if (page->type == WT_PAGE_COL_FIX) {
		r->split_size = r->page_size;
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	} else {
		r->split_size = __wt_split_page_size(btree, r->page_size);
		r->space_avail =
		    r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
		r->min_split_size =
		    __rec_min_split_page_size(btree, r->page_size);
		r->min_space_avail =
		    r->min_split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	}

	/*
	 * Ensure the disk image buffer is large enough for the max object, as
	 * corrected by the underlying block manager.
	 *
	 * Since we want to support split_size more than the page size (to allow
	 * for adjustments based on the compression), this buffer should be the
	 * greater of split_size and page_size.
	 */
	corrected_page_size = r->page_size;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	disk_img_buf_size = WT_MAX(corrected_page_size, r->split_size);

	/* Initialize the first split chunk. */
	WT_RET(
	    __rec_split_chunk_init(session, r, &r->chunkA, disk_img_buf_size));
	r->cur_ptr = &r->chunkA;
	r->prev_ptr = NULL;

	/* Starting record number, entries, first free byte. */
	r->recno = recno;
	r->entries = 0;
	r->first_free = WT_PAGE_HEADER_BYTE(btree, r->cur_ptr->image.mem);

	/* New page, compression off. */
	r->key_pfx_compress = r->key_sfx_compress = false;

	/* Set the first chunk's key. */
	chunk = r->cur_ptr;
	if (btree->type == BTREE_ROW) {
		ref = r->ref;
		if (__wt_ref_is_root(ref))
			WT_RET(__wt_buf_set(session, &chunk->key, "", 1));
		else
			__wt_ref_key(ref->home,
			    ref, &chunk->key.data, &chunk->key.size);
	} else
		chunk->recno = recno;

	return (0);
}

/*
 * __rec_is_checkpoint --
 *	Return if we're writing a checkpoint.
 */
static bool
__rec_is_checkpoint(WT_SESSION_IMPL *session, WT_RECONCILE *r)
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
	return (!F_ISSET(btree, WT_BTREE_NO_CHECKPOINT) &&
	    __wt_ref_is_root(r->ref));
}

/*
 * __rec_split_row_promote_cell --
 *	Get a key from a cell for the purposes of promotion.
 */
static int
__rec_split_row_promote_cell(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE_HEADER *dsk, WT_ITEM *key)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack;

	btree = S2BT(session);
	kpack = &_kpack;

	/*
	 * The cell had better have a zero-length prefix and not be a copy cell;
	 * the first cell on a page cannot refer an earlier cell on the page.
	 */
	cell = WT_PAGE_HEADER_BYTE(btree, dsk);
	__wt_cell_unpack(cell, kpack);
	WT_ASSERT(session,
	    kpack->prefix == 0 && kpack->raw != WT_CELL_VALUE_COPY);

	WT_RET(__wt_cell_data_copy(session, r->page->type, kpack, key));
	return (0);
}

/*
 * __rec_split_row_promote --
 *	Key promotion for a row-store.
 */
static int
__rec_split_row_promote(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_ITEM *key, uint8_t type)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(update);
	WT_DECL_RET;
	WT_ITEM *max;
	WT_SAVE_UPD *supd;
	size_t cnt, len, size;
	uint32_t i;
	const uint8_t *pa, *pb;
	int cmp;

	/*
	 * For a column-store, the promoted key is the recno and we already have
	 * a copy.  For a row-store, it's the first key on the page, a variable-
	 * length byte string, get a copy.
	 *
	 * This function is called from the split code at each split boundary,
	 * but that means we're not called before the first boundary, and we
	 * will eventually have to get the first key explicitly when splitting
	 * a page.
	 *
	 * For the current slot, take the last key we built, after doing suffix
	 * compression.  The "last key we built" describes some process: before
	 * calling the split code, we must place the last key on the page before
	 * the boundary into the "last" key structure, and the first key on the
	 * page after the boundary into the "current" key structure, we're going
	 * to compare them for suffix compression.
	 *
	 * Suffix compression is a hack to shorten keys on internal pages.  We
	 * only need enough bytes in the promoted key to ensure searches go to
	 * the correct page: the promoted key has to be larger than the last key
	 * on the leaf page preceding it, but we don't need any more bytes than
	 * that. In other words, we can discard any suffix bytes not required
	 * to distinguish between the key being promoted and the last key on the
	 * leaf page preceding it.  This can only be done for the first level of
	 * internal pages, you cannot repeat suffix truncation as you split up
	 * the tree, it loses too much information.
	 *
	 * Note #1: if the last key on the previous page was an overflow key,
	 * we don't have the in-memory key against which to compare, and don't
	 * try to do suffix compression.  The code for that case turns suffix
	 * compression off for the next key, we don't have to deal with it here.
	 */
	if (type != WT_PAGE_ROW_LEAF || !r->key_sfx_compress)
		return (__wt_buf_set(session, key, r->cur->data, r->cur->size));

	btree = S2BT(session);
	WT_RET(__wt_scr_alloc(session, 0, &update));

	/*
	 * Note #2: if we skipped updates, an update key may be larger than the
	 * last key stored in the previous block (probable for append-centric
	 * workloads).  If there are skipped updates, check for one larger than
	 * the last key and smaller than the current key.
	 */
	max = r->last;
	if (F_ISSET(r, WT_REC_UPDATE_RESTORE))
		for (i = r->supd_next; i > 0; --i) {
			supd = &r->supd[i - 1];
			if (supd->ins == NULL)
				WT_ERR(__wt_row_leaf_key(session,
				    r->page, supd->ripcip, update, false));
			else {
				update->data = WT_INSERT_KEY(supd->ins);
				update->size = WT_INSERT_KEY_SIZE(supd->ins);
			}

			/* Compare against the current key, it must be less. */
			WT_ERR(__wt_compare(
			    session, btree->collator, update, r->cur, &cmp));
			if (cmp >= 0)
				continue;

			/* Compare against the last key, it must be greater. */
			WT_ERR(__wt_compare(
			    session, btree->collator, update, r->last, &cmp));
			if (cmp >= 0)
				max = update;

			/*
			 * The saved updates are in key-sort order so the entry
			 * we're looking for is either the last or the next-to-
			 * last one in the list.  Once we've compared an entry
			 * against the last key on the page, we're done.
			 */
			break;
		}

	/*
	 * The largest key on the last block must sort before the current key,
	 * so we'll either find a larger byte value in the current key, or the
	 * current key will be a longer key, and the interesting byte is one
	 * past the length of the shorter key.
	 */
	pa = max->data;
	pb = r->cur->data;
	len = WT_MIN(max->size, r->cur->size);
	size = len + 1;
	for (cnt = 1; len > 0; ++cnt, --len, ++pa, ++pb)
		if (*pa != *pb) {
			if (size != cnt) {
				WT_STAT_DATA_INCRV(session,
				    rec_suffix_compression, size - cnt);
				size = cnt;
			}
			break;
		}
	ret = __wt_buf_set(session, key, r->cur->data, size);

err:	__wt_scr_free(session, &update);
	return (ret);
}

/*
 * __rec_split_grow --
 *	Grow the split buffer.
 */
static int
__rec_split_grow(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t add_len)
{
	WT_BM *bm;
	WT_BTREE *btree;
	size_t corrected_page_size, inuse;

	btree = S2BT(session);
	bm = btree->bm;

	inuse = WT_PTRDIFF(r->first_free, r->cur_ptr->image.mem);
	corrected_page_size = inuse + add_len;

	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_grow(session, &r->cur_ptr->image, corrected_page_size));

	r->first_free = (uint8_t *)r->cur_ptr->image.mem + inuse;
	WT_ASSERT(session, corrected_page_size >= inuse);
	r->space_avail = corrected_page_size - inuse;
	WT_ASSERT(session, r->space_avail >= add_len);

	return (0);
}

/*
 * __rec_split --
 *	Handle the page reconciliation bookkeeping.  (Did you know "bookkeeper"
 *	has 3 doubled letters in a row?  Sweet-tooth does, too.)
 */
static int
__rec_split(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
{
	WT_BTREE *btree;
	WT_CHUNK *tmp;
	size_t inuse;

	btree = S2BT(session);

	/* Fixed length col store can call with next_len 0 */
	WT_ASSERT(session, next_len == 0 || __rec_need_split(r, next_len));

	/*
	 * We should never split during salvage, and we're about to drop core
	 * because there's no parent page.
	 */
	if (r->salvage != NULL)
		WT_PANIC_RET(session, WT_PANIC,
		    "%s page too large, attempted split during salvage",
		    __wt_page_type_string(r->page->type));

	inuse = WT_PTRDIFF(r->first_free, r->cur_ptr->image.mem);

	/*
	 * We can get here if the first key/value pair won't fit.
	 * Additionally, grow the buffer to contain the current item if we
	 * haven't already consumed a reasonable portion of a split chunk.
	 */
	if (inuse < r->split_size / 2 && !__rec_need_split(r, 0))
		goto done;

	/* All page boundaries reset the dictionary. */
	__rec_dictionary_reset(r);

	/* Set the number of entries and size for the just finished chunk. */
	r->cur_ptr->entries = r->entries;
	r->cur_ptr->image.size = inuse;

	/*
	 * In case of bulk load, write out chunks as we get them. Otherwise we
	 * keep two chunks in memory at a given time. So, if there is a previous
	 * chunk, write it out, making space in the buffer for the next chunk to
	 * be written.
	 */
	if (r->is_bulk_load)
		WT_RET(__rec_split_write(session, r, r->cur_ptr, NULL, false));
	else  {
		if (r->prev_ptr == NULL) {
			WT_RET(__rec_split_chunk_init(
			    session, r, &r->chunkB, r->cur_ptr->image.memsize));
			r->prev_ptr = &r->chunkB;
		} else
			WT_RET(__rec_split_write(
			    session, r, r->prev_ptr, NULL, false));

		/* Switch chunks. */
		tmp = r->prev_ptr;
		r->prev_ptr = r->cur_ptr;
		r->cur_ptr = tmp;
	}

	/* Initialize the next chunk. */
	WT_RET(__rec_split_chunk_init(session, r, r->cur_ptr, 0));

	/* Reset the element count and fix where free points. */
	r->entries = 0;
	r->first_free = WT_PAGE_HEADER_BYTE(btree, r->cur_ptr->image.mem);

	/*
	 * Set the space available to another split-size and minimum split-size
	 * chunk.
	 */
	r->space_avail = r->split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);
	r->min_space_avail =
	    r->min_split_size - WT_PAGE_HEADER_BYTE_SIZE(btree);

	/* Set the key for the chunk. */
	r->cur_ptr->recno = r->recno;
	if (btree->type == BTREE_ROW)
		WT_RET(__rec_split_row_promote(
		    session, r, &r->cur_ptr->key, r->page->type));

done:  	/*
	 * Overflow values can be larger than the maximum page size but still be
	 * "on-page". If the next key/value pair is larger than space available
	 * after a split has happened (in other words, larger than the maximum
	 * page size), create a page sized to hold that one key/value pair. This
	 * generally splits the page into key/value pairs before a large object,
	 * the object, and key/value pairs after the object. It's possible other
	 * key/value pairs will also be aggregated onto the bigger page before
	 * or after, if the page happens to hold them, but it won't necessarily
	 * happen that way.
	 */
	if (r->space_avail < next_len)
		WT_RET(__rec_split_grow(session, r, next_len));

	return (0);
}

/*
 * __rec_split_crossing_bnd --
 * 	Save the details for the minimum split size boundary or call for a
 * 	split.
 */
static inline int
__rec_split_crossing_bnd(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
{
	WT_BTREE *btree;
	size_t min_offset;

	WT_ASSERT(session, __rec_need_split(r, next_len));

	/*
	 * If crossing the minimum split size boundary, store the boundary
	 * details at the current location in the buffer. If we are crossing the
	 * split boundary at the same time, possible when the next record is
	 * large enough, just split at this point.
	 */
	if (WT_CROSSING_MIN_BND(r, next_len) &&
	    !WT_CROSSING_SPLIT_BND(r, next_len) && !__rec_need_split(r, 0)) {
		btree = S2BT(session);
		WT_ASSERT(session, r->cur_ptr->min_offset == 0);

		/*
		 * If the first record doesn't fit into the minimum split size,
		 * we end up here. Write the record without setting a boundary
		 * here. We will get the opportunity to setup a boundary before
		 * writing out the next record.
		 */
		if (r->entries == 0)
			return (0);

		min_offset = WT_PTRDIFF(r->first_free, r->cur_ptr->image.mem);
		r->cur_ptr->min_offset = min_offset;
		r->cur_ptr->min_entries = r->entries;
		r->cur_ptr->min_recno = r->recno;
		if (btree->type == BTREE_ROW)
			WT_RET(__rec_split_row_promote(
			    session, r, &r->cur_ptr->min_key, r->page->type));

		/* All page boundaries reset the dictionary. */
		__rec_dictionary_reset(r);

		return (0);
	}

	/* We are crossing a split boundary */
	return (__rec_split(session, r, next_len));
}

/*
 * __rec_split_raw --
 *	Raw compression.
 */
static int
__rec_split_raw(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, size_t next_len, bool no_more_rows)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_CHUNK *chunk, *next, *tmp;
	WT_COMPRESSOR *compressor;
	WT_DECL_RET;
	WT_ITEM *dst;
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	WT_SESSION *wt_session;
	size_t corrected_page_size, extra_skip, len, result_len;
	uint64_t recno;
	uint32_t entry, i, max_image_slot, result_slots, slots;
	uint8_t *next_start;
	bool compressed, last_block;

	wt_session = (WT_SESSION *)session;
	btree = S2BT(session);
	bm = btree->bm;

	unpack = &_unpack;
	compressor = btree->compressor;
	dst = &r->raw_destination;
	page = r->page;
	compressed = false;

	chunk = r->cur_ptr;
	if (r->prev_ptr == NULL)
		r->prev_ptr = &r->chunkB;
	next = r->prev_ptr;

	/*
	 * We can get here if the first key/value pair won't fit.
	 */
	if (r->entries == 0 && !__rec_need_split(r, 0))
		goto split_grow;

	/*
	 * Build arrays of offsets and cumulative counts of cells and rows in
	 * the page: the offset is the byte offset to the possible split-point
	 * (adjusted for an initial chunk that cannot be compressed), entries
	 * is the cumulative page entries covered by the byte offset, recnos is
	 * the cumulative rows covered by the byte offset. Allocate to handle
	 * both column- and row-store regardless of this page type, structures
	 * are potentially reused for subsequent reconciliations of different
	 * page types.
	 */
	if (r->entries >= r->raw_max_slots) {
		__wt_free(session, r->raw_entries);
		__wt_free(session, r->raw_offsets);
		__wt_free(session, r->raw_recnos);
		r->raw_max_slots = 0;

		i = r->entries + 100;
		WT_RET(__wt_calloc_def(session, i, &r->raw_entries));
		WT_RET(__wt_calloc_def(session, i, &r->raw_offsets));
		WT_RET(__wt_calloc_def(session, i, &r->raw_recnos));
		r->raw_max_slots = i;
	}

	/*
	 * Walk the disk image looking for places where we can split it, which
	 * requires setting the number of entries.
	 */
	dsk = chunk->image.mem;
	dsk->u.entries = r->entries;

	/*
	 * We track the record number at each column-store split point, set an
	 * initial value.
	 */
	recno = WT_RECNO_OOB;
	if (page->type == WT_PAGE_COL_VAR)
		recno = chunk->recno;

	entry = max_image_slot = slots = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++entry;

		/*
		 * Row-store pages can split at keys, but not at values,
		 * column-store pages can split at values.
		 */
		__wt_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
		case WT_CELL_KEY_SHORT:
			break;
		case WT_CELL_ADDR_DEL:
		case WT_CELL_ADDR_INT:
		case WT_CELL_ADDR_LEAF:
		case WT_CELL_ADDR_LEAF_NO:
		case WT_CELL_DEL:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
		case WT_CELL_VALUE_SHORT:
			if (page->type == WT_PAGE_COL_INT) {
				recno = unpack->v;
				break;
			}
			if (page->type == WT_PAGE_COL_VAR) {
				recno += __wt_cell_rle(unpack);
				break;
			}
			r->raw_entries[slots] = entry;
			continue;
		WT_ILLEGAL_VALUE(session);
		}

		/*
		 * We can't compress the first 64B of the block (it must be
		 * written without compression), and a possible split point
		 * may appear in that 64B; keep it simple, ignore the first
		 * allocation size of data, anybody splitting smaller than
		 * that (as calculated before compression), is doing it wrong.
		 */
		if ((len = WT_PTRDIFF(cell, dsk)) > btree->allocsize)
			r->raw_offsets[++slots] =
			    WT_STORE_SIZE(len - WT_BLOCK_COMPRESS_SKIP);

		if (page->type == WT_PAGE_COL_INT ||
		    page->type == WT_PAGE_COL_VAR)
			r->raw_recnos[slots] = recno;
		r->raw_entries[slots] = entry;

		/*
		 * Don't create an image so large that any future update will
		 * cause a split in memory.
		 */
		if (max_image_slot == 0 && len > (size_t)r->max_raw_page_size)
			max_image_slot = slots;
	}

	/*
	 * If we haven't managed to find at least one split point, we're done,
	 * don't bother calling the underlying compression function.
	 */
	if (slots == 0) {
		result_slots = 0;
		goto no_slots;
	}

	/* The slot at array's end is the total length of the data. */
	r->raw_offsets[++slots] =
	    WT_STORE_SIZE(WT_PTRDIFF(cell, dsk) - WT_BLOCK_COMPRESS_SKIP);

	/*
	 * Allocate a destination buffer. If there's a pre-size function, call
	 * it to determine the destination buffer's size, else the destination
	 * buffer is documented to be at least the source size. (We can't use
	 * the target page size, any single key/value could be larger than the
	 * page size. Don't bother figuring out a minimum, just use the source
	 * size.)
	 *
	 * The destination buffer needs to be large enough for the final block
	 * size, corrected for the requirements of the underlying block manager.
	 * If the final block size is 8KB, that's a multiple of 512B and so the
	 * underlying block manager is fine with it.  But... we don't control
	 * what the pre_size method returns us as a required size, and we don't
	 * want to document the compress_raw method has to skip bytes in the
	 * buffer because that's confusing, so do something more complicated.
	 * First, find out how much space the compress_raw function might need,
	 * either the value returned from pre_size, or the initial source size.
	 * Add the compress-skip bytes, and then correct that value for the
	 * underlying block manager. As a result, we have a destination buffer
	 * that's large enough when calling the compress_raw method, and there
	 * are bytes in the header just for us.
	 */
	if (compressor->pre_size == NULL)
		result_len = (size_t)r->raw_offsets[slots];
	else
		WT_RET(compressor->pre_size(compressor, wt_session,
		    (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP,
		    (size_t)r->raw_offsets[slots], &result_len));
	extra_skip = btree->kencryptor == NULL ? 0 :
	    btree->kencryptor->size_const + WT_ENCRYPT_LEN_SIZE;

	corrected_page_size = result_len + WT_BLOCK_COMPRESS_SKIP;
	WT_RET(bm->write_size(bm, session, &corrected_page_size));
	WT_RET(__wt_buf_init(session, dst, corrected_page_size));

	/*
	 * Copy the header bytes into the destination buffer, then call the
	 * compression function.
	 */
	memcpy(dst->mem, dsk, WT_BLOCK_COMPRESS_SKIP);
	ret = compressor->compress_raw(compressor, wt_session,
	    r->page_size_orig, btree->split_pct,
	    WT_BLOCK_COMPRESS_SKIP + extra_skip,
	    (uint8_t *)dsk + WT_BLOCK_COMPRESS_SKIP, r->raw_offsets,
	    max_image_slot == 0 ? slots : max_image_slot,
	    (uint8_t *)dst->mem + WT_BLOCK_COMPRESS_SKIP,
	    result_len,
	    no_more_rows || max_image_slot != 0,
	    &result_len, &result_slots);
	switch (ret) {
	case EAGAIN:
		/*
		 * The compression function wants more rows, accumulate and
		 * retry if possible.
		 *
		 * First, reset the resulting slots count, just in case the
		 * compression function modified it before giving up.
		 */
		result_slots = 0;

		/*
		 * If the image is too large and there are more rows to gather,
		 * act as if the compression engine gave up on this chunk of
		 * data. That doesn't make sense (we flagged the engine that we
		 * wouldn't give it any more rows, but it's a possible return).
		 */
		if (no_more_rows || max_image_slot == 0)
			break;
		/* FALLTHROUGH */
	case 0:
		/*
		 * If the compression function returned zero result slots, it's
		 * giving up and we write the original data.  (This is a pretty
		 * bad result: we've not done compression on a block much larger
		 * than the maximum page size, but once compression gives up,
		 * there's not much else we can do.)
		 *
		 * If the compression function returned non-zero result slots,
		 * we were successful and have a block to write.
		 */
		if (result_slots == 0) {
			WT_STAT_DATA_INCR(session, compress_raw_fail);

			/*
			 * If there are no more rows, we can write the original
			 * data from the original buffer, else take all but the
			 * last row of the original data (the last row has to be
			 * set as the key for the next block).
			 */
			if (!no_more_rows)
				result_slots = slots - 1;
		} else {
			WT_STAT_DATA_INCR(session, compress_raw_ok);

			/*
			 * If there are more rows and the compression function
			 * consumed all of the current data, there are problems:
			 * First, with row-store objects, we're potentially
			 * skipping updates, we must have a key for the next
			 * block so we know with what block a skipped update is
			 * associated.  Second, if the compression function
			 * compressed all of the data, we're not pushing it
			 * hard enough (unless we got lucky and gave it exactly
			 * the right amount to work with, which is unlikely).
			 * Handle both problems by accumulating more data any
			 * time we're not writing the last block and compression
			 * ate all of the rows.
			 */
			if (result_slots == slots && !no_more_rows)
				result_slots = 0;
			else {
				/*
				 * Finalize the compressed disk image's
				 * information.
				 */
				dst->size = result_len + WT_BLOCK_COMPRESS_SKIP;

				compressed = true;
			}
		}
		break;
	default:
		return (ret);
	}

no_slots:
	/*
	 * Check for the last block we're going to write: if no more rows and
	 * we failed to compress anything, or we compressed everything, it's
	 * the last block.
	 */
	last_block = no_more_rows &&
	    (result_slots == 0 || result_slots == slots);

	if (!last_block && result_slots != 0) {
		/*
		 * Writing the current (possibly compressed), chunk.
		 * Finalize the current chunk's information.
		 */
		chunk->image.size = (size_t)
		    r->raw_offsets[result_slots] + WT_BLOCK_COMPRESS_SKIP;
		chunk->entries = r->raw_entries[result_slots - 1];

		/* Move any remnant to the next chunk. */
		len = WT_PTRDIFF(r->first_free,
		    (uint8_t *)dsk + chunk->image.size);
		WT_ASSERT(session, len > 0);
		WT_RET(__rec_split_chunk_init(
		    session, r, next, chunk->image.memsize));
		next_start = WT_PAGE_HEADER_BYTE(btree, next->image.mem);
		(void)memcpy(next_start, r->first_free - len, len);

		/* Set the key for the next chunk. */
		switch (page->type) {
		case WT_PAGE_COL_INT:
			next->recno = r->raw_recnos[result_slots];
			break;
		case WT_PAGE_COL_VAR:
			next->recno = r->raw_recnos[result_slots - 1];
			break;
		case WT_PAGE_ROW_INT:
		case WT_PAGE_ROW_LEAF:
			next->recno = WT_RECNO_OOB;
			/*
			 * Confirm there was uncompressed data remaining
			 * in the buffer, we're about to read it for the
			 * next chunk's initial key.
			 */
			WT_RET(__rec_split_row_promote_cell(
			    session, r, next->image.mem, &next->key));
			break;
		}

		/* Update the tracking information. */
		r->entries -= r->raw_entries[result_slots - 1];
		r->first_free = next_start + len;
		r->space_avail += r->raw_offsets[result_slots];
		WT_ASSERT(session, r->first_free + r->space_avail <=
		    (uint8_t *)next->image.mem + next->image.memsize);
	} else if (no_more_rows) {
		/*
		 * No more rows to accumulate, writing the entire chunk.
		 * Finalize the current chunk's information.
		 */
		chunk->image.size = WT_PTRDIFF32(r->first_free, dsk);
		chunk->entries = r->entries;

		/* Clear the tracking information. */
		r->entries = 0;
		r->first_free = NULL;
		r->space_avail = 0;
	} else {
		/*
		 * Compression failed, there are more rows to accumulate and the
		 * compression function wants to try again; increase the size of
		 * the "page" and try again after we accumulate some more rows.
		 */
		WT_STAT_DATA_INCR(session, compress_raw_fail_temporary);
		goto split_grow;
	}

	/* Write the chunk. */
	WT_RET(__rec_split_write(session, r,
	    r->cur_ptr, compressed ? dst : NULL, last_block));

	/* Switch chunks. */
	tmp = r->prev_ptr;
	r->prev_ptr = r->cur_ptr;
	r->cur_ptr = tmp;

	/*
	 * We got called because there wasn't enough room in the buffer for the
	 * next key and we might or might not have written a block. In any case,
	 * make sure the next key fits into the buffer.
	 */
	if (r->space_avail < next_len) {
split_grow:	/*
		 * Double the page size and make sure we accommodate at least
		 * one more record. The reason for the latter is that we may
		 * be here because there's a large key/value pair that won't
		 * fit in our initial page buffer, even at its expanded size.
		 */
		r->page_size *= 2;
		return (__rec_split_grow(session, r, r->page_size + next_len));
	}
	return (0);
}

/*
 * __rec_split_finish_process_prev --
 * 	If the two split chunks together fit in a single page, merge them into
 * 	one. If they do not fit in a single page but the last is smaller than
 * 	the minimum desired, move some data from the penultimate chunk to the
 * 	last chunk and write out the previous/penultimate. Finally, update the
 * 	pointer to the current image buffer.  After this function exits, we will
 * 	have one (last) buffer in memory, pointed to by the current image
 * 	pointer.
 */
static int
__rec_split_finish_process_prev(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BTREE *btree;
	WT_CHUNK *cur_ptr, *prev_ptr, *tmp;
	WT_PAGE_HEADER *dsk;
	size_t combined_size, len_to_move;
	uint8_t *cur_dsk_start;

	WT_ASSERT(session, r->prev_ptr != NULL);

	btree = S2BT(session);
	cur_ptr = r->cur_ptr;
	prev_ptr = r->prev_ptr;

	/*
	 * The sizes in the chunk include the header, so when calculating the
	 * combined size, be sure not to include the header twice.
	 */
	combined_size = prev_ptr->image.size +
	    (cur_ptr->image.size - WT_PAGE_HEADER_BYTE_SIZE(btree));

	if (combined_size <= r->page_size) {
		/*
		 * We have two boundaries, but the data in the buffers can fit a
		 * single page. Merge the boundaries and create a single chunk.
		 */
		dsk = r->cur_ptr->image.mem;
		memcpy((uint8_t *)r->prev_ptr->image.mem + prev_ptr->image.size,
		    WT_PAGE_HEADER_BYTE(btree, dsk),
		    cur_ptr->image.size - WT_PAGE_HEADER_BYTE_SIZE(btree));
		prev_ptr->image.size = combined_size;
		prev_ptr->entries += cur_ptr->entries;

		/*
		 * At this point, there is only one disk image in the memory,
		 * the previous chunk. Update the current chunk to that chunk,
		 * discard the unused chunk.
		 */
		tmp = r->prev_ptr;
		r->prev_ptr = r->cur_ptr;
		r->cur_ptr = tmp;
		return (__rec_split_chunk_init(session, r, r->prev_ptr, 0));
	}

	if (prev_ptr->min_offset != 0 &&
	    cur_ptr->image.size < r->min_split_size) {
		/*
		 * The last chunk, pointed to by the current image pointer, has
		 * less than the minimum data. Let's move any data more than the
		 * minimum from the previous image into the current.
		 */
		len_to_move = prev_ptr->image.size - prev_ptr->min_offset;
		/* Grow current buffer if it is not large enough */
		if (r->space_avail < len_to_move)
			WT_RET(__rec_split_grow(session, r, len_to_move));
		cur_dsk_start =
		    WT_PAGE_HEADER_BYTE(btree, r->cur_ptr->image.mem);

		/*
		 * Shift the contents of the current buffer to make space for
		 * the data that will be prepended into the current buffer.
		 * Copy the data from the previous buffer to the start of the
		 * current.
		 */
		memmove(cur_dsk_start + len_to_move, cur_dsk_start,
		    cur_ptr->image.size - WT_PAGE_HEADER_BYTE_SIZE(btree));
		memcpy(cur_dsk_start,
		    (uint8_t *)r->prev_ptr->image.mem + prev_ptr->min_offset,
		    len_to_move);

		/* Update boundary information */
		cur_ptr->image.size += len_to_move;
		prev_ptr->image.size -= len_to_move;
		cur_ptr->entries += prev_ptr->entries - prev_ptr->min_entries;
		prev_ptr->entries = prev_ptr->min_entries;
		cur_ptr->recno = prev_ptr->min_recno;
		WT_RET(__wt_buf_set(session, &cur_ptr->key,
		    prev_ptr->min_key.data, prev_ptr->min_key.size));
	}

	/* Write out the previous image */
	return (__rec_split_write(session, r, r->prev_ptr, NULL, false));
}

/*
 * __rec_split_finish --
 *	Finish processing a page.
 */
static int
__rec_split_finish(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BTREE *btree;
	size_t data_size;

	btree = S2BT(session);

	/*
	 * We're done reconciling, write the final page. Call raw compression
	 * until/unless there's not enough data to compress.
	 */
	if (r->entries != 0 && r->raw_compression) {
		while (r->entries != 0) {
			data_size =
			    WT_PTRDIFF(r->first_free, r->cur_ptr->image.mem);
			if (data_size <= btree->allocsize)
				break;
			WT_RET(__rec_split_raw(session, r, 0, true));
		}
		if (r->entries == 0)
			return (0);
	}

	/*
	 * We may arrive here with no entries to write if the page was entirely
	 * empty or if nothing on the page was visible to us.
	 *
	 * Pages with skipped or not-yet-globally visible updates aren't really
	 * empty; otherwise, the page is truly empty and we will merge it into
	 * its parent during the parent's reconciliation.
	 */
	if (r->entries == 0 && r->supd_next == 0)
		return (0);

	/* Set the number of entries and size for the just finished chunk. */
	r->cur_ptr->image.size =
	    WT_PTRDIFF32(r->first_free, r->cur_ptr->image.mem);
	r->cur_ptr->entries = r->entries;

	/* If not raw compression, potentially reconsider a previous chunk. */
	if (!r->raw_compression && r->prev_ptr != NULL)
		WT_RET(__rec_split_finish_process_prev(session, r));

	/* Write the remaining data/last page. */
	return (__rec_split_write(session, r, r->cur_ptr, NULL, true));
}

/*
 * __rec_supd_move --
 *	Move a saved WT_UPDATE list from the per-page cache to a specific
 *	block's list.
 */
static int
__rec_supd_move(
    WT_SESSION_IMPL *session, WT_MULTI *multi, WT_SAVE_UPD *supd, uint32_t n)
{
	uint32_t i;

	WT_RET(__wt_calloc_def(session, n, &multi->supd));

	for (i = 0; i < n; ++i)
		multi->supd[i] = *supd++;
	multi->supd_entries = n;
	return (0);
}

/*
 * __rec_split_write_supd --
 *	Check if we've saved updates that belong to this block, and move any
 *	to the per-block structure.
 */
static int
__rec_split_write_supd(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_CHUNK *chunk, WT_MULTI *multi, bool last_block)
{
	WT_BTREE *btree;
	WT_CHUNK *next;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_SAVE_UPD *supd;
	WT_UPDATE *upd;
	uint32_t i, j;
	int cmp;

	/*
	 * Check if we've saved updates that belong to this block, and move
	 * any to the per-block structure.
	 *
	 * This code requires a key be filled in for the next block (or the
	 * last block flag be set, if there's no next block).
	 *
	 * The last block gets all remaining saved updates.
	 */
	if (last_block) {
		WT_RET(__rec_supd_move(session, multi, r->supd, r->supd_next));
		r->supd_next = 0;
		r->supd_memsize = 0;
		goto done;
	}

	/*
	 * Get the saved update's key and compare it with the block's key range.
	 * If the saved update list belongs with the block we're about to write,
	 * move it to the per-block memory. Check only to the first update that
	 * doesn't go with the block, they must be in sorted order.
	 *
	 * The other chunk will have the key for the next page, that's what we
	 * compare against.
	 */
	next = chunk == r->cur_ptr ? r->prev_ptr : r->cur_ptr;
	page = r->page;
	if (page->type == WT_PAGE_ROW_LEAF) {
		btree = S2BT(session);
		WT_RET(__wt_scr_alloc(session, 0, &key));

		for (i = 0, supd = r->supd; i < r->supd_next; ++i, ++supd) {
			if (supd->ins == NULL)
				WT_ERR(__wt_row_leaf_key(
				    session, page, supd->ripcip, key, false));
			else {
				key->data = WT_INSERT_KEY(supd->ins);
				key->size = WT_INSERT_KEY_SIZE(supd->ins);
			}
			WT_ERR(__wt_compare(session,
			    btree->collator, key, &next->key, &cmp));
			if (cmp >= 0)
				break;
		}
	} else
		for (i = 0, supd = r->supd; i < r->supd_next; ++i, ++supd)
			if (WT_INSERT_RECNO(supd->ins) >= next->recno)
				break;
	if (i != 0) {
		WT_ERR(__rec_supd_move(session, multi, r->supd, i));

		/*
		 * If there are updates that weren't moved to the block, shuffle
		 * them to the beginning of the cached list (we maintain the
		 * saved updates in sorted order, new saved updates must be
		 * appended to the list).
		 */
		r->supd_memsize = 0;
		for (j = 0; i < r->supd_next; ++j, ++i) {
			/* Account for the remaining update memory. */
			if (r->supd[i].ins == NULL)
				upd = page->modify->mod_row_update[
				    page->type == WT_PAGE_ROW_LEAF ?
				    WT_ROW_SLOT(page, r->supd[i].ripcip) :
				    WT_COL_SLOT(page, r->supd[i].ripcip)];
			else
				upd = r->supd[i].ins->upd;
			r->supd_memsize += __wt_update_list_memsize(upd);
			r->supd[j] = r->supd[i];
		}
		r->supd_next = j;
	}

done:	/* Track the oldest timestamp seen so far. */
	multi->page_las.las_skew_newest = r->las_skew_newest;
	multi->page_las.las_max_txn = r->max_txn;
	WT_ASSERT(session, r->max_txn != WT_TXN_NONE);
#ifdef HAVE_TIMESTAMPS
	__wt_timestamp_set(
	    &multi->page_las.min_timestamp, &r->min_saved_timestamp);
	__wt_timestamp_set(
	    &multi->page_las.onpage_timestamp, &r->max_onpage_timestamp);
#endif

err:	__wt_scr_free(session, &key);
	return (ret);
}

/*
 * __rec_split_write_header --
 *	Initialize a disk page's header.
 */
static void
__rec_split_write_header(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_CHUNK *chunk, WT_MULTI *multi, WT_PAGE_HEADER *dsk)
{
	WT_BTREE *btree;
	WT_PAGE *page;

	btree = S2BT(session);
	page = r->page;

	dsk->recno = btree->type == BTREE_ROW ? WT_RECNO_OOB : multi->key.recno;
	dsk->write_gen = 0;
	dsk->mem_size = multi->size;
	dsk->u.entries = chunk->entries;
	dsk->type = page->type;

	/* Set the zero-length value flag in the page header. */
	if (page->type == WT_PAGE_ROW_LEAF) {
		F_CLR(dsk, WT_PAGE_EMPTY_V_ALL | WT_PAGE_EMPTY_V_NONE);

		if (chunk->entries != 0 && r->all_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_ALL);
		if (chunk->entries != 0 && !r->any_empty_value)
			F_SET(dsk, WT_PAGE_EMPTY_V_NONE);
	}

	/*
	 * Note in the page header if using the lookaside table eviction path
	 * and we found updates that weren't globally visible when reconciling
	 * this page.
	 */
	if (F_ISSET(r, WT_REC_LOOKASIDE) && multi->supd != NULL)
		F_SET(dsk, WT_PAGE_LAS_UPDATE);

	dsk->unused[0] = dsk->unused[1] = 0;

	/*
	 * There are page header fields which need to be cleared for consistent
	 * checksums: specifically, the write generation and the memory owned by
	 * the block manager.
	 */
	memset(WT_BLOCK_HEADER_REF(dsk), 0, btree->block_header);
}

/*
 * __rec_split_write_reuse --
 *	Check if a previously written block can be reused.
 */
static bool
__rec_split_write_reuse(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_MULTI *multi, WT_ITEM *image, bool last_block)
{
	WT_MULTI *multi_match;
	WT_PAGE_MODIFY *mod;

	mod = r->page->modify;

	/*
	 * Don't bother calculating checksums for bulk loads, there's no reason
	 * to believe they'll be useful. Check because LSM does bulk-loads as
	 * part of normal operations and the check is cheap.
	 */
	if (r->is_bulk_load)
		return (false);

	/*
	 * Calculating the checksum is the expensive part, try to avoid it.
	 *
	 * Ignore the last block of any reconciliation. Pages are written in the
	 * same block order every time, so the last block written for a page is
	 * unlikely to match any previously written block or block written in
	 * the future, (absent a point-update earlier in the page which didn't
	 * change the size of the on-page object in any way).
	 */
	if (last_block)
		return (false);

	/*
	 * Quit if evicting with no previously written block to compare against.
	 * (In other words, if there's eviction pressure and the page was never
	 * written by a checkpoint, calculating a checksum is worthless.)
	 *
	 * Quit if evicting and a previous check failed, once there's a miss no
	 * future block will match.
	 */
	if (F_ISSET(r, WT_REC_EVICT)) {
		if (mod->rec_result != WT_PM_REC_MULTIBLOCK ||
		    mod->mod_multi_entries < r->multi_next)
			return (false);
		if (r->evict_matching_checksum_failed)
			return (false);
	}

	/* Calculate the checksum for this block. */
	multi->checksum = __wt_checksum(image->data, image->size);

	/*
	 * Don't check for a block match when writing blocks during compaction,
	 * the whole idea is to move those blocks. Check after calculating the
	 * checksum, we don't distinguish between pages written solely as part
	 * of the compaction and pages written at around the same time, and so
	 * there's a possibility the calculated checksum will be useful in the
	 * future.
	 */
	if (session->compact_state != WT_COMPACT_NONE)
		return (false);

	/*
	 * Pages are written in the same block order every time, only check the
	 * appropriate slot.
	 */
	if (mod->rec_result != WT_PM_REC_MULTIBLOCK ||
	    mod->mod_multi_entries < r->multi_next)
		return (false);

	multi_match = &mod->mod_multi[r->multi_next - 1];
	if (multi_match->size != multi->size ||
	    multi_match->checksum != multi->checksum) {
		r->evict_matching_checksum_failed = true;
		return (false);
	}

	multi_match->addr.reuse = 1;
	multi->addr = multi_match->addr;

	WT_STAT_DATA_INCR(session, rec_page_match);
	return (true);
}

/*
 * __rec_split_write --
 *	Write a disk block out for the split helper functions.
 */
static int
__rec_split_write(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_CHUNK *chunk, WT_ITEM *compressed_image, bool last_block)
{
	WT_BTREE *btree;
	WT_MULTI *multi;
	WT_PAGE *page;
	size_t addr_size;
	uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];
#ifdef HAVE_DIAGNOSTIC
	bool verify_image;
#endif

	btree = S2BT(session);
	page = r->page;
#ifdef HAVE_DIAGNOSTIC
	verify_image = true;
#endif

	/* Make sure there's enough room for another write. */
	WT_RET(__wt_realloc_def(
	    session, &r->multi_allocated, r->multi_next + 1, &r->multi));
	multi = &r->multi[r->multi_next++];

	/* Initialize the address (set the addr type for the parent). */
	switch (page->type) {
	case WT_PAGE_COL_FIX:
		multi->addr.type = WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_VAR:
	case WT_PAGE_ROW_LEAF:
		multi->addr.type =
		    r->ovfl_items ? WT_ADDR_LEAF : WT_ADDR_LEAF_NO;
		break;
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		multi->addr.type = WT_ADDR_INT;
		break;
	WT_ILLEGAL_VALUE(session);
	}
	multi->size = WT_STORE_SIZE(chunk->image.size);
	multi->checksum = 0;

	/* Set the key. */
	if (btree->type == BTREE_ROW)
		WT_RET(__wt_row_ikey_alloc(session, 0,
		    chunk->key.data, chunk->key.size, &multi->key.ikey));
	else
		multi->key.recno = chunk->recno;

	/* Check if there are saved updates that might belong to this block. */
	if (r->supd_next != 0)
		WT_RET(__rec_split_write_supd(
		    session, r, chunk, multi, last_block));

	/* Initialize the page header(s). */
	__rec_split_write_header(session, r, chunk, multi, chunk->image.mem);
	if (compressed_image != NULL)
		__rec_split_write_header(
		    session, r, chunk, multi, compressed_image->mem);

	/*
	 * If we are writing the whole page in our first/only attempt, it might
	 * be a checkpoint (checkpoints are only a single page, by definition).
	 * Checkpoints aren't written here, the wrapup functions do the write.
	 *
	 * Track the buffer with the image. (This is bad layering, but we can't
	 * write the image until the wrapup code, and we don't have a code path
	 * from here to there.)
	 */
	if (last_block &&
	    r->multi_next == 1 && __rec_is_checkpoint(session, r)) {
		WT_ASSERT(session, r->supd_next == 0);

		if (compressed_image == NULL)
			r->wrapup_checkpoint = &chunk->image;
		else {
			r->wrapup_checkpoint = compressed_image;
			r->wrapup_checkpoint_compressed = true;
		}
		return (0);
	}

	/*
	 * If configured for an in-memory database, we can't actually write it.
	 * Instead, we will re-instantiate the page using the disk image and
	 * any list of updates we skipped.
	 */
	if (F_ISSET(r, WT_REC_IN_MEMORY))
		goto copy_image;

	/*
	 * If there are saved updates, either doing update/restore eviction or
	 * lookaside eviction.
	 */
	if (multi->supd != NULL) {
		/*
		 * XXX
		 * If no entries were used, the page is empty and we can only
		 * restore eviction/restore or lookaside updates against
		 * empty row-store leaf pages, column-store modify attempts to
		 * allocate a zero-length array.
		 */
		if (r->page->type != WT_PAGE_ROW_LEAF && chunk->entries == 0)
			return (EBUSY);

		if (F_ISSET(r, WT_REC_LOOKASIDE)) {
			r->cache_write_lookaside = true;

			/*
			 * Lookaside eviction writes disk images, but if no
			 * entries were used, there's no disk image to write.
			 * There's no more work to do in this case, lookaside
			 * eviction doesn't copy disk images.
			 */
			if (chunk->entries == 0)
				return (0);
		} else {
			r->cache_write_restore = true;

			/*
			 * Update/restore never writes a disk image, but always
			 * copies a disk image.
			 */
			goto copy_image;
		}
	}

	/*
	 * If we wrote this block before, re-use it. Prefer a checksum of the
	 * compressed image. It's an identical test and should be faster.
	 */
	if (__rec_split_write_reuse(session, r, multi,
	    compressed_image == NULL ? &chunk->image : compressed_image,
	    last_block))
		goto copy_image;

	/* Write the disk image and get an address. */
	WT_RET(__wt_bt_write(session,
	    compressed_image == NULL ? &chunk->image : compressed_image,
	    addr, &addr_size, false, F_ISSET(r, WT_REC_CHECKPOINT),
	    compressed_image != NULL));
#ifdef HAVE_DIAGNOSTIC
	verify_image = false;
#endif
	WT_RET(__wt_memdup(session, addr, addr_size, &multi->addr.addr));
	multi->addr.size = (uint8_t)addr_size;

copy_image:
#ifdef HAVE_DIAGNOSTIC
	/*
	 * The I/O routines verify all disk images we write, but there are paths
	 * in reconciliation that don't do I/O. Verify those images, too.
	 */
	WT_ASSERT(session, verify_image == false ||
	    __wt_verify_dsk_image(session,
	    "[reconcile-image]", chunk->image.data, 0, true) == 0);
#endif

	/*
	 * If re-instantiating this page in memory (either because eviction
	 * wants to, or because we skipped updates to build the disk image),
	 * save a copy of the disk image.
	 */
	if (F_ISSET(r, WT_REC_SCRUB) ||
	    (F_ISSET(r, WT_REC_UPDATE_RESTORE) && multi->supd != NULL))
		WT_RET(__wt_memdup(session,
		    chunk->image.data, chunk->image.size, &multi->disk_image));

	return (0);
}

/*
 * __wt_bulk_init --
 *	Bulk insert initialization.
 */
int
__wt_bulk_init(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_PAGE_INDEX *pindex;
	WT_RECONCILE *r;
	uint64_t recno;

	btree = S2BT(session);

	/*
	 * Bulk-load is only permitted on newly created files, not any empty
	 * file -- see the checkpoint code for a discussion.
	 */
	if (!btree->original)
		WT_RET_MSG(session, EINVAL,
		    "bulk-load is only possible for newly created trees");

	/*
	 * Get a reference to the empty leaf page; we have exclusive access so
	 * we can take a copy of the page, confident the parent won't split.
	 */
	pindex = WT_INTL_INDEX_GET_SAFE(btree->root.page);
	cbulk->ref = pindex->index[0];
	cbulk->leaf = cbulk->ref->page;

	WT_RET(__rec_init(session, cbulk->ref, 0, NULL, &cbulk->reconcile));
	r = cbulk->reconcile;
	r->is_bulk_load = true;

	recno = btree->type == BTREE_ROW ? WT_RECNO_OOB : 1;

	return (__rec_split_init(
	    session, r, cbulk->leaf, recno, btree->maxleafpage));
}

/*
 * __wt_bulk_wrapup --
 *	Bulk insert cleanup.
 */
int
__wt_bulk_wrapup(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_PAGE *parent;
	WT_RECONCILE *r;

	btree = S2BT(session);
	if ((r = cbulk->reconcile) == NULL)
		return (0);

	switch (btree->type) {
	case BTREE_COL_FIX:
		if (cbulk->entry != 0)
			__rec_incr(session, r, cbulk->entry,
			    __bitstr_size(
			    (size_t)cbulk->entry * btree->bitcnt));
		break;
	case BTREE_COL_VAR:
		if (cbulk->rle != 0)
			WT_RET(__wt_bulk_insert_var(session, cbulk, false));
		break;
	case BTREE_ROW:
		break;
	}

	WT_RET(__rec_split_finish(session, r));
	WT_RET(__rec_write_wrapup(session, r, r->page));
	__rec_write_page_status(session, r);

	/* Mark the page's parent and the tree dirty. */
	parent = r->ref->home;
	WT_RET(__wt_page_modify_init(session, parent));
	__wt_page_modify_set(session, parent);

	__rec_cleanup(session, r);
	__rec_destroy(session, &cbulk->reconcile);

	return (0);
}

/*
 * __wt_bulk_insert_row --
 *	Row-store bulk insert.
 */
int
__wt_bulk_insert_row(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_KV *key, *val;
	WT_RECONCILE *r;
	bool ovfl_key;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	key = &r->k;
	val = &r->v;
	WT_RET(__rec_cell_build_leaf_key(session, r,	/* Build key cell */
	    cursor->key.data, cursor->key.size, &ovfl_key));
	WT_RET(__rec_cell_build_val(session, r,		/* Build value cell */
	    cursor->value.data, cursor->value.size, (uint64_t)0));

	/* Boundary: split or write the page. */
	if (r->raw_compression) {
		if (key->len + val->len > r->space_avail)
			WT_RET(__rec_split_raw(
			    session, r, key->len + val->len, false));
	} else
		if (WT_CROSSING_SPLIT_BND(r, key->len + val->len)) {
			/*
			 * Turn off prefix compression until a full key written
			 * to the new page, and (unless already working with an
			 * overflow key), rebuild the key without compression.
			 */
			if (r->key_pfx_compress_conf) {
				r->key_pfx_compress = false;
				if (!ovfl_key)
					WT_RET(__rec_cell_build_leaf_key(
					    session, r, NULL, 0, &ovfl_key));
			}
			WT_RET(__rec_split_crossing_bnd(
			    session, r, key->len + val->len));
		}

	/* Copy the key/value pair onto the page. */
	__rec_copy_incr(session, r, key);
	if (val->len == 0)
		r->any_empty_value = true;
	else {
		r->all_empty_value = false;
		if (btree->dictionary)
			WT_RET(__rec_dict_replace(session, r, 0, val));
		__rec_copy_incr(session, r, val);
	}

	/* Update compression state. */
	__rec_key_state_update(r, ovfl_key);

	return (0);
}

/*
 * __rec_col_fix_bulk_insert_split_check --
 *	Check if a bulk-loaded fixed-length column store page needs to split.
 */
static inline int
__rec_col_fix_bulk_insert_split_check(WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_RECONCILE *r;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cbulk->cbt.iface.session;
	r = cbulk->reconcile;
	btree = S2BT(session);

	if (cbulk->entry == cbulk->nrecs) {
		if (cbulk->entry != 0) {
			/*
			 * If everything didn't fit, update the counters and
			 * split.
			 *
			 * Boundary: split or write the page.
			 *
			 * No need to have a minimum split size boundary, all
			 * pages are filled 100% except the last, allowing it to
			 * grow in the future.
			 */
			__rec_incr(session, r, cbulk->entry,
			    __bitstr_size(
			    (size_t)cbulk->entry * btree->bitcnt));
			WT_RET(__rec_split(session, r, 0));
		}
		cbulk->entry = 0;
		cbulk->nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
	}
	return (0);
}

/*
 * __wt_bulk_insert_fix --
 *	Fixed-length column-store bulk insert.
 */
int
__wt_bulk_insert_fix(
    WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool deleted)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	WT_RET(__rec_col_fix_bulk_insert_split_check(cbulk));
	__bit_setv(r->first_free, cbulk->entry,
	    btree->bitcnt, deleted ? 0 : ((uint8_t *)cursor->value.data)[0]);
	++cbulk->entry;
	++r->recno;

	return (0);
}

/*
 * __wt_bulk_insert_fix_bitmap --
 *	Fixed-length column-store bulk insert.
 */
int
__wt_bulk_insert_fix_bitmap(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
	WT_BTREE *btree;
	WT_CURSOR *cursor;
	WT_RECONCILE *r;
	uint32_t entries, offset, page_entries, page_size;
	const uint8_t *data;

	r = cbulk->reconcile;
	btree = S2BT(session);
	cursor = &cbulk->cbt.iface;

	if (((r->recno - 1) * btree->bitcnt) & 0x7)
		WT_RET_MSG(session, EINVAL,
		    "Bulk bitmap load not aligned on a byte boundary");
	for (data = cursor->value.data,
	    entries = (uint32_t)cursor->value.size;
	    entries > 0;
	    entries -= page_entries, data += page_size) {
		WT_RET(__rec_col_fix_bulk_insert_split_check(cbulk));

		page_entries = WT_MIN(entries, cbulk->nrecs - cbulk->entry);
		page_size = __bitstr_size(page_entries * btree->bitcnt);
		offset = __bitstr_size(cbulk->entry * btree->bitcnt);
		memcpy(r->first_free + offset, data, page_size);
		cbulk->entry += page_entries;
		r->recno += page_entries;
	}
	return (0);
}

/*
 * __wt_bulk_insert_var --
 *	Variable-length column-store bulk insert.
 */
int
__wt_bulk_insert_var(
    WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool deleted)
{
	WT_BTREE *btree;
	WT_KV *val;
	WT_RECONCILE *r;

	r = cbulk->reconcile;
	btree = S2BT(session);

	val = &r->v;
	if (deleted) {
		val->cell_len = __wt_cell_pack_del(&val->cell, cbulk->rle);
		val->buf.data = NULL;
		val->buf.size = 0;
		val->len = val->cell_len;
	} else
		/*
		 * Store the bulk cursor's last buffer, not the current value,
		 * we're tracking duplicates, which means we want the previous
		 * value seen, not the current value.
		 */
		WT_RET(__rec_cell_build_val(session,
		    r, cbulk->last.data, cbulk->last.size, cbulk->rle));

	/* Boundary: split or write the page. */
	if (r->raw_compression) {
		if (val->len > r->space_avail)
			WT_RET(__rec_split_raw(session, r, val->len, false));
	} else
		if (WT_CROSSING_SPLIT_BND(r, val->len))
			WT_RET(__rec_split_crossing_bnd(session, r, val->len));

	/* Copy the value onto the page. */
	if (btree->dictionary)
		WT_RET(__rec_dict_replace(session, r, cbulk->rle, val));
	__rec_copy_incr(session, r, val);

	/* Update the starting record number in case we split. */
	r->recno += cbulk->rle;

	return (0);
}

/*
 * __rec_vtype --
 *	Return a value cell's address type.
 */
static inline u_int
__rec_vtype(WT_ADDR *addr)
{
	if (addr->type == WT_ADDR_INT)
		return (WT_CELL_ADDR_INT);
	if (addr->type == WT_ADDR_LEAF)
		return (WT_CELL_ADDR_LEAF);
	return (WT_CELL_ADDR_LEAF_NO);
}

/*
 * __rec_col_int --
 *	Reconcile a column-store internal page.
 */
static int
__rec_col_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_CHILD_STATE state;
	WT_DECL_RET;
	WT_KV *val;
	WT_PAGE *child, *page;
	WT_REF *ref;
	bool hazard;

	btree = S2BT(session);
	page = pageref->page;
	child = NULL;
	hazard = false;

	val = &r->v;
	vpack = &_vpack;

	WT_RET(__rec_split_init(
	    session, r, page, pageref->ref_recno, btree->maxintlpage));

	/* For each entry in the in-memory page... */
	WT_INTL_FOREACH_BEGIN(session, page, ref) {
		/* Update the starting record number in case we split. */
		r->recno = ref->ref_recno;

		/*
		 * Modified child.
		 * The page may be emptied or internally created during a split.
		 * Deleted/split pages are merged into the parent and discarded.
		 */
		WT_ERR(__rec_child_modify(session, r, ref, &hazard, &state));
		addr = NULL;
		child = ref->page;

		switch (state) {
		case WT_CHILD_IGNORE:
			/* Ignored child. */
			WT_CHILD_RELEASE_ERR(session, hazard, ref);
			continue;

		case WT_CHILD_MODIFIED:
			/*
			 * Modified child. Empty pages are merged into the
			 * parent and discarded.
			 */
			switch (child->modify->rec_result) {
			case WT_PM_REC_EMPTY:
				/*
				 * Column-store pages are almost never empty, as
				 * discarding a page would remove a chunk of the
				 * name space.  The exceptions are pages created
				 * when the tree is created, and never filled.
				 */
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_MULTIBLOCK:
				WT_ERR(__rec_col_merge(session, r, child));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_REPLACE:
				addr = &child->modify->mod_replace;
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			break;
		case WT_CHILD_ORIGINAL:
			/* Original child. */
			break;
		case WT_CHILD_PROXY:
			/*
			 * Deleted child where we write a proxy cell, not
			 * yet supported for column-store.
			 */
			ret = __wt_illegal_value(session, NULL);
			goto err;
		}

		/*
		 * Build the value cell.  The child page address is in one of 3
		 * places: if the page was replaced, the page's modify structure
		 * references it and we built the value cell just above in the
		 * switch statement.  Else, the WT_REF->addr reference points to
		 * an on-page cell or an off-page WT_ADDR structure: if it's an
		 * on-page cell and we copy it from the page, else build a new
		 * cell.
		 */
		if (addr == NULL && __wt_off_page(page, ref->addr))
			addr = ref->addr;
		if (addr == NULL) {
			__wt_cell_unpack(ref->addr, vpack);
			val->buf.data = ref->addr;
			val->buf.size = __wt_cell_total_len(vpack);
			val->cell_len = 0;
			val->len = val->buf.size;
		} else
			__rec_cell_build_addr(session, r,
			    addr->addr, addr->size,
			    __rec_vtype(addr), ref->ref_recno);
		WT_CHILD_RELEASE_ERR(session, hazard, ref);

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, val->len)) {
			if (r->raw_compression)
				WT_ERR(__rec_split_raw(
				    session, r, val->len, false));
			else
				WT_ERR(__rec_split_crossing_bnd(
				    session, r, val->len));
		}

		/* Copy the value onto the page. */
		__rec_copy_incr(session, r, val);
	} WT_INTL_FOREACH_END;

	/* Write the remnant page. */
	return (__rec_split_finish(session, r));

err:	WT_CHILD_RELEASE(session, hazard, ref);
	return (ret);
}

/*
 * __rec_col_merge --
 *	Merge in a split page.
 */
static int
__rec_col_merge(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_KV *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	mod = page->modify;

	val = &r->v;

	/* For each entry in the split array... */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		/* Update the starting record number in case we split. */
		r->recno = multi->key.recno;

		/* Build the value cell. */
		addr = &multi->addr;
		__rec_cell_build_addr(session, r,
		    addr->addr, addr->size, __rec_vtype(addr), r->recno);

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, val->len)) {
			if (r->raw_compression)
				WT_RET(__rec_split_raw(
				    session, r, val->len, false));
			else
				WT_RET(__rec_split_crossing_bnd(
				    session, r, val->len));
		}

		/* Copy the value onto the page. */
		__rec_copy_incr(session, r, val);
	}
	return (0);
}

/*
 * __rec_col_fix --
 *	Reconcile a fixed-width, column-store leaf page.
 */
static int
__rec_col_fix(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref)
{
	WT_BTREE *btree;
	WT_INSERT *ins;
	WT_PAGE *page;
	WT_UPDATE *upd;
	uint64_t recno;
	uint32_t entry, nrecs;

	btree = S2BT(session);
	page = pageref->page;

	WT_RET(__rec_split_init(
	    session, r, page, pageref->ref_recno, btree->maxleafpage));

	/* Copy the original, disk-image bytes into place. */
	memcpy(r->first_free, page->pg_fix_bitf,
	    __bitstr_size((size_t)page->entries * btree->bitcnt));

	/* Update any changes to the original on-page data items. */
	WT_SKIP_FOREACH(ins, WT_COL_UPDATE_SINGLE(page)) {
		WT_RET(__rec_txn_read(session, r, ins, NULL, NULL, NULL, &upd));
		if (upd != NULL)
			__bit_setv(r->first_free,
			    WT_INSERT_RECNO(ins) - pageref->ref_recno,
			    btree->bitcnt, *upd->data);
	}

	/* Calculate the number of entries per page remainder. */
	entry = page->entries;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail) - page->entries;
	r->recno += entry;

	/* Walk any append list. */
	for (ins =
	    WT_SKIP_FIRST(WT_COL_APPEND(page));; ins = WT_SKIP_NEXT(ins)) {
		if (ins == NULL) {
			/*
			 * If the page split, instantiate any missing records in
			 * the page's name space. (Imagine record 98 is
			 * transactionally visible, 99 wasn't created or is not
			 * yet visible, 100 is visible. Then the page splits and
			 * record 100 moves to another page. When we reconcile
			 * the original page, we write record 98, then we don't
			 * see record 99 for whatever reason. If we've moved
			 * record 100, we don't know to write a deleted record
			 * 99 on the page.)
			 *
			 * The record number recorded during the split is the
			 * first key on the split page, that is, one larger than
			 * the last key on this page, we have to decrement it.
			 */
			if ((recno =
			    page->modify->mod_col_split_recno) == WT_RECNO_OOB)
				break;
			recno -= 1;

			/*
			 * The following loop assumes records to write, and the
			 * previous key might have been visible.
			 */
			if (r->recno > recno)
				break;
			upd = NULL;
		} else {
			WT_RET(__rec_txn_read(
			    session, r, ins, NULL, NULL, NULL, &upd));
			recno = WT_INSERT_RECNO(ins);
		}
		for (;;) {
			/*
			 * The application may have inserted records which left
			 * gaps in the name space.
			 */
			for (;
			    nrecs > 0 && r->recno < recno;
			    --nrecs, ++entry, ++r->recno)
				__bit_setv(
				    r->first_free, entry, btree->bitcnt, 0);

			if (nrecs > 0) {
				__bit_setv(r->first_free, entry, btree->bitcnt,
				    upd == NULL ? 0 : *upd->data);
				--nrecs;
				++entry;
				++r->recno;
				break;
			}

			/*
			 * If everything didn't fit, update the counters and
			 * split.
			 *
			 * Boundary: split or write the page.
			 *
			 * No need to have a minimum split size boundary, all
			 * pages are filled 100% except the last, allowing it to
			 * grow in the future.
			 */
			__rec_incr(session, r, entry,
			    __bitstr_size((size_t)entry * btree->bitcnt));
			WT_RET(__rec_split(session, r, 0));

			/* Calculate the number of entries per page. */
			entry = 0;
			nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);
		}

		/*
		 * Execute this loop once without an insert item to catch any
		 * missing records due to a split, then quit.
		 */
		if (ins == NULL)
			break;
	}

	/* Update the counters. */
	__rec_incr(
	    session, r, entry, __bitstr_size((size_t)entry * btree->bitcnt));

	/* Write the remnant page. */
	return (__rec_split_finish(session, r));
}

/*
 * __rec_col_fix_slvg --
 *	Reconcile a fixed-width, column-store leaf page created during salvage.
 */
static int
__rec_col_fix_slvg(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_REF *pageref, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;
	WT_PAGE *page;
	uint64_t page_start, page_take;
	uint32_t entry, nrecs;

	btree = S2BT(session);
	page = pageref->page;

	/*
	 * !!!
	 * It's vanishingly unlikely and probably impossible for fixed-length
	 * column-store files to have overlapping key ranges.  It's possible
	 * for an entire key range to go missing (if a page is corrupted and
	 * lost), but because pages can't split, it shouldn't be possible to
	 * find pages where the key ranges overlap.  That said, we check for
	 * it during salvage and clean up after it here because it doesn't
	 * cost much and future column-store formats or operations might allow
	 * for fixed-length format ranges to overlap during salvage, and I
	 * don't want to have to retrofit the code later.
	 */
	WT_RET(__rec_split_init(
	    session, r, page, pageref->ref_recno, btree->maxleafpage));

	/* We may not be taking all of the entries on the original page. */
	page_take = salvage->take == 0 ? page->entries : salvage->take;
	page_start = salvage->skip == 0 ? 0 : salvage->skip;

	/* Calculate the number of entries per page. */
	entry = 0;
	nrecs = WT_FIX_BYTES_TO_ENTRIES(btree, r->space_avail);

	for (; nrecs > 0 && salvage->missing > 0;
	    --nrecs, --salvage->missing, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt, 0);

	for (; nrecs > 0 && page_take > 0;
	    --nrecs, --page_take, ++page_start, ++entry)
		__bit_setv(r->first_free, entry, btree->bitcnt,
		    __bit_getv(page->pg_fix_bitf,
			(uint32_t)page_start, btree->bitcnt));

	r->recno += entry;
	__rec_incr(session, r, entry,
	    __bitstr_size((size_t)entry * btree->bitcnt));

	/*
	 * We can't split during salvage -- if everything didn't fit, it's
	 * all gone wrong.
	 */
	if (salvage->missing != 0 || page_take != 0)
		WT_PANIC_RET(session, WT_PANIC,
		    "%s page too large, attempted split during salvage",
		    __wt_page_type_string(page->type));

	/* Write the page. */
	return (__rec_split_finish(session, r));
}

/*
 * __rec_col_var_helper --
 *	Create a column-store variable length record cell and write it onto a
 *	page.
 */
static int
__rec_col_var_helper(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    WT_SALVAGE_COOKIE *salvage,
    WT_ITEM *value, bool deleted, uint8_t overflow_type, uint64_t rle)
{
	WT_BTREE *btree;
	WT_KV *val;

	btree = S2BT(session);

	val = &r->v;

	/*
	 * Occasionally, salvage needs to discard records from the beginning or
	 * end of the page, and because the items may be part of a RLE cell, do
	 * the adjustments here. It's not a mistake we don't bother telling
	 * our caller we've handled all the records from the page we care about,
	 * and can quit processing the page: salvage is a rare operation and I
	 * don't want to complicate our caller's loop.
	 */
	if (salvage != NULL) {
		if (salvage->done)
			return (0);
		if (salvage->skip != 0) {
			if (rle <= salvage->skip) {
				salvage->skip -= rle;
				return (0);
			}
			rle -= salvage->skip;
			salvage->skip = 0;
		}
		if (salvage->take != 0) {
			if (rle <= salvage->take)
				salvage->take -= rle;
			else {
				rle = salvage->take;
				salvage->take = 0;
			}
			if (salvage->take == 0)
				salvage->done = true;
		}
	}

	if (deleted) {
		val->cell_len = __wt_cell_pack_del(&val->cell, rle);
		val->buf.data = NULL;
		val->buf.size = 0;
		val->len = val->cell_len;
	} else if (overflow_type) {
		val->cell_len = __wt_cell_pack_ovfl(
		    &val->cell, overflow_type, rle, value->size);
		val->buf.data = value->data;
		val->buf.size = value->size;
		val->len = val->cell_len + value->size;
	} else
		WT_RET(__rec_cell_build_val(
		    session, r, value->data, value->size, rle));

	/* Boundary: split or write the page. */
	if (__rec_need_split(r, val->len)) {
		if (r->raw_compression)
			WT_RET(__rec_split_raw(session, r, val->len, false));
		else
			WT_RET(__rec_split_crossing_bnd(session, r, val->len));
	}

	/* Copy the value onto the page. */
	if (!deleted && !overflow_type && btree->dictionary)
		WT_RET(__rec_dict_replace(session, r, rle, val));
	__rec_copy_incr(session, r, val);

	/* Update the starting record number in case we split. */
	r->recno += rle;

	return (0);
}

/*
 * __rec_col_var --
 *	Reconcile a variable-width column-store leaf page.
 */
static int
__rec_col_var(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_REF *pageref, WT_SALVAGE_COOKIE *salvage)
{
	enum { OVFL_IGNORE, OVFL_UNUSED, OVFL_USED } ovfl_state;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *vpack, _vpack;
	WT_COL *cip;
	WT_CURSOR_BTREE *cbt;
	WT_DECL_ITEM(orig);
	WT_DECL_RET;
	WT_INSERT *ins;
	WT_ITEM *last;
	WT_PAGE *page;
	WT_UPDATE *upd;
	uint64_t n, nrepeat, repeat_count, rle, skip, src_recno;
	uint32_t i, size;
	bool deleted, last_deleted, orig_deleted, update_no_copy;
	const void *data;

	btree = S2BT(session);
	page = pageref->page;
	last = r->last;
	vpack = &_vpack;
	cbt = &r->update_modify_cbt;

	WT_RET(__rec_split_init(
	    session, r, page, pageref->ref_recno, btree->maxleafpage));

	WT_RET(__wt_scr_alloc(session, 0, &orig));
	data = NULL;
	size = 0;
	upd = NULL;

	/*
	 * The salvage code may be calling us to reconcile a page where there
	 * were missing records in the column-store name space.  If taking the
	 * first record from on the page, it might be a deleted record, so we
	 * have to give the RLE code a chance to figure that out.  Else, if
	 * not taking the first record from the page, write a single element
	 * representing the missing records onto a new page.  (Don't pass the
	 * salvage cookie to our helper function in this case, we're handling
	 * one of the salvage cookie fields on our own, and we don't need the
	 * helper function's assistance.)
	 */
	rle = 0;
	last_deleted = false;
	if (salvage != NULL && salvage->missing != 0) {
		if (salvage->skip == 0) {
			rle = salvage->missing;
			last_deleted = true;

			/*
			 * Correct the number of records we're going to "take",
			 * pretending the missing records were on the page.
			 */
			salvage->take += salvage->missing;
		} else
			WT_ERR(__rec_col_var_helper(session,
			    r, NULL, NULL, true, false, salvage->missing));
	}

	/*
	 * We track two data items through this loop: the previous (last) item
	 * and the current item: if the last item is the same as the current
	 * item, we increment the RLE count for the last item; if the last item
	 * is different from the current item, we write the last item onto the
	 * page, and replace it with the current item.  The r->recno counter
	 * tracks records written to the page, and is incremented by the helper
	 * function immediately after writing records to the page.  The record
	 * number of our source record, that is, the current item, is maintained
	 * in src_recno.
	 */
	src_recno = r->recno + rle;

	/* For each entry in the in-memory page... */
	WT_COL_FOREACH(page, cip, i) {
		ovfl_state = OVFL_IGNORE;
		if ((cell = WT_COL_PTR(page, cip)) == NULL) {
			nrepeat = 1;
			ins = NULL;
			orig_deleted = true;
		} else {
			__wt_cell_unpack(cell, vpack);
			nrepeat = __wt_cell_rle(vpack);
			ins = WT_SKIP_FIRST(WT_COL_UPDATE(page, cip));

			/*
			 * If the original value is "deleted", there's no value
			 * to compare, we're done.
			 */
			orig_deleted = vpack->type == WT_CELL_DEL;
			if (orig_deleted)
				goto record_loop;

			/*
			 * Overflow items are tricky: we don't know until we're
			 * finished processing the set of values if we need the
			 * overflow value or not.  If we don't use the overflow
			 * item at all, we have to discard it from the backing
			 * file, otherwise we'll leak blocks on the checkpoint.
			 * That's safe because if the backing overflow value is
			 * still needed by any running transaction, we'll cache
			 * a copy in the update list.
			 *
			 * Regardless, we avoid copying in overflow records: if
			 * there's a WT_INSERT entry that modifies a reference
			 * counted overflow record, we may have to write copies
			 * of the overflow record, and in that case we'll do the
			 * comparisons, but we don't read overflow items just to
			 * see if they match records on either side.
			 */
			if (vpack->ovfl) {
				ovfl_state = OVFL_UNUSED;
				goto record_loop;
			}

			/*
			 * If data is Huffman encoded, we have to decode it in
			 * order to compare it with the last item we saw, which
			 * may have been an update string.  This guarantees we
			 * find every single pair of objects we can RLE encode,
			 * including applications updating an existing record
			 * where the new value happens (?) to match a Huffman-
			 * encoded value in a previous or next record.
			 */
			WT_ERR(__wt_dsk_cell_data_ref(
			    session, WT_PAGE_COL_VAR, vpack, orig));
		}

record_loop:	/*
		 * Generate on-page entries: loop repeat records, looking for
		 * WT_INSERT entries matching the record number.  The WT_INSERT
		 * lists are in sorted order, so only need check the next one.
		 */
		for (n = 0;
		    n < nrepeat; n += repeat_count, src_recno += repeat_count) {
			upd = NULL;
			if (ins != NULL && WT_INSERT_RECNO(ins) == src_recno) {
				WT_ERR(__rec_txn_read(
				    session, r, ins, cip, vpack, NULL, &upd));
				ins = WT_SKIP_NEXT(ins);
			}

			update_no_copy = true;	/* No data copy */
			repeat_count = 1;	/* Single record */
			deleted = false;

			if (upd != NULL) {
				switch (upd->type) {
				case WT_UPDATE_DELETED:
					deleted = true;
					break;
				case WT_UPDATE_MODIFIED:
					cbt->slot = WT_COL_SLOT(page, cip);
					WT_ERR(__wt_value_return_upd(
					    session, cbt, upd,
					    F_ISSET(r, WT_REC_VISIBLE_ALL)));
					data = cbt->iface.value.data;
					size = (uint32_t)cbt->iface.value.size;
					update_no_copy = false;
					break;
				case WT_UPDATE_STANDARD:
					data = upd->data;
					size = upd->size;
					break;
				WT_ILLEGAL_VALUE_ERR(session);
				}
			} else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				/*
				 * If doing an update save and restore, and the
				 * underlying value is a removed overflow value,
				 * we end up here.
				 *
				 * If necessary, when the overflow value was
				 * originally removed, reconciliation appended
				 * a globally visible copy of the value to the
				 * key's update list, meaning the on-page item
				 * isn't accessed after page re-instantiation.
				 *
				 * Assert the case.
				 */
				WT_ASSERT(session,
				    F_ISSET(r, WT_REC_UPDATE_RESTORE));

				/*
				 * The on-page value will never be accessed,
				 * write a placeholder record.
				 */
				data = "ovfl-unused";
				size = WT_STORE_SIZE(strlen("ovfl-unused"));
			} else {
				update_no_copy = false;	/* Maybe data copy */

				/*
				 * The repeat count is the number of records up
				 * to the next WT_INSERT record, or up to the
				 * end of the entry if we have no more WT_INSERT
				 * records.
				 */
				if (ins == NULL)
					repeat_count = nrepeat - n;
				else
					repeat_count =
					    WT_INSERT_RECNO(ins) - src_recno;

				deleted = orig_deleted;
				if (deleted)
					goto compare;

				/*
				 * If we are handling overflow items, use the
				 * overflow item itself exactly once, after
				 * which we have to copy it into a buffer and
				 * from then on use a complete copy because we
				 * are re-creating a new overflow record each
				 * time.
				 */
				switch (ovfl_state) {
				case OVFL_UNUSED:
					/*
					 * An as-yet-unused overflow item.
					 *
					 * We're going to copy the on-page cell,
					 * write out any record we're tracking.
					 */
					if (rle != 0) {
						WT_ERR(__rec_col_var_helper(
						    session, r, salvage, last,
						    last_deleted, 0, rle));
						rle = 0;
					}

					last->data = vpack->data;
					last->size = vpack->size;
					WT_ERR(__rec_col_var_helper(
					    session, r, salvage, last, false,
					    WT_CELL_VALUE_OVFL, repeat_count));

					/* Track if page has overflow items. */
					r->ovfl_items = true;

					ovfl_state = OVFL_USED;
					continue;
				case OVFL_USED:
					/*
					 * Original is an overflow item; we used
					 * it for a key and now we need another
					 * copy; read it into memory.
					 */
					WT_ERR(__wt_dsk_cell_data_ref(session,
					    WT_PAGE_COL_VAR, vpack, orig));

					ovfl_state = OVFL_IGNORE;
					/* FALLTHROUGH */
				case OVFL_IGNORE:
					/*
					 * Original is an overflow item and we
					 * were forced to copy it into memory,
					 * or the original wasn't an overflow
					 * item; use the data copied into orig.
					 */
					data = orig->data;
					size = (uint32_t)orig->size;
					break;
				}
			}

compare:		/*
			 * If we have a record against which to compare, and
			 * the records compare equal, increment the rle counter
			 * and continue.  If the records don't compare equal,
			 * output the last record and swap the last and current
			 * buffers: do NOT update the starting record number,
			 * we've been doing that all along.
			 */
			if (rle != 0) {
				if ((deleted && last_deleted) ||
				    (!last_deleted && !deleted &&
				    last->size == size &&
				    memcmp(last->data, data, size) == 0)) {
					rle += repeat_count;
					continue;
				}
				WT_ERR(__rec_col_var_helper(session, r,
				    salvage, last, last_deleted, 0, rle));
			}

			/*
			 * Swap the current/last state.
			 *
			 * Reset RLE counter and turn on comparisons.
			 */
			if (!deleted) {
				/*
				 * We can't simply assign the data values into
				 * the last buffer because they may have come
				 * from a copy built from an encoded/overflow
				 * cell and creating the next record is going
				 * to overwrite that memory.  Check, because
				 * encoded/overflow cells aren't that common
				 * and we'd like to avoid the copy.  If data
				 * was taken from the current unpack structure
				 * (which points into the page), or was taken
				 * from an update structure, we can just use
				 * the pointers, they're not moving.
				 */
				if (data == vpack->data || update_no_copy) {
					last->data = data;
					last->size = size;
				} else
					WT_ERR(__wt_buf_set(
					    session, last, data, size));
			}
			last_deleted = deleted;
			rle = repeat_count;
		}

		/*
		 * The first time we find an overflow record we never used,
		 * discard the underlying blocks, they're no longer useful.
		 */
		if (ovfl_state == OVFL_UNUSED &&
		    vpack->raw != WT_CELL_VALUE_OVFL_RM)
			WT_ERR(__wt_ovfl_remove(
			    session, page, vpack, F_ISSET(r, WT_REC_EVICT)));
	}

	/* Walk any append list. */
	for (ins =
	    WT_SKIP_FIRST(WT_COL_APPEND(page));; ins = WT_SKIP_NEXT(ins)) {
		if (ins == NULL) {
			/*
			 * If the page split, instantiate any missing records in
			 * the page's name space. (Imagine record 98 is
			 * transactionally visible, 99 wasn't created or is not
			 * yet visible, 100 is visible. Then the page splits and
			 * record 100 moves to another page. When we reconcile
			 * the original page, we write record 98, then we don't
			 * see record 99 for whatever reason. If we've moved
			 * record 100, we don't know to write a deleted record
			 * 99 on the page.)
			 *
			 * Assert the recorded record number is past the end of
			 * the page.
			 *
			 * The record number recorded during the split is the
			 * first key on the split page, that is, one larger than
			 * the last key on this page, we have to decrement it.
			 */
			if ((n = page->
			    modify->mod_col_split_recno) == WT_RECNO_OOB)
				break;
			WT_ASSERT(session, n >= src_recno);
			n -= 1;

			upd = NULL;
		} else {
			WT_ERR(__rec_txn_read(
			    session, r, ins, NULL, NULL, NULL, &upd));
			n = WT_INSERT_RECNO(ins);
		}
		while (src_recno <= n) {
			deleted = false;
			update_no_copy = true;

			/*
			 * The application may have inserted records which left
			 * gaps in the name space, and these gaps can be huge.
			 * If we're in a set of deleted records, skip the boring
			 * part.
			 */
			if (src_recno < n) {
				deleted = true;
				if (last_deleted) {
					/*
					 * The record adjustment is decremented
					 * by one so we can naturally fall into
					 * the RLE accounting below, where we
					 * increment rle by one, then continue
					 * in the outer loop, where we increment
					 * src_recno by one.
					 */
					skip = (n - src_recno) - 1;
					rle += skip;
					src_recno += skip;
				}
			} else if (upd == NULL)
				deleted = true;
			else
				switch (upd->type) {
				case WT_UPDATE_DELETED:
					deleted = true;
					break;
				case WT_UPDATE_MODIFIED:
					/*
					 * Impossible slot, there's no backing
					 * on-page item.
					 */
					cbt->slot = UINT32_MAX;
					WT_ERR(__wt_value_return_upd(
					    session, cbt, upd,
					    F_ISSET(r, WT_REC_VISIBLE_ALL)));
					data = cbt->iface.value.data;
					size = (uint32_t)cbt->iface.value.size;
					update_no_copy = false;
					break;
				case WT_UPDATE_STANDARD:
					data = upd->data;
					size = upd->size;
					break;
				WT_ILLEGAL_VALUE_ERR(session);
				}

			/*
			 * Handle RLE accounting and comparisons -- see comment
			 * above, this code fragment does the same thing.
			 */
			if (rle != 0) {
				if ((deleted && last_deleted) ||
				    (!last_deleted && !deleted &&
				    last->size == size &&
				    memcmp(last->data, data, size) == 0)) {
					++rle;
					goto next;
				}
				WT_ERR(__rec_col_var_helper(session, r,
				    salvage, last, last_deleted, 0, rle));
			}

			/*
			 * Swap the current/last state. We can't simply assign
			 * the data values into the last buffer because they may
			 * be a temporary copy built from a chain of modified
			 * updates and creating the next record will overwrite
			 * that memory. Check, we'd like to avoid the copy. If
			 * data was taken from an update structure, we can just
			 * use the pointers, they're not moving.
			 */
			if (!deleted) {
				if (update_no_copy) {
					last->data = data;
					last->size = size;
				} else
					WT_ERR(__wt_buf_set(
					    session, last, data, size));
			}

			/* Ready for the next loop, reset the RLE counter. */
			last_deleted = deleted;
			rle = 1;

			/*
			 * Move to the next record. It's not a simple increment
			 * because if it's the maximum record, incrementing it
			 * wraps to 0 and this turns into an infinite loop.
			 */
next:			if (src_recno == UINT64_MAX)
				break;
			++src_recno;
		}

		/*
		 * Execute this loop once without an insert item to catch any
		 * missing records due to a split, then quit.
		 */
		if (ins == NULL)
			break;
	}

	/* If we were tracking a record, write it. */
	if (rle != 0)
		WT_ERR(__rec_col_var_helper(
		    session, r, salvage, last, last_deleted, 0, rle));

	/* Write the remnant page. */
	ret = __rec_split_finish(session, r);

err:	__wt_scr_free(session, &orig);
	return (ret);
}

/*
 * __rec_row_int --
 *	Reconcile a row-store internal page.
 */
static int
__rec_row_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_CHILD_STATE state;
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_KV *key, *val;
	WT_PAGE *child;
	WT_REF *ref;
	size_t size;
	u_int vtype;
	bool hazard, key_onpage_ovfl, ovfl_key;
	const void *p;

	btree = S2BT(session);
	child = NULL;
	hazard = false;

	key = &r->k;
	kpack = &_kpack;
	WT_CLEAR(*kpack);	/* -Wuninitialized */
	val = &r->v;
	vpack = &_vpack;
	WT_CLEAR(*vpack);	/* -Wuninitialized */

	ikey = NULL;		/* -Wuninitialized */
	cell = NULL;
	key_onpage_ovfl = false;

	WT_RET(__rec_split_init(session, r, page, 0, btree->maxintlpage));

	/*
	 * Ideally, we'd never store the 0th key on row-store internal pages
	 * because it's never used during tree search and there's no reason
	 * to waste the space.  The problem is how we do splits: when we split,
	 * we've potentially picked out several "split points" in the buffer
	 * which is overflowing the maximum page size, and when the overflow
	 * happens, we go back and physically split the buffer, at those split
	 * points, into new pages.  It would be both difficult and expensive
	 * to re-process the 0th key at each split point to be an empty key,
	 * so we don't do that.  However, we are reconciling an internal page
	 * for whatever reason, and the 0th key is known to be useless.  We
	 * truncate the key to a single byte, instead of removing it entirely,
	 * it simplifies various things in other parts of the code (we don't
	 * have to special case transforming the page from its disk image to
	 * its in-memory version, for example).
	 */
	r->cell_zero = true;

	/* For each entry in the in-memory page... */
	WT_INTL_FOREACH_BEGIN(session, page, ref) {
		/*
		 * There are different paths if the key is an overflow item vs.
		 * a straight-forward on-page value. If an overflow item, we
		 * would have instantiated it, and we can use that fact to set
		 * things up.
		 *
		 * Note the cell reference and unpacked key cell are available
		 * only in the case of an instantiated, off-page key, we don't
		 * bother setting them if that's not possible.
		 */
		if (F_ISSET_ATOMIC(page, WT_PAGE_OVERFLOW_KEYS)) {
			cell = NULL;
			key_onpage_ovfl = false;
			ikey = __wt_ref_key_instantiated(ref);
			if (ikey != NULL && ikey->cell_offset != 0) {
				cell =
				    WT_PAGE_REF_OFFSET(page, ikey->cell_offset);
				__wt_cell_unpack(cell, kpack);
				key_onpage_ovfl = kpack->ovfl &&
				    kpack->raw != WT_CELL_KEY_OVFL_RM;
			}
		}

		WT_ERR(__rec_child_modify(session, r, ref, &hazard, &state));
		addr = ref->addr;
		child = ref->page;

		switch (state) {
		case WT_CHILD_IGNORE:
			/*
			 * Ignored child.
			 *
			 * Overflow keys referencing pages we're not writing are
			 * no longer useful, schedule them for discard.  Don't
			 * worry about instantiation, internal page keys are
			 * always instantiated.  Don't worry about reuse,
			 * reusing this key in this reconciliation is unlikely.
			 */
			if (key_onpage_ovfl)
				WT_ERR(__wt_ovfl_discard_add(
				    session, page, kpack->cell));
			WT_CHILD_RELEASE_ERR(session, hazard, ref);
			continue;

		case WT_CHILD_MODIFIED:
			/*
			 * Modified child.  Empty pages are merged into the
			 * parent and discarded.
			 */
			switch (child->modify->rec_result) {
			case WT_PM_REC_EMPTY:
				/*
				 * Overflow keys referencing empty pages are no
				 * longer useful, schedule them for discard.
				 * Don't worry about instantiation, internal
				 * page keys are always instantiated.  Don't
				 * worry about reuse, reusing this key in this
				 * reconciliation is unlikely.
				 */
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_MULTIBLOCK:
				/*
				 * Overflow keys referencing split pages are no
				 * longer useful (the split page's key is the
				 * interesting key); schedule them for discard.
				 * Don't worry about instantiation, internal
				 * page keys are always instantiated.  Don't
				 * worry about reuse, reusing this key in this
				 * reconciliation is unlikely.
				 */
				if (key_onpage_ovfl)
					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));

				WT_ERR(__rec_row_merge(session, r, child));
				WT_CHILD_RELEASE_ERR(session, hazard, ref);
				continue;
			case WT_PM_REC_REPLACE:
				/*
				 * If the page is replaced, the page's modify
				 * structure has the page's address.
				 */
				addr = &child->modify->mod_replace;
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
			break;
		case WT_CHILD_ORIGINAL:
			/* Original child. */
			break;
		case WT_CHILD_PROXY:
			/* Deleted child where we write a proxy cell. */
			break;
		}

		/*
		 * Build the value cell, the child page's address.  Addr points
		 * to an on-page cell or an off-page WT_ADDR structure. There's
		 * a special cell type in the case of page deletion requiring
		 * a proxy cell, otherwise use the information from the addr or
		 * original cell.
		 */
		if (__wt_off_page(page, addr)) {
			p = addr->addr;
			size = addr->size;
			vtype = state == WT_CHILD_PROXY ?
			    WT_CELL_ADDR_DEL : __rec_vtype(addr);
		} else {
			__wt_cell_unpack(ref->addr, vpack);
			p = vpack->data;
			size = vpack->size;
			vtype = state == WT_CHILD_PROXY ?
			    WT_CELL_ADDR_DEL : (u_int)vpack->raw;
		}
		__rec_cell_build_addr(session, r, p, size, vtype, WT_RECNO_OOB);
		WT_CHILD_RELEASE_ERR(session, hazard, ref);

		/*
		 * Build key cell.
		 * Truncate any 0th key, internal pages don't need 0th keys.
		 */
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = true;
		} else {
			__wt_ref_key(page, ref, &p, &size);
			WT_ERR(__rec_cell_build_int_key(
			    session, r, p, r->cell_zero ? 1 : size, &ovfl_key));
		}
		r->cell_zero = false;

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, key->len + val->len)) {
			if (r->raw_compression)
				WT_ERR(__rec_split_raw(
				    session, r, key->len + val->len, false));
			else {
				/*
				 * In one path above, we copied address blocks
				 * from the page rather than building the actual
				 * key.  In that case, we have to build the key
				 * now because we are about to promote it.
				 */
				if (key_onpage_ovfl) {
					WT_ERR(__wt_buf_set(session, r->cur,
					    WT_IKEY_DATA(ikey), ikey->size));
					key_onpage_ovfl = false;
				}

				WT_ERR(__rec_split_crossing_bnd(
				    session, r, key->len + val->len));
			}
		}

		/* Copy the key and value onto the page. */
		__rec_copy_incr(session, r, key);
		__rec_copy_incr(session, r, val);

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	} WT_INTL_FOREACH_END;

	/* Write the remnant page. */
	return (__rec_split_finish(session, r));

err:	WT_CHILD_RELEASE(session, hazard, ref);
	return (ret);
}

/*
 * __rec_row_merge --
 *	Merge in a split page.
 */
static int
__rec_row_merge(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_KV *key, *val;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;
	bool ovfl_key;

	mod = page->modify;

	key = &r->k;
	val = &r->v;

	/* For each entry in the split array... */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		/* Build the key and value cells. */
		WT_RET(__rec_cell_build_int_key(session, r,
		    WT_IKEY_DATA(multi->key.ikey),
		    r->cell_zero ? 1 : multi->key.ikey->size, &ovfl_key));
		r->cell_zero = false;

		addr = &multi->addr;
		__rec_cell_build_addr(session, r,
		    addr->addr, addr->size, __rec_vtype(addr), WT_RECNO_OOB);

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, key->len + val->len)) {
			if (r->raw_compression)
				WT_RET(__rec_split_raw(
				    session, r, key->len + val->len, false));
			else
				WT_RET(__rec_split_crossing_bnd(
				    session, r, key->len + val->len));
		}

		/* Copy the key and value onto the page. */
		__rec_copy_incr(session, r, key);
		__rec_copy_incr(session, r, val);

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	}
	return (0);
}

/*
 * __rec_row_leaf --
 *	Reconcile a row-store leaf page.
 */
static int
__rec_row_leaf(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_PAGE *page, WT_SALVAGE_COOKIE *salvage)
{
	WT_BTREE *btree;
	WT_CELL *cell, *val_cell;
	WT_CELL_UNPACK *kpack, _kpack, *vpack, _vpack;
	WT_CURSOR_BTREE *cbt;
	WT_DECL_ITEM(tmpkey);
	WT_DECL_ITEM(tmpval);
	WT_DECL_RET;
	WT_IKEY *ikey;
	WT_INSERT *ins;
	WT_KV *key, *val;
	WT_ROW *rip;
	WT_UPDATE *upd;
	size_t size;
	uint64_t slvg_skip;
	uint32_t i;
	bool dictionary, key_onpage_ovfl, ovfl_key;
	void *copy;
	const void *p;

	btree = S2BT(session);
	cbt = &r->update_modify_cbt;
	slvg_skip = salvage == NULL ? 0 : salvage->skip;

	key = &r->k;
	val = &r->v;

	WT_RET(__rec_split_init(session, r, page, 0, btree->maxleafpage));

	/*
	 * Write any K/V pairs inserted into the page before the first from-disk
	 * key on the page.
	 */
	if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT_SMALLEST(page))) != NULL)
		WT_RET(__rec_row_leaf_insert(session, r, ins));

	/*
	 * Temporary buffers in which to instantiate any uninstantiated keys
	 * or value items we need.
	 */
	WT_ERR(__wt_scr_alloc(session, 0, &tmpkey));
	WT_ERR(__wt_scr_alloc(session, 0, &tmpval));

	/* For each entry in the page... */
	WT_ROW_FOREACH(page, rip, i) {
		/*
		 * The salvage code, on some rare occasions, wants to reconcile
		 * a page but skip some leading records on the page.  Because
		 * the row-store leaf reconciliation function copies keys from
		 * the original disk page, this is non-trivial -- just changing
		 * the in-memory pointers isn't sufficient, we have to change
		 * the WT_CELL structures on the disk page, too.  It's ugly, but
		 * we pass in a value that tells us how many records to skip in
		 * this case.
		 */
		if (slvg_skip != 0) {
			--slvg_skip;
			continue;
		}

		/*
		 * Figure out the key: set any cell reference (and unpack it),
		 * set any instantiated key reference.
		 */
		copy = WT_ROW_KEY_COPY(rip);
		(void)__wt_row_leaf_key_info(
		    page, copy, &ikey, &cell, NULL, NULL);
		if (cell == NULL)
			kpack = NULL;
		else {
			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
		}

		/* Unpack the on-page value cell, and look for an update. */
		if ((val_cell =
		    __wt_row_leaf_value_cell(page, rip, NULL)) == NULL)
			vpack = NULL;
		else {
			vpack = &_vpack;
			__wt_cell_unpack(val_cell, vpack);
		}
		WT_ERR(__rec_txn_read(
		    session, r, NULL, rip, vpack, NULL, &upd));

		/* Build value cell. */
		dictionary = false;
		if (upd == NULL) {
			/*
			 * When the page was read into memory, there may not
			 * have been a value item.
			 *
			 * If there was a value item, check if it's a dictionary
			 * cell (a copy of another item on the page).  If it's a
			 * copy, we have to create a new value item as the old
			 * item might have been discarded from the page.
			 */
			if (vpack == NULL) {
				val->buf.data = NULL;
				val->cell_len = val->len = val->buf.size = 0;
			} else if (vpack->raw == WT_CELL_VALUE_COPY) {
				/* If the item is Huffman encoded, decode it. */
				if (btree->huffman_value == NULL) {
					p = vpack->data;
					size = vpack->size;
				} else {
					WT_ERR(__wt_huffman_decode(session,
					    btree->huffman_value,
					    vpack->data, vpack->size,
					    tmpval));
					p = tmpval->data;
					size = tmpval->size;
				}
				WT_ERR(__rec_cell_build_val(
				    session, r, p, size, (uint64_t)0));
				dictionary = true;
			} else if (vpack->raw == WT_CELL_VALUE_OVFL_RM) {
				/*
				 * If doing an update save and restore, and the
				 * underlying value is a removed overflow value,
				 * we end up here.
				 *
				 * If necessary, when the overflow value was
				 * originally removed, reconciliation appended
				 * a globally visible copy of the value to the
				 * key's update list, meaning the on-page item
				 * isn't accessed after page re-instantiation.
				 *
				 * Assert the case.
				 */
				WT_ASSERT(session,
				    F_ISSET(r, WT_REC_UPDATE_RESTORE));

				/*
				 * If the key is also a removed overflow item,
				 * don't write anything at all.
				 *
				 * We don't have to write anything because the
				 * code re-instantiating the page gets the key
				 * to match the saved list of updates from the
				 * original page.  By not putting the key on
				 * the page, we'll move the key/value set from
				 * a row-store leaf page slot to an insert list,
				 * but that shouldn't matter.
				 *
				 * The reason we bother with the test is because
				 * overflows are expensive to write.  It's hard
				 * to imagine a real workload where this test is
				 * worth the effort, but it's a simple test.
				 */
				if (kpack != NULL &&
				    kpack->raw == WT_CELL_KEY_OVFL_RM)
					goto leaf_insert;

				/*
				 * The on-page value will never be accessed,
				 * write a placeholder record.
				 */
				WT_ERR(__rec_cell_build_val(session, r,
				    "ovfl-unused", strlen("ovfl-unused"),
				    (uint64_t)0));
			} else {
				val->buf.data = val_cell;
				val->buf.size = __wt_cell_total_len(vpack);
				val->cell_len = 0;
				val->len = val->buf.size;

				/* Track if page has overflow items. */
				if (vpack->ovfl)
					r->ovfl_items = true;
			}
		} else {
			/*
			 * The first time we find an overflow record we're not
			 * going to use, discard the underlying blocks.
			 */
			if (vpack != NULL &&
			    vpack->ovfl && vpack->raw != WT_CELL_VALUE_OVFL_RM)
				WT_ERR(__wt_ovfl_remove(session,
				    page, vpack, F_ISSET(r, WT_REC_EVICT)));

			switch (upd->type) {
			case WT_UPDATE_DELETED:
				/*
				 * If this key/value pair was deleted, we're
				 * done.
				 *
				 * Overflow keys referencing discarded values
				 * are no longer useful, discard the backing
				 * blocks.  Don't worry about reuse, reusing
				 * keys from a row-store page reconciliation
				 * seems unlikely enough to ignore.
				 */
				if (kpack != NULL && kpack->ovfl &&
				    kpack->raw != WT_CELL_KEY_OVFL_RM) {
					/*
					 * Keys are part of the name-space, we
					 * can't remove them from the in-memory
					 * tree; if an overflow key was deleted
					 * without being instantiated (for
					 * example, cursor-based truncation), do
					 * it now.
					 */
					if (ikey == NULL)
						WT_ERR(__wt_row_leaf_key(
						    session,
						    page, rip, tmpkey, true));

					WT_ERR(__wt_ovfl_discard_add(
					    session, page, kpack->cell));
				}

				/*
				 * We aren't actually creating the key so we
				 * can't use bytes from this key to provide
				 * prefix information for a subsequent key.
				 */
				tmpkey->size = 0;

				/* Proceed with appended key/value pairs. */
				goto leaf_insert;
			case WT_UPDATE_MODIFIED:
				cbt->slot = WT_ROW_SLOT(page, rip);
				WT_ERR(__wt_value_return_upd(session, cbt, upd,
				    F_ISSET(r, WT_REC_VISIBLE_ALL)));
				WT_ERR(__rec_cell_build_val(session, r,
				    cbt->iface.value.data,
				    cbt->iface.value.size, (uint64_t)0));
				dictionary = true;
				break;
			case WT_UPDATE_STANDARD:
				/*
				 * If no value, nothing needs to be copied.
				 * Otherwise, build the value's chunk from the
				 * update value.
				 */
				if (upd->size == 0) {
					val->buf.data = NULL;
					val->cell_len =
					    val->len = val->buf.size = 0;
				} else {
					WT_ERR(__rec_cell_build_val(session, r,
					    upd->data, upd->size,
					    (uint64_t)0));
					dictionary = true;
				}
				break;
			WT_ILLEGAL_VALUE_ERR(session);
			}
		}

		/*
		 * Build key cell.
		 *
		 * If the key is an overflow key that hasn't been removed, use
		 * the original backing blocks.
		 */
		key_onpage_ovfl = kpack != NULL &&
		    kpack->ovfl && kpack->raw != WT_CELL_KEY_OVFL_RM;
		if (key_onpage_ovfl) {
			key->buf.data = cell;
			key->buf.size = __wt_cell_total_len(kpack);
			key->cell_len = 0;
			key->len = key->buf.size;
			ovfl_key = true;

			/*
			 * We aren't creating a key so we can't use this key as
			 * a prefix for a subsequent key.
			 */
			tmpkey->size = 0;

			/* Track if page has overflow items. */
			r->ovfl_items = true;
		} else {
			/*
			 * Get the key from the page or an instantiated key, or
			 * inline building the key from a previous key (it's a
			 * fast path for simple, prefix-compressed keys), or by
			 * by building the key from scratch.
			 */
			if (__wt_row_leaf_key_info(page, copy,
			    NULL, &cell, &tmpkey->data, &tmpkey->size))
				goto build;

			kpack = &_kpack;
			__wt_cell_unpack(cell, kpack);
			if (btree->huffman_key == NULL &&
			    kpack->type == WT_CELL_KEY &&
			    tmpkey->size >= kpack->prefix) {
				/*
				 * The previous clause checked for a prefix of
				 * zero, which means the temporary buffer must
				 * have a non-zero size, and it references a
				 * valid key.
				 */
				WT_ASSERT(session, tmpkey->size != 0);

				/*
				 * Grow the buffer as necessary, ensuring data
				 * data has been copied into local buffer space,
				 * then append the suffix to the prefix already
				 * in the buffer.
				 *
				 * Don't grow the buffer unnecessarily or copy
				 * data we don't need, truncate the item's data
				 * length to the prefix bytes.
				 */
				tmpkey->size = kpack->prefix;
				WT_ERR(__wt_buf_grow(session,
				    tmpkey, tmpkey->size + kpack->size));
				memcpy((uint8_t *)tmpkey->mem + tmpkey->size,
				    kpack->data, kpack->size);
				tmpkey->size += kpack->size;
			} else
				WT_ERR(__wt_row_leaf_key_copy(
				    session, page, rip, tmpkey));
build:
			WT_ERR(__rec_cell_build_leaf_key(session, r,
			    tmpkey->data, tmpkey->size, &ovfl_key));
		}

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, key->len + val->len)) {
			if (r->raw_compression)
				WT_ERR(__rec_split_raw(
				    session, r, key->len + val->len, false));
			else {
				/*
				 * If we copied address blocks from the page
				 * rather than building the actual key, we have
				 * to build the key now because we are about to
				 * promote it.
				 */
				if (key_onpage_ovfl) {
					WT_ERR(__wt_dsk_cell_data_ref(session,
					    WT_PAGE_ROW_LEAF, kpack, r->cur));
					key_onpage_ovfl = false;
				}

				/*
				 * Turn off prefix compression until a full key
				 * written to the new page, and (unless already
				 * working with an overflow key), rebuild the
				 * key without compression.
				 */
				if (r->key_pfx_compress_conf) {
					r->key_pfx_compress = false;
					if (!ovfl_key)
						WT_ERR(
						    __rec_cell_build_leaf_key(
						    session, r, NULL, 0,
						    &ovfl_key));
				}

				WT_ERR(__rec_split_crossing_bnd(
				    session, r, key->len + val->len));
			}
		}

		/* Copy the key/value pair onto the page. */
		__rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = true;
		else {
			r->all_empty_value = false;
			if (dictionary && btree->dictionary)
				WT_ERR(__rec_dict_replace(session, r, 0, val));
			__rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);

leaf_insert:	/* Write any K/V pairs inserted into the page after this key. */
		if ((ins = WT_SKIP_FIRST(WT_ROW_INSERT(page, rip))) != NULL)
		    WT_ERR(__rec_row_leaf_insert(session, r, ins));
	}

	/* Write the remnant page. */
	ret = __rec_split_finish(session, r);

err:	__wt_scr_free(session, &tmpkey);
	__wt_scr_free(session, &tmpval);
	return (ret);
}

/*
 * __rec_row_leaf_insert --
 *	Walk an insert chain, writing K/V pairs.
 */
static int
__rec_row_leaf_insert(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_INSERT *ins)
{
	WT_BTREE *btree;
	WT_CURSOR_BTREE *cbt;
	WT_KV *key, *val;
	WT_UPDATE *upd;
	bool ovfl_key, upd_saved;

	btree = S2BT(session);
	cbt = &r->update_modify_cbt;

	key = &r->k;
	val = &r->v;

	for (; ins != NULL; ins = WT_SKIP_NEXT(ins)) {
		WT_RET(__rec_txn_read(
		    session, r, ins, NULL, NULL, &upd_saved, &upd));

		if (upd == NULL) {
			/*
			 * If no update is visible but some were saved, check
			 * for splits.
			 */
			if (!upd_saved)
				continue;
			if (!__rec_need_split(r, WT_INSERT_KEY_SIZE(ins)))
				continue;

			/* Copy the current key into place and then split. */
			WT_RET(__wt_buf_set(session, r->cur,
			    WT_INSERT_KEY(ins), WT_INSERT_KEY_SIZE(ins)));
			WT_RET(__rec_split_crossing_bnd(
			    session, r, WT_INSERT_KEY_SIZE(ins)));

			/*
			 * Turn off prefix and suffix compression until a full
			 * key is written into the new page.
			 */
			r->key_pfx_compress = r->key_sfx_compress = false;
			continue;
		}

		switch (upd->type) {
		case WT_UPDATE_DELETED:
			continue;
		case WT_UPDATE_MODIFIED:
			/*
			 * Impossible slot, there's no backing on-page
			 * item.
			 */
			cbt->slot = UINT32_MAX;
			WT_RET(__wt_value_return_upd(
			    session, cbt, upd, F_ISSET(r, WT_REC_VISIBLE_ALL)));
			WT_RET(__rec_cell_build_val(session, r,
			    cbt->iface.value.data,
			    cbt->iface.value.size, (uint64_t)0));
			break;
		case WT_UPDATE_STANDARD:
			if (upd->size == 0)
				val->len = 0;
			else
				WT_RET(__rec_cell_build_val(session,
				    r, upd->data, upd->size,
				    (uint64_t)0));
			break;
		WT_ILLEGAL_VALUE(session);
		}

		/* Build key cell. */
		WT_RET(__rec_cell_build_leaf_key(session, r,
		    WT_INSERT_KEY(ins), WT_INSERT_KEY_SIZE(ins), &ovfl_key));

		/* Boundary: split or write the page. */
		if (__rec_need_split(r, key->len + val->len)) {
			if (r->raw_compression)
				WT_RET(__rec_split_raw(
				    session, r, key->len + val->len, false));
			else {
				/*
				 * Turn off prefix compression until a full key
				 * written to the new page, and (unless already
				 * working with an overflow key), rebuild the
				 * key without compression.
				 */
				if (r->key_pfx_compress_conf) {
					r->key_pfx_compress = false;
					if (!ovfl_key)
						WT_RET(
						    __rec_cell_build_leaf_key(
						    session, r, NULL, 0,
						    &ovfl_key));
				}

				WT_RET(__rec_split_crossing_bnd(
				    session, r, key->len + val->len));
			}
		}

		/* Copy the key/value pair onto the page. */
		__rec_copy_incr(session, r, key);
		if (val->len == 0)
			r->any_empty_value = true;
		else {
			r->all_empty_value = false;
			if (btree->dictionary)
				WT_RET(__rec_dict_replace(session, r, 0, val));
			__rec_copy_incr(session, r, val);
		}

		/* Update compression state. */
		__rec_key_state_update(r, ovfl_key);
	}

	return (0);
}

/*
 * __rec_split_discard --
 *	Discard the pages resulting from a previous split.
 */
static int
__rec_split_discard(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BTREE *btree;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	uint32_t i;

	btree = S2BT(session);
	mod = page->modify;

	/*
	 * A page that split is being reconciled for the second, or subsequent
	 * time; discard underlying block space used in the last reconciliation
	 * that is not being reused for this reconciliation.
	 */
	for (multi = mod->mod_multi,
	    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
		if (btree->type == BTREE_ROW)
			__wt_free(session, multi->key);

		__wt_free(session, multi->disk_image);
		__wt_free(session, multi->supd);

		/*
		 * If the page was re-written free the backing disk blocks used
		 * in the previous write (unless the blocks were reused in this
		 * write). The page may instead have been a disk image with
		 * associated saved updates: ownership of the disk image is
		 * transferred when rewriting the page in-memory and there may
		 * not have been saved updates. We've gotten this wrong a few
		 * times, so use the existence of an address to confirm backing
		 * blocks we care about, and free any disk image/saved updates.
		 */
		if (multi->addr.addr != NULL && !multi->addr.reuse) {
			WT_RET(__wt_btree_block_free(
			    session, multi->addr.addr, multi->addr.size));
			__wt_free(session, multi->addr.addr);
		}
	}
	__wt_free(session, mod->mod_multi);
	mod->mod_multi_entries = 0;

	/*
	 * This routine would be trivial, and only walk a single page freeing
	 * any blocks written to support the split, except for root splits.
	 * In the case of root splits, we have to cope with multiple pages in
	 * a linked list, and we also have to discard overflow items written
	 * for the page.
	 */
	if (WT_PAGE_IS_INTERNAL(page) && mod->mod_root_split != NULL) {
		WT_RET(__rec_split_discard(session, mod->mod_root_split));
		WT_RET(__wt_ovfl_track_wrapup(session, mod->mod_root_split));
		__wt_page_out(session, &mod->mod_root_split);
	}

	return (0);
}

/*
 * __rec_split_dump_keys --
 *	Dump out the split keys in verbose mode.
 */
static int
__rec_split_dump_keys(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_BTREE *btree;
	WT_DECL_ITEM(tkey);
	WT_MULTI *multi;
	uint32_t i;

	btree = S2BT(session);

	__wt_verbose(
	    session, WT_VERB_SPLIT, "split: %" PRIu32 " pages", r->multi_next);

	if (btree->type == BTREE_ROW) {
		WT_RET(__wt_scr_alloc(session, 0, &tkey));
		for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
			__wt_verbose(session, WT_VERB_SPLIT,
			    "starting key %s",
			    __wt_buf_set_printable(session,
			    WT_IKEY_DATA(multi->key.ikey),
			    multi->key.ikey->size, tkey));
		__wt_scr_free(session, &tkey);
	} else
		for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
			__wt_verbose(session, WT_VERB_SPLIT,
			    "starting recno %" PRIu64, multi->key.recno);
	return (0);
}

/*
 * __rec_write_wrapup --
 *	Finish the reconciliation.
 */
static int
__rec_write_wrapup(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_MULTI *multi;
	WT_PAGE_MODIFY *mod;
	WT_REF *ref;
	uint32_t i;

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
		WT_RET(__wt_ref_block_free(session, ref));
		break;
	case WT_PM_REC_EMPTY:				/* Page deleted */
		break;
	case WT_PM_REC_MULTIBLOCK:			/* Multiple blocks */
		/*
		 * Discard the multiple replacement blocks.
		 */
		WT_RET(__rec_split_discard(session, page));
		break;
	case WT_PM_REC_REPLACE:				/* 1-for-1 page swap */
		/*
		 * Discard the replacement leaf page's blocks.
		 *
		 * The exception is root pages are never tracked or free'd, they
		 * are checkpoints, and must be explicitly dropped.
		 */
		if (!__wt_ref_is_root(ref))
			WT_RET(__wt_btree_block_free(session,
			    mod->mod_replace.addr, mod->mod_replace.size));

		/* Discard the replacement page's address and disk image. */
		__wt_free(session, mod->mod_replace.addr);
		mod->mod_replace.size = 0;
		__wt_free(session, mod->mod_disk_image);
		break;
	WT_ILLEGAL_VALUE(session);
	}

	/* Reset the reconciliation state. */
	mod->rec_result = 0;

	/*
	 * If using the lookaside table eviction path and we found updates that
	 * weren't globally visible when reconciling this page, copy them into
	 * the database's lookaside store.
	 */
	if (F_ISSET(r, WT_REC_LOOKASIDE))
		WT_RET(__rec_las_wrapup(session, r));

	/*
	 * Wrap up overflow tracking.  If we are about to create a checkpoint,
	 * the system must be entirely consistent at that point (the underlying
	 * block manager is presumably going to do some action to resolve the
	 * list of allocated/free/whatever blocks that are associated with the
	 * checkpoint).
	 */
	WT_RET(__wt_ovfl_track_wrapup(session, page));

	__wt_verbose(session, WT_VERB_RECONCILE,
	    "%p reconciled into %" PRIu32 " pages", (void *)ref, r->multi_next);

	switch (r->multi_next) {
	case 0:						/* Page delete */
		WT_STAT_CONN_INCR(session, rec_page_delete);
		WT_STAT_DATA_INCR(session, rec_page_delete);

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
		 *
		 * If in-memory, or saving/restoring changes for this page and
		 * there's only one block, there's nothing to write. Set up
		 * a single block as if to split, then use that disk image to
		 * rewrite the page in memory. This is separate from simple
		 * replacements where eviction has decided to retain the page
		 * in memory because the latter can't handle update lists and
		 * splits can.
		 */
		if (F_ISSET(r, WT_REC_IN_MEMORY) ||
		    (F_ISSET(r, WT_REC_UPDATE_RESTORE) &&
		    r->multi->supd_entries != 0))
			goto split;

		/*
		 * We may have a root page, create a sync point. (The write code
		 * ignores root page updates, leaving that work to us.)
		 */
		if (r->wrapup_checkpoint == NULL) {
			mod->mod_replace = r->multi->addr;
			r->multi->addr.addr = NULL;
			mod->mod_disk_image = r->multi->disk_image;
			r->multi->disk_image = NULL;
			mod->mod_page_las = r->multi->page_las;
		} else
			WT_RET(__wt_bt_write(session, r->wrapup_checkpoint,
			    NULL, NULL, true, F_ISSET(r, WT_REC_CHECKPOINT),
			    r->wrapup_checkpoint_compressed));

		mod->rec_result = WT_PM_REC_REPLACE;
		break;
	default:					/* Page split */
		if (WT_PAGE_IS_INTERNAL(page))
			WT_STAT_DATA_INCR(session, rec_multiblock_internal);
		else
			WT_STAT_DATA_INCR(session, rec_multiblock_leaf);

		/* Optionally display the actual split keys in verbose mode. */
		if (WT_VERBOSE_ISSET(session, WT_VERB_SPLIT))
			WT_RET(__rec_split_dump_keys(session, r));

		/*
		 * The reuse flag was set in some cases, but we have to clear
		 * it, otherwise on subsequent reconciliation we would fail to
		 * remove blocks that are being discarded.
		 */
split:		for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
			multi->addr.reuse = 0;
		mod->mod_multi = r->multi;
		mod->mod_multi_entries = r->multi_next;
		mod->rec_result = WT_PM_REC_MULTIBLOCK;

		r->multi = NULL;
		r->multi_next = 0;
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
	for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
		if (multi->addr.addr != NULL) {
			if (multi->addr.reuse)
				multi->addr.addr = NULL;
			else
				WT_TRET(__wt_btree_block_free(session,
				    multi->addr.addr, multi->addr.size));
		}

	/*
	 * If using the lookaside table eviction path and we found updates that
	 * weren't globally visible when reconciling this page, we might have
	 * already copied them into the database's lookaside store. Remove them.
	 */
	if (F_ISSET(r, WT_REC_LOOKASIDE))
		WT_TRET(__rec_las_wrapup_err(session, r));

	WT_TRET(__wt_ovfl_track_wrapup_err(session, page));

	return (ret);
}

/*
 * __rec_las_wrapup --
 *	Copy all of the saved updates into the database's lookaside buffer.
 */
static int
__rec_las_wrapup(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_CURSOR *cursor;
	WT_DECL_ITEM(key);
	WT_DECL_RET;
	WT_MULTI *multi;
	uint32_t i, session_flags;

	/* Check if there's work to do. */
	for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
		if (multi->supd != NULL)
			break;
	if (i == r->multi_next)
		return (0);

	/* Ensure enough room for a column-store key without checking. */
	WT_RET(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

	__wt_las_cursor(session, &cursor, &session_flags);

	for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
		if (multi->supd != NULL)
			WT_ERR(__wt_las_insert_block(
			    session, cursor, r->page, multi, key));

err:	WT_TRET(__wt_las_cursor_close(session, &cursor, session_flags));

	__wt_scr_free(session, &key);
	return (ret);
}

/*
 * __rec_las_wrapup_err --
 *	Discard any saved updates from the database's lookaside buffer.
 */
static int
__rec_las_wrapup_err(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	WT_DECL_RET;
	WT_MULTI *multi;
	uint32_t btree_id, i;

	btree_id = S2BT(session)->id;

	/*
	 * Note the additional check for a non-zero lookaside page ID, that
	 * flags if lookaside table entries for this page have been written.
	 */
	for (multi = r->multi, i = 0; i < r->multi_next; ++multi, ++i)
		if (multi->supd != NULL && multi->page_las.las_pageid != 0)
			WT_TRET(__wt_las_remove_block(session, NULL,
			    btree_id, multi->page_las.las_pageid));

	return (ret);
}

/*
 * __rec_cell_build_int_key --
 *	Process a key and return a WT_CELL structure and byte string to be
 *	stored on a row-store internal page.
 */
static int
__rec_cell_build_int_key(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, const void *data, size_t size, bool *is_ovflp)
{
	WT_BTREE *btree;
	WT_KV *key;

	*is_ovflp = false;

	btree = S2BT(session);

	key = &r->k;

	/* Copy the bytes into the "current" and key buffers. */
	WT_RET(__wt_buf_set(session, r->cur, data, size));
	WT_RET(__wt_buf_set(session, &key->buf, data, size));

	/* Create an overflow object if the data won't fit. */
	if (size > btree->maxintlkey) {
		WT_STAT_DATA_INCR(session, rec_overflow_key_internal);

		*is_ovflp = true;
		return (__rec_cell_build_ovfl(
		    session, r, key, WT_CELL_KEY_OVFL, (uint64_t)0));
	}

	key->cell_len = __wt_cell_pack_int_key(&key->cell, key->buf.size);
	key->len = key->cell_len + key->buf.size;

	return (0);
}

/*
 * __rec_cell_build_leaf_key --
 *	Process a key and return a WT_CELL structure and byte string to be
 *	stored on a row-store leaf page.
 */
static int
__rec_cell_build_leaf_key(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, const void *data, size_t size, bool *is_ovflp)
{
	WT_BTREE *btree;
	WT_KV *key;
	size_t pfx_max;
	const uint8_t *a, *b;
	uint8_t pfx;

	*is_ovflp = false;

	btree = S2BT(session);

	key = &r->k;

	pfx = 0;
	if (data == NULL)
		/*
		 * When data is NULL, our caller has a prefix compressed key
		 * they can't use (probably because they just crossed a split
		 * point).  Use the full key saved when last called, instead.
		 */
		WT_RET(__wt_buf_set(
		    session, &key->buf, r->cur->data, r->cur->size));
	else {
		/*
		 * Save a copy of the key for later reference: we use the full
		 * key for prefix-compression comparisons, and if we are, for
		 * any reason, unable to use the compressed key we generate.
		 */
		WT_RET(__wt_buf_set(session, r->cur, data, size));

		/*
		 * Do prefix compression on the key.  We know by definition the
		 * previous key sorts before the current key, which means the
		 * keys must differ and we just need to compare up to the
		 * shorter of the two keys.
		 */
		if (r->key_pfx_compress) {
			/*
			 * We can't compress out more than 256 bytes, limit the
			 * comparison to that.
			 */
			pfx_max = UINT8_MAX;
			if (size < pfx_max)
				pfx_max = size;
			if (r->last->size < pfx_max)
				pfx_max = r->last->size;
			for (a = data, b = r->last->data; pfx < pfx_max; ++pfx)
				if (*a++ != *b++)
					break;

			/*
			 * Prefix compression may cost us CPU and memory when
			 * the page is re-loaded, don't do it unless there's
			 * reasonable gain.
			 */
			if (pfx < btree->prefix_compression_min)
				pfx = 0;
			else
				WT_STAT_DATA_INCRV(
				    session, rec_prefix_compression, pfx);
		}

		/* Copy the non-prefix bytes into the key buffer. */
		WT_RET(__wt_buf_set(
		    session, &key->buf, (uint8_t *)data + pfx, size - pfx));
	}

	/* Optionally compress the key using the Huffman engine. */
	if (btree->huffman_key != NULL)
		WT_RET(__wt_huffman_encode(session, btree->huffman_key,
		    key->buf.data, (uint32_t)key->buf.size, &key->buf));

	/* Create an overflow object if the data won't fit. */
	if (key->buf.size > btree->maxleafkey) {
		/*
		 * Overflow objects aren't prefix compressed -- rebuild any
		 * object that was prefix compressed.
		 */
		if (pfx == 0) {
			WT_STAT_DATA_INCR(session, rec_overflow_key_leaf);

			*is_ovflp = true;
			return (__rec_cell_build_ovfl(
			    session, r, key, WT_CELL_KEY_OVFL, (uint64_t)0));
		}
		return (
		    __rec_cell_build_leaf_key(session, r, NULL, 0, is_ovflp));
	}

	key->cell_len = __wt_cell_pack_leaf_key(&key->cell, pfx, key->buf.size);
	key->len = key->cell_len + key->buf.size;

	return (0);
}

/*
 * __rec_cell_build_addr --
 *	Process an address reference and return a cell structure to be stored
 *	on the page.
 */
static void
__rec_cell_build_addr(WT_SESSION_IMPL *session, WT_RECONCILE *r,
    const void *addr, size_t size, u_int cell_type, uint64_t recno)
{
	WT_KV *val;

	val = &r->v;

	WT_ASSERT(session, size != 0 || cell_type == WT_CELL_ADDR_DEL);

	/*
	 * We don't check the address size because we can't store an address on
	 * an overflow page: if the address won't fit, the overflow page's
	 * address won't fit either.  This possibility must be handled by Btree
	 * configuration, we have to disallow internal page sizes that are too
	 * small with respect to the largest address cookie the underlying block
	 * manager might return.
	 */

	/*
	 * We don't copy the data into the buffer, it's not necessary; just
	 * re-point the buffer's data/length fields.
	 */
	val->buf.data = addr;
	val->buf.size = size;
	val->cell_len =
	    __wt_cell_pack_addr(&val->cell, cell_type, recno, val->buf.size);
	val->len = val->cell_len + val->buf.size;
}

/*
 * __rec_cell_build_val --
 *	Process a data item and return a WT_CELL structure and byte string to
 *	be stored on the page.
 */
static int
__rec_cell_build_val(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, const void *data, size_t size, uint64_t rle)
{
	WT_BTREE *btree;
	WT_KV *val;

	btree = S2BT(session);

	val = &r->v;

	/*
	 * We don't copy the data into the buffer, it's not necessary; just
	 * re-point the buffer's data/length fields.
	 */
	val->buf.data = data;
	val->buf.size = size;

	/* Handle zero-length cells quickly. */
	if (size != 0) {
		/* Optionally compress the data using the Huffman engine. */
		if (btree->huffman_value != NULL)
			WT_RET(__wt_huffman_encode(
			    session, btree->huffman_value,
			    val->buf.data, (uint32_t)val->buf.size, &val->buf));

		/* Create an overflow object if the data won't fit. */
		if (val->buf.size > btree->maxleafvalue) {
			WT_STAT_DATA_INCR(session, rec_overflow_value);

			return (__rec_cell_build_ovfl(
			    session, r, val, WT_CELL_VALUE_OVFL, rle));
		}
	}
	val->cell_len = __wt_cell_pack_data(&val->cell, rle, val->buf.size);
	val->len = val->cell_len + val->buf.size;

	return (0);
}

/*
 * __rec_cell_build_ovfl --
 *	Store overflow items in the file, returning the address cookie.
 */
static int
__rec_cell_build_ovfl(WT_SESSION_IMPL *session,
    WT_RECONCILE *r, WT_KV *kv, uint8_t type, uint64_t rle)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_HEADER *dsk;
	size_t size;
	uint8_t *addr, buf[WT_BTREE_MAX_ADDR_COOKIE];

	btree = S2BT(session);
	bm = btree->bm;
	page = r->page;

	/* Track if page has overflow items. */
	r->ovfl_items = true;

	/*
	 * See if this overflow record has already been written and reuse it if
	 * possible, otherwise write a new overflow record.
	 */
	WT_RET(__wt_ovfl_reuse_search(
	    session, page, &addr, &size, kv->buf.data, kv->buf.size));
	if (addr == NULL) {
		/* Allocate a buffer big enough to write the overflow record. */
		size = kv->buf.size;
		WT_RET(bm->write_size(bm, session, &size));
		WT_RET(__wt_scr_alloc(session, size, &tmp));

		/* Initialize the buffer: disk header and overflow record. */
		dsk = tmp->mem;
		memset(dsk, 0, WT_PAGE_HEADER_SIZE);
		dsk->type = WT_PAGE_OVFL;
		dsk->u.datalen = (uint32_t)kv->buf.size;
		memcpy(WT_PAGE_HEADER_BYTE(btree, dsk),
		    kv->buf.data, kv->buf.size);
		dsk->mem_size =
		    WT_PAGE_HEADER_BYTE_SIZE(btree) + (uint32_t)kv->buf.size;
		tmp->size = dsk->mem_size;

		/* Write the buffer. */
		addr = buf;
		WT_ERR(__wt_bt_write(session, tmp,
		    addr, &size, false, F_ISSET(r, WT_REC_CHECKPOINT), false));

		/*
		 * Track the overflow record (unless it's a bulk load, which
		 * by definition won't ever reuse a record.
		 */
		if (!r->is_bulk_load)
			WT_ERR(__wt_ovfl_reuse_add(session, page,
			    addr, size, kv->buf.data, kv->buf.size));
	}

	/* Set the callers K/V to reference the overflow record's address. */
	WT_ERR(__wt_buf_set(session, &kv->buf, addr, size));

	/* Build the cell and return. */
	kv->cell_len = __wt_cell_pack_ovfl(&kv->cell, type, rle, kv->buf.size);
	kv->len = kv->cell_len + kv->buf.size;

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

/*
 * __rec_dictionary_skip_search --
 *	Search a dictionary skiplist.
 */
static WT_DICTIONARY *
__rec_dictionary_skip_search(WT_DICTIONARY **head, uint64_t hash)
{
	WT_DICTIONARY **e;
	int i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			--i;
			--e;
			continue;
		}

		/*
		 * Return any exact matches: we don't care in what search level
		 * we found a match.
		 */
		if ((*e)->hash == hash)		/* Exact match */
			return (*e);
		if ((*e)->hash > hash) {	/* Drop down a level */
			--i;
			--e;
		} else				/* Keep going at this level */
			e = &(*e)->next[i];
	}
	return (NULL);
}

/*
 * __rec_dictionary_skip_search_stack --
 *	Search a dictionary skiplist, returning an insert/remove stack.
 */
static void
__rec_dictionary_skip_search_stack(
    WT_DICTIONARY **head, WT_DICTIONARY ***stack, uint64_t hash)
{
	WT_DICTIONARY **e;
	int i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = WT_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;)
		if (*e == NULL || (*e)->hash > hash)
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
}

/*
 * __rec_dictionary_skip_insert --
 *	Insert an entry into the dictionary skip-list.
 */
static void
__rec_dictionary_skip_insert(
    WT_DICTIONARY **head, WT_DICTIONARY *e, uint64_t hash)
{
	WT_DICTIONARY **stack[WT_SKIP_MAXDEPTH];
	u_int i;

	/* Insert the new entry into the skiplist. */
	__rec_dictionary_skip_search_stack(head, stack, hash);
	for (i = 0; i < e->depth; ++i) {
		e->next[i] = *stack[i];
		*stack[i] = e;
	}
}

/*
 * __rec_dictionary_init --
 *	Allocate and initialize the dictionary.
 */
static int
__rec_dictionary_init(WT_SESSION_IMPL *session, WT_RECONCILE *r, u_int slots)
{
	u_int depth, i;

	/* Free any previous dictionary. */
	__rec_dictionary_free(session, r);

	r->dictionary_slots = slots;
	WT_RET(__wt_calloc(session,
	    r->dictionary_slots, sizeof(WT_DICTIONARY *), &r->dictionary));
	for (i = 0; i < r->dictionary_slots; ++i) {
		depth = __wt_skip_choose_depth(session);
		WT_RET(__wt_calloc(session, 1,
		    sizeof(WT_DICTIONARY) + depth * sizeof(WT_DICTIONARY *),
		    &r->dictionary[i]));
		r->dictionary[i]->depth = depth;
	}
	return (0);
}

/*
 * __rec_dictionary_free --
 *	Free the dictionary.
 */
static void
__rec_dictionary_free(WT_SESSION_IMPL *session, WT_RECONCILE *r)
{
	u_int i;

	if (r->dictionary == NULL)
		return;

	/*
	 * We don't correct dictionary_slots when we fail during allocation,
	 * but that's OK, the value is either NULL or a memory reference to
	 * be free'd.
	 */
	for (i = 0; i < r->dictionary_slots; ++i)
		__wt_free(session, r->dictionary[i]);
	__wt_free(session, r->dictionary);
}

/*
 * __rec_dictionary_reset --
 *	Reset the dictionary when reconciliation restarts and when crossing a
 *	page boundary (a potential split).
 */
static void
__rec_dictionary_reset(WT_RECONCILE *r)
{
	if (r->dictionary_slots) {
		r->dictionary_next = 0;
		memset(r->dictionary_head, 0, sizeof(r->dictionary_head));
	}
}

/*
 * __rec_dictionary_lookup --
 *	Check the dictionary for a matching value on this page.
 */
static int
__rec_dictionary_lookup(
    WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_KV *val, WT_DICTIONARY **dpp)
{
	WT_DICTIONARY *dp, *next;
	uint64_t hash;
	bool match;

	*dpp = NULL;

	/* Search the dictionary, and return any match we find. */
	hash = __wt_hash_fnv64(val->buf.data, val->buf.size);
	for (dp = __rec_dictionary_skip_search(r->dictionary_head, hash);
	    dp != NULL && dp->hash == hash; dp = dp->next[0]) {
		WT_RET(__wt_cell_pack_data_match(
		    (WT_CELL *)((uint8_t *)r->cur_ptr->image.mem + dp->offset),
		    &val->cell, val->buf.data, &match));
		if (match) {
			WT_STAT_DATA_INCR(session, rec_dictionary);
			*dpp = dp;
			return (0);
		}
	}

	/*
	 * We're not doing value replacement in the dictionary.  We stop adding
	 * new entries if we run out of empty dictionary slots (but continue to
	 * use the existing entries).  I can't think of any reason a leaf page
	 * value is more likely to be seen because it was seen more recently
	 * than some other value: if we find working sets where that's not the
	 * case, it shouldn't be too difficult to maintain a pointer which is
	 * the next dictionary slot to re-use.
	 */
	if (r->dictionary_next >= r->dictionary_slots)
		return (0);

	/*
	 * Set the hash value, we'll add this entry into the dictionary when we
	 * write it into the page's disk image buffer (because that's when we
	 * know where on the page it will be written).
	 */
	next = r->dictionary[r->dictionary_next++];
	next->offset = 0;		/* Not necessary, just cautious. */
	next->hash = hash;
	__rec_dictionary_skip_insert(r->dictionary_head, next, hash);
	*dpp = next;
	return (0);
}
