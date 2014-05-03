/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_ref_is_root --
 *	Return if the page reference is for the root page.
 */
static inline int
__wt_ref_is_root(WT_REF *ref)
{
	return (ref->home == NULL ? 1 : 0);
}

/*
 * __wt_page_is_modified --
 *	Return if the page is dirty.
 */
static inline int
__wt_page_is_modified(WT_PAGE *page)
{
	return (page->modify != NULL && page->modify->write_gen != 0 ? 1 : 0);
}

/*
 * Estimate the per-allocation overhead.  All implementations of malloc / free
 * have some kind of header and pad for alignment.  We can't know for sure what
 * that adds up to, but this is an estimate based on some measurements of heap
 * size versus bytes in use.
 */
#define	WT_ALLOC_OVERHEAD	32

/*
 * __wt_cache_page_inmem_incr --
 *	Increment a page's memory footprint in the cache.
 */
static inline void
__wt_cache_page_inmem_incr(WT_SESSION_IMPL *session, WT_PAGE *page, size_t size)
{
	WT_CACHE *cache;

	size += WT_ALLOC_OVERHEAD;

	cache = S2C(session)->cache;
	(void)WT_ATOMIC_ADD(cache->bytes_inmem, size);
	(void)WT_ATOMIC_ADD(page->memory_footprint, size);
	if (__wt_page_is_modified(page)) {
		(void)WT_ATOMIC_ADD(cache->bytes_dirty, size);
		(void)WT_ATOMIC_ADD(page->modify->bytes_dirty, size);
	}
}

/*
 * __wt_cache_page_inmem_decr --
 *	Decrement a page's memory footprint in the cache.
 */
static inline void
__wt_cache_page_inmem_decr(WT_SESSION_IMPL *session, WT_PAGE *page, size_t size)
{
	WT_CACHE *cache;

	size += WT_ALLOC_OVERHEAD;

	cache = S2C(session)->cache;
	(void)WT_ATOMIC_SUB(cache->bytes_inmem, size);
	(void)WT_ATOMIC_SUB(page->memory_footprint, size);
	if (__wt_page_is_modified(page)) {
		(void)WT_ATOMIC_SUB(cache->bytes_dirty, size);
		(void)WT_ATOMIC_SUB(page->modify->bytes_dirty, size);
	}
}

/*
 * __wt_cache_dirty_incr --
 *	Increment the cache dirty page/byte counts.
 */
static inline void
__wt_cache_dirty_incr(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CACHE *cache;
	size_t size;

	cache = S2C(session)->cache;
	(void)WT_ATOMIC_ADD(cache->pages_dirty, 1);

	/*
	 * Take care to read the memory_footprint once in case we are racing
	 * with updates.
	 */
	size = page->memory_footprint;
	(void)WT_ATOMIC_ADD(cache->bytes_dirty, size);
	(void)WT_ATOMIC_ADD(page->modify->bytes_dirty, size);
}

/*
 * __wt_cache_dirty_decr --
 *	Decrement the cache dirty page/byte counts.
 */
static inline void
__wt_cache_dirty_decr(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CACHE *cache;
	size_t size;

	cache = S2C(session)->cache;

	if (cache->pages_dirty < 1) {
		(void)__wt_errx(session,
		   "cache dirty decrement failed: cache dirty page count went "
		   "negative");
		cache->pages_dirty = 0;
	} else
		(void)WT_ATOMIC_SUB(cache->pages_dirty, 1);

	/*
	 * It is possible to decrement the footprint of the page without making
	 * the page dirty (for example when freeing an obsolete update list),
	 * so the footprint could change between read and decrement, and we
	 * might attempt to decrement by a different amount than the bytes held
	 * by the page.
	 *
	 * We catch that by maintaining a per-page dirty size, and fixing the
	 * cache stats if that is non-zero when the page is discarded.
	 *
	 * Also take care that the global size doesn't go negative.  This may
	 * lead to small accounting errors (particularly on the last page of the
	 * last file in a checkpoint), but that will come out in the wash when
	 * the page is evicted.
	 */
	size = WT_MIN(page->memory_footprint, cache->bytes_dirty);
	(void)WT_ATOMIC_SUB(cache->bytes_dirty, size);
	(void)WT_ATOMIC_SUB(page->modify->bytes_dirty, size);
}

/*
 * __wt_cache_page_evict --
 *	Evict pages from the cache.
 */
static inline void
__wt_cache_page_evict(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CACHE *cache;
	WT_PAGE_MODIFY *mod;

	cache = S2C(session)->cache;
	mod = page->modify;

	/*
	 * In rare cases, we may race tracking a page's dirty footprint.
	 * If so, we will get here with a non-zero dirty_size in the page, and
	 * we can fix the global stats.
	 */
	if (mod != NULL && mod->bytes_dirty != 0)
		(void)WT_ATOMIC_SUB(cache->bytes_dirty, mod->bytes_dirty);

	WT_ASSERT(session, page->memory_footprint != 0);
	(void)WT_ATOMIC_ADD(cache->bytes_evict, page->memory_footprint);
	page->memory_footprint = 0;

	(void)WT_ATOMIC_ADD(cache->pages_evict, 1);
}

static inline uint64_t
__wt_cache_read_gen(WT_SESSION_IMPL *session)
{
	return (S2C(session)->cache->read_gen);
}

static inline void
__wt_cache_read_gen_incr(WT_SESSION_IMPL *session)
{
	++S2C(session)->cache->read_gen;
}

static inline uint64_t
__wt_cache_read_gen_set(WT_SESSION_IMPL *session)
{
	/*
	 * We return read-generations from the future (where "the future" is
	 * measured by increments of the global read generation).  The reason
	 * is because when acquiring a new hazard pointer for a page, we can
	 * check its read generation, and if the read generation isn't less
	 * than the current global generation, we don't bother updating the
	 * page.  In other words, the goal is to avoid some number of updates
	 * immediately after each update we have to make.
	 */
	return (__wt_cache_read_gen(session) + WT_READGEN_STEP);
}

/*
 * __wt_cache_pages_inuse --
 *	Return the number of pages in use.
 */
static inline uint64_t
__wt_cache_pages_inuse(WT_CACHE *cache)
{
	return (cache->pages_inmem - cache->pages_evict);
}

/*
 * __wt_cache_bytes_inuse --
 *	Return the number of bytes in use.
 */
static inline uint64_t
__wt_cache_bytes_inuse(WT_CACHE *cache)
{
	return (cache->bytes_inmem - cache->bytes_evict);
}

/*
 * __wt_page_refp --
 *      Return the page's index and slot for a reference.
 */
static inline void
__wt_page_refp(WT_SESSION_IMPL *session,
    WT_REF *ref, WT_PAGE_INDEX **pindexp, uint32_t *slotp)
{
	WT_PAGE_INDEX *pindex;
	uint32_t i;

	WT_ASSERT(session,
	    WT_SESSION_TXN_STATE(session)->snap_min != WT_TXN_NONE);

	/*
	 * Copy the parent page's index value: the page can split at any time,
	 * but the index's value is always valid, even if it's not up-to-date.
	 */
retry:	pindex = WT_INTL_INDEX_COPY(ref->home);

	/*
	 * Use the page's reference hint: it should be correct unless the page
	 * split before our slot.  If the page splits after our slot, the hint
	 * will point earlier in the array than our actual slot, so the first
	 * loop is from the hint to the end of the list, and the second loop
	 * is from the start of the list to the end of the list.  (The second
	 * loop overlaps the first, but that only happen in cases where we've
	 * deepened the tree and aren't going to find our slot at all, that's
	 * not worth optimizing.)
	 *
	 * It's not an error for the reference hint to be wrong, it just means
	 * the first retrieval (which sets the hint for subsequent retrievals),
	 * is slower.
	 */
	for (i = ref->ref_hint; i < pindex->entries; ++i)
		if (pindex->index[i]->page == ref->page) {
			*pindexp = pindex;
			*slotp = ref->ref_hint = i;
			return;
		}
	for (i = 0; i < pindex->entries; ++i)
		if (pindex->index[i]->page == ref->page) {
			*pindexp = pindex;
			*slotp = ref->ref_hint = i;
			return;
		}

	/*
	 * If we don't find our reference, the page split into a new level and
	 * our home pointer references the wrong page.  After internal pages
	 * deepen, their reference structure home value are updated; yield and
	 * wait for that to happen.
	 */
	__wt_yield();
	goto retry;
}

/*
 * __wt_page_modify_init --
 *	A page is about to be modified, allocate the modification structure.
 */
static inline int
__wt_page_modify_init(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CONNECTION_IMPL *conn;
	WT_PAGE_MODIFY *modify;

	if (page->modify != NULL)
		return (0);

	conn = S2C(session);

	WT_RET(__wt_calloc_def(session, 1, &modify));

	/*
	 * Select a spinlock for the page; let the barrier immediately below
	 * keep things from racing too badly.
	 */
	modify->page_lock = ++conn->page_lock_cnt % WT_PAGE_LOCKS(conn);

	/*
	 * Multiple threads of control may be searching and deciding to modify
	 * a page.  If our modify structure is used, update the page's memory
	 * footprint, else discard the modify structure, another thread did the
	 * work.
	 */
	if (WT_ATOMIC_CAS(page->modify, NULL, modify))
		__wt_cache_page_inmem_incr(session, page, sizeof(*modify));
	else
		__wt_free(session, modify);
	return (0);
}

/*
 * __wt_page_only_modify_set --
 *	Mark the page (but only the page) dirty.
 */
static inline void
__wt_page_only_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_TXN_GLOBAL *txn_global;

	/*
	 * We depend on atomic-add being a write barrier, that is, a barrier to
	 * ensure all changes to the page are flushed before updating the page
	 * write generation and/or marking the tree dirty, otherwise checkpoints
	 * and/or page reconciliation might be looking at a clean page/tree.
	 *
	 * Every time the page transitions from clean to dirty, update the cache
	 * and transactional information.
	 */
	if (WT_ATOMIC_ADD(page->modify->write_gen, 1) == 1) {
		__wt_cache_dirty_incr(session, page);

		/*
		 * The page can never end up with changes older than the oldest
		 * running transaction.
		 */
		if (F_ISSET(&session->txn, TXN_HAS_SNAPSHOT))
			page->modify->disk_snap_min = session->txn.snap_min;

		txn_global = &S2C(session)->txn_global;
		page->modify->rec_skipped_txn = txn_global->last_running;

		/*
		 * Set the checkpoint generation: if a checkpoint is already
		 * running, these changes cannot be included, by definition.
		 */
		page->modify->checkpoint_gen = S2BT(session)->checkpoint_gen;
	}

	/* Check if this is the largest transaction ID to update the page. */
	if (TXNID_LT(page->modify->update_txn, session->txn.id))
		page->modify->update_txn = session->txn.id;
}

/*
 * __wt_page_modify_set --
 *	Mark the page and tree dirty.
 */
static inline void
__wt_page_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	/*
	 * Mark the tree dirty (even if the page is already marked dirty), newly
	 * created pages to support "empty" files are dirty, but the file isn't
	 * marked dirty until there's a real change needing to be written. Test
	 * before setting the dirty flag, it's a hot cache line.
	 *
	 * The tree's modified flag is cleared by the checkpoint thread: set it
	 * and insert a barrier before dirtying the page.  (I don't think it's
	 * a problem if the tree is marked dirty with all the pages clean, it
	 * might result in an extra checkpoint that doesn't do any work but it
	 * shouldn't cause problems; regardless, let's play it safe.)
	 */
	if (S2BT(session)->modified == 0) {
		S2BT(session)->modified = 1;
		WT_FULL_BARRIER();
	}

	__wt_page_only_modify_set(session, page);
}

/*
 * __wt_page_parent_modify_set --
 *	Mark the parent page and tree dirty.
 */
static inline int
__wt_page_parent_modify_set(
    WT_SESSION_IMPL *session, WT_REF *ref, int page_only)
{
	WT_PAGE *parent;

	/*
	 * This function exists as a place to stash this comment.  There are a
	 * few places where we need to dirty a page's parent.  The trick is the
	 * page's parent might split at any point, and the page parent might be
	 * the wrong parent at any particular time.  We ignore this and dirty
	 * whatever page the page's reference structure points to.  This is safe
	 * because if we're pointing to the wrong parent, that parent must have
	 * split, deepening the tree, which implies marking the original parent
	 * and all of the newly-created children as dirty.  In other words, if
	 * we have the wrong parent page, everything was marked dirty already.
	 */
	parent = ref->home;
	WT_RET(__wt_page_modify_init(session, parent));
	if (page_only)
		__wt_page_only_modify_set(session, parent);
	else
		__wt_page_modify_set(session, parent);
	return (0);
}

/*
 * __wt_off_page --
 *	Return if a pointer references off-page data.
 */
static inline int
__wt_off_page(WT_PAGE *page, const void *p)
{
	/*
	 * There may be no underlying page, in which case the reference is
	 * off-page by definition.
	 */
	return (page->dsk == NULL ||
	    p < (void *)page->dsk ||
	    p >= (void *)((uint8_t *)page->dsk + page->dsk->mem_size));
}

/*
 * __wt_ref_key --
 *	Return a reference to a row-store internal page key as cheaply as
 * possible.
 */
static inline void
__wt_ref_key(WT_PAGE *page, WT_REF *ref, void *keyp, size_t *sizep)
{
	/*
	 * An internal page key is in one of two places: if we instantiated the
	 * key (for example, when reading the page), WT_REF.key.ikey references
	 * a WT_IKEY structure, otherwise, WT_REF.key.pkey references an on-page
	 * key.
	 *
	 * Now the magic: Any allocated memory will have a low-order bit of 0
	 * (the return from malloc must be aligned to store any standard type,
	 * and we assume there's always going to be a standard type requiring
	 * even-byte alignment).  An on-page key consists of an offset/length
	 * pair.  We can fit the maximum page size into 31 bits, so we use the
	 * low-order bit in the on-page value to flag the next 31 bits as a
	 * page offset and the other 32 bits as the key's length, not a WT_IKEY
	 * pointer.  This breaks if allocation chunks aren't even-byte aligned
	 * or pointers and uint64_t's don't always map their low-order bits to
	 * the same location.
	 */
	if (ref->key.pkey & 0x01) {
		*(void **)keyp =
		    WT_PAGE_REF_OFFSET(page, (ref->key.pkey & 0xFFFFFFFF) >> 1);
		*sizep = ref->key.pkey >> 32;
	} else {
		*(void **)keyp = WT_IKEY_DATA(ref->key.ikey);
		*sizep = ((WT_IKEY *)ref->key.ikey)->size;
	}
}

/*
 * __wt_ref_key_onpage_set --
 *	Set a WT_REF to reference an on-page key.
 */
static inline void
__wt_ref_key_onpage_set(WT_PAGE *page, WT_REF *ref, WT_CELL_UNPACK *unpack)
{
	/*
	 * See the comment in __wt_ref_key for an explanation of the magic.
	 */
	ref->key.pkey =
	    (uint64_t)unpack->size << 32 |
	    (uint32_t)WT_PAGE_DISK_OFFSET(page, unpack->data) << 1 |
	    0x01;
}

/*
 * __wt_ref_key_instantiated --
 *	Return an instantiated key from a WT_REF.
 */
static inline WT_IKEY *
__wt_ref_key_instantiated(WT_REF *ref)
{
	/*
	 * See the comment in __wt_ref_key for an explanation of the magic.
	 */
	return (ref->key.pkey & 0x01 ? NULL : ref->key.ikey);
}

/*
 * __wt_ref_key_clear --
 *	Clear a WT_REF key.
 */
static inline void
__wt_ref_key_clear(WT_REF *ref)
{
	/* The key union has 3 fields, all of which are 8B. */
	ref->key.recno = 0;
}

/*
 * __wt_row_leaf_key --
 *	Set a buffer to reference a row-store leaf page key as cheaply as
 * possible.
 */
static inline int
__wt_row_leaf_key(WT_SESSION_IMPL *session,
    WT_PAGE *page, WT_ROW *rip, WT_ITEM *key, WT_CELL **valuep, int instantiate)
{
	WT_BTREE *btree;
	WT_IKEY *ikey;
	WT_CELL_UNPACK unpack;

	btree = S2BT(session);

	/*
	 * A subset of __wt_row_leaf_key_work, that is, calling that function
	 * should give you the same results as calling this one; this function
	 * exists to inline fast-path checks for already instantiated keys and
	 * on-page uncompressed keys.
	 */
	ikey = WT_ROW_KEY_COPY(rip);

	/*
	 * Key copied.
	 * If the key has been instantiated for any reason, off-page, use it.
	 */
	if (__wt_off_page(page, ikey)) {
		key->data = WT_IKEY_DATA(ikey);
		key->size = ikey->size;
		return (0);
	}

	/* If the key isn't compressed or an overflow, take it from the page. */
	if (btree->huffman_key == NULL) {
		__wt_cell_unpack((WT_CELL *)ikey, &unpack);
		if (unpack.type == WT_CELL_KEY && unpack.prefix == 0) {
			key->data = unpack.data;
			key->size = unpack.size;
			return (0);
		}
	}

	/*
	 * We have to build the key (it's never been instantiated, and it's some
	 * kind of compressed or overflow key).
	 */
	return (__wt_row_leaf_key_work(
	    session, page, rip, key, valuep, instantiate));
}

/*
 * __wt_cursor_row_leaf_key --
 *	Set a buffer to reference a cursor-referenced row-store leaf page key.
 */
static inline int
__wt_cursor_row_leaf_key(WT_CURSOR_BTREE *cbt, WT_ITEM *key)
{
	WT_PAGE *page;
	WT_ROW *rip;
	WT_SESSION_IMPL *session;

	/*
	 * If the cursor references a WT_INSERT item, take the key from there,
	 * else take the key from the original page.
	 */
	if (cbt->ins == NULL) {
		session = (WT_SESSION_IMPL *)cbt->iface.session;
		page = cbt->ref->page;
		rip = &page->u.row.d[cbt->slot];
		WT_RET(__wt_row_leaf_key(session, page, rip, key, NULL, 0));
	} else {
		key->data = WT_INSERT_KEY(cbt->ins);
		key->size = WT_INSERT_KEY_SIZE(cbt->ins);
	}
	return (0);
}

/*
 * __wt_row_leaf_value --
 *	Return a pointer to the value cell for a row-store leaf page key, or
 * NULL if there isn't one.
 */
static inline WT_CELL *
__wt_row_leaf_value(WT_PAGE *page, WT_ROW *rip)
{
	WT_CELL *cell;
	WT_CELL_UNPACK unpack;

	cell = WT_ROW_KEY_COPY(rip);

	/*
	 * Key copied.
	 *
	 * Cell now either references a WT_IKEY structure with a cell offset,
	 * or references the on-page key WT_CELL.  Both can be processed
	 * regardless of what other threads are doing.  If it's the former,
	 * use it to get the latter.
	 */
	if (__wt_off_page(page, cell))
		cell = WT_PAGE_REF_OFFSET(page, ((WT_IKEY *)cell)->cell_offset);

	/* Unpack the key cell, then return its associated value cell. */
	__wt_cell_unpack(cell, &unpack);
	cell = (WT_CELL *)((uint8_t *)cell + __wt_cell_total_len(&unpack));
	return (__wt_cell_leaf_value_parse(page, cell));
}

/*
 * __wt_ref_info --
 *	Return the addr/size and type triplet for a reference.
 */
static inline int
__wt_ref_info(WT_SESSION_IMPL *session,
    WT_REF *ref, const uint8_t **addrp, size_t *sizep, u_int *typep)
{
	WT_ADDR *addr;
	WT_CELL_UNPACK *unpack, _unpack;

	addr = ref->addr;
	unpack = &_unpack;

	/*
	 * If NULL, there is no location.
	 * If off-page, the pointer references a WT_ADDR structure.
	 * If on-page, the pointer references a cell.
	 *
	 * The type is of a limited set: internal, leaf or no-overflow leaf.
	 */
	if (addr == NULL) {
		*addrp = NULL;
		*sizep = 0;
		if (typep != NULL)
			*typep = 0;
	} else if (__wt_off_page(ref->home, addr)) {
		*addrp = addr->addr;
		*sizep = addr->size;
		if (typep != NULL)
			switch (addr->type) {
			case WT_ADDR_INT:
				*typep = WT_CELL_ADDR_INT;
				break;
			case WT_ADDR_LEAF:
				*typep = WT_CELL_ADDR_LEAF;
				break;
			case WT_ADDR_LEAF_NO:
				*typep = WT_CELL_ADDR_LEAF_NO;
				break;
			WT_ILLEGAL_VALUE(session);
			}
	} else {
		__wt_cell_unpack((WT_CELL *)addr, unpack);
		*addrp = unpack->data;
		*sizep = unpack->size;
		if (typep != NULL)
			*typep = unpack->type;
	}
	return (0);
}

/*
 * __wt_page_release --
 *	Release a reference to a page.
 */
static inline int
__wt_page_release(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	int locked;

	btree = S2BT(session);

	/*
	 * Discard our hazard pointer.  Ignore pages we don't have and the root
	 * page, which sticks in memory, regardless.
	 */
	if (ref == NULL || __wt_ref_is_root(ref))
		return (0);
	page = ref->page;

	/* Attempt to evict pages with the special "oldest" read generation. */
	if (page->read_gen != WT_READGEN_OLDEST ||
	    F_ISSET(btree, WT_BTREE_NO_EVICTION) ||
	    (btree->checkpointing && __wt_page_is_modified(page)))
		return (__wt_hazard_clear(session, page));

	/*
	 * Take some care with order of operations: if we release the hazard
	 * reference without first locking the page, it could be evicted in
	 * between.
	 */
	locked = WT_ATOMIC_CAS(ref->state, WT_REF_MEM, WT_REF_LOCKED);
	WT_TRET(__wt_hazard_clear(session, page));
	if (!locked)
		return (ret);

	(void)WT_ATOMIC_ADD(btree->evict_busy, 1);
	if ((ret = __wt_evict_page(session, ref)) == 0)
		WT_STAT_FAST_CONN_INCR(session, cache_eviction_force);
	else {
		WT_STAT_FAST_CONN_INCR(session, cache_eviction_force_fail);
		if (ret == EBUSY)
			ret = 0;
	}
	(void)WT_ATOMIC_SUB(btree->evict_busy, 1);

	return (ret);
}

/*
 * __wt_page_swap_func --
 *	Swap one page's hazard pointer for another one when hazard pointer
 * coupling up/down the tree.
 */
static inline int
__wt_page_swap_func(WT_SESSION_IMPL *session, WT_REF *held,
    WT_REF *want, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	WT_DECL_RET;
	int acquired;

	/*
	 * This function is here to simplify the error handling during hazard
	 * pointer coupling so we never leave a hazard pointer dangling.  The
	 * assumption is we're holding a hazard pointer on "held", and want to
	 * acquire a hazard pointer on "want", releasing the hazard pointer on
	 * "held" when we're done.
	 */
	ret = __wt_page_in_func(session, want, flags
#ifdef HAVE_DIAGNOSTIC
	    , file, line
#endif
	    );

	/* An expected failure: WT_NOTFOUND when doing a cache-only read. */
	if (LF_ISSET(WT_READ_CACHE) && ret == WT_NOTFOUND)
		return (WT_NOTFOUND);

	/* An expected failure: WT_RESTART */
	if (ret == WT_RESTART)
		return (WT_RESTART);

	/* Discard the original held page. */
	acquired = ret == 0;
	WT_TRET(__wt_page_release(session, held));

	/*
	 * If there was an error discarding the original held page, discard
	 * the acquired page too, keeping it is never useful.
	 */
	if (acquired && ret != 0)
		WT_TRET(__wt_page_release(session, want));
	return (ret);
}

/*
 * __wt_eviction_force_check --
 *	Check if a page matches the criteria for forced eviction.
 */
static inline int
__wt_eviction_force_check(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_BTREE *btree;

	btree = S2BT(session);

	/* Pages are usually small enough, check that first. */
	if (page->memory_footprint < btree->maxmempage)
		return (0);

	/* Leaf pages only. */
	if (page->type != WT_PAGE_COL_FIX &&
	    page->type != WT_PAGE_COL_VAR &&
	    page->type != WT_PAGE_ROW_LEAF)
		return (0);

	/* Eviction may be turned off, although that's rare. */
	if (F_ISSET(btree, WT_BTREE_NO_EVICTION))
		return (0);

	/*
	 * It's hard to imagine a page with a huge memory footprint that has
	 * never been modified, but check to be sure.
	 */
	if (page->modify == NULL)
		return (0);

	/* Trigger eviction on the next page release. */
	page->read_gen = WT_READGEN_OLDEST;

	return (1);
}

/*
 * __wt_page_hazard_check --
 *	Return if there's a hazard pointer to the page in the system.
 */
static inline WT_HAZARD *
__wt_page_hazard_check(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CONNECTION_IMPL *conn;
	WT_HAZARD *hp;
	WT_SESSION_IMPL *s;
	uint32_t i, hazard_size, session_cnt;

	conn = S2C(session);

	/*
	 * No lock is required because the session array is fixed size, but it
	 * may contain inactive entries.  We must review any active session
	 * that might contain a hazard pointer, so insert a barrier before
	 * reading the active session count.  That way, no matter what sessions
	 * come or go, we'll check the slots for all of the sessions that could
	 * have been active when we started our check.
	 */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (s = conn->sessions, i = 0; i < session_cnt; ++s, ++i) {
		if (!s->active)
			continue;
		WT_ORDERED_READ(hazard_size, s->hazard_size);
		for (hp = s->hazard; hp < s->hazard + hazard_size; ++hp)
			if (hp->page == page)
				return (hp);
	}
	return (NULL);
}

/*
 * __wt_skip_choose_depth --
 *	Randomly choose a depth for a skiplist insert.
 */
static inline u_int
__wt_skip_choose_depth(void)
{
	u_int d;

	for (d = 1; d < WT_SKIP_MAXDEPTH &&
	    __wt_random() < WT_SKIP_PROBABILITY; d++)
		;
	return (d);
}

/*
 * __wt_btree_size_overflow --
 *	Check if the size of an in-memory tree with a single leaf page is over
 * a specified maximum.  If called on anything other than a simple tree with a
 * single leaf page, returns true so the calling code will switch to a new tree.
 */
static inline int
__wt_btree_size_overflow(WT_SESSION_IMPL *session, uint64_t maxsize)
{
	WT_BTREE *btree;
	WT_PAGE *child, *root;
	WT_PAGE_INDEX *pindex;
	WT_REF *first;

	btree = S2BT(session);
	root = btree->root.page;

	/* Check for a non-existent tree. */
	if (root == NULL)
		return (0);

	/* A tree that can be evicted always requires a switch. */
	if (!F_ISSET(btree, WT_BTREE_NO_EVICTION))
		return (1);

	/* Check for a tree with a single leaf page. */
	pindex = WT_INTL_INDEX_COPY(root);
	if (pindex->entries != 1)		/* > 1 child page, switch */
		return (1);

	first = pindex->index[0];
	if (first->state != WT_REF_MEM)		/* no child page, ignore */
		return (0);

	/*
	 * We're reaching down into the page without a hazard pointer, but
	 * that's OK because we know that no-eviction is set and so the page
	 * cannot disappear.
	 */
	child = first->page;
	if (child->type != WT_PAGE_ROW_LEAF)	/* not a single leaf page */
		return (1);

	return (child->memory_footprint > maxsize);
}

/*
 * __wt_lex_compare --
 *	Lexicographic comparison routine.
 *
 * Returns:
 *	< 0 if user_item is lexicographically < tree_item
 *	= 0 if user_item is lexicographically = tree_item
 *	> 0 if user_item is lexicographically > tree_item
 *
 * We use the names "user" and "tree" so it's clear in the btree code which
 * the application is looking at when we call its comparison func.
 */
static inline int
__wt_lex_compare(const WT_ITEM *user_item, const WT_ITEM *tree_item)
{
	const uint8_t *userp, *treep;
	size_t len, usz, tsz;

	usz = user_item->size;
	tsz = tree_item->size;
	len = WT_MIN(usz, tsz);

	for (userp = user_item->data, treep = tree_item->data;
	    len > 0;
	    --len, ++userp, ++treep)
		if (*userp != *treep)
			return (*userp < *treep ? -1 : 1);

	/* Contents are equal up to the smallest length. */
	return ((usz == tsz) ? 0 : (usz < tsz) ? -1 : 1);
}

#define	WT_LEX_CMP(s, collator, k1, k2, cmp)				\
	((collator) == NULL ?						\
	(((cmp) = __wt_lex_compare((k1), (k2))), 0) :			\
	(collator)->compare(collator, &(s)->iface, (k1), (k2), &(cmp)))

/*
 * __wt_lex_compare_skip --
 *	Lexicographic comparison routine, but skipping leading bytes.
 *
 * Returns:
 *	< 0 if user_item is lexicographically < tree_item
 *	= 0 if user_item is lexicographically = tree_item
 *	> 0 if user_item is lexicographically > tree_item
 *
 * We use the names "user" and "tree" so it's clear in the btree code which
 * the application is looking at when we call its comparison func.
 */
static inline int
__wt_lex_compare_skip(
    const WT_ITEM *user_item, const WT_ITEM *tree_item, size_t *matchp)
{
	const uint8_t *userp, *treep;
	size_t len, usz, tsz;

	usz = user_item->size;
	tsz = tree_item->size;
	len = WT_MIN(usz, tsz) - *matchp;

	for (userp = (uint8_t *)user_item->data + *matchp,
	    treep = (uint8_t *)tree_item->data + *matchp;
	    len > 0;
	    --len, ++userp, ++treep, ++*matchp)
		if (*userp != *treep)
			return (*userp < *treep ? -1 : 1);

	/* Contents are equal up to the smallest length. */
	return ((usz == tsz) ? 0 : (usz < tsz) ? -1 : 1);
}

#define	WT_LEX_CMP_SKIP(s, collator, k1, k2, cmp, matchp)		\
	((collator) == NULL ?						\
	(((cmp) = __wt_lex_compare_skip((k1), (k2), matchp)), 0) :	\
	(collator)->compare(collator, &(s)->iface, (k1), (k2), &(cmp)))
