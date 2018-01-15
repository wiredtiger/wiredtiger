/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	WT_RECNO_OOB	0		/* Illegal record number */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_READ_CACHE			0x0001u
#define	WT_READ_IGNORE_CACHE_SIZE	0x0002u
#define	WT_READ_LOOKASIDE		0x0004u
#define	WT_READ_NOTFOUND_OK		0x0008u
#define	WT_READ_NO_EMPTY		0x0010u
#define	WT_READ_NO_GEN			0x0020u
#define	WT_READ_NO_SPLIT		0x0040u
#define	WT_READ_NO_WAIT			0x0080u
#define	WT_READ_PREV			0x0100u
#define	WT_READ_RESTART_OK		0x0200u
#define	WT_READ_SKIP_INTL		0x0400u
#define	WT_READ_TRUNCATE		0x0800u
#define	WT_READ_WONT_NEED		0x1000u
/* AUTOMATIC FLAG VALUE GENERATION STOP */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_REC_CHECKPOINT	0x01u
#define	WT_REC_EVICT		0x02u
#define	WT_REC_IN_MEMORY	0x04u
#define	WT_REC_LOOKASIDE	0x08u
#define	WT_REC_SCRUB		0x10u
#define	WT_REC_UPDATE_RESTORE	0x20u
#define	WT_REC_VISIBILITY_ERR	0x40u
#define	WT_REC_VISIBLE_ALL	0x80u
/* AUTOMATIC FLAG VALUE GENERATION STOP */

/*
 * WT_PAGE_HEADER --
 *	Blocks have a common header, a WT_PAGE_HEADER structure followed by a
 * block-manager specific structure.
 */
struct __wt_page_header {
	/*
	 * The record number of the first record of the page is stored on disk
	 * so we can figure out where the column-store leaf page fits into the
	 * key space during salvage.
	 */
	uint64_t recno;			/* 00-07: column-store starting recno */

	/*
	 * We maintain page write-generations in the non-transactional case
	 * as that's how salvage can determine the most recent page between
	 * pages overlapping the same key range.
	 */
	uint64_t write_gen;		/* 08-15: write generation */

	/*
	 * The page's in-memory size isn't rounded or aligned, it's the actual
	 * number of bytes the disk-image consumes when instantiated in memory.
	 */
	uint32_t mem_size;		/* 16-19: in-memory page size */

	union {
		uint32_t entries;	/* 20-23: number of cells on page */
		uint32_t datalen;	/* 20-23: overflow data length */
	} u;

	uint8_t type;			/* 24: page type */

	/*
	 * No automatic generation: flag values cannot change, they're written
	 * to disk.
	 */
#define	WT_PAGE_COMPRESSED	0x01u	/* Page is compressed on disk */
#define	WT_PAGE_EMPTY_V_ALL	0x02u	/* Page has all zero-length values */
#define	WT_PAGE_EMPTY_V_NONE	0x04u	/* Page has no zero-length values */
#define	WT_PAGE_ENCRYPTED	0x08u	/* Page is encrypted on disk */
#define	WT_PAGE_LAS_UPDATE	0x10u	/* Page updates in lookaside store */
	uint8_t flags;			/* 25: flags */

	/*
	 * End the structure with 2 bytes of padding: it wastes space, but it
	 * leaves the structure 32-bit aligned and having a few bytes to play
	 * with in the future can't hurt.
	 */
	uint8_t unused[2];		/* 26-27: unused padding */
};
/*
 * WT_PAGE_HEADER_SIZE is the number of bytes we allocate for the structure: if
 * the compiler inserts padding it will break the world.
 */
#define	WT_PAGE_HEADER_SIZE		28

/*
 * __wt_page_header_byteswap --
 *	Handle big- and little-endian transformation of a page header.
 */
static inline void
__wt_page_header_byteswap(WT_PAGE_HEADER *dsk)
{
#ifdef WORDS_BIGENDIAN
	dsk->recno = __wt_bswap64(dsk->recno);
	dsk->write_gen = __wt_bswap64(dsk->write_gen);
	dsk->mem_size = __wt_bswap32(dsk->mem_size);
	dsk->u.entries = __wt_bswap32(dsk->u.entries);
#else
	WT_UNUSED(dsk);
#endif
}

/*
 * The block-manager specific information immediately follows the WT_PAGE_HEADER
 * structure.
 */
#define	WT_BLOCK_HEADER_REF(dsk)					\
	((void *)((uint8_t *)(dsk) + WT_PAGE_HEADER_SIZE))

/*
 * WT_PAGE_HEADER_BYTE --
 * WT_PAGE_HEADER_BYTE_SIZE --
 *	The first usable data byte on the block (past the combined headers).
 */
#define	WT_PAGE_HEADER_BYTE_SIZE(btree)					\
	((u_int)(WT_PAGE_HEADER_SIZE + (btree)->block_header))
#define	WT_PAGE_HEADER_BYTE(btree, dsk)					\
	((void *)((uint8_t *)(dsk) + WT_PAGE_HEADER_BYTE_SIZE(btree)))

/*
 * WT_ADDR --
 *	An in-memory structure to hold a block's location.
 */
struct __wt_addr {
	uint8_t *addr;			/* Block-manager's cookie */
	uint8_t  size;			/* Block-manager's cookie length */

#define	WT_ADDR_INT	1		/* Internal page */
#define	WT_ADDR_LEAF	2		/* Leaf page */
#define	WT_ADDR_LEAF_NO	3		/* Leaf page, no overflow */
	uint8_t  type;

	/*
	 * If an address is both as an address for the previous and the current
	 * multi-block reconciliations, that is, a block we're writing matches
	 * the block written the last time, it will appear in both the current
	 * boundary points as well as the page modification's list of previous
	 * blocks.  The reuse flag is how we know that's happening so the block
	 * is treated correctly (not free'd on error, for example).
	 */
	uint8_t	 reuse;
};

/*
 * Overflow tracking for reuse: When a page is reconciled, we write new K/V
 * overflow items.  If pages are reconciled multiple times, we need to know
 * if we've already written a particular overflow record (so we don't write
 * it again), as well as if we've modified an overflow record previously
 * written (in which case we want to write a new record and discard blocks
 * used by the previously written record).  Track overflow records written
 * for the page, storing the values in a skiplist with the record's value as
 * the "key".
 */
struct __wt_ovfl_reuse {
	uint32_t value_offset;		/* Overflow value offset */
	uint32_t value_size;		/* Overflow value size */
	uint8_t  addr_offset;		/* Overflow addr offset */
	uint8_t  addr_size;		/* Overflow addr size */

	/*
	 * On each page reconciliation, we clear the entry's in-use flag, and
	 * reset it as the overflow record is re-used.  After reconciliation
	 * completes, unused skiplist entries are discarded, along with their
	 * underlying blocks.
	 *
	 * On each page reconciliation, set the just-added flag for each new
	 * skiplist entry; if reconciliation fails for any reason, discard the
	 * newly added skiplist entries, along with their underlying blocks.
	 */
/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_OVFL_REUSE_INUSE		0x1u
#define	WT_OVFL_REUSE_JUST_ADDED	0x2u
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t	 flags;

	/*
	 * The untyped address immediately follows the WT_OVFL_REUSE structure,
	 * the untyped value immediately follows the address.
	 */
#define	WT_OVFL_REUSE_ADDR(p)						\
	((void *)((uint8_t *)(p) + (p)->addr_offset))
#define	WT_OVFL_REUSE_VALUE(p)						\
	((void *)((uint8_t *)(p) + (p)->value_offset))

	WT_OVFL_REUSE *next[0];		/* Forward-linked skip list */
};

/*
 * Lookaside table support: when a page is being reconciled for eviction and has
 * updates that might be required by earlier readers in the system, the updates
 * are written into a lookaside table, and restored as necessary if the page is
 * read.
 *
 * The key is a unique marker for the page (a page ID plus a file ID, ordered
 * this way so that overall the lookaside table is append-mostly), a counter
 * (used to ensure the update records remain in the original order), and the
 * record's key (byte-string for row-store, record number for column-store).
 * The value is the WT_UPDATE structure's transaction ID, timestamp, update
 * type and value.
 *
 * As the key for the lookaside table is different for row- and column-store, we
 * store both key types in a WT_ITEM, building/parsing them in the code, because
 * otherwise we'd need two lookaside files with different key formats. We could
 * make the lookaside table's key standard by moving the source key into the
 * lookaside table value, but that doesn't make the coding any simpler, and it
 * makes the lookaside table's value more likely to overflow the page size when
 * the row-store key is relatively large.
 */
#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
#define	WT_LOOKASIDE_COMPRESSOR	"snappy"
#else
#define	WT_LOOKASIDE_COMPRESSOR	"none"
#endif
#define	WT_LAS_CONFIG							\
    "key_format=" WT_UNCHECKED_STRING(QIQu)				\
    ",value_format=" WT_UNCHECKED_STRING(QuBu)				\
    ",block_compressor=" WT_LOOKASIDE_COMPRESSOR			\
    ",leaf_value_max=64MB"						\
    ",prefix_compression=true"

/*
 * WT_PAGE_LOOKASIDE --
 *	Related information for on-disk pages with lookaside entries.
 */
struct __wt_page_lookaside {
	uint64_t las_pageid;		/* Page ID in lookaside */
	uint64_t las_max_txn;		/* Max transaction ID in lookaside */
	WT_DECL_TIMESTAMP(min_timestamp)/* Min timestamp in lookaside */
					/* Max timestamp on page */
	WT_DECL_TIMESTAMP(onpage_timestamp)
	bool eviction_to_lookaside;	/* Revert to lookaside on eviction */
	bool las_skew_newest;		/* On-page skewed to newest */
	bool invalid;			/* History is required correct reads */
};

/*
 * WT_PAGE_MODIFY --
 *	When a page is modified, there's additional information to maintain.
 */
struct __wt_page_modify {
	/* The first unwritten transaction ID (approximate). */
	uint64_t first_dirty_txn;

	/* The transaction state last time eviction was attempted. */
	uint64_t last_evict_pass_gen;
	uint64_t last_eviction_id;
	WT_DECL_TIMESTAMP(last_eviction_timestamp)

#ifdef HAVE_DIAGNOSTIC
	/* Check that transaction time moves forward. */
	uint64_t last_oldest_id;
#endif

	/* Avoid checking for obsolete updates during checkpoints. */
	uint64_t obsolete_check_txn;
	WT_DECL_TIMESTAMP(obsolete_check_timestamp)

	/* The largest transaction seen on the page by reconciliation. */
	uint64_t rec_max_txn;
	WT_DECL_TIMESTAMP(rec_max_timestamp)

	/* The largest update transaction ID (approximate). */
	uint64_t update_txn;

	/* Dirty bytes added to the cache. */
	size_t bytes_dirty;

	/*
	 * When pages are reconciled, the result is one or more replacement
	 * blocks.  A replacement block can be in one of two states: it was
	 * written to disk, and so we have a block address, or it contained
	 * unresolved modifications and we have a disk image for it with a
	 * list of those unresolved modifications.  The former is the common
	 * case: we only build lists of unresolved modifications when we're
	 * evicting a page, and we only expect to see unresolved modifications
	 * on a page being evicted in the case of a hot page that's too large
	 * to keep in memory as it is.  In other words, checkpoints will skip
	 * unresolved modifications, and will write the blocks rather than
	 * build lists of unresolved modifications.
	 *
	 * Ugly union/struct layout to conserve memory, we never have both
	 * a replace address and multiple replacement blocks.
	 */
	union {
	struct {			/* Single, written replacement block */
		WT_ADDR	 replace;

		/*
		 * A disk image that may or may not have been written, used to
		 * re-instantiate the page in memory.
		 */
		void	*disk_image;

		/* The page has lookaside entries. */
		WT_PAGE_LOOKASIDE page_las;
	} r;
#undef	mod_replace
#define	mod_replace	u1.r.replace
#undef	mod_disk_image
#define	mod_disk_image	u1.r.disk_image
#undef	mod_page_las
#define	mod_page_las	u1.r.page_las

	struct {			/* Multiple replacement blocks */
	struct __wt_multi {
		/*
		 * Block's key: either a column-store record number or a
		 * row-store variable length byte string.
		 */
		union {
			uint64_t recno;
			WT_IKEY *ikey;
		} key;

		/*
		 * A disk image that may or may not have been written, used to
		 * re-instantiate the page in memory.
		 */
		void	*disk_image;

		/*
		 * List of unresolved updates. Updates are either a row-store
		 * insert or update list, or column-store insert list. When
		 * creating lookaside records, there is an additional value,
		 * the committed item's transaction information.
		 *
		 * If there are unresolved updates, the block wasn't written and
		 * there will always be a disk image.
		 */
		struct __wt_save_upd {
			WT_INSERT *ins;		/* Insert list reference */
			WT_ROW	  *ripcip;	/* Original on-page reference */
			WT_UPDATE *onpage_upd;
		} *supd;
		uint32_t supd_entries;

		/*
		 * Disk image was written: address, size and checksum.
		 * On subsequent reconciliations of this page, we avoid writing
		 * the block if it's unchanged by comparing size and checksum;
		 * the reuse flag is set when the block is unchanged and we're
		 * reusing a previous address.
		 */
		WT_ADDR	 addr;
		uint32_t size;
		uint32_t checksum;

		WT_PAGE_LOOKASIDE page_las;
	} *multi;
	uint32_t multi_entries;		/* Multiple blocks element count */
	} m;
#undef	mod_multi
#define	mod_multi		u1.m.multi
#undef	mod_multi_entries
#define	mod_multi_entries	u1.m.multi_entries
	} u1;

	/*
	 * Internal pages need to be able to chain root-page splits and have a
	 * special transactional eviction requirement.  Column-store leaf pages
	 * need update and append lists.
	 *
	 * Ugly union/struct layout to conserve memory, a page is either a leaf
	 * page or an internal page.
	 */
	union {
	struct {
		/*
		 * When a root page splits, we create a new page and write it;
		 * the new page can also split and so on, and we continue this
		 * process until we write a single replacement root page.  We
		 * use the root split field to track the list of created pages
		 * so they can be discarded when no longer needed.
		 */
		WT_PAGE *root_split;	/* Linked list of root split pages */
	} intl;
#undef	mod_root_split
#define	mod_root_split		u2.intl.root_split
	struct {
		/*
		 * Appended items to column-stores: there is only a single one
		 * of these active at a time per column-store tree.
		 */
		WT_INSERT_HEAD **append;

		/*
		 * Updated items in column-stores: variable-length RLE entries
		 * can expand to multiple entries which requires some kind of
		 * list we can expand on demand.  Updated items in fixed-length
		 * files could be done based on an WT_UPDATE array as in
		 * row-stores, but there can be a very large number of bits on
		 * a single page, and the cost of the WT_UPDATE array would be
		 * huge.
		 */
		WT_INSERT_HEAD **update;

		/*
		 * Split-saved last column-store page record. If a column-store
		 * page is split, we save the first record number moved so that
		 * during reconciliation we know the page's last record and can
		 * write any implicitly created deleted records for the page.
		 */
		uint64_t split_recno;
	} column_leaf;
#undef	mod_col_append
#define	mod_col_append		u2.column_leaf.append
#undef	mod_col_update
#define	mod_col_update		u2.column_leaf.update
#undef	mod_col_split_recno
#define	mod_col_split_recno	u2.column_leaf.split_recno
	struct {
		/* Inserted items for row-store. */
		WT_INSERT_HEAD	**insert;

		/* Updated items for row-stores. */
		WT_UPDATE	**update;
	} row_leaf;
#undef	mod_row_insert
#define	mod_row_insert		u2.row_leaf.insert
#undef	mod_row_update
#define	mod_row_update		u2.row_leaf.update
	} u2;

	/*
	 * Overflow record tracking for reconciliation.  We assume overflow
	 * records are relatively rare, so we don't allocate the structures
	 * to track them until we actually see them in the data.
	 */
	struct __wt_ovfl_track {
		/*
		 * Overflow key/value address/byte-string pairs we potentially
		 * reuse each time we reconcile the page.
		 */
		WT_OVFL_REUSE	*ovfl_reuse[WT_SKIP_MAXDEPTH];

		/*
		 * Overflow key/value addresses to be discarded from the block
		 * manager after reconciliation completes successfully.
		 */
		WT_CELL **discard;
		size_t	  discard_entries;
		size_t	  discard_allocated;

		/* Cached overflow value cell/update address pairs. */
		struct {
			WT_CELL	*cell;
			uint8_t	*data;
			size_t	 size;
		} *remove;
		size_t	 remove_allocated;
		uint32_t remove_next;
	} *ovfl_track;

#define	WT_PAGE_LOCK(s, p) 						\
	__wt_spin_lock((s), &(p)->modify->page_lock)
#define	WT_PAGE_TRYLOCK(s, p)						\
	__wt_spin_trylock((s), &(p)->modify->page_lock)
#define	WT_PAGE_UNLOCK(s, p) 						\
	__wt_spin_unlock((s), &(p)->modify->page_lock)
	WT_SPINLOCK page_lock;		/* Page's spinlock */

	/*
	 * The write generation is incremented when a page is modified, a page
	 * is clean if the write generation is 0.
	 */
	uint32_t write_gen;

#define	WT_PM_REC_EMPTY		1	/* Reconciliation: no replacement */
#define	WT_PM_REC_MULTIBLOCK	2	/* Reconciliation: multiple blocks */
#define	WT_PM_REC_REPLACE	3	/* Reconciliation: single block */
	uint8_t rec_result;		/* Reconciliation state */

	uint8_t update_restored;	/* Page created by restoring updates */
};

/*
 * WT_COL_RLE --
 * Variable-length column-store pages have an array of page entries with RLE
 * counts greater than 1 when reading the page, so it's not necessary to walk
 * the page counting records to find a specific entry. We can do a binary search
 * in this array, then an offset calculation to find the cell.
 */
WT_PACKED_STRUCT_BEGIN(__wt_col_rle)
	uint64_t recno;			/* Record number of first repeat. */
	uint64_t rle;			/* Repeat count. */
	uint32_t indx;			/* Slot of entry in col_var. */
WT_PACKED_STRUCT_END

/*
 * WT_PAGE --
 * The WT_PAGE structure describes the in-memory page information.
 */
struct __wt_page {
	/* Per page-type information. */
	union {
		/*
		 * Internal pages (both column- and row-store).
		 *
		 * In-memory internal pages have an array of pointers to child
		 * structures, maintained in collated order.  When a page is
		 * read into memory, the initial list of children is stored in
		 * the "orig_index" field, and it and the collated order are
		 * the same.  After a page splits, the collated order and the
		 * original order will differ.
		 *
		 * Multiple threads of control may be searching the in-memory
		 * internal page and a child page of the internal page may
		 * cause a split at any time.  When a page splits, a new array
		 * is allocated and atomically swapped into place.  Threads in
		 * the old array continue without interruption (the old array is
		 * still valid), but have to avoid racing.  No barrier is needed
		 * because the array reference is updated atomically, but code
		 * reading the fields multiple times would be a very bad idea.
		 * Specifically, do not do this:
		 *	WT_REF **refp = page->u.intl__index->index;
		 *	uint32_t entries = page->u.intl__index->entries;
		 *
		 * The field is declared volatile (so the compiler knows not to
		 * read it multiple times), and we obscure the field name and
		 * use a copy macro in all references to the field (so the code
		 * doesn't read it multiple times).
		 */
		struct {
			WT_REF	*parent_ref;	/* Parent reference */
			uint64_t split_gen;	/* Generation of last split */

			struct __wt_page_index {
				uint32_t entries;
				uint32_t deleted_entries;
				WT_REF	**index;
			} * volatile __index;	/* Collated children */
		} intl;
#undef	pg_intl_parent_ref
#define	pg_intl_parent_ref		u.intl.parent_ref
#undef	pg_intl_split_gen
#define	pg_intl_split_gen		u.intl.split_gen

	/*
	 * Macros to copy/set the index because the name is obscured to ensure
	 * the field isn't read multiple times.
	 *
	 * There are two versions of WT_INTL_INDEX_GET because the session split
	 * generation is usually set, but it's not always required: for example,
	 * if a page is locked for splitting, or being created or destroyed.
	 */
#define	WT_INTL_INDEX_GET_SAFE(page)					\
	((page)->u.intl.__index)
#define	WT_INTL_INDEX_GET(session, page, pindex) do {			\
	WT_ASSERT(session,						\
	    __wt_session_gen(session, WT_GEN_SPLIT) != 0);		\
	(pindex) = WT_INTL_INDEX_GET_SAFE(page);			\
} while (0)
#define	WT_INTL_INDEX_SET(page, v) do {					\
	WT_WRITE_BARRIER();						\
	((page)->u.intl.__index) = (v);					\
} while (0)

	/*
	 * Macro to walk the list of references in an internal page.
	 */
#define	WT_INTL_FOREACH_BEGIN(session, page, ref) do {			\
	WT_PAGE_INDEX *__pindex;					\
	WT_REF **__refp;						\
	uint32_t __entries;						\
	WT_INTL_INDEX_GET(session, page, __pindex);			\
	for (__refp = __pindex->index,					\
	    __entries = __pindex->entries; __entries > 0; --__entries) {\
		(ref) = *__refp++;
#define	WT_INTL_FOREACH_END						\
	}								\
} while (0)

		/* Row-store leaf page. */
		WT_ROW *row;			/* Key/value pairs */
#undef	pg_row
#define	pg_row		u.row

		/* Fixed-length column-store leaf page. */
		uint8_t *fix_bitf;		/* Values */
#undef	pg_fix_bitf
#define	pg_fix_bitf	u.fix_bitf

		/* Variable-length column-store leaf page. */
		struct {
			WT_COL *col_var;	/* Values */

			/*
			 * Variable-length column-store pages have an array
			 * of page entries with RLE counts greater than 1 when
			 * reading the page, so it's not necessary to walk the
			 * page counting records to find a specific entry. We
			 * can do a binary search in this array, then an offset
			 * calculation to find the cell.
			 *
			 * It's a separate structure to keep the page structure
			 * as small as possible.
			 */
			struct __wt_col_var_repeat {
				uint32_t   nrepeats;	/* repeat slots */
				WT_COL_RLE repeats[0];	/* lookup RLE array */
			} *repeats;
#define	WT_COL_VAR_REPEAT_SET(page)					\
	((page)->u.col_var.repeats != NULL)
		} col_var;
#undef	pg_var
#define	pg_var		u.col_var.col_var
#undef	pg_var_repeats
#define	pg_var_repeats	u.col_var.repeats->repeats
#undef	pg_var_nrepeats
#define	pg_var_nrepeats	u.col_var.repeats->nrepeats
	} u;

	/*
	 * Page entries, type and flags are positioned at the end of the WT_PAGE
	 * union to reduce cache misses in the row-store search function.
	 *
	 * The entries field only applies to leaf pages, internal pages use the
	 * page-index entries instead.
	 */
	uint32_t entries;		/* Leaf page entries */

#define	WT_PAGE_IS_INTERNAL(page)					\
	((page)->type == WT_PAGE_COL_INT || (page)->type == WT_PAGE_ROW_INT)
#define	WT_PAGE_INVALID		0	/* Invalid page */
#define	WT_PAGE_BLOCK_MANAGER	1	/* Block-manager page */
#define	WT_PAGE_COL_FIX		2	/* Col-store fixed-len leaf */
#define	WT_PAGE_COL_INT		3	/* Col-store internal page */
#define	WT_PAGE_COL_VAR		4	/* Col-store var-length leaf page */
#define	WT_PAGE_OVFL		5	/* Overflow page */
#define	WT_PAGE_ROW_INT		6	/* Row-store internal page */
#define	WT_PAGE_ROW_LEAF	7	/* Row-store leaf page */
	uint8_t type;			/* Page type */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_PAGE_BUILD_KEYS	0x01u	/* Keys have been built in memory */
#define	WT_PAGE_DISK_ALLOC	0x02u	/* Disk image in allocated memory */
#define	WT_PAGE_DISK_MAPPED	0x04u	/* Disk image in mapped memory */
#define	WT_PAGE_EVICT_LRU	0x08u	/* Page is on the LRU queue */
#define	WT_PAGE_EVICT_NO_PROGRESS 0x10u	/* Eviction doesn't count as progress */
#define	WT_PAGE_OVERFLOW_KEYS	0x20u	/* Page has overflow keys */
#define	WT_PAGE_SPLIT_INSERT	0x40u	/* A leaf page was split for append */
#define	WT_PAGE_UPDATE_IGNORE	0x80u	/* Ignore updates on page discard */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t flags_atomic;		/* Atomic flags, use F_*_ATOMIC */

	uint8_t unused[2];		/* Unused padding */

	/*
	 * The page's read generation acts as an LRU value for each page in the
	 * tree; it is used by the eviction server thread to select pages to be
	 * discarded from the in-memory tree.
	 *
	 * The read generation is a 64-bit value, if incremented frequently, a
	 * 32-bit value could overflow.
	 *
	 * The read generation is a piece of shared memory potentially read
	 * by many threads.  We don't want to update page read generations for
	 * in-cache workloads and suffer the cache misses, so we don't simply
	 * increment the read generation value on every access.  Instead, the
	 * read generation is incremented by the eviction server each time it
	 * becomes active.  To avoid incrementing a page's read generation too
	 * frequently, it is set to a future point.
	 *
	 * Because low read generation values have special meaning, and there
	 * are places where we manipulate the value, use an initial value well
	 * outside of the special range.
	 */
#define	WT_READGEN_NOTSET	0
#define	WT_READGEN_OLDEST	1
#define	WT_READGEN_WONT_NEED	2
#define	WT_READGEN_EVICT_SOON(readgen) 					\
	((readgen) != WT_READGEN_NOTSET && (readgen) < WT_READGEN_START_VALUE)
#define	WT_READGEN_START_VALUE	100
#define	WT_READGEN_STEP		100
	uint64_t read_gen;

	size_t memory_footprint;	/* Memory attached to the page */

	/* Page's on-disk representation: NULL for pages created in memory. */
	const WT_PAGE_HEADER *dsk;

	/* If/when the page is modified, we need lots more information. */
	WT_PAGE_MODIFY *modify;

	/* This is the 64 byte boundary, try to keep hot fields above here. */

	uint64_t cache_create_gen;	/* Page create timestamp */
	uint64_t evict_pass_gen;	/* Eviction pass generation */
};

/*
 * WT_PAGE_DISK_OFFSET, WT_PAGE_REF_OFFSET --
 *	Return the offset/pointer of a pointer/offset in a page disk image.
 */
#define	WT_PAGE_DISK_OFFSET(page, p)					\
	WT_PTRDIFF32(p, (page)->dsk)
#define	WT_PAGE_REF_OFFSET(page, o)					\
	((void *)((uint8_t *)((page)->dsk) + (o)))

/*
 * Page state.
 *
 * Synchronization is based on the WT_REF->state field, which has a number of
 * possible states:
 *
 * WT_REF_DISK:
 *	The initial setting before a page is brought into memory, and set as a
 *	result of page eviction; the page is on disk, and must be read into
 *	memory before use.  WT_REF_DISK has a value of 0 (the default state
 *	after allocating cleared memory).
 *
 * WT_REF_DELETED:
 *	The page is on disk, but has been deleted from the tree; we can delete
 *	row-store leaf pages without reading them if they don't reference
 *	overflow items.
 *
 * WT_REF_LIMBO:
 *	The page image has been loaded into memory but there is additional
 *	history in the lookaside table that has not been applied.
 *
 * WT_REF_LOCKED:
 *	Locked for exclusive access.  In eviction, this page or a parent has
 *	been selected for eviction; once hazard pointers are checked, the page
 *	will be evicted.  When reading a page that was previously deleted, it
 *	is locked until the page is in memory with records marked deleted.  The
 *	thread that set the page to WT_REF_LOCKED has exclusive access, no
 *	other thread may use the WT_REF until the state is changed.
 *
 * WT_REF_LOOKASIDE:
 *	The page is on disk (as per WT_REF_DISK) and has entries in the
 *	lookaside table that must be applied before the page can be read.
 *
 * WT_REF_MEM:
 *	Set by a reading thread once the page has been read from disk; the page
 *	is in the cache and the page reference is OK.
 *
 * WT_REF_READING:
 *	Set by a reading thread before reading an ordinary page from disk;
 *	other readers of the page wait until the read completes.  Sync can
 *	safely skip over such pages: they are clean by definition.
 *
 * WT_REF_SPLIT:
 *	Set when the page is split; the WT_REF is dead and can no longer be
 *	used.
 *
 * The life cycle of a typical page goes like this: pages are read into memory
 * from disk and their state set to WT_REF_MEM.  When the page is selected for
 * eviction, the page state is set to WT_REF_LOCKED.  In all cases, evicting
 * threads reset the page's state when finished with the page: if eviction was
 * successful (a clean page was discarded, and a dirty page was written to disk
 * and then discarded), the page state is set to WT_REF_DISK; if eviction failed
 * because the page was busy, page state is reset to WT_REF_MEM.
 *
 * Readers check the state field and if it's WT_REF_MEM, they set a hazard
 * pointer to the page, flush memory and re-confirm the page state.  If the
 * page state is unchanged, the reader has a valid reference and can proceed.
 *
 * When an evicting thread wants to discard a page from the tree, it sets the
 * WT_REF_LOCKED state, flushes memory, then checks hazard pointers.  If a
 * hazard pointer is found, state is reset to WT_REF_MEM, restoring the page
 * to the readers.  If the evicting thread does not find a hazard pointer,
 * the page is evicted.
 */

/*
 * WT_PAGE_DELETED --
 *	Related information for fast-delete, on-disk pages.
 */
struct __wt_page_deleted {
	volatile uint64_t txnid;		/* Transaction ID */
	WT_DECL_TIMESTAMP(timestamp)

	WT_UPDATE **update_list;		/* List of updates for abort */
};

/*
 * WT_REF --
 *	A single in-memory page and the state information used to determine if
 * it's OK to dereference the pointer to the page.
 */
struct __wt_ref {
	WT_PAGE *page;			/* Page */

	/*
	 * When the tree deepens as a result of a split, the home page value
	 * changes.  Don't cache it, we need to see that change when looking
	 * up our slot in the page's index structure.
	 */
	WT_PAGE * volatile home;	/* Reference page */
	volatile uint32_t pindex_hint;	/* Reference page index hint */

#define	WT_REF_DISK	 0		/* Page is on disk */
#define	WT_REF_DELETED	 1		/* Page is on disk, but deleted */
#define	WT_REF_LIMBO	 2		/* Page is in cache without history */
#define	WT_REF_LOCKED	 3		/* Page locked for exclusive access */
#define	WT_REF_LOOKASIDE 4		/* Page is on disk with lookaside */
#define	WT_REF_MEM	 5		/* Page is in cache and valid */
#define	WT_REF_READING	 6		/* Page being read */
#define	WT_REF_SPLIT	 7		/* Parent page split (WT_REF dead) */
	volatile uint32_t state;	/* Page state */

	/*
	 * Address: on-page cell if read from backing block, off-page WT_ADDR
	 * if instantiated in-memory, or NULL if page created in-memory.
	 */
	void	*addr;

	/*
	 * The child page's key.  Do NOT change this union without reviewing
	 * __wt_ref_key.
	 */
	union {
		uint64_t recno;		/* Column-store: starting recno */
		void	*ikey;		/* Row-store: key */
	} key;
#undef	ref_recno
#define	ref_recno	key.recno
#undef	ref_ikey
#define	ref_ikey	key.ikey

	WT_PAGE_DELETED	  *page_del;	/* Deleted page information */
	WT_PAGE_LOOKASIDE *page_las;	/* Lookaside information */
};
/*
 * WT_REF_SIZE is the expected structure size -- we verify the build to ensure
 * the compiler hasn't inserted padding which would break the world.
 */
#define	WT_REF_SIZE	56

/*
 * WT_ROW --
 * Each in-memory page row-store leaf page has an array of WT_ROW structures:
 * this is created from on-page data when a page is read from the file.  It's
 * sorted by key, fixed in size, and starts with a reference to on-page data.
 *
 * Multiple threads of control may be searching the in-memory row-store pages,
 * and the key may be instantiated at any time.  Code must be able to handle
 * both when the key has not been instantiated (the key field points into the
 * page's disk image), and when the key has been instantiated (the key field
 * points outside the page's disk image).  We don't need barriers because the
 * key is updated atomically, but code that reads the key field multiple times
 * is a very, very bad idea.  Specifically, do not do this:
 *
 *	key = rip->key;
 *	if (key_is_on_page(key)) {
 *		cell = rip->key;
 *	}
 *
 * The field is declared volatile (so the compiler knows it shouldn't read it
 * multiple times), and we obscure the field name and use a copy macro in all
 * references to the field (so the code doesn't read it multiple times), all
 * to make sure we don't introduce this bug (again).
 */
struct __wt_row {	/* On-page key, on-page cell, or off-page WT_IKEY */
	void * volatile __key;
};
#define	WT_ROW_KEY_COPY(rip)	((rip)->__key)
#define	WT_ROW_KEY_SET(rip, v)	((rip)->__key) = (void *)(v)

/*
 * WT_ROW_FOREACH --
 *	Walk the entries of an in-memory row-store leaf page.
 */
#define	WT_ROW_FOREACH(page, rip, i)					\
	for ((i) = (page)->entries,					\
	    (rip) = (page)->pg_row; (i) > 0; ++(rip), --(i))
#define	WT_ROW_FOREACH_REVERSE(page, rip, i)				\
	for ((i) = (page)->entries,					\
	    (rip) = (page)->pg_row + ((page)->entries - 1);		\
	    (i) > 0; --(rip), --(i))

/*
 * WT_ROW_SLOT --
 *	Return the 0-based array offset based on a WT_ROW reference.
 */
#define	WT_ROW_SLOT(page, rip)						\
	((uint32_t)(((WT_ROW *)(rip)) - (page)->pg_row))

/*
 * WT_COL --
 * Each in-memory variable-length column-store leaf page has an array of WT_COL
 * structures: this is created from on-page data when a page is read from the
 * file.  It's fixed in size, and references data on the page.
 */
struct __wt_col {
	/*
	 * Variable-length column-store data references are page offsets, not
	 * pointers (we boldly re-invent short pointers).  The trade-off is 4B
	 * per K/V pair on a 64-bit machine vs. a single cycle for the addition
	 * of a base pointer.  The on-page data is a WT_CELL (same as row-store
	 * pages).
	 *
	 * If the value is 0, it's a single, deleted record.
	 *
	 * Obscure the field name, code shouldn't use WT_COL->__col_value, the
	 * public interface is WT_COL_PTR and WT_COL_PTR_SET.
	 */
	uint32_t __col_value;
};

/*
 * WT_COL_PTR, WT_COL_PTR_SET --
 *	Return/Set a pointer corresponding to the data offset. (If the item does
 * not exist on the page, return a NULL.)
 */
#define	WT_COL_PTR(page, cip)						\
	((cip)->__col_value == 0 ?					\
	    NULL : WT_PAGE_REF_OFFSET(page, (cip)->__col_value))
#define	WT_COL_PTR_SET(cip, value)					\
	(cip)->__col_value = (value)

/*
 * WT_COL_FOREACH --
 *	Walk the entries of variable-length column-store leaf page.
 */
#define	WT_COL_FOREACH(page, cip, i)					\
	for ((i) = (page)->entries,					\
	    (cip) = (page)->pg_var; (i) > 0; ++(cip), --(i))

/*
 * WT_COL_SLOT --
 *	Return the 0-based array offset based on a WT_COL reference.
 */
#define	WT_COL_SLOT(page, cip)						\
	((uint32_t)(((WT_COL *)(cip)) - (page)->pg_var))

/*
 * WT_IKEY --
 * Instantiated key: row-store keys are usually prefix compressed and sometimes
 * Huffman encoded or overflow objects.  Normally, a row-store page in-memory
 * key points to the on-page WT_CELL, but in some cases, we instantiate the key
 * in memory, in which case the row-store page in-memory key points to a WT_IKEY
 * structure.
 */
struct __wt_ikey {
	uint32_t size;			/* Key length */

	/*
	 * If we no longer point to the key's on-page WT_CELL, we can't find its
	 * related value.  Save the offset of the key cell in the page.
	 *
	 * Row-store cell references are page offsets, not pointers (we boldly
	 * re-invent short pointers).  The trade-off is 4B per K/V pair on a
	 * 64-bit machine vs. a single cycle for the addition of a base pointer.
	 */
	uint32_t  cell_offset;

	/* The key bytes immediately follow the WT_IKEY structure. */
#define	WT_IKEY_DATA(ikey)						\
	((void *)((uint8_t *)(ikey) + sizeof(WT_IKEY)))
};

/*
 * WT_UPDATE --
 * Entries on leaf pages can be updated, either modified or deleted.  Updates
 * to entries referenced from the WT_ROW and WT_COL arrays are stored in the
 * page's WT_UPDATE array.  When the first element on a page is updated, the
 * WT_UPDATE array is allocated, with one slot for every existing element in
 * the page.  A slot points to a WT_UPDATE structure; if more than one update
 * is done for an entry, WT_UPDATE structures are formed into a forward-linked
 * list.
 */
struct __wt_update {
	volatile uint64_t txnid;	/* transaction ID */
#if WT_TIMESTAMP_SIZE == 8
	WT_DECL_TIMESTAMP(timestamp)	/* aligned uint64_t timestamp */
#endif

	WT_UPDATE *next;		/* forward-linked list */

	uint32_t size;			/* data length */

#define	WT_UPDATE_INVALID	0	/* diagnostic check */
#define	WT_UPDATE_BIRTHMARK	1	/* transaction for on-page value */
#define	WT_UPDATE_MODIFY	2	/* partial-update modify value */
#define	WT_UPDATE_RESERVE	3	/* reserved */
#define	WT_UPDATE_STANDARD	4	/* complete value */
#define	WT_UPDATE_TOMBSTONE	5	/* deleted */
	uint8_t type;			/* type (one byte to conserve memory) */

	/* If the update includes a complete value. */
#define	WT_UPDATE_DATA_VALUE(upd)					\
	((upd)->type == WT_UPDATE_STANDARD ||				\
	(upd)->type == WT_UPDATE_TOMBSTONE)

#if WT_TIMESTAMP_SIZE != 8
	WT_DECL_TIMESTAMP(timestamp)	/* unaligned uint8_t array timestamp */
#endif

	/*
	 * Zero or more bytes of value (the payload) immediately follows the
	 * WT_UPDATE structure.  We use a C99 flexible array member which has
	 * the semantics we want.
	 */
	uint8_t data[];			/* start of the data */
};

/*
 * WT_UPDATE_SIZE is the expected structure size excluding the payload data --
 * we verify the build to ensure the compiler hasn't inserted padding.
 */
#define	WT_UPDATE_SIZE	(21 + WT_TIMESTAMP_SIZE)

/*
 * The memory size of an update: include some padding because this is such a
 * common case that overhead of tiny allocations can swamp our cache overhead
 * calculation.
 */
#define	WT_UPDATE_MEMSIZE(upd)						\
	WT_ALIGN(WT_UPDATE_SIZE + (upd)->size, 32)

/*
 * WT_MAX_MODIFY_UPDATE --
 *	Limit update chains to a small value to avoid penalizing reads and
 * permit truncation.
 */
#define	WT_MAX_MODIFY_UPDATE	10

/*
 * WT_INSERT --
 *
 * Row-store leaf pages support inserts of new K/V pairs.  When the first K/V
 * pair is inserted, the WT_INSERT_HEAD array is allocated, with one slot for
 * every existing element in the page, plus one additional slot.  A slot points
 * to a WT_INSERT_HEAD structure for the items which sort after the WT_ROW
 * element that references it and before the subsequent WT_ROW element; the
 * skiplist structure has a randomly chosen depth of next pointers in each
 * inserted node.
 *
 * The additional slot is because it's possible to insert items smaller than any
 * existing key on the page: for that reason, the first slot of the insert array
 * holds keys smaller than any other key on the page.
 *
 * In column-store variable-length run-length encoded pages, a single indx
 * entry may reference a large number of records, because there's a single
 * on-page entry representing many identical records. (We don't expand those
 * entries when the page comes into memory, as that would require resources as
 * pages are moved to/from the cache, including read-only files.)  Instead, a
 * single indx entry represents all of the identical records originally found
 * on the page.
 *
 * Modifying (or deleting) run-length encoded column-store records is hard
 * because the page's entry no longer references a set of identical items.  We
 * handle this by "inserting" a new entry into the insert array, with its own
 * record number.  (This is the only case where it's possible to insert into a
 * column-store: only appends are allowed, as insert requires re-numbering
 * subsequent records.  Berkeley DB did support mutable records, but it won't
 * scale and it isn't useful enough to re-implement, IMNSHO.)
 */
struct __wt_insert {
	WT_UPDATE *upd;				/* value */

	union {
		uint64_t recno;			/* column-store record number */
		struct {
			uint32_t offset;	/* row-store key data start */
			uint32_t size;		/* row-store key data size */
		} key;
	} u;

#define	WT_INSERT_KEY_SIZE(ins) (((WT_INSERT *)(ins))->u.key.size)
#define	WT_INSERT_KEY(ins)						\
	((void *)((uint8_t *)(ins) + ((WT_INSERT *)(ins))->u.key.offset))
#define	WT_INSERT_RECNO(ins)	(((WT_INSERT *)(ins))->u.recno)

	WT_INSERT *next[0];			/* forward-linked skip list */
};

/*
 * Skiplist helper macros.
 */
#define	WT_SKIP_FIRST(ins_head)						\
	(((ins_head) == NULL) ? NULL : ((WT_INSERT_HEAD *)(ins_head))->head[0])
#define	WT_SKIP_LAST(ins_head)						\
	(((ins_head) == NULL) ? NULL : ((WT_INSERT_HEAD *)(ins_head))->tail[0])
#define	WT_SKIP_NEXT(ins)  ((ins)->next[0])
#define	WT_SKIP_FOREACH(ins, ins_head)					\
	for ((ins) = WT_SKIP_FIRST(ins_head);				\
	    (ins) != NULL;						\
	    (ins) = WT_SKIP_NEXT(ins))

/*
 * Atomically allocate and swap a structure or array into place.
 */
#define	WT_PAGE_ALLOC_AND_SWAP(s, page, dest, v, count)	do {		\
	if (((v) = (dest)) == NULL) {					\
		WT_ERR(__wt_calloc_def(s, count, &(v)));		\
		if (__wt_atomic_cas_ptr(&(dest), NULL, v))		\
			__wt_cache_page_inmem_incr(			\
			    s, page, (count) * sizeof(*(v)));		\
		else							\
			__wt_free(s, v);				\
	}								\
} while (0)

/*
 * WT_INSERT_HEAD --
 * 	The head of a skiplist of WT_INSERT items.
 */
struct __wt_insert_head {
	WT_INSERT *head[WT_SKIP_MAXDEPTH];	/* first item on skiplists */
	WT_INSERT *tail[WT_SKIP_MAXDEPTH];	/* last item on skiplists */
};

/*
 * The row-store leaf page insert lists are arrays of pointers to structures,
 * and may not exist.  The following macros return an array entry if the array
 * of pointers and the specific structure exist, else NULL.
 */
#define	WT_ROW_INSERT_SLOT(page, slot)					\
	((page)->modify == NULL ||					\
	    (page)->modify->mod_row_insert == NULL ?			\
	    NULL : (page)->modify->mod_row_insert[slot])
#define	WT_ROW_INSERT(page, ip)						\
	WT_ROW_INSERT_SLOT(page, WT_ROW_SLOT(page, ip))
#define	WT_ROW_UPDATE(page, ip)						\
	((page)->modify == NULL ||					\
	    (page)->modify->mod_row_update == NULL ?			\
	    NULL : (page)->modify->mod_row_update[WT_ROW_SLOT(page, ip)])
/*
 * WT_ROW_INSERT_SMALLEST references an additional slot past the end of the
 * the "one per WT_ROW slot" insert array.  That's because the insert array
 * requires an extra slot to hold keys that sort before any key found on the
 * original page.
 */
#define	WT_ROW_INSERT_SMALLEST(page)					\
	((page)->modify == NULL ||					\
	    (page)->modify->mod_row_insert == NULL ?			\
	    NULL : (page)->modify->mod_row_insert[(page)->entries])

/*
 * The column-store leaf page update lists are arrays of pointers to structures,
 * and may not exist.  The following macros return an array entry if the array
 * of pointers and the specific structure exist, else NULL.
 */
#define	WT_COL_UPDATE_SLOT(page, slot)					\
	((page)->modify == NULL ||					\
	    (page)->modify->mod_col_update == NULL ?			\
	    NULL : (page)->modify->mod_col_update[slot])
#define	WT_COL_UPDATE(page, ip)						\
	WT_COL_UPDATE_SLOT(page, WT_COL_SLOT(page, ip))

/*
 * WT_COL_UPDATE_SINGLE is a single WT_INSERT list, used for any fixed-length
 * column-store updates for a page.
 */
#define	WT_COL_UPDATE_SINGLE(page)					\
	WT_COL_UPDATE_SLOT(page, 0)

/*
 * WT_COL_APPEND is an WT_INSERT list, used for fixed- and variable-length
 * appends.
 */
#define	WT_COL_APPEND(page)						\
	((page)->modify == NULL ||					\
	    (page)->modify->mod_col_append == NULL ?			\
	    NULL : (page)->modify->mod_col_append[0])

/* WT_FIX_FOREACH walks fixed-length bit-fields on a disk page. */
#define	WT_FIX_FOREACH(btree, dsk, v, i)				\
	for ((i) = 0,							\
	    (v) = (i) < (dsk)->u.entries ?				\
	    __bit_getv(							\
	    WT_PAGE_HEADER_BYTE(btree, dsk), 0, (btree)->bitcnt) : 0;	\
	    (i) < (dsk)->u.entries; ++(i),				\
	    (v) = __bit_getv(						\
	    WT_PAGE_HEADER_BYTE(btree, dsk), i, (btree)->bitcnt))

/*
 * Manage split generation numbers.  Splits walk the list of sessions to check
 * when it is safe to free structures that have been replaced.  We also check
 * that list periodically (e.g., when wrapping up a transaction) to free any
 * memory we can.
 *
 * Before a thread enters code that will examine page indexes (which are
 * swapped out by splits), it publishes a copy of the current split generation
 * into its session.  Don't assume that threads never re-enter this code: if we
 * already have a split generation, leave it alone.  If our caller is examining
 * an index, we don't want the oldest split generation to move forward and
 * potentially free it.
 */
#define	WT_ENTER_PAGE_INDEX(session) do {				\
	uint64_t __prev_split_gen =					\
	    __wt_session_gen(session, WT_GEN_SPLIT);			\
	if (__prev_split_gen == 0)					\
		__wt_session_gen_enter(session, WT_GEN_SPLIT);

#define	WT_LEAVE_PAGE_INDEX(session)					\
	if (__prev_split_gen == 0)					\
		__wt_session_gen_leave(session, WT_GEN_SPLIT);		\
	} while (0)

#define	WT_WITH_PAGE_INDEX(session, e)					\
	WT_ENTER_PAGE_INDEX(session);					\
	(e);								\
	WT_LEAVE_PAGE_INDEX(session)
