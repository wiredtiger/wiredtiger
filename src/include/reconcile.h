/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_CELL --
 *	Variable-length cell type.
 *
 * Pages containing variable-length keys or values data (the WT_PAGE_ROW_INT,
 * WT_PAGE_ROW_LEAF, WT_PAGE_COL_INT and WT_PAGE_COL_VAR page types), have
 * cells after the page header.
 *
 * There are 4 basic cell types: keys and data (each of which has an overflow
 * form), deleted cells and off-page references.  The cell is usually followed
 * by additional data, varying by type: keys are followed by a chunk of data,
 * data is followed by optional timestamps and a chunk of data, overflow and
 * off-page cells are followed by optional timestamps and an address cookie.
 *
 * Deleted cells are place-holders for column-store files, where entries cannot
 * be removed in order to preserve the record count.
 *
 * Here's the cell use by page type:
 *
 * WT_PAGE_ROW_INT (row-store internal page):
 *	Keys and offpage-reference pairs (a WT_CELL_KEY or WT_CELL_KEY_OVFL
 * cell followed by a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_ROW_LEAF (row-store leaf page):
 *	Keys with optional data cells (a WT_CELL_KEY or WT_CELL_KEY_OVFL cell,
 *	normally followed by a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell).
 *
 *	WT_PAGE_ROW_LEAF pages optionally prefix-compress keys, using a single
 *	byte count immediately following the cell.
 *
 * WT_PAGE_COL_INT (Column-store internal page):
 *	Off-page references (a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_COL_VAR (Column-store leaf page storing variable-length cells):
 *	Data cells (a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell), or deleted
 * cells (a WT_CELL_DEL cell).
 *
 * Each cell starts with a descriptor byte:
 *
 * Bits 1 and 2 are reserved for "short" key and value cells (that is, a cell
 * carrying data less than 64B, where we can store the data length in the cell
 * descriptor byte):
 *	0x00	Not a short key/data cell
 *	0x01	Short key cell
 *	0x10	Short key cell, with a following prefix-compression byte
 *	0x11	Short value cell
 * In the "short" variants, the other 6 bits of the descriptor byte are the
 * data length.
 *
 * Bit 3 marks an 8B packed, uint64_t value following the cell description byte.
 * (A run-length counter or a record number for variable-length column store.)
 *
 * Bit 4 marks a value with associated timestamps (globally visible values don't
 * require timestamps).
 *
 * Bits 5-8 are cell "types".
 */
#define	WT_CELL_KEY_SHORT	0x01		/* Short key */
#define	WT_CELL_KEY_SHORT_PFX	0x02		/* Short key with prefix byte */
#define	WT_CELL_VALUE_SHORT	0x03		/* Short data */
#define	WT_CELL_SHORT_TYPE(v)	((v) & 0x03U)

#define	WT_CELL_SHORT_MAX	63		/* Maximum short key/value */
#define	WT_CELL_SHORT_SHIFT	2		/* Shift for short key/value */

#define	WT_CELL_64V		0x04		/* Associated value */
#define	WT_CELL_TIMESTAMPS	0x08		/* Associated timestamps */

/*
 * WT_CELL_ADDR_INT is an internal block location, WT_CELL_ADDR_LEAF is a leaf
 * block location, and WT_CELL_ADDR_LEAF_NO is a leaf block location where the
 * page has no overflow items.  (The goal is to speed up truncation as we don't
 * have to read pages without overflow items in order to delete them.  Note,
 * WT_CELL_ADDR_LEAF_NO is not guaranteed to be set on every page without
 * overflow items, the only guarantee is that if set, the page has no overflow
 * items.)
 *
 * WT_CELL_VALUE_COPY is a reference to a previous cell on the page, supporting
 * value dictionaries: if the two values are the same, we only store them once
 * and have any second and subsequent uses reference the original.
 */
#define	WT_CELL_ADDR_DEL	 (0)		/* Address: deleted */
#define	WT_CELL_ADDR_INT	 (1 << 4)	/* Address: internal  */
#define	WT_CELL_ADDR_LEAF	 (2 << 4)	/* Address: leaf */
#define	WT_CELL_ADDR_LEAF_NO	 (3 << 4)	/* Address: leaf no overflow */
#define	WT_CELL_DEL		 (4 << 4)	/* Deleted value */
#define	WT_CELL_KEY		 (5 << 4)	/* Key */
#define	WT_CELL_KEY_OVFL	 (6 << 4)	/* Overflow key */
#define	WT_CELL_KEY_OVFL_RM	(12 << 4)	/* Overflow key (removed) */
#define	WT_CELL_KEY_PFX		 (7 << 4)	/* Key with prefix byte */
#define	WT_CELL_VALUE		 (8 << 4)	/* Value */
#define	WT_CELL_VALUE_COPY	 (9 << 4)	/* Value copy */
#define	WT_CELL_VALUE_OVFL	(10 << 4)	/* Overflow value */
#define	WT_CELL_VALUE_OVFL_RM	(11 << 4)	/* Overflow value (removed) */

#define	WT_CELL_TYPE_MASK	(0x0fU << 4)	/* Maximum 16 cell types */
#define	WT_CELL_TYPE(v)		((v) & WT_CELL_TYPE_MASK)

/*
 * When unable to create a short key or value (and where it wasn't an associated
 * RLE or timestamps that prevented creating a short value), the data must be at
 * least 64B, else we'd have used a short cell. When packing/unpacking the size,
 * decrement/increment the size, in the hopes that a smaller size will pack into
 * a single byte instead of two.
 */
#define	WT_CELL_SIZE_ADJUST	(WT_CELL_SHORT_MAX + 1)

/*
 * WT_CELL --
 *	Variable-length, on-page cell header.
 */
struct __wt_cell {
	/*
	 * Maximum of 34 bytes:
	 * 1: cell descriptor byte
	 * 1: prefix compression count
	 * 9: start timestamp		(uint64_t encoding, max 9 bytes)
	 * 9: stop timestamp		(uint64_t encoding, max 9 bytes)
	 * 9: associated 64-bit value	(uint64_t encoding, max 9 bytes)
	 * 5: data length		(uint32_t encoding, max 5 bytes)
	 *
	 * This calculation is pessimistic: the prefix compression count and
	 * 64V value overlap, the 64V value and data length are optional, and
	 * timestamps only appear in values.
	 */
	uint8_t __chunk[1 + 1 +
	    3 * WT_INTPACK64_MAXSIZE + WT_INTPACK32_MAXSIZE];
};

/*
 * WT_CELL_UNPACK --
 *	Unpacked cell.
 */
struct __wt_cell_unpack {
	WT_CELL *cell;			/* Cell's disk image address */

	uint64_t v;			/* RLE count or recno */

					/* Start/stop timestamps for a value */
	wt_timestamp_t start_ts, stop_ts;
					/* Aggregated timestamp information */
	wt_timestamp_t oldest_start_ts, newest_durable_ts, newest_stop_ts;

	/*
	 * !!!
	 * The size and __len fields are reasonably type size_t; don't change
	 * the type, performance drops significantly if they're type size_t.
	 */
	const void *data;		/* Data */
	uint32_t    size;		/* Data size */

	uint32_t __len;			/* Cell + data length (usually) */

	uint8_t prefix;			/* Cell prefix length */

	uint8_t raw;			/* Raw cell type (include "shorts") */
	uint8_t type;			/* Cell type */

	uint8_t ovfl;			/* boolean: cell is an overflow */
};

/*
 * WT_RECONCILE --
 *	Information tracking a single page reconciliation.
 */
struct __wt_reconcile {
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
	wt_timestamp_t max_timestamp;

	/* Lookaside boundary tracking. */
	uint64_t unstable_txn;
	wt_timestamp_t unstable_durable_timestamp;
	wt_timestamp_t unstable_timestamp;

	u_int updates_seen;		/* Count of updates seen. */
	u_int updates_unstable;		/* Count of updates not visible_all. */

	bool update_uncommitted;	/* An update was uncommitted */
	bool update_used;		/* An update could be used */

	/* All the updates are with prepare in-progress state. */
	bool all_upd_prepare_in_prog;

	/*
	 * When we can't mark the page clean (for example, checkpoint found some
	 * uncommitted updates), there's a leave-dirty flag.
	 */
	bool leave_dirty;

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
	 * XXX
	 * This was originally done because raw compression couldn't do better,
	 * now that raw compression has been removed, we should do better.
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
	 * First, the target size of the page we're building.
	 */
	uint32_t page_size;		/* Page size */

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
	struct __wt_rec_chunk {
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
		wt_timestamp_t oldest_start_ts;
		wt_timestamp_t newest_durable_ts;
		wt_timestamp_t newest_stop_ts;

		/* Saved minimum split-size boundary information. */
		uint32_t min_entries;
		uint64_t min_recno;
		WT_ITEM  min_key;
		wt_timestamp_t min_oldest_start_ts;
		wt_timestamp_t min_newest_durable_ts;
		wt_timestamp_t min_newest_stop_ts;

		size_t   min_offset;			/* byte offset */

		WT_ITEM image;				/* disk-image */
	} chunkA, chunkB, *cur_ptr, *prev_ptr;

	/*
	 * We track current information about the current record number, the
	 * number of entries copied into the disk image buffer, where we are
	 * in the buffer, how much memory remains, and the current min/max of
	 * the timestamps. Those values are packaged here rather than passing
	 * pointers to stack locations around the code.
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
	 * WT_REC_DICTIONARY --
	 *	We optionally build a dictionary of values for leaf pages. Where
	 * two value cells are identical, only write the value once, the second
	 * and subsequent copies point to the original cell. The dictionary is
	 * fixed size, but organized in a skip-list to make searches faster.
	 */
	struct __wt_rec_dictionary {
		uint64_t hash;				/* Hash value */
		uint32_t offset;			/* Matching cell */

		u_int depth;				/* Skiplist */
		WT_REC_DICTIONARY *next[0];
	} **dictionary;					/* Dictionary */
	u_int dictionary_next, dictionary_slots;	/* Next, max entries */
							/* Skiplist head. */
	WT_REC_DICTIONARY *dictionary_head[WT_SKIP_MAXDEPTH];

	/*
	 * WT_REC_KV--
	 *	An on-page key/value item we're building.
	 */
	struct __wt_rec_kv {
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
};

struct __wt_update_select {
	WT_UPDATE *upd;			/* Update to write (or NULL) */

	uint64_t txnid;			/* Transaction ID, timestamps */
	wt_timestamp_t start_ts, durable_ts, stop_ts;

	bool upd_saved;			/* Updates saved to list */

};

#define	WT_CROSSING_MIN_BND(r, next_len)				\
	((r)->cur_ptr->min_offset == 0 &&				\
	    (next_len) > (r)->min_space_avail)
#define	WT_CROSSING_SPLIT_BND(r, next_len) ((next_len) > (r)->space_avail)
#define	WT_CHECK_CROSSING_BND(r, next_len)				\
	(WT_CROSSING_MIN_BND(r, next_len) || WT_CROSSING_SPLIT_BND(r, next_len))
