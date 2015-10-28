/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __rec_boundary;		typedef struct __rec_boundary WT_BOUNDARY;
struct __rec_dictionary;	typedef struct __rec_dictionary WT_DICTIONARY;
struct __rec_kv;		typedef struct __rec_kv WT_KV;

/*
 * WT_CHILD_RELEASE, WT_CHILD_RELEASE_ERR --
 *	Macros to clean up during internal-page reconciliation, releasing the
 * hazard pointer we're holding on child pages.
 */
#define	WT_CHILD_RELEASE(session, hazard, ref) do {			\
	if (hazard) {							\
		hazard = false;						\
		WT_TRET(						\
		    __wt_page_release(session, ref, WT_READ_NO_EVICT));	\
	}								\
} while (0)
#define	WT_CHILD_RELEASE_ERR(session, hazard, ref) do {			\
	WT_CHILD_RELEASE(session, hazard, ref);				\
	WT_ERR(ret);							\
} while (0)

typedef enum {
    WT_CHILD_IGNORE,				/* Deleted child: ignore */
    WT_CHILD_MODIFIED,				/* Modified child */
    WT_CHILD_ORIGINAL,				/* Original child */
    WT_CHILD_PROXY				/* Deleted child: proxy */
} WT_CHILD_STATE;

/*
 * Macros from fixed-length entries to/from bytes.
 */
#define	WT_FIX_BYTES_TO_ENTRIES(btree, bytes)				\
    ((uint32_t)((((bytes) * 8) / (btree)->bitcnt)))
#define	WT_FIX_ENTRIES_TO_BYTES(btree, entries)				\
	((uint32_t)WT_ALIGN((entries) * (btree)->bitcnt, 8))

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

	WT_ITEM	 dsk;			/* Temporary disk-image buffer */

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
	 * Track maximum transaction ID seen and first unwritten transaction ID.
	 */
	uint64_t max_txn;

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

	/*
	 * Second, the split size: if we're doing the page layout, split to a
	 * smaller-than-maximum page size when a split is required so we don't
	 * repeatedly split a packed page.
	 */
	uint32_t split_size;		/* Split page size */

	/*
	 * The problem with splits is we've done a lot of work by the time we
	 * realize we're going to have to split, we don't want to start over.
	 *
	 * To keep from having to start over when we hit the maximum page size,
	 * we track the page information when we approach a split boundary.
	 * If we eventually have to split, we walk this structure and pretend
	 * we were splitting all along.  After that, we continue to append to
	 * this structure, and eventually walk it to create a new internal page
	 * that references all of our split pages.
	 */
	struct __rec_boundary {
		/*
		 * Offset is the byte offset in the initial split buffer of the
		 * first byte of the split chunk, recorded before we decide to
		 * split the page; the difference between chunk[1]'s offset and
		 * chunk[0]'s offset is chunk[0]'s length.
		 *
		 * Once we split a page, we stop filling in offset values, we're
		 * writing the split chunks as we find them.
		 */
		size_t offset;		/* Split's first byte */

		/*
		 * The recno and entries fields are the starting record number
		 * of the split chunk (for column-store splits), and the number
		 * of entries in the split chunk.  These fields are used both
		 * to write the split chunk, and to create a new internal page
		 * to reference the split pages.
		 */
		uint64_t recno;		/* Split's starting record */
		uint32_t entries;	/* Split's entries */

		WT_ADDR addr;		/* Split's written location */
		uint32_t size;		/* Split's size */
		uint32_t cksum;		/* Split's checksum */
		void    *dsk;		/* Split's disk image */

		/*
		 * Saved update list, supporting the WT_EVICT_UPDATE_RESTORE and
		 * WT_EVICT_LOOKASIDE configurations.
		 */
		WT_SAVE_UPD *supd;	/* Saved updates */
		uint32_t     supd_next;
		size_t	     supd_allocated;

		/*
		 * The key for a row-store page; no column-store key is needed
		 * because the page's recno, stored in the recno field, is the
		 * column-store key.
		 */
		WT_ITEM key;		/* Promoted row-store key */

		/*
		 * During wrapup, after reconciling the root page, we write a
		 * final block as part of a checkpoint.  If raw compression
		 * was configured, that block may have already been compressed.
		 */
		bool already_compressed;
	} *bnd;				/* Saved boundaries */
	uint32_t bnd_next;		/* Next boundary slot */
	uint32_t bnd_next_max;		/* Maximum boundary slots used */
	size_t	 bnd_entries;		/* Total boundary slots */
	size_t   bnd_allocated;		/* Bytes allocated */

	/*
	 * We track the total number of page entries copied into split chunks
	 * so we can easily figure out how many entries in the current split
	 * chunk.
	 */
	uint32_t total_entries;		/* Total entries in splits */

	/*
	 * And there's state information as to where in this process we are:
	 * (1) tracking split boundaries because we can still fit more split
	 * chunks into the maximum page size, (2) tracking the maximum page
	 * size boundary because we can't fit any more split chunks into the
	 * maximum page size, (3) not performing boundary checks because it's
	 * either not useful with the current page size configuration, or
	 * because we've already been forced to split.
	 */
	enum {	SPLIT_BOUNDARY=0,	/* Next: a split page boundary */
		SPLIT_MAX=1,		/* Next: the maximum page boundary */
		SPLIT_TRACKING_OFF=2,	/* No boundary checks */
		SPLIT_TRACKING_RAW=3 }	/* Underlying compression decides */
	bnd_state;

	/*
	 * We track current information about the current record number, the
	 * number of entries copied into the temporary buffer, where we are
	 * in the temporary buffer, and how much memory remains.  Those items
	 * are packaged here rather than passing pointers to stack locations
	 * around the code.
	 */
	uint64_t recno;			/* Current record number */
	uint32_t entries;		/* Current number of entries */
	uint8_t *first_free;		/* Current first free byte */
	size_t	 space_avail;		/* Remaining space in this chunk */

	/*
	 * Saved update list, supporting the WT_EVICT_UPDATE_RESTORE and
	 * WT_EVICT_LOOKASIDE configurations. While reviewing updates for each
	 * page, we save WT_UPDATE lists here, and then move them to per-block
	 * areas as the blocks are defined.
	 */
	WT_SAVE_UPD *supd;		/* Saved updates */
	uint32_t     supd_next;
	size_t	     supd_allocated;

	/*
	 * We don't need to keep the 0th key around on internal pages, the
	 * search code ignores them as nothing can sort less by definition.
	 * There's some trickiness here, see the code for comments on how
	 * these fields work.
	 */
	bool	 cell_zero;		/* Row-store internal page 0th key */

	/*
	 * WT_DICTIONARY --
	 *	We optionally build a dictionary of row-store values for leaf
	 * pages.  Where two value cells are identical, only write the value
	 * once, the second and subsequent copies point to the original cell.
	 * The dictionary is fixed size, but organized in a skip-list to make
	 * searches faster.
	 */
	struct __rec_dictionary {
		uint64_t hash;				/* Hash value */
		void	*cell;				/* Matching cell */

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
} WT_RECONCILE;
