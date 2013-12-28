/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Supported btree formats: the "current" version is the maximum supported
 * major/minor versions.
 */
#define	WT_BTREE_MAJOR_VERSION_MIN	1	/* Oldest version supported */
#define	WT_BTREE_MINOR_VERSION_MIN	1

#define	WT_BTREE_MAJOR_VERSION_MAX	1	/* Newest version supported */
#define	WT_BTREE_MINOR_VERSION_MAX	1

/*
 * The maximum btree leaf and internal page size is 512MB (2^29).  The maximum
 * of 512MB is enforced by the software, it could be set as high as 2GB (2^31).
 * (The limit of 31 bits is because we steal a single bit from page offsets in
 * the WT_REF structure, see __wt_ref_key.)
 */
#define	WT_BTREE_PAGE_SIZE_MAX		(512 * WT_MEGABYTE)

/*
 * The length of variable-length column-store values and row-store keys/values
 * are stored in a 4B type, so the largest theoretical key/value item is 4GB.
 * However, in the WT_UPDATE structure we use the UINT32_MAX size as a "deleted"
 * flag, and second, the size of an overflow object is constrained by what an
 * underlying block manager can actually write.  (For example, in the default
 * block manager, writing an overflow item includes the underlying block's page
 * header and block manager specific structure, aligned to an allocation-sized
 * unit).  The btree engine limits the size of a single object to (4GB - 1KB);
 * that gives us additional bytes if we ever want to store a structure length
 * plus the object size in 4B, or if we need additional flag values.  Attempts
 * to store large key/value items in the tree trigger an immediate check to the
 * block manager, to make sure it can write the item.  Storing 4GB objects in a
 * btree borders on clinical insanity, anyway.
 *
 * Record numbers are stored in 64-bit unsigned integers, meaning the largest
 * record number is "really, really big".
 */
#define	WT_BTREE_MAX_OBJECT_SIZE	(UINT32_MAX - 1024)

/*
 * A location in a file is a variable-length cookie, but it has a maximum size
 * so it's easy to create temporary space in which to store them.  (Locations
 * can't be much larger than this anyway, they must fit onto the minimum size
 * page because a reference to an overflow page is itself a location.)
 */
#define	WT_BTREE_MAX_ADDR_COOKIE	255	/* Maximum address cookie */

/*
 * WT_BTREE --
 *	A btree handle.
 */
struct __wt_btree {
	WT_DATA_HANDLE *dhandle;

	WT_CKPT	  *ckpt;		/* Checkpoint information */

	enum {	BTREE_COL_FIX=1,	/* Fixed-length column store */
		BTREE_COL_VAR=2,	/* Variable-length column store */
		BTREE_ROW=3		/* Row-store */
	} type;				/* Type */

	const char *key_format;		/* Key format */
	const char *value_format;	/* Value format */
	uint8_t bitcnt;			/* Fixed-length field size in bits */

					/* Row-store comparison function */
	WT_COLLATOR *collator;		/* Comparison function */

	uint32_t id;			/* File ID, for logging */

	uint32_t key_gap;		/* Row-store prefix key gap */

	uint32_t allocsize;		/* Allocation size */
	uint32_t maxintlpage;		/* Internal page max size */
	uint32_t maxintlitem;		/* Internal page max item size */
	uint32_t maxleafpage;		/* Leaf page max size */
	uint32_t maxleafitem;		/* Leaf page max item size */
	uint64_t maxmempage;		/* In memory page max size */

	void *huffman_key;		/* Key huffman encoding */
	void *huffman_value;		/* Value huffman encoding */

	enum {	CKSUM_ON=1,		/* On */
		CKSUM_OFF=2,		/* Off */
		CKSUM_UNCOMPRESSED=3	/* Uncompressed blocks only */
	} checksum;			/* Checksum configuration */

	u_int dictionary;		/* Reconcile: dictionary slots */
	int   internal_key_truncate;	/* Reconcile: internal key truncate */
	int   maximum_depth;		/* Reconcile: maximum tree depth */
	int   prefix_compression;	/* Reconcile: prefix compression */
	u_int prefix_compression_min;	/* Reconcile: prefix compression min */
	int   split_pct;		/* Reconcile: split page percent */
	WT_COMPRESSOR *compressor;	/* Reconcile: page compressor */
	WT_RWLOCK *ovfl_lock;		/* Reconcile: overflow lock */

	uint64_t last_recno;		/* Column-store last record number */

	WT_PAGE *root_page;		/* Root page */
	int modified;			/* If the tree ever modified */
	int bulk_load_ok;		/* Bulk-load is a possibility */

	WT_BM	*bm;			/* Block manager reference */
	u_int	 block_header;		/* WT_PAGE_HEADER_BYTE_SIZE */

	uint64_t write_gen;		/* Write generation */

	WT_PAGE *evict_page;		/* Eviction thread's location */
	uint64_t evict_priority;	/* Relative priority of cached pages. */
	volatile uint32_t lru_count;	/* Count of threads in LRU eviction */

	volatile int checkpointing;	/* Checkpoint in progress */

	/* Flags values up to 0xff are reserved for WT_DHANDLE_* */
#define	WT_BTREE_BULK		0x00100	/* Bulk-load handle */
#define	WT_BTREE_NO_EVICTION	0x00200	/* Disable eviction */
#define	WT_BTREE_NO_HAZARD	0x00400	/* Disable hazard pointers */
#define	WT_BTREE_SALVAGE	0x00800	/* Handle is for salvage */
#define	WT_BTREE_UPGRADE	0x01000	/* Handle is for upgrade */
#define	WT_BTREE_VERIFY		0x02000	/* Handle is for verify */
	uint32_t flags;
};

/* Flags that make a btree handle special (not for normal use). */
#define	WT_BTREE_SPECIAL_FLAGS	 					\
	(WT_BTREE_BULK | WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY)

/*
 * WT_SALVAGE_COOKIE --
 *	Encapsulation of salvage information for reconciliation.
 */
struct __wt_salvage_cookie {
	uint64_t missing;			/* Initial items to create */
	uint64_t skip;				/* Initial items to skip */
	uint64_t take;				/* Items to take */

	int	 done;				/* Ignore the rest */
};
