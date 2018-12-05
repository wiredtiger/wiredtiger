/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Initialize a static WT_CURSOR structure.
 */
#define	WT_CURSOR_STATIC_INIT(n,					\
	get_key,							\
	get_value,							\
	set_key,							\
	set_value,							\
	compare,							\
	equals,								\
	next,								\
	prev,								\
	reset,								\
	search,								\
	search_near,							\
	insert,								\
	modify,								\
	update,								\
	remove,								\
	reserve,							\
	reconfigure,							\
	cache,								\
	reopen,								\
	close)								\
	static const WT_CURSOR n = {					\
	NULL,				/* session */			\
	NULL,				/* uri */			\
	NULL,				/* key_format */		\
	NULL,				/* value_format */		\
	get_key,							\
	get_value,							\
	set_key,							\
	set_value,							\
	compare,							\
	equals,								\
	next,								\
	prev,								\
	reset,								\
	search,								\
	search_near,							\
	insert,								\
	modify,								\
	update,								\
	remove,								\
	reserve,							\
	close,								\
	reconfigure,							\
	cache,								\
	reopen,								\
	0,				/* uri_hash */			\
	{ NULL, NULL },			/* TAILQ_ENTRY q */		\
	0,				/* recno key */			\
	{ 0 },				/* recno raw buffer */		\
	NULL,				/* json_private */		\
	NULL,				/* lang_private */		\
	{ NULL, 0, NULL, 0, 0 },	/* WT_ITEM key */		\
	{ NULL, 0, NULL, 0, 0 },	/* WT_ITEM value */		\
	0,				/* int saved_err */		\
	NULL,				/* internal_uri */		\
	0				/* uint32_t flags */		\
}

struct __wt_cursor_backup {
	WT_CURSOR iface;

	size_t next;			/* Cursor position */
	WT_FSTREAM *bfs;		/* Backup file stream */
	uint32_t maxid;			/* Maximum log file ID seen */

	char **list;			/* List of files to be copied. */
	size_t list_allocated;
	size_t list_next;

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CURBACKUP_DUP	0x1u	/* Duplicated backup cursor */
#define	WT_CURBACKUP_LOCKER	0x2u	/* Hot-backup started */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t	flags;
};
#define	WT_CURSOR_BACKUP_ID(cursor)	(((WT_CURSOR_BACKUP *)(cursor))->maxid)

struct __wt_cursor_btree {
	WT_CURSOR iface;

	/*
	 * The btree field is safe to use when the cursor is open.  When the
	 * cursor is cached, the btree may be closed, so it is only safe
	 * initially to look at the underlying data handle.
	 */
	WT_BTREE *btree;		/* Enclosing btree */
	WT_DATA_HANDLE *dhandle;	/* Data handle for the btree */

	/*
	 * The following fields are set by the search functions as a precursor
	 * to page modification: we have a page, a WT_COL/WT_ROW slot on the
	 * page, an insert head, insert list and a skiplist stack (the stack of
	 * skiplist entries leading to the insert point).  The search functions
	 * also return the relationship of the search key to the found key.
	 */
	WT_REF	  *ref;			/* Current page */
	uint32_t   slot;		/* WT_COL/WT_ROW 0-based slot */

	WT_INSERT_HEAD	*ins_head;	/* Insert chain head */
	WT_INSERT	*ins;		/* Current insert node */
					/* Search stack */
	WT_INSERT	**ins_stack[WT_SKIP_MAXDEPTH];

					/* Next item(s) found during search */
	WT_INSERT	*next_stack[WT_SKIP_MAXDEPTH];

	uint32_t page_deleted_count;	/* Deleted items on the page */

	uint64_t recno;			/* Record number */

	/*
	 * Next-random cursors can optionally be configured to step through a
	 * percentage of the total leaf pages to their next value. Note the
	 * configured value and the calculated number of leaf pages to skip.
	 */
	uint64_t next_random_leaf_skip;
	u_int	 next_random_sample_size;

	/*
	 * The search function sets compare to:
	 *	< 1 if the found key is less than the specified key
	 *	  0 if the found key matches the specified key
	 *	> 1 if the found key is larger than the specified key
	 */
	int	compare;

	/*
	 * A key returned from a binary search or cursor movement on a row-store
	 * page; if we find an exact match on a row-store leaf page in a search
	 * operation, keep a copy of key we built during the search to avoid
	 * doing the additional work of getting the key again for return to the
	 * application. Note, this only applies to exact matches when searching
	 * disk-image structures, so it's not, for example, a key from an insert
	 * list. Additionally, this structure is used to build keys when moving
	 * a cursor through a row-store leaf page.
	 */
	WT_ITEM *row_key, _row_key;

	/*
	 * It's relatively expensive to calculate the last record on a variable-
	 * length column-store page because of the repeat values.  Calculate it
	 * once per page and cache it.  This value doesn't include the skiplist
	 * of appended entries on the last page.
	 */
	uint64_t last_standard_recno;

	/*
	 * For row-store pages, we need a single item that tells us the part of
	 * the page we're walking (otherwise switching from next to prev and
	 * vice-versa is just too complicated), so we map the WT_ROW and
	 * WT_INSERT_HEAD insert array slots into a single name space: slot 1
	 * is the "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is
	 * WT_INSERT_HEAD[0], and so on.  This means WT_INSERT lists are
	 * odd-numbered slots, and WT_ROW array slots are even-numbered slots.
	 */
	uint32_t row_iteration_slot;	/* Row-store iteration slot */

	/*
	 * Variable-length column-store values are run-length encoded and may
	 * be overflow values or Huffman encoded. To avoid repeatedly reading
	 * overflow values or decompressing encoded values, process it once and
	 * store the result in a temporary buffer.  The cip_saved field is used
	 * to determine if we've switched columns since our last cursor call.
	 */
	WT_COL *cip_saved;		/* Last iteration reference */

	/*
	 * We don't instantiate prefix-compressed keys on pages where there's no
	 * Huffman encoding because we don't want to waste memory if only moving
	 * a cursor through the page, and it's faster to build keys while moving
	 * through the page than to roll-forward from a previously instantiated
	 * key (we don't instantiate all of the keys, just the ones at binary
	 * search points).  We can't use the application's WT_CURSOR key field
	 * as a copy of the last-returned key because it may have been altered
	 * by the API layer, for example, dump cursors.  Instead we store the
	 * last-returned key in a temporary buffer.  The rip_saved field is used
	 * to determine if the key in the temporary buffer has the prefix needed
	 * for building the current key.
	 */
	WT_ROW *rip_saved;		/* Last-returned key reference */

	/*
	 * A temporary buffer for caching RLE values for column-store files (if
	 * RLE is non-zero, then we don't unpack the value every time we move
	 * to the next cursor position, we re-use the unpacked value we stored
	 * here the first time we hit the value).
	 *
	 * A temporary buffer for building on-page keys when searching row-store
	 * files.
	 */
	WT_ITEM *tmp, _tmp;

	/*
	 * The update structure allocated by the row- and column-store modify
	 * functions, used to avoid a data copy in the WT_CURSOR.update call.
	 */
	WT_UPDATE *modify_update;

	/*
	 * Fixed-length column-store items are a single byte, and it's simpler
	 * and cheaper to allocate the space for it now than keep checking to
	 * see if we need to grow the buffer.
	 */
	uint8_t v;			/* Fixed-length return value */

	uint8_t	append_tree;		/* Cursor appended to the tree */

#ifdef HAVE_DIAGNOSTIC
	/* Check that cursor next/prev never returns keys out-of-order. */
	WT_ITEM *lastkey, _lastkey;
	uint64_t lastrecno;
#endif

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CBT_ACTIVE		0x001u	/* Active in the tree */
#define	WT_CBT_ITERATE_APPEND	0x002u	/* Col-store: iterating append list */
#define	WT_CBT_ITERATE_NEXT	0x004u	/* Next iteration configuration */
#define	WT_CBT_ITERATE_PREV	0x008u	/* Prev iteration configuration */
#define	WT_CBT_ITERATE_RETRY_NEXT	0x010u	/* Prepare conflict by next. */
#define	WT_CBT_ITERATE_RETRY_PREV	0x020u	/* Prepare conflict by prev. */
#define	WT_CBT_NO_TXN   	0x040u	/* Non-txn cursor (e.g. a checkpoint) */
#define	WT_CBT_READ_ONCE	0x080u	/* Page in with WT_READ_WONT_NEED */
#define	WT_CBT_SEARCH_SMALLEST	0x100u	/* Row-store: small-key insert list */
#define	WT_CBT_VAR_ONPAGE_MATCH	0x200u	/* Var-store: on-page recno match */
/* AUTOMATIC FLAG VALUE GENERATION STOP */

#define	WT_CBT_POSITION_MASK		/* Flags associated with position */ \
	(WT_CBT_ITERATE_APPEND | WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV | \
	WT_CBT_ITERATE_RETRY_NEXT | WT_CBT_ITERATE_RETRY_PREV |		     \
	WT_CBT_SEARCH_SMALLEST | WT_CBT_VAR_ONPAGE_MATCH)

	uint32_t flags;
};

struct __wt_cursor_bulk {
	WT_CURSOR_BTREE cbt;

	/*
	 * Variable-length column store compares values during bulk load as
	 * part of RLE compression, row-store compares keys during bulk load
	 * to avoid corruption.
	 */
	bool	first_insert;		/* First insert */
	WT_ITEM	last;			/* Last key/value inserted */

	/*
	 * Additional column-store bulk load support.
	 */
	uint64_t recno;			/* Record number */
	uint64_t rle;			/* Variable-length RLE counter */

	/*
	 * Additional fixed-length column store bitmap bulk load support:
	 * current entry in memory chunk count, and the maximum number of
	 * records per chunk.
	 */
	bool	 bitmap;		/* Bitmap bulk load */
	uint32_t entry;			/* Entry count */
	uint32_t nrecs;			/* Max records per chunk */

	void	*reconcile;		/* Reconciliation support */
	WT_REF	*ref;			/* The leaf page */
	WT_PAGE *leaf;
};

struct __wt_cursor_config {
	WT_CURSOR iface;
};

struct __wt_cursor_data_source {
	WT_CURSOR iface;

	WT_COLLATOR *collator;		/* Configured collator */
	int collator_owned;		/* Collator needs to be terminated */

	WT_CURSOR *source;		/* Application-owned cursor */
};

struct __wt_cursor_dump {
	WT_CURSOR iface;

	WT_CURSOR *child;
};

struct __wt_cursor_index {
	WT_CURSOR iface;

	WT_TABLE *table;
	WT_INDEX *index;
	const char *key_plan, *value_plan;

	WT_CURSOR *child;
	WT_CURSOR **cg_cursors;
	uint8_t	*cg_needvalue;
};

/*
 * A join iterator structure is used to generate candidate primary keys. It
 * is the responsibility of the caller of the iterator to filter these
 * primary key against the other conditions of the join before returning
 * them the caller of WT_CURSOR::next.
 *
 * For a conjunction join (the default), entry_count will be 1, meaning that
 * the iterator only consumes the first entry (WT_CURSOR_JOIN_ENTRY).  That
 * is, it successively returns primary keys from a cursor for the first
 * index that was joined.  When the values returned by that cursor are
 * exhausted, the iterator has completed.  For a disjunction join,
 * exhausting a cursor just means that the iterator advances to the next
 * entry. If the next entry represents an index, a new cursor is opened and
 * primary keys from that index are then successively returned.
 *
 * When positioned on an entry that represents a nested join, a new child
 * iterator is created that will be bound to the nested WT_CURSOR_JOIN.
 * That iterator is then used to generate candidate primary keys.  When its
 * iteration is completed, that iterator is destroyed and the parent
 * iterator advances to the next entry.  Thus, depending on how deeply joins
 * are nested, a similarly deep stack of iterators is created.
 */
struct __wt_cursor_join_iter {
	WT_SESSION_IMPL		*session;
	WT_CURSOR_JOIN		*cjoin;
	WT_CURSOR_JOIN_ENTRY	*entry;
	WT_CURSOR_JOIN_ITER	*child;
	WT_CURSOR		*cursor;	/* has null projection */
	WT_ITEM			*curkey;	/* primary key */
	WT_ITEM			 idxkey;
	u_int			 entry_pos;	/* the current entry */
	u_int			 entry_count;	/* entries to walk */
	u_int			 end_pos;	/* the current endpoint */
	u_int			 end_count;	/* endpoints to walk */
	u_int			 end_skip;	/* when testing for inclusion */
						/* can we skip current end? */
	bool			 positioned;
	bool			 is_equal;
};

/*
 * A join endpoint represents a positioned cursor that is 'captured' by a
 * WT_SESSION::join call.
 */
struct __wt_cursor_join_endpoint {
	WT_ITEM			 key;
	uint8_t			 recno_buf[10];	/* holds packed recno */
	WT_CURSOR		*cursor;

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CURJOIN_END_EQ		0x1u	/* include values == cursor */
#define	WT_CURJOIN_END_GT		0x2u	/* include values >  cursor */
#define	WT_CURJOIN_END_LT		0x4u	/* include values <  cursor */
#define	WT_CURJOIN_END_OWN_CURSOR	0x8u	/* must close cursor */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
#define	WT_CURJOIN_END_GE	(WT_CURJOIN_END_GT | WT_CURJOIN_END_EQ)
#define	WT_CURJOIN_END_LE	(WT_CURJOIN_END_LT | WT_CURJOIN_END_EQ)
	uint8_t			 flags;		/* range for this endpoint */
};
#define	WT_CURJOIN_END_RANGE(endp)					\
	((endp)->flags &						\
	    (WT_CURJOIN_END_GT | WT_CURJOIN_END_EQ | WT_CURJOIN_END_LT))

/*
 * Each join entry typically represents an index's participation in a join.
 * For example, if 'k' is an index, then "t.k > 10 && t.k < 20" would be
 * represented by a single entry, with two endpoints.  When the index and
 * subjoin fields are NULL, the join is on the main table.  When subjoin is
 * non-NULL, there is a nested join clause.
 */
struct __wt_cursor_join_entry {
	WT_INDEX		*index;
	WT_CURSOR		*main;		/* raw main table cursor */
	WT_CURSOR_JOIN		*subjoin;	/* a nested join clause */
	WT_BLOOM		*bloom;		/* Bloom filter handle */
	char			*repack_format; /* target format for repack */
	uint32_t		 bloom_bit_count; /* bits per item in bloom */
	uint32_t		 bloom_hash_count; /* hash functions in bloom */
	uint64_t		 count;		/* approx number of matches */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CURJOIN_ENTRY_BLOOM		 0x1u	/* use a bloom filter */
#define	WT_CURJOIN_ENTRY_DISJUNCTION	 0x2u	/* endpoints are or-ed */
#define	WT_CURJOIN_ENTRY_FALSE_POSITIVES 0x4u	/* don't filter false pos */
#define	WT_CURJOIN_ENTRY_OWN_BLOOM	 0x8u	/* this entry owns the bloom */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t			 flags;

	WT_CURSOR_JOIN_ENDPOINT	*ends;		/* reference endpoints */
	size_t			 ends_allocated;
	u_int			 ends_next;

	WT_JOIN_STATS		 stats;		/* Join statistics */
};

struct __wt_cursor_join {
	WT_CURSOR iface;

	WT_TABLE		*table;
	const char		*projection;
	WT_CURSOR		*main;		/* main table with projection */
	WT_CURSOR_JOIN		*parent;	/* parent of nested group */
	WT_CURSOR_JOIN_ITER	*iter;		/* chain of iterators */
	WT_CURSOR_JOIN_ENTRY	*entries;
	size_t			 entries_allocated;
	u_int			 entries_next;
	uint8_t			 recno_buf[10];	/* holds packed recno */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CURJOIN_DISJUNCTION		0x1u	/* Entries are or-ed */
#define	WT_CURJOIN_ERROR		0x2u	/* Error in initialization */
#define	WT_CURJOIN_INITIALIZED		0x4u	/* Successful initialization */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t			 flags;
};

struct __wt_cursor_json {
	char	*key_buf;		/* JSON formatted string */
	char	*value_buf;		/* JSON formatted string */
	WT_CONFIG_ITEM key_names;	/* Names of key columns */
	WT_CONFIG_ITEM value_names;	/* Names of value columns */
};

struct __wt_cursor_log {
	WT_CURSOR iface;

	WT_LSN		*cur_lsn;	/* LSN of current record */
	WT_LSN		*next_lsn;	/* LSN of next record */
	WT_ITEM		*logrec;	/* Copy of record for cursor */
	WT_ITEM		*opkey, *opvalue;	/* Op key/value copy */
	const uint8_t	*stepp, *stepp_end;	/* Pointer within record */
	uint8_t		*packed_key;	/* Packed key for 'raw' interface */
	uint8_t		*packed_value;	/* Packed value for 'raw' interface */
	uint32_t	step_count;	/* Intra-record count */
	uint32_t	rectype;	/* Record type */
	uint64_t	txnid;		/* Record txnid */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_CURLOG_ARCHIVE_LOCK	0x1u	/* Archive lock held */
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t		flags;
};

struct __wt_cursor_metadata {
	WT_CURSOR iface;

	WT_CURSOR *file_cursor;		/* Queries of regular metadata */
	WT_CURSOR *create_cursor;	/* Extra cursor for create option */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_MDC_CREATEONLY	0x1u
#define	WT_MDC_ONMETADATA	0x2u
#define	WT_MDC_POSITIONED	0x4u
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint8_t	flags;
};

struct __wt_join_stats_group {
	const char *desc_prefix;	/* Prefix appears before description */
	WT_CURSOR_JOIN *join_cursor;
	ssize_t join_cursor_entry;	/* Position in entries */
	WT_JOIN_STATS join_stats;
};

struct __wt_cursor_stat {
	WT_CURSOR iface;

	bool	notinitialized;		/* Cursor not initialized */
	bool	notpositioned;		/* Cursor not positioned */

	int64_t	     *stats;		/* Statistics */
	int	      stats_base;	/* Base statistics value */
	int	      stats_count;	/* Count of statistics values */
	int	     (*stats_desc)(WT_CURSOR_STAT *, int, const char **);
					/* Statistics descriptions */
	int	     (*next_set)(WT_SESSION_IMPL *, WT_CURSOR_STAT *, bool,
			 bool);		/* Advance to next set */

	union {				/* Copies of the statistics */
		WT_DSRC_STATS dsrc_stats;
		WT_CONNECTION_STATS conn_stats;
		WT_JOIN_STATS_GROUP join_stats_group;
		WT_SESSION_STATS session_stats;
	} u;

	const char **cfg;		/* Original cursor configuration */
	char	*desc_buf;		/* Saved description string */

	int	 key;			/* Current stats key */
	uint64_t v;			/* Current stats value */
	WT_ITEM	 pv;			/* Current stats value (string) */

	/* Options declared in flags.py, shared by WT_CONNECTION::stat_flags */
	uint32_t flags;
};

/*
 * WT_CURSOR_STATS --
 *	Return a reference to a statistic cursor's stats structures.
 */
#define	WT_CURSOR_STATS(cursor)						\
	(((WT_CURSOR_STAT *)(cursor))->stats)

struct __wt_cursor_table {
	WT_CURSOR iface;

	WT_TABLE *table;
	const char *plan;

	const char **cfg;		/* Saved configuration string */

	WT_CURSOR **cg_cursors;
	WT_ITEM *cg_valcopy;		/*
					 * Copies of column group values, for
					 * overlapping set_value calls.
					 */
	WT_CURSOR **idx_cursors;
};

#define	WT_CURSOR_PRIMARY(cursor)					\
	(((WT_CURSOR_TABLE *)(cursor))->cg_cursors[0])

#define	WT_CURSOR_RECNO(cursor)	WT_STREQ((cursor)->key_format, "r")

#define	WT_CURSOR_RAW_OK						\
	(WT_CURSTD_DUMP_HEX | WT_CURSTD_DUMP_PRINT | WT_CURSTD_RAW)
