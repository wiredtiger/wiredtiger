/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_DATA_HANDLE_CACHE --
 *	Per-session cache of handles to avoid synchronization when opening
 *	cursors.
 */
struct __wt_data_handle_cache {
	WT_DATA_HANDLE *dhandle;

	TAILQ_ENTRY(__wt_data_handle_cache) q;
	TAILQ_ENTRY(__wt_data_handle_cache) hashq;
};

/*
 * WT_HAZARD --
 *	A hazard pointer.
 */
struct __wt_hazard {
	WT_PAGE *page;			/* Page address */
#ifdef HAVE_DIAGNOSTIC
	const char *file;		/* File/line where hazard acquired */
	int	    line;
#endif
};

/* Get the connection implementation for a session */
#define	S2C(session)	  ((WT_CONNECTION_IMPL *)(session)->iface.connection)

/* Get the btree for a session */
#define	S2BT(session)	   ((WT_BTREE *)(session)->dhandle->handle)
#define	S2BT_SAFE(session) ((session)->dhandle == NULL ? NULL : S2BT(session))

/*
 * WT_OP_TRACKER_ENTRY --
 *	Per-cursor structure used to track timing for individual operations.
 */
struct __wt_op_tracker_entry {
	/*
	 * Non API operation types. The API operation type descriptors are
	 * automatically generated via dist/op_track.py. Allocate these
	 * identifiers about 1000 to avoid namespace conflicts.
	 */
#define	WT_OP_TYPE_EVICT_APP		1000
#define	WT_OP_TYPE_EVICT_CHECK		1001
#define	WT_OP_TYPE_EVICT_FORCE		1002
#define	WT_OP_TYPE_EVICT_PAGE		1003
#define	WT_OP_TYPE_EVICT_PARENT_SPLIT	1004
#define	WT_OP_TYPE_IO_ASYNC		1005
#define	WT_OP_TYPE_IO_DSYNC		1006
#define	WT_OP_TYPE_IO_FSYNC		1007
#define	WT_OP_TYPE_IO_READ		1008
#define	WT_OP_TYPE_IO_WRITE		1009
#define	WT_OP_TYPE_PAGE_IN		1010
#define	WT_OP_TYPE_RECONCILE_BULK	1011
#define	WT_OP_TYPE_TXN_BEGIN_CHECK	1012
#define	WT_OP_TYPE_TXN_COMMIT		1013
#define	WT_OP_TYPE_TXN_ROLLBACK		1014
	uint32_t type;

	struct timespec end, start;	/* Begin and end time stamps */
	struct timespec last_start;	/* Record when the child finishes */
	uint64_t start_offset_ns;	/* Time since parent started */
	uint64_t self_time_ns;		/* Time consumed by this operation */
	WT_ITEM *msg;			/* Optional additional information */
	int api_boundary;
	int depth;			/* Nesting depth */
	int done;

	TAILQ_ENTRY(__wt_op_tracker_entry) q;	/* Queue of operations */
	TAILQ_ENTRY(__wt_op_tracker_entry) aq;	/* Available queue */
};

/*
 * WT_SESSION_IMPL --
 *	Implementation of WT_SESSION.
 */
struct WT_COMPILER_TYPE_ALIGN(WT_CACHE_LINE_ALIGNMENT) __wt_session_impl {
	WT_SESSION iface;

	void	*lang_private;		/* Language specific private storage */

	u_int active;			/* Non-zero if the session is in-use */

	const char *name;		/* Name */
	const char *lastop;		/* Last operation */
	uint32_t id;			/* UID, offset in session array */

	WT_CONDVAR *cond;		/* Condition variable */

	WT_EVENT_HANDLER *event_handler;/* Application's event handlers */

	WT_DATA_HANDLE *dhandle;	/* Current data handle */

	/*
	 * Each session keeps a cache of data handles. The set of handles
	 * can grow quite large so we maintain both a simple list and a hash
	 * table of lists. The hash table key is based on a hash of the table
	 * URI. The hash table list is kept in allocated memory that lives
	 * across session close - so it is declared further down.
	 */
					/* Session handle reference list */
	TAILQ_HEAD(__dhandles, __wt_data_handle_cache) dhandles;
	time_t last_sweep;		/* Last sweep for dead handles */

	WT_CURSOR *cursor;		/* Current cursor */
					/* Cursors closed with the session */
	TAILQ_HEAD(__cursors, __wt_cursor) cursors;

	WT_CURSOR_BACKUP *bkp_cursor;	/* Hot backup cursor */
	WT_COMPACT	 *compact;	/* Compact state */

	/*
	 * Lookaside table cursor, sweep and eviction worker threads only.
	 */
	WT_CURSOR	*las_cursor;	/* Lookaside table cursor */

	WT_DATA_HANDLE *meta_dhandle;	/* Metadata file */
	void	*meta_track;		/* Metadata operation tracking */
	void	*meta_track_next;	/* Current position */
	void	*meta_track_sub;	/* Child transaction / save point */
	size_t	 meta_track_alloc;	/* Currently allocated */
	int	 meta_track_nest;	/* Nesting level of meta transaction */
#define	WT_META_TRACKING(session)	(session->meta_track_next != NULL)

	/*
	 * Each session keeps a cache of table handles. The set of handles
	 * can grow quite large so we maintain both a simple list and a hash
	 * table of lists. The hash table list is kept in allocated memory
	 * that lives across session close - so it is declared further down.
	 */
	TAILQ_HEAD(__tables, __wt_table) tables;

	WT_ITEM	**scratch;		/* Temporary memory for any function */
	u_int	  scratch_alloc;	/* Currently allocated */
	size_t	  scratch_cached;	/* Scratch bytes cached */
#ifdef HAVE_DIAGNOSTIC
	/*
	 * It's hard to figure out from where a buffer was allocated after it's
	 * leaked, so in diagnostic mode we track them; DIAGNOSTIC can't simply
	 * add additional fields to WT_ITEM structures because they are visible
	 * to applications, create a parallel structure instead.
	 */
	struct __wt_scratch_track {
		const char *file;	/* Allocating file, line */
		int line;
	} *scratch_track;
#endif

	WT_ITEM err;			/* Error buffer */

	WT_TXN_ISOLATION isolation;
	WT_TXN	txn;			/* Transaction state */
	WT_LSN	bg_sync_lsn;		/* Background sync operation LSN. */
	u_int	ncursors;		/* Count of active file cursors. */

	void	*block_manager;		/* Block-manager support */
	int	(*block_manager_cleanup)(WT_SESSION_IMPL *);

					/* Checkpoint support */
	struct {
		WT_DATA_HANDLE *dhandle;
		const char *name;
	} *ckpt_handle;			/* Handle list */
	u_int   ckpt_handle_next;	/* Next empty slot */
	size_t  ckpt_handle_allocated;	/* Bytes allocated */

	void	*reconcile;		/* Reconciliation support */
	int	(*reconcile_cleanup)(WT_SESSION_IMPL *);

	int compaction;			/* Compaction did some work */

	uint32_t flags;

	/*
	 * The split stash memory and hazard information persist past session
	 * close because they are accessed by threads of control other than the
	 * thread owning the session.
	 *
	 * The random number state persists past session close because we don't
	 * want to repeatedly allocate repeated values for skiplist depth if the
	 * application isn't caching sessions.
	 *
	 * All of these fields live at the end of the structure so it's easier
	 * to clear everything but the fields that persist.
	 */
#define	WT_SESSION_CLEAR_SIZE(s)					\
	(WT_PTRDIFF(&(s)->rnd, s))

	WT_RAND_STATE rnd;		/* Random number generation state */

					/* Hashed handle reference list array */
	TAILQ_HEAD(__dhandles_hash, __wt_data_handle_cache) *dhhash;
					/* Hashed table reference list array */
	TAILQ_HEAD(__tables_hash, __wt_table) *tablehash;

	/*
	 * Splits can "free" memory that may still be in use, and we use a
	 * split generation number to track it, that is, the session stores a
	 * reference to the memory and allocates a split generation; when no
	 * session is reading from that split generation, the memory can be
	 * freed for real.
	 */
	struct __wt_split_stash {
		uint64_t    split_gen;	/* Split generation */
		void       *p;		/* Memory, length */
		size_t	    len;
	} *split_stash;			/* Split stash array */
	size_t  split_stash_cnt;	/* Array entries */
	size_t  split_stash_alloc;	/* Allocated bytes */

	uint64_t split_gen;		/* Reading split generation */

	/*
	 * Structure used to store timing information about a single operation
	 * on a cursor.
	 */
	uint64_t api_call_depth;
	uint64_t op_trace_min;
	TAILQ_HEAD(__op_tracker, __wt_op_tracker_entry) op_trackerq;
	/* Don't reallocate slow ops all the time. */
	TAILQ_HEAD(__op_tracker_avail, __wt_op_tracker_entry) op_tracker_availq;

	/*
	 * Hazard pointers.
	 *
	 * Use the non-NULL state of the hazard field to know if the session has
	 * previously been initialized.
	 */
#define	WT_SESSION_FIRST_USE(s)						\
	((s)->hazard == NULL)

	/* The number of hazard pointers grows dynamically. */
#define	WT_HAZARD_INCR		10
	uint32_t   hazard_size;		/* Allocated slots in hazard array. */
	uint32_t   nhazard;		/* Count of active hazard pointers */
	WT_HAZARD *hazard;		/* Hazard pointer array */
};
