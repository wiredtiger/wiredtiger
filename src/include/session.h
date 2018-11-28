/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
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
	WT_REF *ref;			/* Page reference */
#ifdef HAVE_DIAGNOSTIC
	const char *func;		/* Function/line hazard acquired */
	int	    line;
#endif
};

/* Get the connection implementation for a session */
#define	S2C(session)	  ((WT_CONNECTION_IMPL *)(session)->iface.connection)

/* Get the btree for a session */
#define	S2BT(session)	   ((WT_BTREE *)(session)->dhandle->handle)
#define	S2BT_SAFE(session) ((session)->dhandle == NULL ? NULL : S2BT(session))

typedef TAILQ_HEAD(__wt_cursor_list, __wt_cursor)	WT_CURSOR_LIST;

/* Number of cursors cached to trigger cursor sweep. */
#define	WT_SESSION_CURSOR_SWEEP_COUNTDOWN	20

/* Minimum number of buckets to visit during cursor sweep. */
#define	WT_SESSION_CURSOR_SWEEP_MIN		5

/* Maximum number of buckets to visit during cursor sweep. */
#define	WT_SESSION_CURSOR_SWEEP_MAX		32
/*
 * WT_SESSION_IMPL --
 *	Implementation of WT_SESSION.
 */
struct __wt_session_impl {
	WT_SESSION iface;

	void	*lang_private;		/* Language specific private storage */

	u_int active;			/* Non-zero if the session is in-use */

	const char *name;		/* Name */
	const char *lastop;		/* Last operation */
	uint32_t id;			/* UID, offset in session array */

	WT_EVENT_HANDLER *event_handler;/* Application's event handlers */

	WT_DATA_HANDLE *dhandle;	/* Current data handle */

	/*
	 * Each session keeps a cache of data handles. The set of handles can
	 * grow quite large so we maintain both a simple list and a hash table
	 * of lists. The hash table key is based on a hash of the data handle's
	 * URI.  The hash table list is kept in allocated memory that lives
	 * across session close - so it is declared further down.
	 */
					/* Session handle reference list */
	TAILQ_HEAD(__dhandles, __wt_data_handle_cache) dhandles;
	time_t last_sweep;		/* Last sweep for dead handles */
	struct timespec last_epoch;	/* Last epoch time returned */

	WT_CURSOR_LIST cursors;		/* Cursors closed with the session */
	uint32_t cursor_sweep_position;	/* Position in cursor_cache for sweep */
	uint32_t cursor_sweep_countdown;/* Countdown to cursor sweep */
	time_t last_cursor_sweep;	/* Last sweep for dead cursors */

	WT_CURSOR_BACKUP *bkp_cursor;	/* Hot backup cursor */

	WT_COMPACT_STATE *compact;	/* Compaction information */
	enum { WT_COMPACT_NONE=0,
	    WT_COMPACT_RUNNING, WT_COMPACT_SUCCESS } compact_state;

	WT_CURSOR	*las_cursor;	/* Lookaside table cursor */

	WT_CURSOR *meta_cursor;		/* Metadata file */
	void	  *meta_track;		/* Metadata operation tracking */
	void	  *meta_track_next;	/* Current position */
	void	  *meta_track_sub;	/* Child transaction / save point */
	size_t	   meta_track_alloc;	/* Currently allocated */
	int	   meta_track_nest;	/* Nesting level of meta transaction */
#define	WT_META_TRACKING(session)	((session)->meta_track_next != NULL)

	/* Current rwlock for callback. */
	WT_RWLOCK *current_rwlock;
	uint8_t current_rwticket;

	WT_ITEM	**scratch;		/* Temporary memory for any function */
	u_int	  scratch_alloc;	/* Currently allocated */
	size_t	  scratch_cached;	/* Scratch bytes cached */
#ifdef HAVE_DIAGNOSTIC
	/*
	 * Variables used to look for violations of the contract that a
	 * session is only used by a single session at once.
	 */
	volatile uintmax_t api_tid;
	volatile uint32_t api_enter_refcnt;
	/*
	 * It's hard to figure out from where a buffer was allocated after it's
	 * leaked, so in diagnostic mode we track them; DIAGNOSTIC can't simply
	 * add additional fields to WT_ITEM structures because they are visible
	 * to applications, create a parallel structure instead.
	 */
	struct __wt_scratch_track {
		const char *func;	/* Allocating function, line */
		int line;
	} *scratch_track;
#endif

	WT_ITEM err;			/* Error buffer */

	WT_TXN_ISOLATION isolation;
	WT_TXN	txn;			/* Transaction state */
#define	WT_SESSION_BG_SYNC_MSEC		1200000
	WT_LSN	bg_sync_lsn;		/* Background sync operation LSN. */
	u_int	ncursors;		/* Count of active file cursors. */

	void	*block_manager;		/* Block-manager support */
	int	(*block_manager_cleanup)(WT_SESSION_IMPL *);

					/* Checkpoint handles */
	WT_DATA_HANDLE **ckpt_handle;	/* Handle list */
	u_int   ckpt_handle_next;	/* Next empty slot */
	size_t  ckpt_handle_allocated;	/* Bytes allocated */

	uint64_t cache_wait_us; /* Wait time for cache for current operation */

	/*
	 * Operations acting on handles.
	 *
	 * The preferred pattern is to gather all of the required handles at
	 * the beginning of an operation, then drop any other locks, perform
	 * the operation, then release the handles.  This cannot be easily
	 * merged with the list of checkpoint handles because some operations
	 * (such as compact) do checkpoints internally.
	 */
	WT_DATA_HANDLE **op_handle;	/* Handle list */
	u_int   op_handle_next;		/* Next empty slot */
	size_t  op_handle_allocated;	/* Bytes allocated */

	void	*reconcile;		/* Reconciliation support */
	int	(*reconcile_cleanup)(WT_SESSION_IMPL *);

	/* Sessions have an associated statistics bucket based on its ID. */
	u_int	stat_bucket;		/* Statistics bucket offset */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define	WT_SESSION_BACKUP_CURSOR		0x0000001u
#define	WT_SESSION_BACKUP_DUP			0x0000002u
#define	WT_SESSION_CACHE_CURSORS		0x0000004u
#define	WT_SESSION_CAN_WAIT			0x0000008u
#define	WT_SESSION_IGNORE_CACHE_SIZE		0x0000010u
#define	WT_SESSION_INTERNAL			0x0000020u
#define	WT_SESSION_LOCKED_CHECKPOINT		0x0000040u
#define	WT_SESSION_LOCKED_HANDLE_LIST_READ	0x0000080u
#define	WT_SESSION_LOCKED_HANDLE_LIST_WRITE	0x0000100u
#define	WT_SESSION_LOCKED_METADATA		0x0000200u
#define	WT_SESSION_LOCKED_PASS			0x0000400u
#define	WT_SESSION_LOCKED_SCHEMA		0x0000800u
#define	WT_SESSION_LOCKED_SLOT			0x0001000u
#define	WT_SESSION_LOCKED_TABLE_READ		0x0002000u
#define	WT_SESSION_LOCKED_TABLE_WRITE		0x0004000u
#define	WT_SESSION_LOCKED_TURTLE		0x0008000u
#define	WT_SESSION_LOGGING_INMEM		0x0010000u
#define	WT_SESSION_LOOKASIDE_CURSOR		0x0020000u
#define	WT_SESSION_NO_DATA_HANDLES		0x0040000u
#define	WT_SESSION_NO_LOGGING			0x0080000u
#define	WT_SESSION_NO_RECONCILE			0x0100000u
#define	WT_SESSION_NO_SCHEMA_LOCK		0x0200000u
#define	WT_SESSION_QUIET_CORRUPT_FILE		0x0400000u
#define	WT_SESSION_READ_WONT_NEED		0x0800000u
#define	WT_SESSION_SCHEMA_TXN			0x1000000u
#define	WT_SESSION_SERVER_ASYNC			0x2000000u
/* AUTOMATIC FLAG VALUE GENERATION STOP */
	uint32_t flags;

	/*
	 * All of the following fields live at the end of the structure so it's
	 * easier to clear everything but the fields that persist.
	 */
#define	WT_SESSION_CLEAR_SIZE	(offsetof(WT_SESSION_IMPL, rnd))

	/*
	 * The random number state persists past session close because we don't
	 * want to repeatedly use the same values for skiplist depth when the
	 * application isn't caching sessions.
	 */
	WT_RAND_STATE rnd;		/* Random number generation state */

	/*
	 * Hash tables are allocated lazily as sessions are used to keep the
	 * size of this structure from growing too large.
	 */
	WT_CURSOR_LIST *cursor_cache;	/* Hash table of cached cursors */

					/* Hashed handle reference list array */
	TAILQ_HEAD(__dhandles_hash, __wt_data_handle_cache) *dhhash;

					/* Generations manager */
#define	WT_GEN_CHECKPOINT	0	/* Checkpoint generation */
#define	WT_GEN_COMMIT		1	/* Commit generation */
#define	WT_GEN_EVICT		2	/* Eviction generation */
#define	WT_GEN_HAZARD		3	/* Hazard pointer */
#define	WT_GEN_SPLIT		4	/* Page splits */
#define	WT_GENERATIONS		5	/* Total generation manager entries */
	volatile uint64_t generations[WT_GENERATIONS];

	/*
	 * Session memory persists past session close because it's accessed by
	 * threads of control other than the thread owning the session. For
	 * example, btree splits and hazard pointers can "free" memory that's
	 * still in use. In order to eventually free it, it's stashed here with
	 * with its generation number; when no thread is reading in generation,
	 * the memory can be freed for real.
	 */
	struct __wt_session_stash {
		struct __wt_stash {
			void	*p;	/* Memory, length */
			size_t	 len;
			uint64_t gen;	/* Generation */
		} *list;
		size_t  cnt;		/* Array entries */
		size_t  alloc;		/* Allocated bytes */
	} stash[WT_GENERATIONS];

	/*
	 * Hazard pointers.
	 *
	 * Hazard information persists past session close because it's accessed
	 * by threads of control other than the thread owning the session.
	 *
	 * Use the non-NULL state of the hazard field to know if the session has
	 * previously been initialized.
	 */
#define	WT_SESSION_FIRST_USE(s)						\
	((s)->hazard == NULL)

	/*
	 * The hazard pointer array grows as necessary, initialize with 250
	 * slots.
	 */
#define	WT_SESSION_INITIAL_HAZARD_SLOTS	250
	uint32_t   hazard_size;		/* Hazard pointer array slots */
	uint32_t   hazard_inuse;	/* Hazard pointer array slots in-use */
	uint32_t   nhazard;		/* Count of active hazard pointers */
	WT_HAZARD *hazard;		/* Hazard pointer array */

	/*
	 * Operation tracking.
	 */
	WT_OPTRACK_RECORD *optrack_buf;
	u_int optrackbuf_ptr;
	uint64_t optrack_offset;
	WT_FH *optrack_fh;

	WT_SESSION_STATS stats;
};
