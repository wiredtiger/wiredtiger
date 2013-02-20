/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*******************************************
 * Global per-process structure.
 *******************************************/
/*
 * WT_PROCESS --
 *	Per-process information for the library.
 */
struct __wt_process {
	WT_SPINLOCK spinlock;		/* Per-process spinlock */

					/* Locked: connection queue */
	TAILQ_HEAD(__wt_connection_impl_qh, __wt_connection_impl) connqh;
	WT_CACHE_POOL *cache_pool;
};
extern WT_PROCESS __wt_process;

/*
 * WT_NAMED_COLLATOR --
 *	A collator list entry
 */
struct __wt_named_collator {
	const char *name;		/* Name of collator */
	WT_COLLATOR *collator;		/* User supplied object */
	TAILQ_ENTRY(__wt_named_collator) q;	/* Linked list of collators */
};

/*
 * WT_NAMED_COMPRESSOR --
 *	A compressor list entry
 */
struct __wt_named_compressor {
	const char *name;		/* Name of compressor */
	WT_COMPRESSOR *compressor;	/* User supplied callbacks */
					/* Linked list of compressors */
	TAILQ_ENTRY(__wt_named_compressor) q;
};

/*
 * WT_NAMED_DATA_SOURCE --
 *	A data source list entry
 */
struct __wt_named_data_source {
	const char *prefix;		/* Name of data source */
	WT_DATA_SOURCE *dsrc;		/* User supplied callbacks */
					/* Linked list of data sources */
	TAILQ_ENTRY(__wt_named_data_source) q;
};

/*
 * Allocate some additional slots for internal sessions.  There is a default
 * session for each connection, plus a session for the eviction thread.
 */
#define	WT_NUM_INTERNAL_SESSIONS	2

/*
 * WT_CONNECTION_IMPL --
 *	Implementation of WT_CONNECTION
 */
struct __wt_connection_impl {
	WT_CONNECTION iface;

	/* For operations without an application-supplied session */
	WT_SESSION_IMPL *default_session;
	WT_SESSION_IMPL  dummy_session;

	WT_SPINLOCK api_lock;		/* Connection API spinlock */
	WT_SPINLOCK fh_lock;		/* File handle queue spinlock */
	WT_SPINLOCK metadata_lock;	/* Metadata spinlock */
	WT_SPINLOCK schema_lock;	/* Schema operation spinlock */
	WT_SPINLOCK serial_lock;	/* Serial function call spinlock */

	int ckpt_backup;		/* Backup: don't delete checkpoints */

					/* Connection queue */
	TAILQ_ENTRY(__wt_connection_impl) q;
					/* Cache pool queue */
	TAILQ_ENTRY(__wt_connection_impl) cpq;

	const char *home;		/* Database home */
	int is_new;			/* Connection created database */

	WT_FH *lock_fh;			/* Lock file handle */

	pthread_t cache_evict_tid;	/* Cache eviction server thread ID */

					/* Locked: btree list */
	TAILQ_HEAD(__wt_btree_qh, __wt_btree) btqh;
					/* Locked: LSM handle list. */
	TAILQ_HEAD(__wt_lsm_qh, __wt_lsm_tree) lsmqh;
					/* Locked: file list */
	TAILQ_HEAD(__wt_fh_qh, __wt_fh) fhqh;
					/* Locked: library list */
	TAILQ_HEAD(__wt_dlh_qh, __wt_dlh) dlhqh;

	WT_SPINLOCK block_lock;		/* Locked: block manager list */
	TAILQ_HEAD(__wt_block_qh, __wt_block) blockqh;

	u_int open_btree_count;		/* Locked: open writable btree count */
	u_int next_file_id;		/* Locked: file ID counter */

	/*
	 * WiredTiger allocates space for 50 simultaneous sessions (threads of
	 * control) by default.  Growing the number of threads dynamically is
	 * possible, but tricky since server threads are walking the array
	 * without locking it.
	 *
	 * There's an array of WT_SESSION_IMPL pointers that reference the
	 * allocated array; we do it that way because we want an easy way for
	 * the server thread code to avoid walking the entire array when only a
	 * few threads are running.
	 */
	WT_SESSION_IMPL	*sessions;	/* Session reference */
	uint32_t	 session_size;	/* Session array size */
	uint32_t	 session_cnt;	/* Session count */

	/*
	 * WiredTiger allocates space for a fixed number of hazard pointers
	 * in each thread of control.
	 */
	uint32_t   hazard_max;		/* Hazard array size */

	WT_CACHE  *cache;		/* Page cache */
	uint64_t   cache_size;

	WT_TXN_GLOBAL txn_global;	/* Global transaction state. */

	WT_CONNECTION_STATS *stats;	/* Connection statistics */
	WT_SESSION_IMPL *stat_session;	/* Statistics log session */
	pthread_t	 stat_tid;	/* Statistics log thread */
	WT_CONDVAR	*stat_cond;	/* Statistics log wait mutex */
	int		 stat_clear;	/* Statistics log clear */
	const char	*stat_path;	/* Statistics log path format */
	const char	*stat_stamp;	/* Statistics log timestamp format */
	long		 stat_usecs;	/* Statistics log period */

	WT_FH	   *log_fh;		/* Logging file handle */

					/* Locked: collator list */
	TAILQ_HEAD(__wt_coll_qh, __wt_named_collator) collqh;

					/* Locked: compressor list */
	TAILQ_HEAD(__wt_comp_qh, __wt_named_compressor) compqh;

					/* Locked: data source list */
	TAILQ_HEAD(__wt_dsrc_qh, __wt_named_data_source) dsrcqh;

	FILE *msgfile;
	void (*msgcall)(const WT_CONNECTION_IMPL *, const char *);

	/* If non-zero, all buffers used for I/O will be aligned to this. */
	size_t buffer_alignment;

	uint32_t direct_io;		/* O_DIRECT configuration */
	int	 mmap;			/* mmap configuration */
	uint32_t verbose;

	uint32_t flags;
};
