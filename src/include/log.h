/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	WT_LOG_FILENAME	"WiredTigerLog"		/* Log file name */

/* Logging subsystem declarations. */
#define	LOG_ALIGN		128
#define	WT_LOG_SLOT_BUF_INIT_SIZE	64 * 1024

/*
 * We rely on this structure being aligned at 64 bits by the compiler,
 * if we were paranoid we could add an unused field to ensure the padding
 * is correct.
 */
struct __wt_lsn {
	uint32_t	file;		/* Log file number */
	off_t		offset;		/* Log file offset */
};

#define	INIT_LSN(l)	do {						\
	(l)->file = 1;							\
	(l)->offset = 0;						\
} while (0)

#define	IS_INIT_LSN(l)	((l)->file == 1 && (l)->offset == 0)

#define	MAX_LSN(l)	do {						\
	(l)->file = UINT32_MAX;						\
	(l)->offset = INT64_MAX;					\
} while (0)

/*
 * Compare 2 LSNs, return -1 if lsn0 < lsn1, 0 if lsn0 == lsn1
 * and 1 if lsn0 > lsn1.
 */
#define	LOG_CMP(lsn1, lsn2)						\
	((lsn1)->file != (lsn2)->file ?                                 \
	((lsn1)->file < (lsn2)->file ? -1 : 1) :                        \
	((lsn1)->offset != (lsn2)->offset ?                             \
	((lsn1)->offset < (lsn2)->offset ? -1 : 1) : 0))

/*
 * Possible values for the consolidation array slot states:
 * < WT_LOG_SLOT_DONE - threads are actively writing to the log.
 * WT_LOG_SLOT_DONE - all activity on this slot is complete.
 * WT_LOG_SLOT_FREE - slot is available for allocation.
 * WT_LOG_SLOT_PENDING - slot is transitioning from ready to active.
 * WT_LOG_SLOT_READY - slot is ready for threads to join.
 * > WT_LOG_SLOT_READY - threads are actively consolidating on this slot.
 */
#define	WT_LOG_SLOT_DONE	0
#define	WT_LOG_SLOT_FREE	1
#define	WT_LOG_SLOT_PENDING	2
#define	WT_LOG_SLOT_READY	3
typedef struct {
	int64_t	 slot_state;		/* Slot state */
	uint64_t slot_group_size;	/* Group size */
	int32_t	 slot_error;		/* Error value */
#define	SLOT_INVALID_INDEX	0xffffffff
	uint32_t slot_index;		/* Active slot index */
	off_t	 slot_start_offset;	/* Starting file offset */
	WT_LSN	slot_release_lsn;	/* Slot release LSN */
	WT_LSN	slot_start_lsn;	/* Slot starting LSN */
	WT_LSN	slot_end_lsn;	/* Slot ending LSN */
	WT_FH	*slot_fh;		/* File handle for this group */
	WT_ITEM slot_buf;		/* Buffer for grouped writes */
	WT_CONDVAR *slot_done_cond;	/* Signalled when write done */
	int32_t	slot_churn;		/* Active slots are scarce. */
#define	SLOT_BUF_GROW	0x01			/* Grow buffer on release */
#define	SLOT_BUFFERED	0x02			/* Buffer writes */
#define	SLOT_CLOSEFH	0x04			/* Close old fh on release */
#define	SLOT_SYNC	0x08			/* Needs sync on release */
	uint32_t flags;		/* Flags */
} WT_LOGSLOT WT_GCC_ATTRIBUTE((aligned(WT_CACHE_LINE_ALIGNMENT)));

typedef struct {
	WT_LOGSLOT	*slot;
	off_t		 offset;
} WT_MYSLOT;

#define	LOG_FIRST_RECORD	log->allocsize	/* Offset of first record */

typedef struct {
	uint32_t	allocsize;	/* Allocation alignment size */
	/*
	 * Log file information
	 */
	uint32_t	 fileid;	/* Current log file number */
	WT_FH           *log_fh;	/* Logging file handle */
	WT_FH           *log_close_fh;	/* Logging file handle to close */

	/*
	 * System LSNs
	 */
	WT_LSN		alloc_lsn;	/* Next LSN for allocation */
	WT_LSN		ckpt_lsn;	/* Last checkpoint LSN */
	WT_LSN		first_lsn;	/* First LSN */
	WT_LSN		sync_lsn;	/* LSN of the last sync */
	WT_LSN		trunc_lsn;	/* End LSN for recovery truncation */
	WT_LSN		write_lsn;	/* Last LSN written to log file */

	/*
	 * Synchronization resources
	 */
	WT_SPINLOCK      log_lock;      /* Locked: Logging fields */
	WT_SPINLOCK      log_slot_lock; /* Locked: Consolidation array */

	/* Notify any waiting slots when write_lsn is updated. */
	WT_CONDVAR	*log_release_cond;

	/*
	 * Consolidation array information
	 * SLOT_ACTIVE must be less than SLOT_POOL.
	 * Our testing shows that the more consolidation we generate the
	 * better the performance we see which equates to an active slot
	 * slot count of one.
	 */
#define	SLOT_ACTIVE	1
#define	SLOT_POOL	16
	uint32_t	 pool_index;		/* Global pool index */
	WT_LOGSLOT	*slot_array[SLOT_ACTIVE];	/* Active slots */
	WT_LOGSLOT	 slot_pool[SLOT_POOL];	/* Pool of all slots */

#define	WT_LOG_FORCE_CONSOLIDATE	0x01	/* Disable direct writes */
	uint32_t	 flags;
} WT_LOG;

typedef struct {
	uint32_t	len;		/* 00-03: Record length including hdr */
	uint32_t	checksum;	/* 04-07: Checksum of the record */
	uint8_t		unused[8];	/* 08-15: Padding */
	uint8_t		record[0];	/* Beginning of actual data */
} WT_LOG_RECORD;

/*
 * WT_LOG_DESC --
 *	The log file's description.
 */
struct __wt_log_desc {
#define	WT_LOG_MAGIC		0x101064
	uint32_t	log_magic;	/* 00-03: Magic number */
#define	WT_LOG_MAJOR_VERSION	1
	uint16_t	majorv;		/* 04-05: Major version */
#define	WT_LOG_MINOR_VERSION	0
	uint16_t	minorv;		/* 06-07: Minor version */
	uint64_t	log_size;	/* 08-15: Log file size */
};

/*
 * WT_LOG_REC_DESC --
 *	A descriptor for a log record type.
 */
struct __wt_log_rec_desc {
	const char *fmt;
	int (*print)(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};

/*
 * WT_LOG_OP_DESC --
 *	A descriptor for a log operation type.
 */
struct __wt_log_op_desc {
	const char *fmt;
	int (*print)(WT_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};

/*
 * DO NOT EDIT: automatically built by dist/log.py.
 * Log record declarations: BEGIN
 */
#define	WT_LOGREC_CHECKPOINT	0
#define	WT_LOGREC_COMMIT	1
#define	WT_LOGREC_FILE_SYNC	2
#define	WT_LOGREC_MESSAGE	3
#define	WT_LOGOP_COL_PUT	0
#define	WT_LOGOP_COL_REMOVE	1
#define	WT_LOGOP_COL_TRUNCATE	2
#define	WT_LOGOP_ROW_PUT	3
#define	WT_LOGOP_ROW_REMOVE	4
#define	WT_LOGOP_ROW_TRUNCATE	5
/*
 * Log record declarations: END
 * DO NOT EDIT: automatically built by dist/log.py.
 */
