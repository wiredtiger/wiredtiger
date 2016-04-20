/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Number of directory entries can grow dynamically.
 */
#define	WT_DIR_ENTRY	32

#define	WT_DIRLIST_EXCLUDE	0x1	/* Exclude files matching prefix */
#define	WT_DIRLIST_INCLUDE	0x2	/* Include files matching prefix */

#define	WT_SYSCALL_RETRY(call, ret) do {				\
	int __retry;							\
	for (__retry = 0; __retry < 10; ++__retry) {			\
		if ((call) == 0) {					\
			(ret) = 0;					\
			break;						\
		}							\
		switch ((ret) = __wt_errno()) {				\
		case 0:							\
			/* The call failed but didn't set errno. */	\
			(ret) = WT_ERROR;				\
			break;						\
		case EAGAIN:						\
		case EBUSY:						\
		case EINTR:						\
		case EIO:						\
		case EMFILE:						\
		case ENFILE:						\
		case ENOSPC:						\
			__wt_sleep(0L, 50000L);				\
			continue;					\
		default:						\
			break;						\
		}							\
		break;							\
	}								\
} while (0)

#define	WT_TIMEDIFF_NS(end, begin)					\
	(WT_BILLION * (uint64_t)((end).tv_sec - (begin).tv_sec) +	\
	    (uint64_t)(end).tv_nsec - (uint64_t)(begin).tv_nsec)
#define	WT_TIMEDIFF_US(end, begin)					\
	(WT_TIMEDIFF_NS((end), (begin)) / WT_THOUSAND)
#define	WT_TIMEDIFF_MS(end, begin)					\
	(WT_TIMEDIFF_NS((end), (begin)) / WT_MILLION)
#define	WT_TIMEDIFF_SEC(end, begin)					\
	(WT_TIMEDIFF_NS((end), (begin)) / WT_BILLION)

#define	WT_TIMECMP(t1, t2)						\
	((t1).tv_sec < (t2).tv_sec ? -1 :				\
	     (t1).tv_sec == (t2.tv_sec) ?				\
	     (t1).tv_nsec < (t2).tv_nsec ? -1 :				\
	     (t1).tv_nsec == (t2).tv_nsec ? 0 : 1 : 1)

/*
 * The underlying OS calls return ENOTSUP if posix_fadvise functionality isn't
 * available, but WiredTiger uses the POSIX flag names in the API. Use distinct
 * values so the underlying code can distinguish.
 */
#ifndef	POSIX_FADV_DONTNEED
#define	POSIX_FADV_DONTNEED	0x01
#endif
#ifndef	POSIX_FADV_WILLNEED
#define	POSIX_FADV_WILLNEED	0x02
#endif

#define	WT_OPEN_CREATE		0x001	/* Create */
#define	WT_OPEN_DIRECTIO	0x002	/* Direct I/O (if available) */
#define	WT_OPEN_EXCLUSIVE	0x004	/* Open exclusively */
#define	WT_OPEN_FIXED		0x008	/* Path isn't relative to home */
#define	WT_OPEN_READONLY	0x010	/* Open readonly (internal) */

struct __wt_fh {
	/*
	 * There is a file name field in both the WT_FH and WT_FILE_HANDLE
	 * structures, which isn't ideal. There would be compromises to keeping
	 * a single copy: If it were in WT_FH, file systems could not access
	 * the name field, if it were just in the WT_FILE_HANDLE internal
	 * WiredTiger code would need to maintain a string inside a structure
	 * that is owned by the user (since we care about the content of the
	 * file name). Keeping two copies seems most reasonable.
	 */
	const char *name;                       /* File name */

	uint64_t name_hash;                     /* hash of name */
	TAILQ_ENTRY(__wt_fh) q;                 /* internal queue */
	TAILQ_ENTRY(__wt_fh) hashq;              /* internal queue */
	u_int   ref;                            /* reference count */

	WT_FILE_HANDLE *handle;
};

#ifdef _WIN32

struct __wt_file_handle_win {
	WT_FILE_HANDLE iface;
	/*
	 * Windows specific file handle fields
	 */
	HANDLE filehandle;			/* Windows file handle */
	HANDLE filehandle_secondary;		/* Windows file handle
						   for file size changes */
	bool	 direct_io;			/* O_DIRECT configured */
};

#else

struct __wt_file_handle_posix {
	WT_FILE_HANDLE iface;

	/*
	 * POSIX specific file handle fields
	 */
	int	 fd;				/* POSIX file handle */

	bool	 direct_io;			/* O_DIRECT configured */

	enum {					/* file extend configuration */
	    WT_FALLOCATE_AVAILABLE,
	    WT_FALLOCATE_NOT_AVAILABLE,
	    WT_FALLOCATE_POSIX,
	    WT_FALLOCATE_STD,
	    WT_FALLOCATE_SYS } fallocate_available;
	bool fallocate_requires_locking;
};
#endif

struct __wt_file_handle_inmem {
	WT_FILE_HANDLE iface;
	/*
	 * In memory specific file handle fields
	 */

	size_t	 off;				/* Read/write offset */
	WT_ITEM  buf;				/* Data */
	u_int    ref;				/* Reference count */

	TAILQ_ENTRY(__wt_file_handle_inmem) closedq; /* queue of closed files*/
};

struct __wt_fstream {
	const char *name;                       /* Stream name */

	FILE *fp;                               /* stdio FILE stream */
	WT_FH *fh;				/* WT file handle */
	wt_off_t off;				/* Read/write offset */
	wt_off_t size;				/* File size */
	WT_ITEM  buf;				/* Data */

#define	WT_STREAM_APPEND	0x01		/* Open a stream for append */
#define	WT_STREAM_LINE_BUFFER	0x02		/* Line buffer the stream */
#define	WT_STREAM_READ		0x04		/* Open a stream for read */
#define	WT_STREAM_WRITE		0x08		/* Open a stream for write */
	uint32_t flags;

	int (*close)(WT_SESSION_IMPL *, WT_FSTREAM *);
	int (*flush)(WT_SESSION_IMPL *, WT_FSTREAM *);
	int (*getline)(WT_SESSION_IMPL *, WT_FSTREAM *, WT_ITEM *);
	int (*printf)(WT_SESSION_IMPL *, WT_FSTREAM *, const char *, va_list);
};
