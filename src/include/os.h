/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * System call macros to retry system/library calls on "expected" errors,
 * three versions.
 *
 * WT_SYSCALL_ERROR_VALUE
 *	Expects a non-zero return of an errno value on error.
 * WT_SYSCALL_NEGATIVE_ONE
 *	Expects a -1 return with errno set on error (but checks and returns
 *	WT_ERROR if errno is 0).
 * WT_SYSCALL_NON_ZERO
 *	Expects a non-zero return with errno set on error (but checks and
 *	returns WT_ERROR if errno is 0).
 */
#define	WT_SYSCALL_RETRY(error)						\
		switch (error) {					\
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
		}
#define	WT_SYSCALL_ERROR_VALUE(call, ret) do {				\
	int __retry;							\
	for (__retry = 0; __retry < 10; ++__retry) {			\
		if (((ret) = (call)) == 0)				\
			break;						\
		WT_SYSCALL_RETRY(ret);					\
		break;							\
	}								\
} while (0)
#define	WT_SYSCALL_NEGATIVE_ONE(call, ret) do {				\
	int __retry;							\
	for (__retry = 0; __retry < 10; ++__retry) {			\
		if ((call) != -1) {					\
			(ret) = 0;					\
			break;						\
		}							\
		/*							\
		 * A failing call may not have set errno (as an example,\
		 * the ISO C standard does not mandate rename set errno	\
		 * on failure, although POSIX 1003.1 extends C with that\
		 * requirement).					\
		 */							\
		if (((ret) = __wt_errno()) == 0)			\
			(ret) = WT_ERROR;				\
		WT_SYSCALL_RETRY(ret);					\
		break;							\
	}								\
} while (0)
#define	WT_SYSCALL_NON_ZERO(call, ret) do {				\
	int __retry;							\
	for (__retry = 0; __retry < 10; ++__retry) {			\
		if ((call) == 0) {					\
			(ret) = 0;					\
			break;						\
		}							\
		/*							\
		 * A failing call may not have set errno (as an example,\
		 * the ISO C standard does not mandate rename set errno	\
		 * on failure, although POSIX 1003.1 extends C with that\
		 * requirement).					\
		 */							\
		if (((ret) = __wt_errno()) == 0)			\
			(ret) = WT_ERROR;				\
		WT_SYSCALL_RETRY(ret);					\
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
 * Macros to ensure a file handle is inserted or removed from both the main and
 * the hashed queue, used by connection-level and in-memory data structures.
 */
#define	WT_FILE_HANDLE_INSERT(h, fh, bucket) do {			\
	TAILQ_INSERT_HEAD(&(h)->fhqh, fh, q);				\
	TAILQ_INSERT_HEAD(&(h)->fhhash[bucket], fh, hashq);		\
} while (0)

#define	WT_FILE_HANDLE_REMOVE(h, fh, bucket) do {			\
	TAILQ_REMOVE(&(h)->fhqh, fh, q);				\
	TAILQ_REMOVE(&(h)->fhhash[bucket], fh, hashq);			\
} while (0)

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
	const char *name;			/* File name */

	uint64_t name_hash;			/* hash of name */
	TAILQ_ENTRY(__wt_fh) q;			/* internal queue */
	TAILQ_ENTRY(__wt_fh) hashq;		/* internal hash queue */
	u_int ref;				/* reference count */

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
};
#endif

struct __wt_file_handle_inmem {
	WT_FILE_HANDLE iface;

	/*
	 * In memory specific file handle fields
	 */
	uint64_t name_hash;			/* hash of name */
	TAILQ_ENTRY(__wt_file_handle_inmem) q;	/* internal queue, hash queue */
	TAILQ_ENTRY(__wt_file_handle_inmem) hashq;

	size_t	 off;				/* Read/write offset */
	WT_ITEM  buf;				/* Data */
	u_int	 ref;				/* Reference count */
};

struct __wt_fstream {
	const char *name;			/* Stream name */

	FILE *fp;				/* stdio FILE stream */
	WT_FH *fh;				/* WT file handle */
	wt_off_t off;				/* Read/write offset */
	wt_off_t size;				/* File size */
	WT_ITEM  buf;				/* Data */

#define	WT_STREAM_APPEND	0x01		/* Open a stream for append */
#define	WT_STREAM_READ		0x02		/* Open a stream for read */
#define	WT_STREAM_WRITE		0x04		/* Open a stream for write */
	uint32_t flags;

	int (*close)(WT_SESSION_IMPL *, WT_FSTREAM *);
	int (*fstr_flush)(WT_SESSION_IMPL *, WT_FSTREAM *);
	int (*fstr_getline)(WT_SESSION_IMPL *, WT_FSTREAM *, WT_ITEM *);
	int (*fstr_printf)(
	    WT_SESSION_IMPL *, WT_FSTREAM *, const char *, va_list);
};
