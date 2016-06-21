/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Error-handling in WiredTiger is messy. First, WiredTiger's upper-level code
 * sometimes checks for POSIX/ANSI error values, for example, EACCES or EBUSY.
 * Second, Windows error codes overlap with POSIX/ANSI error codes.
 *
 * Handle the first issue by mapping Windows errors to POSIX/ANSI errors as part
 * of returning errors out of the Windows-specific code.
 *
 * Handle the second issue by mapping Windows errors into values outside of the
 * POSIX/ANSI and WiredTiger error name spaces (POSIX/ANSI errors are positive
 * integers from 0-1000, WiredTiger errors are negative integers from -31,800
 * to -31,999, use values less than -32,000). There's a Windows-specific error
 * message processing routine that distinguishes between negative and positive
 * error codes. This is only useful for errors inside the Windows code because
 * of the eventual mapping of them to POSIX/ANSI errors, but at least the error
 * messages from inside the Windows code will report all of the information we
 * have.
 */
#define	WT_WINDOWS_ERROR_OFFSET	32000

/*
 * __encode_windows_error --
 *	Return an encoded Windows error.
 */
static int
__encode_windows_error(DWORD error)
{
	return (-WT_WINDOWS_ERROR_OFFSET - error);
}

/*
 * __decode_windows_error --
 *	Return a decoded Windows error.
 */
static DWORD
__decode_windows_error(int error)
{
	return ((DWORD)(-error - WT_WINDOWS_ERROR_OFFSET));
}

/*
 * __is_encoded_windows_error --
 *	Return if we're looking at an encoded Windows error.
 */
static bool
__is_encoded_windows_error(int error)
{
	return (error < -WT_WINDOWS_ERROR_OFFSET);
}

/*
 * __wt_map_windows_error_to_posix_error --
 *	Map Windows errors to POSIX errors.
 */
int
__wt_map_windows_error_to_posix_error(int error)
{
	static const struct {
		int	windows_error;
		int	posix_error;
	} list[] = {
		{ ERROR_ACCESS_DENIED,		EACCES },
		{ ERROR_ALREADY_EXISTS,		EEXIST },
		{ ERROR_ARENA_TRASHED,		EFAULT },
		{ ERROR_BAD_COMMAND,		EFAULT },
		{ ERROR_BAD_ENVIRONMENT,	EFAULT },
		{ ERROR_BAD_FORMAT,		EFAULT },
		{ ERROR_BAD_NETPATH,		ENOENT },
		{ ERROR_BAD_NET_NAME,		ENOENT },
		{ ERROR_BAD_PATHNAME,		ENOENT },
		{ ERROR_BROKEN_PIPE,		EPIPE },
		{ ERROR_CANNOT_MAKE,		EACCES },
		{ ERROR_CHILD_NOT_COMPLETE,	ECHILD },
		{ ERROR_CURRENT_DIRECTORY,	EACCES },
		{ ERROR_DIRECT_ACCESS_HANDLE,	EBADF },
		{ ERROR_DIR_NOT_EMPTY,		ENOTEMPTY },
		{ ERROR_DISK_FULL,		ENOSPC },
		{ ERROR_DRIVE_LOCKED,		EACCES },
		{ ERROR_FAIL_I24,		EACCES },
		{ ERROR_FILENAME_EXCED_RANGE,	ENOENT },
		{ ERROR_FILE_EXISTS,		EEXIST },
		{ ERROR_FILE_NOT_FOUND,		ENOENT },
		{ ERROR_GEN_FAILURE,		EFAULT },
		{ ERROR_INVALID_ACCESS,		EACCES },
		{ ERROR_INVALID_BLOCK,		EFAULT },
		{ ERROR_INVALID_DATA,		EFAULT },
		{ ERROR_INVALID_DRIVE,		ENOENT },
		{ ERROR_INVALID_FUNCTION,	EINVAL },
		{ ERROR_INVALID_HANDLE,		EBADF },
		{ ERROR_INVALID_PARAMETER,	EINVAL },
		{ ERROR_INVALID_TARGET_HANDLE,	EBADF },
		{ ERROR_LOCK_FAILED,		EBUSY },
		{ ERROR_LOCK_VIOLATION,		EBUSY },
		{ ERROR_MAX_THRDS_REACHED,	EAGAIN },
		{ ERROR_NEGATIVE_SEEK,		EINVAL },
		{ ERROR_NESTING_NOT_ALLOWED,	EAGAIN },
		{ ERROR_NETWORK_ACCESS_DENIED,	EACCES },
		{ ERROR_NOT_ENOUGH_MEMORY,	ENOMEM },
		{ ERROR_NOT_ENOUGH_QUOTA,	ENOMEM },
		{ ERROR_NOT_LOCKED,		EACCES },
		{ ERROR_NOT_READY,		EBUSY },
		{ ERROR_NOT_SAME_DEVICE,	EXDEV },
		{ ERROR_NO_DATA,		EPIPE },
		{ ERROR_NO_MORE_FILES,		EMFILE },
		{ ERROR_NO_PROC_SLOTS,		EAGAIN },
		{ ERROR_PATH_NOT_FOUND,		ENOENT },
		{ ERROR_READ_FAULT,		EFAULT },
		{ ERROR_RETRY,			EINTR },
		{ ERROR_SEEK_ON_DEVICE,		EACCES },
		{ ERROR_SHARING_VIOLATION,	EBUSY },
		{ ERROR_TOO_MANY_OPEN_FILES,	EMFILE },
		{ ERROR_WAIT_NO_CHILDREN,	ECHILD },
		{ ERROR_WRITE_FAULT,		EFAULT },
		{ ERROR_WRITE_PROTECT,		EACCES },
	};
	DWORD windows_error;
	int i;

	/* Ignore anything other than encoded Windows errors. */
	if (!__is_encoded_windows_error(error))
		return (error);

	windows_error = __decode_windows_error(error);
	for (i = 0; i < WT_ELEMENTS(list); ++i)
		if (windows_error == list[i].windows_error)
			return (list[i].posix_error);

	/* Untranslatable error, go generic. */
	return (WT_ERROR);
}

/*
 * __wt_errno --
 *	Return errno, or WT_ERROR if errno not set.
 */
int
__wt_errno(void)
{
	/*
	 * Check for 0:
	 * It's easy to introduce a problem by calling the wrong error function,
	 * for example, this function when the MSVC function set the system
	 * error code. Handle gracefully and always return an error.
	 */
	return (errno == 0 ? WT_ERROR : errno);
}

/*
 * __wt_getlasterror --
 *	Return GetLastError, or WT_ERROR if error not set.
 */
int
__wt_getlasterror(void)
{
	/*
	 * Called when we know an error occurred, and we want the system error
	 * code.
	 */
	DWORD windows_error = GetLastError();

	/*
	 * Check for ERROR_SUCCESS:
	 * It's easy to introduce a problem by calling the wrong error function,
	 * for example, this function when the MSVC function set the C runtime
	 * error value. Handle gracefully and always return an error.
	 */
	return (windows_error == ERROR_SUCCESS ?
	    WT_ERROR : __encode_windows_error(windows_error));
}

/*
 * __wt_strerror --
 *	Windows implementation of WT_SESSION.strerror and wiredtiger_strerror.
 */
const char *
__wt_strerror(WT_SESSION_IMPL *session, int error, char *errbuf, size_t errlen)
{
	DWORD lasterror, windows_error;
	const char *p;
	char buf[512];

	/*
	 * Check for a WiredTiger or POSIX constant string, no buffer needed.
	 */
	if ((p = __wt_wiredtiger_error(error)) != NULL)
		return (p);

	/*
	 * Check for Windows errors.
	 *
	 * When called from wiredtiger_strerror, write a passed-in buffer.
	 * When called from WT_SESSION.strerror, write the session's buffer.
	 */
	if (__is_encoded_windows_error(error)) {
		windows_error = __decode_windows_error(error);

		lasterror = FormatMessageA(
			FORMAT_MESSAGE_FROM_SYSTEM |
			    FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			windows_error,
			0, /* let system choose the correct LANGID */
			buf,
			sizeof(buf),
			NULL);

		if (lasterror != 0 && session == NULL &&
		    snprintf(errbuf, errlen, "%s", buf) > 0)
			return (errbuf);
		if (lasterror != 0 && session != NULL &&
		    __wt_buf_fmt(session, &session->err, "%s", buf) == 0)
			return (session->err.data);

		/* Fallback to a generic message. */
		if (session == NULL && snprintf(
		    errbuf, errlen, "Windows error code: %d", error) > 0)
			return (errbuf);
		if (session != NULL && __wt_buf_fmt(session,
		    &session->err, "Windows error code: %d", error) == 0)
			return (session->err.data);
	} else {
		/* Fallback to a generic message. */
		if (session == NULL && snprintf(
		    errbuf, errlen, "POSIX/ANSI error code: %d", error) > 0)
			return (errbuf);
		if (session != NULL && __wt_buf_fmt(session,
		    &session->err, "POSIX/ANSI error code: %d", error) == 0)
			return (session->err.data);
	}

	/* Defeated. */
	return ("unable to return error string");
}
