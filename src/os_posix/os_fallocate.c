/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if defined(__linux__)
#include <linux/falloc.h>
#include <sys/syscall.h>
#endif
/*
 * __wt_posix_file_allocate_configure --
 *	Configure POSIX file-extension behavior for a file handle.
 */
void
__wt_posix_file_allocate_configure(
    WT_SESSION_IMPL *session, WT_FILE_HANDLE_POSIX *file_handle)
{
	WT_UNUSED(session);

	file_handle->fallocate_available = WT_FALLOCATE_NOT_AVAILABLE;
	file_handle->fallocate_requires_locking = false;

	/*
	 * Check for the availability of some form of fallocate; in all cases,
	 * start off requiring locking, we'll relax that requirement once we
	 * know which system calls work with the handle's underlying filesystem.
	 */
#if defined(HAVE_FALLOCATE) || defined(HAVE_POSIX_FALLOCATE)
	file_handle->fallocate_available = WT_FALLOCATE_AVAILABLE;
	file_handle->fallocate_requires_locking = true;
#endif
#if defined(__linux__) && defined(SYS_fallocate)
	file_handle->fallocate_available = WT_FALLOCATE_AVAILABLE;
	file_handle->fallocate_requires_locking = true;
#endif
}

/*
 * __posix_std_fallocate --
 *	Linux fallocate call.
 */
static int
__posix_std_fallocate(WT_FILE_HANDLE_POSIX *pfh, wt_off_t offset, wt_off_t len)
{
#if defined(HAVE_FALLOCATE)
	WT_DECL_RET;

	WT_SYSCALL_RETRY(fallocate(pfh->fd, 0, offset, len), ret);
	return (ret);
#else
	WT_UNUSED(pfh);
	WT_UNUSED(offset);
	WT_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __posix_sys_fallocate --
 *	Linux fallocate call (system call version).
 */
static int
__posix_sys_fallocate(WT_FILE_HANDLE_POSIX *pfh, wt_off_t offset, wt_off_t len)
{
#if defined(__linux__) && defined(SYS_fallocate)
	WT_DECL_RET;

	/*
	 * Try the system call for fallocate even if the C library wrapper was
	 * not found.  The system call actually exists in the kernel for some
	 * Linux versions (RHEL 5.5), but not in the version of the C library.
	 * This allows it to work everywhere the kernel supports it.
	 */
	WT_SYSCALL_RETRY(syscall(SYS_fallocate, pfh->fd, 0, offset, len), ret);
	return (ret);
#else
	WT_UNUSED(pfh);
	WT_UNUSED(offset);
	WT_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __posix_posix_fallocate --
 *	POSIX fallocate call.
 */
static int
__posix_posix_fallocate(
    WT_FILE_HANDLE_POSIX *pfh, wt_off_t offset, wt_off_t len)
{
#if defined(HAVE_POSIX_FALLOCATE)
	WT_DECL_RET;

	WT_SYSCALL_RETRY(posix_fallocate(pfh->fd, offset, len), ret);
	return (ret);
#else
	WT_UNUSED(pfh);
	WT_UNUSED(offset);
	WT_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __wt_posix_file_allocate --
 *	POSIX fallocate.
 */
int
__wt_posix_file_allocate(
    WT_FILE_HANDLE *fh, WT_SESSION *wtsession, wt_off_t offset, wt_off_t len)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_POSIX *pfh;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wtsession;
	pfh = (WT_FILE_HANDLE_POSIX *)fh;

	switch (pfh->fallocate_available) {
	/*
	 * Check for already configured handles and make the configured call.
	 */
	case WT_FALLOCATE_POSIX:
		if ((ret = __posix_posix_fallocate(pfh, offset, len)) == 0)
			return (0);
		WT_RET_MSG(session, ret, "%s: posix_fallocate", fh->name);
	case WT_FALLOCATE_STD:
		if ((ret = __posix_std_fallocate(pfh, offset, len)) == 0)
			return (0);
		WT_RET_MSG(session, ret, "%s: fallocate", fh->name);
	case WT_FALLOCATE_SYS:
		if ((ret = __posix_sys_fallocate(pfh, offset, len)) == 0)
			return (0);
		WT_RET_MSG(session, ret, "%s: sys_fallocate", fh->name);

	/*
	 * Figure out what allocation call this system/filesystem supports, if
	 * any.
	 */
	case WT_FALLOCATE_AVAILABLE:
		/*
		 * We've seen Linux systems where posix_fallocate has corrupted
		 * existing file data (even though that is explicitly disallowed
		 * by POSIX). FreeBSD and Solaris support posix_fallocate, and
		 * so far we've seen no problems leaving it unlocked. Check for
		 * fallocate (and the system call version of fallocate) first to
		 * avoid locking on Linux if at all possible.
		 */
		if ((ret = __posix_std_fallocate(pfh, offset, len)) == 0) {
			pfh->fallocate_available = WT_FALLOCATE_STD;
			pfh->fallocate_requires_locking = false;
			return (0);
		}
		if ((ret = __posix_sys_fallocate(pfh, offset, len)) == 0) {
			pfh->fallocate_available = WT_FALLOCATE_SYS;
			pfh->fallocate_requires_locking = false;
			return (0);
		}
		if ((ret = __posix_posix_fallocate(pfh, offset, len)) == 0) {
			pfh->fallocate_available = WT_FALLOCATE_POSIX;
#if !defined(__linux__)
			pfh->fallocate_requires_locking = false;
#endif
			return (0);
		}
		/* FALLTHROUGH */
	case WT_FALLOCATE_NOT_AVAILABLE:
	default:
		pfh->fallocate_available = WT_FALLOCATE_NOT_AVAILABLE;
		return (ENOTSUP);
	}
	/* NOTREACHED */
}
