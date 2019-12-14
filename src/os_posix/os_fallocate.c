/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
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
 * __posix_std_fallocate --
 *     Linux fallocate call.
 */
static int
__posix_std_fallocate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset)
{
#if defined(HAVE_FALLOCATE)
    WT_DECL_RET;
    WT_FILE_HANDLE_POSIX *pfh;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    pfh = (WT_FILE_HANDLE_POSIX *)file_handle;

#if WT_IO_VIA_MMAP
    if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset)
	__wt_drain_mmap_users(file_handle, wt_session);
#endif

    WT_SYSCALL_RETRY(fallocate(pfh->fd, 0, (wt_off_t)0, offset), ret);
#if WT_IO_VIA_MMAP
    if (ret == 0)
	/* Remap the region with the new size */
	if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset) {
	    __wt_remap_region(file_handle, wt_session);
	    WT_STAT_CONN_INCRV(session, block_remap_region_extend, 1);
	}
#endif
    return (ret);
#else
    WT_UNUSED(file_handle);
    WT_UNUSED(offset);

    return (__wt_set_return((WT_SESSION_IMPL *)wt_session, ENOTSUP));
#endif
}

/*
 * __posix_sys_fallocate --
 *     Linux fallocate call (system call version).
 */
static int
__posix_sys_fallocate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset)
{
#if defined(__linux__) && defined(SYS_fallocate)
    WT_DECL_RET;
    WT_FILE_HANDLE_POSIX *pfh;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    pfh = (WT_FILE_HANDLE_POSIX *)file_handle;

#if WT_IO_VIA_MMAP
    if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset)
	__wt_drain_mmap_users(file_handle, wt_session);
#endif

    /*
     * Try the system call for fallocate even if the C library wrapper was not found. The system
     * call actually exists in the kernel for some Linux versions (RHEL 5.5), but not in the version
     * of the C library. This allows it to work everywhere the kernel supports it.
     */
    WT_SYSCALL_RETRY(syscall(SYS_fallocate, pfh->fd, 0, (wt_off_t)0, offset), ret);

#if WT_IO_VIA_MMAP
    if (ret == 0)
	/* Remap the region with the new size */
	if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset) {
	    __wt_remap_region(file_handle, wt_session);
	    WT_STAT_CONN_INCRV(session, block_remap_region_extend, 1);
	}
#endif
    return (ret);
#else
    WT_UNUSED(file_handle);
    WT_UNUSED(offset);

    return (__wt_set_return((WT_SESSION_IMPL *)wt_session, ENOTSUP));
#endif
}

/*
 * __posix_posix_fallocate --
 *     POSIX fallocate call.
 */
static int
__posix_posix_fallocate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset)
{
#if defined(HAVE_POSIX_FALLOCATE)
    WT_DECL_RET;
    WT_FILE_HANDLE_POSIX *pfh;
    WT_SESSION_IMPL *session;

    pfh = (WT_FILE_HANDLE_POSIX *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;

#if WT_IO_VIA_MMAP
    if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset)
	__wt_drain_mmap_users(file_handle, wt_session);
#endif

    WT_SYSCALL_RETRY(posix_fallocate(pfh->fd, (wt_off_t)0, offset), ret);
#if WT_IO_VIA_MMAP
    if (ret == 0)
	/* Remap the region with the new size */
	if (pfh->mmap_file_mappable && (wt_off_t)pfh->mmap_size != offset) {
	    __wt_remap_region(file_handle, wt_session);
	    WT_STAT_CONN_INCRV(session, block_remap_region_extend, 1);
	}
#endif
    return (ret);
#else
    WT_UNUSED(file_handle);
    WT_UNUSED(offset);

    return (__wt_set_return((WT_SESSION_IMPL *)wt_session, ENOTSUP));
#endif
}

/*
 * __wt_posix_file_extend --
 *     Extend the file.
 */
int
__wt_posix_file_extend(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset)
{
    /*
     * The first file extension call: figure out what this system has.
     *
     * This function is configured as a locking call, so we know we're
     * single-threaded through here. Set the nolock function first, then
     * publish the NULL replacement to ensure the handle functions are
     * always correct.
     *
     * We've seen Linux systems where posix_fallocate has corrupted existing
     * file data (even though that is explicitly disallowed by POSIX).
     * FreeBSD and Solaris support posix_fallocate, and so far we've seen
     * no problems leaving it unlocked. Check for fallocate (and the system
     * call version of fallocate) first to avoid locking on Linux if at all
     * possible.
     */
    if (__posix_std_fallocate(file_handle, wt_session, offset) == 0) {
        file_handle->fh_extend_nolock = __posix_std_fallocate;
        WT_PUBLISH(file_handle->fh_extend, NULL);
        return (0);
    }
    if (__posix_sys_fallocate(file_handle, wt_session, offset) == 0) {
        file_handle->fh_extend_nolock = __posix_sys_fallocate;
        WT_PUBLISH(file_handle->fh_extend, NULL);
        return (0);
    }
    if (__posix_posix_fallocate(file_handle, wt_session, offset) == 0) {
#if defined(__linux__)
        file_handle->fh_extend = __posix_posix_fallocate;
        WT_WRITE_BARRIER();
#else
        file_handle->fh_extend_nolock = __posix_posix_fallocate;
        WT_PUBLISH(file_handle->fh_extend, NULL);
#endif
        return (0);
    }

    /*
     * Use the POSIX ftruncate call if there's nothing else, it can extend files. Note ftruncate
     * requires locking.
     */
    if (file_handle->fh_truncate != NULL &&
      file_handle->fh_truncate(file_handle, wt_session, offset) == 0) {
        file_handle->fh_extend = file_handle->fh_truncate;
        WT_WRITE_BARRIER();
        return (0);
    }

    file_handle->fh_extend = NULL;
    WT_WRITE_BARRIER();
    return (ENOTSUP);
}
