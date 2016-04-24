/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * File system interface for in-memory implementation.
 */
typedef struct {
	WT_FILE_SYSTEM iface;

	TAILQ_HEAD(__wt_closed_file_handle_qh, __wt_file_handle_inmem) fileq;

	WT_SPINLOCK lock;
} WT_INMEMORY_FILE_SYSTEM;

static int __im_file_size(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);

/*
 * __im_handle_search --
 *	Return a matching handle, if one exists.
 */
static WT_FILE_HANDLE_INMEM *
__im_handle_search(WT_FILE_SYSTEM *file_system, const char *name)
{
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;

	TAILQ_FOREACH(im_fh, &im_fs->fileq, q)
		if (strcmp(im_fh->iface.name, name) == 0)
			break;
	return (im_fh);
}

/*
 * __im_handle_remove --
 *	Destroy an in-memory file handle. Should only happen on remove or
 *	shutdown.
 */
static int
__im_handle_remove(WT_SESSION_IMPL *session, WT_FILE_HANDLE_INMEM *im_fh)
{
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_FILE_HANDLE *fhp;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);

	if (im_fh->ref != 0)
		WT_RET_MSG(session, EBUSY,
		    "%s: file-remove", im_fh->iface.name);

	TAILQ_REMOVE(&im_fs->fileq, im_fh, q);

	/* Clean up private information. */
	__wt_buf_free(session, &im_fh->buf);

	/* Clean up public information. */
	fhp = (WT_FILE_HANDLE *)im_fh;
	__wt_free(session, fhp->name);

	__wt_free(session, im_fh);

	return (0);
}

/*
 * __im_fs_exist --
 *	Return if the file exists.
 */
static int
__im_fs_exist(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wt_session, const char *name, bool *existp)
{
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;
	session = (WT_SESSION_IMPL *)wt_session;

	__wt_spin_unlock(session, &im_fs->lock);

	*existp = __im_handle_search(file_system, name) != NULL;

	__wt_spin_lock(session, &im_fs->lock);
	return (0);
}

/*
 * __im_fs_remove --
 *	POSIX remove.
 */
static int
__im_fs_remove(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;
	session = (WT_SESSION_IMPL *)wt_session;

	__wt_spin_lock(session, &im_fs->lock);

	ret = ENOENT;
	if ((im_fh = __im_handle_search(file_system, name)) != NULL)
		ret = __im_handle_remove(session, im_fh);

	__wt_spin_lock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_fs_rename --
 *	POSIX rename.
 */
static int
__im_fs_rename(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wt_session, const char *from, const char *to)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	char *copy;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;
	session = (WT_SESSION_IMPL *)wt_session;

	__wt_spin_lock(session, &im_fs->lock);

	ret = ENOENT;
	if ((im_fh = __im_handle_search(file_system, from)) != NULL) {
		WT_ERR(__wt_strdup(session, to, &copy));

		__wt_free(session, im_fh->iface.name);
		im_fh->iface.name = copy;
	}

err:	__wt_spin_lock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_fs_size --
 *	Get the size of a file in bytes, by file name.
 */
static int
__im_fs_size(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wt_session, const char *name, bool silent, wt_off_t *sizep)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	WT_UNUSED(silent);

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;

	__wt_spin_lock(session, &im_fs->lock);

	ret = ENOENT;
	if ((im_fh = __im_handle_search(file_system, name)) != NULL)
		ret = __im_file_size(
		    (WT_FILE_HANDLE *)im_fh, wt_session, sizep);

	__wt_spin_unlock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_file_close --
 *	ANSI C close.
 */
static int
__im_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
	WT_FILE_HANDLE_INMEM *im_fh;

	WT_UNUSED(wt_session);

	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;
	--im_fh->ref;

	return (0);
}

/*
 * __im_file_lock --
 *	Lock/unlock a file.
 */
static int
__im_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool lock)
{
	/* Locks are always granted. */
	WT_UNUSED(file_handle);
	WT_UNUSED(wt_session);
	WT_UNUSED(lock);
	return (0);
}

/*
 * __im_file_read --
 *	POSIX pread.
 */
static int
__im_file_read(WT_FILE_HANDLE *file_handle,
    WT_SESSION *wt_session, wt_off_t offset, size_t len, void *buf)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);
	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;

	/*
	 * TODO: Each file handle should probably reference the file system,
	 * so external implementations can easily access fields in their file
	 * system. Maybe alternatively they could allocate customized file
	 * handle structures that reference them - that would allow for type
	 * checking.
	 */
	__wt_spin_lock(session, &im_fs->lock);

	off = (size_t)offset;
	if (off < im_fh->buf.size) {
		len = WT_MIN(len, im_fh->buf.size - off);
		memcpy(buf, (uint8_t *)im_fh->buf.mem + off, len);
		im_fh->off = off + len;
	} else
		ret = WT_ERROR;

	__wt_spin_unlock(session, &im_fs->lock);
	if (ret == 0)
		return (0);
	WT_RET_MSG(session, WT_ERROR,
	    "%s: handle-read: failed to read %" WT_SIZET_FMT " bytes at "
	    "offset %" WT_SIZET_FMT,
	    im_fh->iface.name, len, off);
}

/*
 * __im_file_size --
 *	Get the size of a file in bytes, by file handle.
 */
static int
__im_file_size(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep)
{
	WT_FILE_HANDLE_INMEM *im_fh;

	WT_UNUSED(wt_session);
	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;

	/*
	 * XXX hack - MongoDB assumes that any file with content will have a
	 * non-zero size. In memory tables generally are zero-sized, make
	 * MongoDB happy.
	 */
	*sizep = im_fh->buf.size == 0 ? 1024 : (wt_off_t)im_fh->buf.size;
	return (0);
}

/*
 * __im_file_sync --
 *	POSIX fflush/fsync.
 */
static int
__im_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool block)
{
	WT_UNUSED(file_handle);
	WT_UNUSED(wt_session);

	/*
	 * Callers attempting asynchronous flush handle ENOTSUP returns, and
	 * won't make further attempts.
	 */
	return (block ? 0 : ENOTSUP);
}

/*
 * __im_file_truncate --
 *	POSIX ftruncate.
 */
static int
__im_file_truncate(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);
	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;

	__wt_spin_lock(session, &im_fs->lock);

	/*
	 * Grow the buffer as necessary, clear any new space in the file,
	 * and reset the file's data length.
	 */
	off = (size_t)offset;
	WT_ERR(__wt_buf_grow(session, &im_fh->buf, off));
	if (im_fh->buf.size < off)
		memset((uint8_t *)im_fh->buf.data + im_fh->buf.size,
		    0, off - im_fh->buf.size);
	im_fh->buf.size = off;

err:	__wt_spin_unlock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_file_write --
 *	POSIX pwrite.
 */
static int
__im_file_write(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session,
    wt_off_t offset, size_t len, const void *buf)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);
	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;

	__wt_spin_lock(session, &im_fs->lock);

	off = (size_t)offset;
	WT_ERR(__wt_buf_grow(session, &im_fh->buf, off + len + 1024));

	memcpy((uint8_t *)im_fh->buf.data + off, buf, len);
	if (off + len > im_fh->buf.size)
		im_fh->buf.size = off + len;
	im_fh->off = off + len;

err:	__wt_spin_unlock(session, &im_fs->lock);
	if (ret == 0)
		return (0);
	WT_RET_MSG(session, ret,
	    "%s: handle-write: failed to write %" WT_SIZET_FMT " bytes at "
	    "offset %" WT_SIZET_FMT,
	    im_fh->iface.name, len, off);
}

/*
 * __im_file_open --
 *	POSIX fopen/open.
 */
static int
__im_file_open(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
    const char *name, int file_type, u_int flags, WT_FILE_HANDLE **file_handlep)
{
	WT_DECL_RET;
	WT_FILE_HANDLE *file_handle;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	WT_UNUSED(file_type);
	WT_UNUSED(flags);

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;
	im_fh = NULL;

	/*
	 * First search the file queue, if we find it, assert there's only a
	 * single reference, in-memory only supports a single handle on any
	 * file, for now.
	 */
	__wt_spin_lock(session, &im_fs->lock);
	im_fh = __im_handle_search(file_system, name);
	if (im_fh != NULL) {
		__wt_spin_unlock(session, &im_fs->lock);

		if (im_fh->ref != 0)
			WT_ERR_MSG(session, EBUSY,
			    "%s: file-open: already open", name);

		im_fh->ref = 1;
		im_fh->off = 0;

		*file_handlep = (WT_FILE_HANDLE *)im_fh;
		return (0);
	}

	/* The file hasn't been opened before, create a new one. */
	WT_ERR(__wt_calloc_one(session, &im_fh));

	/* Initialize private information. */
	im_fh->ref = 1;
	im_fh->off = 0;

	/* Initialize public information. */
	file_handle = (WT_FILE_HANDLE *)im_fh;
	WT_ERR(__wt_strdup(session, name, &file_handle->name));

	file_handle->close = __im_file_close;
	file_handle->lock = __im_file_lock;
	file_handle->read = __im_file_read;
	file_handle->size = __im_file_size;
	file_handle->sync = __im_file_sync;
	file_handle->truncate = __im_file_truncate;
	file_handle->write = __im_file_write;

	TAILQ_INSERT_HEAD(&im_fs->fileq, im_fh, q);

	*file_handlep = file_handle;

err:	__wt_spin_unlock(session, &im_fs->lock);
	if (ret != 0)
		__wt_free(session, im_fh);
	return (ret);
}

/*
 * __im_terminate --
 *	Terminate an in-memory configuration.
 */
static int
__im_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	WT_UNUSED(file_system);

	session = (WT_SESSION_IMPL *)wt_session;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)S2C(session)->file_system;

	while ((im_fh = TAILQ_FIRST(&im_fs->fileq)) != NULL)
		WT_TRET(__im_handle_remove(session, im_fh));

	__wt_spin_destroy(session, &im_fs->lock);
	__wt_free(session, im_fs);

	return (ret);
}

/*
 * __wt_os_inmemory --
 *	Initialize an in-memory configuration.
 */
int
__wt_os_inmemory(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_INMEMORY_FILE_SYSTEM *im_fs;

	WT_RET(__wt_calloc_one(session, &im_fs));

	/* Initialize private information. */
	TAILQ_INIT(&im_fs->fileq);
	WT_ERR(__wt_spin_init(session, &im_fs->lock, "in-memory I/O"));

	/* Initialize the in-memory jump table. */
	file_system = (WT_FILE_SYSTEM *)im_fs;
	file_system->directory_list = NULL;
	file_system->directory_sync = NULL;
	file_system->exist = __im_fs_exist;
	file_system->open_file = __im_file_open;
	file_system->remove = __im_fs_remove;
	file_system->rename = __im_fs_rename;
	file_system->size = __im_fs_size;
	file_system->terminate = __im_terminate;

	/* Switch the file system into place. */
	S2C(session)->file_system = (WT_FILE_SYSTEM *)im_fs;

	return (0);

err:	__wt_free(session, im_fs);
	return (ret);
}
