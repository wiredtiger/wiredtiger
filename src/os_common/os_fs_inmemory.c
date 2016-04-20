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

	TAILQ_HEAD(__wt_closed_file_handle_qh, __wt_file_handle_inmem) closedq;

	WT_SPINLOCK lock;
} WT_INMEMORY_FILE_SYSTEM;

static int __im_handle_remove(WT_SESSION_IMPL *, WT_FILE_HANDLE_INMEM *);

/*
 * __im_directory_list --
 *	Get a list of files from a directory, in-memory version.
 */
static int
__im_directory_list(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wtsession, const char *dir,
    const char *prefix, uint32_t flags, char ***dirlist, u_int *countp)
{
	WT_SESSION_IMPL *session;

	WT_UNUSED(file_system);
	WT_UNUSED(dir);
	WT_UNUSED(prefix);
	WT_UNUSED(flags);
	WT_UNUSED(dirlist);
	WT_UNUSED(countp);

	session = (WT_SESSION_IMPL *)wtsession;

	WT_RET_MSG(session, ENOTSUP, "directory-list");
}

/*
 * __im_directory_sync --
 *	Flush a directory to ensure file creation is durable.
 */
static int
__im_directory_sync(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wtsession, const char *path)
{
	WT_UNUSED(file_system);
	WT_UNUSED(wtsession);
	WT_UNUSED(path);
	return (0);
}

/*
 * __im_fs_exist --
 *	Return if the file exists.
 */
static int
__im_fs_exist(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wtsession, const char *name, int *existp)
{
	WT_SESSION_IMPL *session;

	WT_UNUSED(file_system);

	session = (WT_SESSION_IMPL *)wtsession;

	/*
	 * TODO: The in-memory file system really shouldn't be searching
	 * in the WiredTiger handle cache. It needs to track its own files.
	 */
	*existp = __wt_handle_search(session, name, false, NULL, NULL);
	return (0);
}

/*
 * __im_fs_remove --
 *	POSIX remove.
 */
static int
__im_fs_remove(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wtsession, const char *name)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wtsession;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;

	__wt_spin_lock(session, &im_fs->lock);

	/* The handle must be on the closed queue */
	ret = ENOENT;
	TAILQ_FOREACH(im_fh, &im_fs->closedq, q)
		if (WT_STRING_MATCH(im_fh->iface.name, name, strlen(name))) {
			__im_handle_remove(session, im_fh);
			ret = 0;
		}

	__wt_spin_lock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_fs_rename --
 *	POSIX rename.
 */
static int
__im_fs_rename(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wtsession, const char *from, const char *to)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_FH *fh;
	uint64_t bucket, hash;
	char *to_name;

	/*
	 * TODO: This is broken - needs to use internal handle information.
	 */
	WT_UNUSED(file_system);
	session = (WT_SESSION_IMPL *)wtsession;
	conn = S2C(session);

	/* We'll need a copy of the target name. */
	WT_RET(__wt_strdup(session, to, &to_name));

	__wt_spin_lock(session, &conn->fh_lock);

	/* Make sure the target name isn't active. */
	hash = __wt_hash_city64(to, strlen(to));
	bucket = hash % WT_HASH_ARRAY_SIZE;
	TAILQ_FOREACH(fh, &conn->fhhash[bucket], hashq)
		if (strcmp(to, fh->name) == 0)
			WT_ERR(EPERM);

	/* Find the source name. */
	hash = __wt_hash_city64(from, strlen(from));
	bucket = hash % WT_HASH_ARRAY_SIZE;
	TAILQ_FOREACH(fh, &conn->fhhash[bucket], hashq)
		if (strcmp(from, fh->name) == 0)
			break;
	if (fh == NULL)
		WT_ERR(ENOENT);

	/* Remove source from the list. */
	WT_CONN_FILE_REMOVE(conn, fh, bucket);

	/* Swap the names. */
	__wt_free(session, fh->name);
	fh->name = to_name;
	to_name = NULL;

	/* Put source back on the list. */
	hash = __wt_hash_city64(to, strlen(to));
	bucket = hash % WT_HASH_ARRAY_SIZE;
	WT_CONN_FILE_INSERT(conn, fh, bucket);

	if (0) {
err:		__wt_free(session, to_name);
	}
	__wt_spin_unlock(session, &conn->fh_lock);

	return (ret);
}

/*
 * __im_fs_size --
 *	Get the size of a file in bytes, by file name.
 */
static int
__im_fs_size(WT_FILE_SYSTEM *file_system,
    WT_SESSION *wtsession, const char *name, int silent, wt_off_t *sizep)
{
	WT_DECL_RET;
	WT_FH *fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	WT_UNUSED(silent);

	session = (WT_SESSION_IMPL *)wtsession;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)file_system;

	__wt_spin_lock(session, &im_fs->lock);

	/*
	 * TODO: Should use search internal to in-memory file system.
	 */
	if (__wt_handle_search(session, name, true, NULL, &fh)) {
		WT_ERR(fh->handle->size(fh->handle, wtsession, sizep));
		WT_ERR(__wt_close(session, &fh));
	} else
		ret = ENOENT;

err:	__wt_spin_unlock(session, &im_fs->lock);
	return (ret);
}

/*
 * __im_file_close --
 *	ANSI C close.
 */
static int
__im_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession)
{
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *fs_im;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wtsession;
	fs_im = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);
	im_fh = (WT_FILE_HANDLE_INMEM *)file_handle;

	if (--im_fh->ref == 0) {
		im_fh->off = 0;
		TAILQ_INSERT_HEAD(&fs_im->closedq, im_fh, q);
	}

	return (0);
}

/*
 * __im_file_lock --
 *	Lock/unlock a file.
 */
static int
__im_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession, int lock)
{
	/* Locks are always granted. */
	WT_UNUSED(file_handle);
	WT_UNUSED(wtsession);
	WT_UNUSED(lock);
	return (0);
}

/*
 * __im_file_read --
 *	POSIX pread.
 */
static int
__im_file_read(WT_FILE_HANDLE *file_handle,
    WT_SESSION *wtsession, wt_off_t offset, size_t len, void *buf)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)wtsession;
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
    WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession, wt_off_t *sizep)
{
	WT_FILE_HANDLE_INMEM *im_fh;

	WT_UNUSED(wtsession);
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
__im_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession, int block)
{
	WT_UNUSED(file_handle);
	WT_UNUSED(wtsession);

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
    WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession, wt_off_t offset)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)session;
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
__im_file_write(WT_FILE_HANDLE *file_handle, WT_SESSION *wtsession,
    wt_off_t offset, size_t len, const void *buf)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;
	size_t off;

	session = (WT_SESSION_IMPL *)wtsession;
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
__im_file_open(WT_FILE_SYSTEM *file_system, WT_SESSION *wtsession,
    const char *name, uint32_t file_type, uint32_t flags, WT_FILE_HANDLE **fhpp)
{
	WT_DECL_RET;
	WT_FILE_HANDLE *fhp;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;
	WT_SESSION_IMPL *session;

	WT_UNUSED(file_type);
	WT_UNUSED(flags);

	session = (WT_SESSION_IMPL *)wtsession;
	im_fs = (WT_INMEMORY_FILE_SYSTEM *)(S2C(session)->file_system);
	im_fh = NULL;

	/* First search the closed queue */
	TAILQ_FOREACH(im_fh, &im_fs->closedq, q)
		if (WT_STRING_MATCH(im_fh->iface.name, name, strlen(name))) {
			TAILQ_REMOVE(&im_fs->closedq, im_fh, q);
			*fhpp = (WT_FILE_HANDLE *)im_fh;
			return (0);
		}

	/* The file hasn't been opened before, create a new one. */
	WT_RET(__wt_calloc_one(session, &im_fh));

	fhp = (WT_FILE_HANDLE *)im_fh;
	WT_ERR(__wt_strdup(session, name, &fhp->name));

	fhp->close = __im_file_close;
	fhp->lock = __im_file_lock;
	fhp->read = __im_file_read;
	fhp->size = __im_file_size;
	fhp->sync = __im_file_sync;
	fhp->truncate = __im_file_truncate;
	fhp->write = __im_file_write;

	*fhpp = fhp;

	if (0) {
err:		__wt_free(session, im_fh);
	}
	return (ret);
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

	WT_ASSERT(session, im_fh->ref == 0);
	TAILQ_REMOVE(&im_fs->closedq, im_fh, q);

	/* Clean up private information. */
	__wt_buf_free(session, &im_fh->buf);

	/* Clean up public information. */
	fhp = (WT_FILE_HANDLE *)im_fh;
	__wt_free(session, fhp->name);

	__wt_free(session, im_fh);

	return (0);
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

	/*
	 * Initialize private information.
	 *
	 * Maintain a list of handles that have been closed. We keep a reference
	 * to all files until shutdown, otherwise WiredTiger periodic cleanup
	 * can cause tables to be removed unexpectedly.
	 */
	TAILQ_INIT(&im_fs->closedq);
	WT_ERR(__wt_spin_init(session, &im_fs->lock, "in-memory I/O"));

	/* Initialize the in-memory jump table. */
	file_system = (WT_FILE_SYSTEM *)im_fs;
	file_system->directory_list = __im_directory_list;
	file_system->directory_sync = __im_directory_sync;
	file_system->exist = __im_fs_exist;
	file_system->open_file = __im_file_open;
	file_system->remove = __im_fs_remove;
	file_system->rename = __im_fs_rename;
	file_system->size = __im_fs_size;

	/* Switch the file system into place. */
	S2C(session)->file_system = (WT_FILE_SYSTEM *)im_fs;

	return (0);

err:	__wt_free(session, im_fs);
	return (ret);
}

/*
 * __wt_os_inmemory_cleanup --
 *	Discard an in-memory configuration.
 */
int
__wt_os_inmemory_cleanup(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_INMEM *im_fh;
	WT_INMEMORY_FILE_SYSTEM *im_fs;

	im_fs = (WT_INMEMORY_FILE_SYSTEM *)S2C(session)->file_system;

	while ((im_fh = TAILQ_FIRST(&im_fs->closedq)) != NULL)
		WT_TRET(__im_handle_remove(session, im_fh));

	__wt_spin_destroy(session, &im_fs->lock);
	__wt_free(session, im_fs);

	return (ret);
}
