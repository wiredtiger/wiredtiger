/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __open_directory_sync --
 *	Fsync the directory in which we created the file.
 */
static int
__open_directory_sync(WT_SESSION_IMPL *session, char *path)
{
	WT_UNUSED(session);
	WT_UNUSED(path);
	return (0);
}

/*
 * __wt_open --
 *	Open a file handle.
 */
int
__wt_open(WT_SESSION_IMPL *session,
    const char *name, int ok_create, int exclusive, int dio_type, WT_FH **fhp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *fh, *tfh;
	int direct_io, f, matched;
	int share_mode;
	DWORD dwCreationDisposition;
	char *path;
	HANDLE filehandle;
	HANDLE filehandletrunc;

	conn = S2C(session);
	fh = NULL;
	path = NULL;
	filehandle = INVALID_HANDLE_VALUE;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: open", name));

	/* Increment the reference count if we already have the file open. */
	matched = 0;
	__wt_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhqh, q)
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = 1;
			break;
		}
	__wt_spin_unlock(session, &conn->fh_lock);
	if (matched)
		return (0);

	WT_RET(__wt_filename(session, name, &path));

	share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.
	 *
	 * TODO: Set tighter file permissions but set bInheritHandle to false
	 * to prevent inheritance
	 */

	f = FILE_ATTRIBUTE_NORMAL;

	dwCreationDisposition = 0;
	if (ok_create) {
		dwCreationDisposition = CREATE_NEW;
		if (exclusive)
			dwCreationDisposition = CREATE_ALWAYS;
	} else
		dwCreationDisposition = OPEN_EXISTING;

	direct_io = 0;

	if (dio_type && FLD_ISSET(conn->direct_io, dio_type)) {
		f |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
		direct_io = 1;
	}

	if (dio_type == WT_FILE_TYPE_LOG &&
	    FLD_ISSET(conn->txn_logsync, WT_LOG_DSYNC)) {
		f |= FILE_FLAG_WRITE_THROUGH;
	}

	/* Disable read-ahead on trees: it slows down random read workloads. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		f |= FILE_FLAG_RANDOM_ACCESS;

	filehandle = CreateFile(path,
				(GENERIC_READ | GENERIC_WRITE),
				share_mode,
				NULL,
				dwCreationDisposition,
				f,
				NULL);
	if (filehandle == INVALID_HANDLE_VALUE) {
		if (GetLastError() == ERROR_FILE_EXISTS && ok_create)
			filehandle = CreateFile(path,
						(GENERIC_READ | GENERIC_WRITE),
						share_mode,
						NULL,
						OPEN_EXISTING,
						f,
						NULL);

		if (filehandle == INVALID_HANDLE_VALUE)
			WT_ERR_MSG(session, __wt_errno(),
			    direct_io ?
			    "%s: open failed with direct I/O configured, some "
			    "filesystem types do not support direct I/O" :
			    "%s", path);
	}

	/*
	 * Open a second handle to file to support allocation/truncation
	 * concurrently with reads on the file. Writes would also move the file
	 * pointer.
	 */
	filehandletrunc = CreateFile(path,
	    (GENERIC_READ | GENERIC_WRITE),
	    share_mode,
	    NULL,
	    OPEN_EXISTING,
	    f,
	    NULL);

	if (F_ISSET(conn, WT_CONN_CKPT_SYNC))
		WT_ERR(__open_directory_sync(session, path));

	WT_ERR(__wt_calloc(session, 1, sizeof(WT_FH), &fh));
	WT_ERR(__wt_strdup(session, name, &fh->name));
	fh->filehandle = filehandle;
	fh->filehandletrunc = filehandletrunc;
	fh->ref = 1;
	fh->direct_io = direct_io;

	/* Set the file's size. */
	WT_ERR(__wt_filesize(session, fh, &fh->size));

	/* Configure file extension. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		fh->extend_len = conn->data_extend_len;

	/* Configure fallocate/posix_fallocate calls. */
	__wt_fallocate_config(session, fh);

	/*
	 * Repeat the check for a match, but then link onto the database's list
	 * of files.
	 */
	matched = 0;
	__wt_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhqh, q)
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = 1;
			break;
		}
	if (!matched) {
		TAILQ_INSERT_TAIL(&conn->fhqh, fh, q);
		WT_STAT_FAST_CONN_INCR(session, file_open);

		*fhp = fh;
	}
	__wt_spin_unlock(session, &conn->fh_lock);
	if (matched) {
err:		if (fh != NULL) {
			__wt_free(session, fh->name);
			__wt_free(session, fh);
		}
		if (filehandle != INVALID_HANDLE_VALUE)
			(void)CloseHandle(filehandle);
	}

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_close --
 *	Close a file handle.
 */
int
__wt_close(WT_SESSION_IMPL *session, WT_FH *fh)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	conn = S2C(session);

	__wt_spin_lock(session, &conn->fh_lock);
	if (fh == NULL || fh->ref == 0 || --fh->ref > 0) {
		__wt_spin_unlock(session, &conn->fh_lock);
		return (0);
	}

	/* Remove from the list. */
	TAILQ_REMOVE(&conn->fhqh, fh, q);
	WT_STAT_FAST_CONN_DECR(session, file_open);

	__wt_spin_unlock(session, &conn->fh_lock);

	/* Discard the memory. */
	if (!CloseHandle(fh->filehandle) != 0) {
		ret = __wt_errno();
		__wt_err(session, ret, "%s", fh->name);
	}

	if (!CloseHandle(fh->filehandletrunc) != 0) {
		ret = __wt_errno();
		__wt_err(session, ret, "%s", fh->name);
	}

	__wt_free(session, fh->name);
	__wt_free(session, fh);
	return (ret);
}
