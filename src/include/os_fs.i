/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_fs_directory_list --
 *	Get a list of files from a directory.
 */
static inline int
__wt_fs_directory_list(WT_SESSION_IMPL *session,
    const char *dir, const char *prefix, char ***dirlistp, u_int *countp)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	*dirlistp = NULL;
	*countp = 0;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS,
	    "%s: directory-list: %s prefix %s",
	    dir, prefix == NULL ? "all" : prefix));

	WT_RET(__wt_filename(session, dir, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->fs_directory_list(
	    file_system, wt_session, path, prefix, dirlistp, countp);

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_fs_directory_list_free --
 *	Free memory allocated by __wt_fs_directory_list.
 */
static inline int
__wt_fs_directory_list_free(
    WT_SESSION_IMPL *session, char ***dirlistp, u_int count)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;

	if (*dirlistp != NULL) {
		file_system = S2C(session)->file_system;
		wt_session = (WT_SESSION *)session;
		ret = file_system->fs_directory_list_free(
		    file_system, wt_session, *dirlistp, count);
	}

	*dirlistp = NULL;
	return (ret);
}

/*
 * __wt_fs_directory_sync --
 *	Flush a directory to ensure file creation is durable.
 */
static inline int
__wt_fs_directory_sync(WT_SESSION_IMPL *session, const char *name)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *copy, *dir;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(
	    session, WT_VERB_FILEOPS, "%s: directory-sync", name));

	/*
	 * POSIX 1003.1 does not require that fsync of a file handle ensures the
	 * entry in the directory containing the file has also reached disk (and
	 * there are historic Linux filesystems requiring it). If the underlying
	 * filesystem method is set, do an explicit fsync on a file descriptor
	 * for the directory to be sure.
	 *
	 * directory-sync is not a required call, no method means the call isn't
	 * needed.
	 */
	file_system = S2C(session)->file_system;
	if (file_system->fs_directory_sync == NULL)
		return (0);

	copy = NULL;
	if (name == NULL || strchr(name, '/') == NULL)
		name = S2C(session)->home;
	else {
		/*
		 * File name construction should not return a path without any
		 * slash separator, but caution isn't unreasonable.
		 */
		WT_RET(__wt_filename(session, name, &copy));
		if ((dir = strrchr(copy, '/')) == NULL)
			name = S2C(session)->home;
		else {
			dir[1] = '\0';
			name = copy;
		}
	}

	wt_session = (WT_SESSION *)session;
	ret = file_system->fs_directory_sync(file_system, wt_session, name);

	__wt_free(session, copy);
	return (ret);
}

/*
 * __wt_fs_exist --
 *	Return if the file exists.
 */
static inline int
__wt_fs_exist(WT_SESSION_IMPL *session, const char *name, bool *existp)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-exist", name));

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->fs_exist(file_system, wt_session, path, existp);

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_fs_remove --
 *	POSIX remove.
 */
static inline int
__wt_fs_remove(WT_SESSION_IMPL *session, const char *name)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-remove", name));

#ifdef HAVE_DIAGNOSTIC
	/*
	 * It is a layering violation to retrieve a WT_FH here, but it is a
	 * useful diagnostic to ensure WiredTiger doesn't have the handle open.
	 */
	if (__wt_handle_is_open(session, name))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-remove: file has open handles", name);
#endif

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	WT_ERR(file_system->fs_remove(file_system, wt_session, path));

	/* Flush the backing directory to guarantee the remove. */
	ret = __wt_fs_directory_sync(session, name);

err:	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_fs_rename --
 *	POSIX rename.
 */
static inline int
__wt_fs_rename(WT_SESSION_IMPL *session, const char *from, const char *to)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	const char *fp, *tp;
	char *from_path, *to_path;
	bool same_directory;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(
	    session, WT_VERB_FILEOPS, "%s to %s: file-rename", from, to));

#ifdef HAVE_DIAGNOSTIC
	/*
	 * It is a layering violation to retrieve a WT_FH here, but it is a
	 * useful diagnostic to ensure WiredTiger doesn't have the handle open.
	 */
	if (__wt_handle_is_open(session, from))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-rename: file has open handles", from);
	if (__wt_handle_is_open(session, to))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-rename: file has open handles", to);
#endif

	from_path = to_path = NULL;
	WT_ERR(__wt_filename(session, from, &from_path));
	WT_ERR(__wt_filename(session, to, &to_path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	WT_ERR(file_system->fs_rename(
	    file_system, wt_session, from_path, to_path));

	/*
	 * Flush the backing directory to guarantee the rename. My reading of
	 * POSIX 1003.1 is there's no guarantee flushing only one of the from
	 * or to directories, or flushing a common parent, is sufficient, and
	 * even if POSIX were to make that guarantee, existing filesystems are
	 * known to not provide the guarantee or only provide the guarantee
	 * with specific mount options. Flush both of the from/to directories
	 * until it's a performance problem.
	 */
	WT_ERR(__wt_fs_directory_sync(session, from));

	/*
	 * In almost all cases, we're going to be renaming files in the same
	 * directory, we can at least fast-path that.
	 */
	fp = strrchr(from, '/');
	tp = strrchr(to, '/');
	same_directory = (fp == NULL && tp == NULL) ||
	    (fp != NULL && tp != NULL &&
	    fp - from == tp - to && memcmp(from, to, (size_t)(fp - from)) == 0);

	ret = same_directory ? 0 : __wt_fs_directory_sync(session, to);

err:	__wt_free(session, from_path);
	__wt_free(session, to_path);
	return (ret);
}

/*
 * __wt_fs_size --
 *	Get the size of a file in bytes, by file name.
 */
static inline int
__wt_fs_size(WT_SESSION_IMPL *session, const char *name, wt_off_t *sizep)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-size", name));

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->fs_size(file_system, wt_session, path, sizep);

	__wt_free(session, path);
	return (ret);
}
