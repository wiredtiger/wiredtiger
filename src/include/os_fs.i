/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_dirlist --
 *	Get a list of files from a directory.
 */
static inline int
__wt_dirlist(WT_SESSION_IMPL *session, const char *dir,
    const char *prefix, uint32_t flags, char ***dirlist, u_int *countp)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_IN_MEMORY));

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS,
	    "%s: directory-list: %s prefix %s",
	    dir, LF_ISSET(WT_DIRLIST_INCLUDE) ? "include" : "exclude",
	    prefix == NULL ? "all" : prefix));

	WT_RET(__wt_filename(session, dir, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->directory_list(
	    file_system, wt_session, path, prefix, flags, dirlist, countp);

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_directory_sync --
 *	Flush a directory to ensure file creation is durable.
 */
static inline int
__wt_directory_sync(WT_SESSION_IMPL *session, const char *name)
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
	 */
	file_system = S2C(session)->file_system;
	if (file_system->directory_sync == NULL)
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
	ret = file_system->directory_sync(file_system, wt_session, name);

	__wt_free(session, copy);
	return (ret);
}

/*
 * __wt_exist --
 *	Return if the file exists.
 */
static inline int
__wt_exist(WT_SESSION_IMPL *session, const char *name, bool *existp)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-exist", name));

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->exist(file_system, wt_session, path, existp);

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_remove --
 *	POSIX remove.
 */
static inline int
__wt_remove(WT_SESSION_IMPL *session, const char *name)
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
	 * useful diagnostic to ensure WiredTiger doesn't hold the handle open
	 * at this stage.
	 */
	if (__wt_handle_search(session, name, false, NULL, NULL))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-remove: file has open handles", name);
#endif

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->remove(file_system, wt_session, path);

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_rename --
 *	POSIX rename.
 */
static inline int
__wt_rename(WT_SESSION_IMPL *session, const char *from, const char *to)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *from_path, *to_path;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(
	    session, WT_VERB_FILEOPS, "%s to %s: file-rename", from, to));

#ifdef HAVE_DIAGNOSTIC
	/*
	 * It is a layering violation to retrieve a WT_FH here, but it is a
	 * useful diagnostic to ensure WiredTiger doesn't hold the handle open
	 * at this stage.
	 */
	if (__wt_handle_search(session, from, false, NULL, NULL))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-rename: file has open handles", from);
	if (__wt_handle_search(session, to, false, NULL, NULL))
		WT_RET_MSG(session, EINVAL,
		    "%s: file-rename: file has open handles", to);
#endif

	from_path = to_path = NULL;
	WT_ERR(__wt_filename(session, from, &from_path));
	WT_ERR(__wt_filename(session, to, &to_path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->rename(file_system, wt_session, from_path, to_path);

err:	__wt_free(session, from_path);
	__wt_free(session, to_path);
	return (ret);
}

/*
 * __wt_filesize_name --
 *	Get the size of a file in bytes, by file name.
 */
static inline int
__wt_filesize_name(
    WT_SESSION_IMPL *session, const char *name, bool silent, wt_off_t *sizep)
{
	WT_DECL_RET;
	WT_FILE_SYSTEM *file_system;
	WT_SESSION *wt_session;
	char *path;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-size", name));

	WT_RET(__wt_filename(session, name, &path));

	file_system = S2C(session)->file_system;
	wt_session = (WT_SESSION *)session;
	ret = file_system->size(file_system, wt_session, path, silent, sizep);

	__wt_free(session, path);
	return (ret);
}
