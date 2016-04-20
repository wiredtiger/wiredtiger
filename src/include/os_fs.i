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
	WT_FILE_SYSTEM *file_system;
	file_system = S2C(session)->file_system;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_IN_MEMORY));

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS,
	    "%s: directory-list: %s prefix %s",
	    dir, LF_ISSET(WT_DIRLIST_INCLUDE) ? "include" : "exclude",
	    prefix == NULL ? "all" : prefix));

	return (file_system->directory_list(
	    file_system, &session->iface, dir, prefix, flags, dirlist, countp));
}

/*
 * __wt_directory_sync --
 *	Flush a directory to ensure file creation is durable.
 */
static inline int
__wt_directory_sync(WT_SESSION_IMPL *session, const char *name)
{
	WT_FILE_SYSTEM *file_system;
	file_system = S2C(session)->file_system;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(
	    session, WT_VERB_FILEOPS, "%s: directory-sync", name));

	return (file_system->directory_sync(
	    file_system, &session->iface, name));
}

/*
 * __wt_exist --
 *	Return if the file exists.
 */
static inline int
__wt_exist(WT_SESSION_IMPL *session, const char *name, bool *existp)
{
	WT_FILE_SYSTEM *file_system;
	int exist;

	file_system = S2C(session)->file_system;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-exist", name));

	WT_RET(file_system->exist(file_system, &session->iface, name, &exist));
	*existp = exist == 0;
	return (0);
}

/*
 * __wt_remove --
 *	POSIX remove.
 */
static inline int
__wt_remove(WT_SESSION_IMPL *session, const char *name)
{
	WT_FILE_SYSTEM *file_system;
	file_system = S2C(session)->file_system;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-remove", name));

	return (file_system->remove(file_system, &session->iface, name));
}

/*
 * __wt_rename --
 *	POSIX rename.
 */
static inline int
__wt_rename(WT_SESSION_IMPL *session, const char *from, const char *to)
{
	WT_FILE_SYSTEM *file_system;
	file_system = S2C(session)->file_system;

	WT_ASSERT(session, !F_ISSET(S2C(session), WT_CONN_READONLY));

	WT_RET(__wt_verbose(
	    session, WT_VERB_FILEOPS, "%s to %s: file-rename", from, to));

	return (file_system->rename(file_system, &session->iface, from, to));
}

/*
 * __wt_filesize_name --
 *	Get the size of a file in bytes, by file name.
 */
static inline int
__wt_filesize_name(
    WT_SESSION_IMPL *session, const char *name, bool silent, wt_off_t *sizep)
{
	WT_FILE_SYSTEM *file_system;
	file_system = S2C(session)->file_system;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: file-size", name));

	return (file_system->size(
	    file_system, &session->iface, name, silent, sizep));
}
