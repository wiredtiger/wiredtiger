/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
/* I'm sure we need to config this */
#include <dirent.h>

/*
 * __wt_posix_directory_list --
 *	Get a list of files from a directory, POSIX version.
 */
int
__wt_posix_directory_list(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *dir,
    const char *prefix, uint32_t flags, char ***dirlist, u_int *countp)
{
	struct dirent *dp;
	DIR *dirp;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	size_t dirallocsz;
	u_int count, dirsz;
	bool match;
	char **entries;

	WT_UNUSED(file_system);

	session = (WT_SESSION_IMPL *)wt_session;

	*dirlist = NULL;
	*countp = 0;

	dirp = NULL;
	dirallocsz = 0;
	dirsz = 0;
	entries = NULL;

	WT_SYSCALL_RETRY(((dirp = opendir(dir)) == NULL ? 1 : 0), ret);
	if (ret != 0)
		WT_RET_MSG(session, ret, "%s: directory-list: opendir", dir);

	for (count = 0; (dp = readdir(dirp)) != NULL;) {
		/*
		 * Skip . and ..
		 */
		if (strcmp(dp->d_name, ".") == 0 ||
		    strcmp(dp->d_name, "..") == 0)
			continue;

		/* The list of files is optionally filtered by a prefix. */
		match = false;
		if (prefix != NULL &&
		    ((LF_ISSET(WT_DIRLIST_INCLUDE) &&
		    WT_PREFIX_MATCH(dp->d_name, prefix)) ||
		    (LF_ISSET(WT_DIRLIST_EXCLUDE) &&
		    !WT_PREFIX_MATCH(dp->d_name, prefix))))
			match = true;
		if (prefix == NULL || match) {
			/*
			 * We have a file name we want to return.
			 */
			count++;
			if (count > dirsz) {
				dirsz += WT_DIR_ENTRY;
				WT_ERR(__wt_realloc_def(
				    session, &dirallocsz, dirsz, &entries));
			}
			WT_ERR(__wt_strdup(
			    session, dp->d_name, &entries[count-1]));
		}
	}
	if (count > 0)
		*dirlist = entries;
	*countp = count;

err:	if (dirp != NULL)
		(void)closedir(dirp);

	if (ret == 0)
		return (0);

	if (*dirlist != NULL) {
		for (count = dirsz; count > 0; count--)
			__wt_free(session, entries[count]);
		__wt_free(session, entries);
	}
	WT_RET_MSG(session, ret,
	    "%s: directory-list, prefix \"%s\"",
	    dir, prefix == NULL ? "" : prefix);
}
