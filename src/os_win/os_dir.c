/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_win_directory_list --
 *	Get a list of files from a directory, MSVC version.
 */
int
__wt_win_directory_list(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
    const char *dir, const char *prefix, char ***dirlist, u_int *countp)
{
	HANDLE findhandle;
	WIN32_FIND_DATA finddata;
	WT_DECL_ITEM(pathbuf);
	WT_DECL_RET;
	WT_SESSION *session;
	size_t dirallocsz, pathlen;
	u_int count;
	bool match;
	char **entries;

	WT_UNUSED(file_system);

	session = (WT_SESSION_IMPL *)wt_session;

	*dirlist = NULL;
	*countp = 0;

	findhandle = INVALID_HANDLE_VALUE;
	dirallocsz = 0;
	entries = NULL;

	pathlen = strlen(dir);
	if (dir[pathlen - 1] == '\\')
		dir[pathlen - 1] = '\0';
	WT_ERR(__wt_scr_alloc(session, pathlen + 3, &pathbuf));
	WT_ERR(__wt_buf_fmt(session, pathbuf, "%s\\*", dir));

	findhandle = FindFirstFileA(pathbuf->data, &finddata);
	if (findhandle == INVALID_HANDLE_VALUE)
		WT_ERR_MSG(session, __wt_getlasterror(),
		    "%s: directory-list: FindFirstFile", pathbuf->data);

	count = 0;
	do {
		/*
		 * Skip . and ..
		 */
		if (strcmp(finddata.cFileName, ".") == 0 ||
		    strcmp(finddata.cFileName, "..") == 0)
			continue;

		/* The list of files is optionally filtered by a prefix. */
		match = false;
		if (prefix == NULL || WT_PREFIX_MATCH(dp->d_name, prefix)) {
			WT_ERR(__wt_realloc_def(session,
			    &dirallocsz, count + 1, &entries));
			WT_ERR(__wt_strdup(session,
			    finddata.cFileName, &entries[count]));
			++count;
		}
	} while (FindNextFileA(findhandle, &finddata) != 0);
	*dirlist = entries;
	*countp = count;

err:	if (findhandle != INVALID_HANDLE_VALUE)
		(void)FindClose(findhandle);
	__wt_scr_free(session, &pathbuf);

	if (ret == 0)
		return (0);

	if (entries != NULL) {
		while (count > 0)
			__wt_free(session, entries[--count]);
		__wt_free(session, entries);
	}

	WT_RET_MSG(session, ret,
	    "%s: directory-list, prefix \"%s\"",
	    dir, prefix == NULL ? "" : prefix);
}
