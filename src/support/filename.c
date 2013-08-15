/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_absolute_path --
 *	Return if a filename is an absolute path.
 */
int
__wt_absolute_path(const char *path)
{
	return (path[0] == '/' ? 1 : 0);
}

/*
 * __wt_filename --
 *	Build a file name in a scratch buffer, automatically calculate the
 *	length of the file name.
 */
int
__wt_filename(WT_SESSION_IMPL *session, const char *name, const char **path)
{
	return (__wt_nfilename(session, name, strlen(name), path));
}

/*
 * __wt_nfilename --
 *	Build a file name in a scratch buffer. If the name is already an
 *	absolute path duplicate it, otherwise generate a path relative to the
 *	connection home directory.
 */
int
__wt_nfilename(WT_SESSION_IMPL *session,
    const char *name, size_t namelen, const char **path)
{
	WT_CONNECTION_IMPL *conn;
	size_t len;
	char *buf;

	conn = S2C(session);
	*path = NULL;

	if (__wt_absolute_path(name))
		WT_RET(__wt_strndup(session, name, namelen, path));
	else {
		len = strlen(conn->home) + 1 + namelen + 1;
		WT_RET(__wt_calloc(session, 1, len, &buf));
		snprintf(buf, len, "%s/%.*s", conn->home, (int)namelen, name);
		*path = buf;
	}

	return (0);
}
