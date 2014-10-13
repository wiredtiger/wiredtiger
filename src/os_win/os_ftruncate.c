/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_ftruncate --
 *	Truncate a file.
 */
int
__wt_ftruncate(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t len)
{
	WT_DECL_RET;
	LARGE_INTEGER largeint;
	uint32_t lasterror;

	largeint.QuadPart = len;

	if ((ret = SetFilePointerEx(
	    fh->filehandle_secondary, largeint, NULL, FILE_BEGIN)) == FALSE)
		WT_RET_MSG(session, __wt_errno(), "%s SetFilePointerEx error",
		    fh->name);

	ret = SetEndOfFile(fh->filehandle_secondary);
	if (ret != FALSE) {
		fh->size = fh->extend_size = len;
	}

	lasterror = GetLastError();

	if (lasterror = ERROR_USER_MAPPED_FILE)
		return (EBUSY);

	WT_RET_MSG(session, lasterror, "%s SetEndOfFile error", fh->name);
}
