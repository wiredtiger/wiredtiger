/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
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
__wt_ftruncate(WT_SESSION_IMPL *session, WT_FH *fh, off_t len)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(ftruncate(fh->fd, len), ret);
	if (ret == 0) {
		fh->size = fh->extend_size  = len;
		return (0);
	}

	WT_RET_MSG(session, ret, "%s ftruncate error", fh->name);
}
