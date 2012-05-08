/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_read --
 *	Read a chunk.
 */
int
__wt_read(WT_SESSION_IMPL *session,
    WT_FH *fh, off_t offset, uint32_t bytes, void *buf)
{
	WT_CSTAT_INCR(session, total_read_io);

	WT_VERBOSE_RET(session, fileops,
	    "%s: read %" PRIu32 " bytes at offset %" PRIuMAX,
	    fh->name, bytes, (uintmax_t)offset);

	if (pread(fh->fd, buf, (size_t)bytes, offset) == (ssize_t)bytes)
		return (0);

	WT_RET_MSG(session, __wt_errno(),
	    "%s read error: failed to read %" PRIu32 " bytes at offset %"
	    PRIuMAX,
	    fh->name, bytes, (uintmax_t)offset);
}

/*
 * __wt_write --
 *	Write a chunk.
 */
int
__wt_write(WT_SESSION_IMPL *session,
    WT_FH *fh, off_t offset, uint32_t bytes, const void *buf)
{
	WT_CSTAT_INCR(session, total_write_io);

	WT_VERBOSE_RET(session, fileops,
	    "%s: write %" PRIu32 " bytes at offset %" PRIuMAX,
	    fh->name, bytes, (uintmax_t)offset);

	if (pwrite(fh->fd, buf, (size_t)bytes, offset) == (ssize_t)bytes)
		return (0);

	WT_RET_MSG(session, __wt_errno(),
	    "%s write error: failed to write %" PRIu32 " bytes at offset %"
	    PRIuMAX,
	    fh->name, bytes, (uintmax_t)offset);
}
