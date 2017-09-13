/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_optrack_record_funcid --
 *	Record optrack function id
 */
void
__wt_optrack_record_funcid(WT_SESSION_IMPL *session, uint64_t op_id,
			   void *func, size_t funcsize,
			   volatile bool *id_recorded)
{
	char endline[] = "\n";
	char id_buf[sizeof(uint64_t) + sizeof(char)+4];
	WT_CONNECTION_IMPL *conn;
	wt_off_t fsize;

	conn = S2C(session);

	__wt_spin_lock(session, &conn->optrack_map_spinlock);
	if (!*id_recorded) {
		WT_IGNORE_RET(__wt_snprintf(id_buf,
		    sizeof(id_buf), "%p ", (void*)op_id));
		WT_IGNORE_RET(__wt_filesize(session, conn->optrack_map_fh,
					    &fsize));
		WT_IGNORE_RET(__wt_write(session, conn->optrack_map_fh, fsize,
					 sizeof(id_buf)-1, id_buf));
		WT_IGNORE_RET(__wt_filesize(session, conn->optrack_map_fh,
					    &fsize));
		WT_IGNORE_RET(__wt_write(session, conn->optrack_map_fh,
					 fsize, funcsize-1, func));
		WT_IGNORE_RET(__wt_filesize(session, conn->optrack_map_fh,
					    &fsize));
		WT_IGNORE_RET(__wt_write(session, conn->optrack_map_fh,
					 fsize, sizeof(endline)-1, endline));
		*id_recorded = 1;
	}
	__wt_spin_unlock(session, &conn->optrack_map_spinlock);
}

/*
 * __wt_optrack_flush_buffer --
 *	Flush optrack buffer
 */
size_t
__wt_optrack_flush_buffer(WT_SESSION_IMPL *s)
{
	WT_DECL_RET;
	ret = s->optrack_fh->handle->fh_write(s->optrack_fh->handle,
					      (WT_SESSION *)s,
					      (wt_off_t)s->optrack_offset,
					      s->optrackbuf_ptr *
					      sizeof(WT_TRACK_RECORD),
					      s->optrack_buf);
	if (ret == 0)
		return s->optrackbuf_ptr * sizeof(WT_TRACK_RECORD);
	else
		return (0);
}

/*
 * __wt_optrack_get_expensive_timestamp --
 *       Obtain a timestamp via a system call on platforms where
 *       obtaining it directly from the hardware register is not
 *       supported.
 */
uint64_t
__wt_optrack_get_expensive_timestamp(WT_SESSION_IMPL *session)
{
	struct timespec tsp;

	__wt_epoch_raw(session, &tsp);
	return (tsp.tv_sec * WT_BILLION + tsp.tv_nsec);
}
