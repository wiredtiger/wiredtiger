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
__wt_optrack_record_funcid(
    WT_SESSION_IMPL *session, void *func, volatile bool *id_recorded)
{
	WT_CONNECTION_IMPL *conn;
	wt_off_t fsize;
	char id_buf[sizeof(uint64_t) * 2 + sizeof(uint8_t) * 4];

	conn = S2C(session);

	__wt_spin_lock(session, &conn->optrack_map_spinlock);
	if (!*id_recorded) {
		WT_IGNORE_RET(__wt_snprintf(id_buf,
		    sizeof(id_buf), "%p ", func));
		WT_IGNORE_RET(__wt_filesize(session,
		    conn->optrack_map_fh, &fsize));
		WT_IGNORE_RET(__wt_write(session,
		    conn->optrack_map_fh, fsize, WT_MIN(strnlen(id_buf,
		    sizeof(id_buf) - 1), sizeof(id_buf) - 1), id_buf));
		WT_IGNORE_RET(__wt_filesize(session,
		    conn->optrack_map_fh, &fsize));
		WT_IGNORE_RET(__wt_write(session,
		    conn->optrack_map_fh, fsize, sizeof(void *) - 1, func));
		WT_IGNORE_RET(__wt_filesize(session,
		    conn->optrack_map_fh, &fsize));
		WT_IGNORE_RET(__wt_write(session,
		    conn->optrack_map_fh, fsize, 1, "\n"));
		*id_recorded = true;
	}
	__wt_spin_unlock(session, &conn->optrack_map_spinlock);
}

/*
 * __wt_optrack_open_file --
 * Open the per-session operation-tracking file.
 */
int
__wt_optrack_open_file(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	WT_OPTRACK_HEADER optrack_header = {WT_OPTRACK_VERSION, 0};

	conn = S2C(session);

	if (!F_ISSET(conn, WT_CONN_OPTRACK))
		return (WT_ERROR);

	WT_RET(__wt_scr_alloc(session, 0, &buf));
	WT_ERR(__wt_filename_construct(session, conn->optrack_path,
	    "optrack", conn->optrack_pid, session->id, buf));
	WT_ERR(__wt_open(session,
	    (const char *)buf->data, WT_FS_OPEN_FILE_TYPE_REGULAR,
	    WT_FS_OPEN_CREATE, &session->optrack_fh));

	/* Write the header into the operation-tracking file. */
	if (F_ISSET(session, WT_SESSION_INTERNAL))
	    optrack_header.optrack_session_internal = true;

	ret = session->optrack_fh->handle->fh_write(session->optrack_fh->handle,
	    (WT_SESSION *)session, 0, sizeof(WT_OPTRACK_HEADER),
	    &optrack_header);
	if (ret == 0)
		session->optrack_offset = sizeof(WT_OPTRACK_HEADER);
	else {
		WT_TRET(__wt_close(session, &session->optrack_fh));
		session->optrack_fh = NULL;
	}
err:	__wt_scr_free(session, &buf);

	return (ret);
}

/*
 * __wt_optrack_flush_buffer --
 *	Flush optrack buffer. Returns the number of bytes flushed to the file.
 */
size_t
__wt_optrack_flush_buffer(WT_SESSION_IMPL *s)
{
	WT_DECL_RET;

	if (s->optrack_fh == NULL)
		if (__wt_optrack_open_file(s))
			return (0);

	ret = s->optrack_fh->handle->fh_write(s->optrack_fh->handle,
	    (WT_SESSION *)s, (wt_off_t)s->optrack_offset,
	    s->optrackbuf_ptr * sizeof(WT_OPTRACK_RECORD), s->optrack_buf);
	if (ret == 0)
		return (s->optrackbuf_ptr * sizeof(WT_OPTRACK_RECORD));
	else
		return (0);
}

/*
 * __wt_optrack_get_expensive_timestamp --
 *       Obtain a timestamp via a system call on platforms where obtaining it
 *       directly from the hardware register is not supported.
 */
uint64_t
__wt_optrack_get_expensive_timestamp(WT_SESSION_IMPL *session)
{
	struct timespec tsp;

	__wt_epoch_raw(session, &tsp);
	return (uint64_t)(tsp.tv_sec * WT_BILLION + tsp.tv_nsec);
}
