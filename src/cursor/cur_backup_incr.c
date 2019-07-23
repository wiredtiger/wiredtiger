/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curbackup_incr_get_key --
 *	WT_CURSOR->get_key for incremental hot backup cursors.
 */
static int
__curbackup_incr_get_key(WT_CURSOR *cursor, ...)
{
	WT_CURSOR_BACKUP *cb;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	uint64_t offset, size;
	va_list ap;

	va_start(ap, cursor);
	cb = (WT_CURSOR_BACKUP *)cursor;
	CURSOR_API_CALL(cursor, session, get_key, NULL);

	WT_ERR(__cursor_needkey(cursor));

	offset = cb->incr_list[cb->incr_list_offset];
	size = WT_MIN(cb->incr_list[cb->incr_list_offset + 1], cb->incr_size);

	*va_arg(ap, uint64_t *) = offset;
	*va_arg(ap, uint64_t *) = size;

err:	va_end(ap);
	API_END_RET(session, ret);
}

/*
 * __curbackup_incr_get_value --
 *	WT_CURSOR->get_value for incremental hot backup cursors.
 */
static int
__curbackup_incr_get_value(WT_CURSOR *cursor, ...)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CURSOR_BACKUP *cb;
	WT_DECL_RET;
	WT_FH *fh;
	WT_ITEM *value;
	WT_SESSION_IMPL *session;
	uint64_t offset, size;
	va_list ap;

	fh = NULL;
	va_start(ap, cursor);

	cb = (WT_CURSOR_BACKUP *)cursor;
	btree = cb->incr_cursor == NULL ?
	    NULL : ((WT_CURSOR_BTREE *)cb->incr_cursor)->btree;
	CURSOR_API_CALL(cursor, session, get_value, btree);

	WT_ERR(__cursor_needvalue(cursor));

	offset = cb->incr_list[cb->incr_list_offset];
	size = WT_MIN(cb->incr_list[cb->incr_list_offset + 1], cb->incr_size);

	if (btree == NULL) {
		WT_ERR(__wt_open(session, cb->incr_file,
		    WT_FS_OPEN_FILE_TYPE_REGULAR, WT_FS_OPEN_READONLY, &fh));
		WT_ERR(__wt_buf_init(session, cb->incr_block, size));
		WT_ERR(__wt_read(session, fh,
		    (wt_off_t)offset, (size_t)size, cb->incr_block->mem));
		cb->incr_block->size = size;
	} else {
		bm = btree->bm;
		WT_ERR(bm->read_raw(bm, session, cb->incr_block, offset, size));
	}
	value = va_arg(ap, WT_ITEM *);
	value->data = cb->incr_block->data;
	value->size = cb->incr_block->size;

err:	va_end(ap);

	WT_TRET(__wt_close(session, &fh));
	API_END_RET(session, ret);
}
/*
 * __curbackup_incr_next --
 *	WT_CURSOR->next method for the btree cursor type when configured with
 * incremental_backup.
 */
static int
__curbackup_incr_next(WT_CURSOR *cursor)
{
	WT_BM *bm;
	WT_BTREE *btree;
	WT_CKPT *ckpt, *ckptbase;
	WT_CURSOR_BACKUP *cb;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	wt_off_t size;
	bool start, stop;

	ckptbase = NULL;

	cb = (WT_CURSOR_BACKUP *)cursor;
	btree = cb->incr_cursor == NULL ?
	    NULL : ((WT_CURSOR_BTREE *)cb->incr_cursor)->btree;
	CURSOR_API_CALL(cursor, session, get_value, btree);

	/*
	 * If we have this object's incremental information, step past the
	 * current offset, otherwise call the block manager to get it.
	 */
	if (cb->incr_init) {
		if (cb->incr_list_offset >= cb->incr_list_count - 2)
			return (WT_NOTFOUND);

		/*
		 * If we didn't manage to transfer all of the data, move to the
		 * next chunk.
		 */
		if (cb->incr_list[cb->incr_list_offset + 1] <= cb->incr_size)
			cb->incr_list_offset += 2;
		else {
			cb->incr_list[cb->incr_list_offset] += cb->incr_size;
			cb->incr_list[cb->incr_list_offset + 1] -=
			    cb->incr_size;
		}
	} else if (btree == NULL) {
		WT_ERR(__wt_fs_size(session, cb->incr_file, &size));

		WT_ERR(__wt_calloc_def(session, 2, &cb->incr_list));
		cb->incr_list[0] = 0;
		cb->incr_list[1] = (uint64_t)size;
		cb->incr_list_count = 2;
		cb->incr_list_offset = 0;
		WT_ERR(__wt_scr_alloc(session, 0, &cb->incr_block));
		cb->incr_init = true;

		F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	} else {
		/*
		 * Get a list of the checkpoints available for the file and
		 * flag the starting/stopping ones. It shouldn't be possible
		 * to specify checkpoints that no longer exist, but paranoia
		 * is good.
		 */
		ret = __wt_meta_ckptlist_get(
		    session, cb->incr_file, false, &ckptbase);
		WT_ERR(ret == WT_NOTFOUND ? ENOENT : ret);

		start = stop = false;
		WT_CKPT_FOREACH(ckptbase, ckpt) {
			if (strcmp(
			    ckpt->name, cb->incr_checkpoint_start) == 0) {
				start = true;
				F_SET(ckpt, WT_CKPT_INCR_START);
			}
			if (start == true && strcmp(
			    ckpt->name, cb->incr_checkpoint_stop) == 0) {
				stop = true;
				F_SET(ckpt, WT_CKPT_INCR_STOP);
			}
		}
		if (!start)
			WT_PANIC_ERR(session, ENOENT,
			    "start checkpoint %s not found",
			    cb->incr_checkpoint_start);
		if (!stop)
			WT_PANIC_ERR(session, ENOENT,
			    "stop checkpoint %s not found",
			    cb->incr_checkpoint_stop);

		bm = btree->bm;
		WT_ERR(bm->checkpoint_rewrite(bm, session,
		    ckptbase, &cb->incr_list, &cb->incr_list_count));

		if (cb->incr_list_count == 0)
			WT_ERR(WT_NOTFOUND);
		if (cb->incr_list_count % 2 != 0)
			WT_PANIC_MSG(session, EINVAL,
			    "unexpected return from block manager checkpoint "
			    "rewrite API");

		cb->incr_list_offset = 0;
		WT_ERR(__wt_scr_alloc(session, 0, &cb->incr_block));
		cb->incr_init = true;

		F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	}

err:
	__wt_meta_ckptlist_free(session, &ckptbase);
	API_END_RET(session, ret);
}

/*
 * __wt_curbackup_free_incr --
 *	Free the duplicate backup cursor for a file-based incremental
 * backup.
 */
void
__wt_curbackup_free_incr(WT_SESSION_IMPL *session, WT_CURSOR_BACKUP *cb)
{
	__wt_free(session, cb->incr_file);
	__wt_cursor_close(cb->incr_cursor);
	__wt_free(session, cb->incr_checkpoint_start);
	__wt_free(session, cb->incr_checkpoint_stop);
	__wt_free(session, cb->incr_list);
	__wt_scr_free(session, &cb->incr_block);
}

/*
 * __wt_curbackup_open_incr --
 *	Initialize the duplicate backup cursor for a file-based incremental
 * backup.
 */
int
__wt_curbackup_open_incr(WT_SESSION_IMPL *session, const char *uri,
    WT_CURSOR *other, WT_CURSOR *cursor, const char *cfg[], WT_CURSOR **cursorp)
{
	static const char *copy_entire[] = {
	    WT_BASECONFIG,
	    WT_METADATA_BACKUP,
	    WT_WIREDTIGER,
	    NULL
	};
	WT_CURSOR_BACKUP *cb, *other_cb;
	WT_DECL_ITEM(open_checkpoint);
	WT_DECL_ITEM(open_uri);
	WT_DECL_RET;
	size_t i;
	const char **p, **new_cfg;

	cb = (WT_CURSOR_BACKUP *)cursor;
	other_cb = (WT_CURSOR_BACKUP *)other;
	new_cfg = NULL;

	cursor->key_format = "qq";
	cursor->value_format = "u";

	cursor->next = __curbackup_incr_next;
	cursor->get_key = __curbackup_incr_get_key;
	cursor->get_value = __curbackup_incr_get_value;

	/* We need a starting checkpoint. */
	if (other_cb->incr_checkpoint_start == NULL)
		WT_ERR_MSG(session, EINVAL,
		    "a starting checkpoint must be specified to open a hot "
		    "backup cursor for file-based incremental backups");

	/* The two checkpoints aren't supposed to be the same. */
	if (strcmp(other_cb->incr_checkpoint_start, other->checkpoint) == 0)
		WT_ERR_MSG(session, EINVAL,
		    "incremental backup start and stop checkpoints are the "
		    "same: %s",
		    other_cb->incr_checkpoint_start);

	/* Copy information from the primary cursor to the current file. */
	WT_ERR(__wt_strdup(session,
	    other_cb->incr_checkpoint_start, &cb->incr_checkpoint_start));
	WT_ERR(__wt_strdup(session,
	    other->checkpoint, &cb->incr_checkpoint_stop));
	cb->incr_size = other_cb->incr_size;

	/*
	 * Files that aren't underlying block-manager files have to be copied in
	 * their entirety. Catch them up front and don't try and read them.
	 */
	for (p = copy_entire; *p != NULL; ++p)
		if (strcmp(*p, cb->incr_file) == 0)
			return (__wt_cursor_init(
			    cursor, uri, NULL, cfg, cursorp));

	/*
	 * If doing a file-based incremental backup, we need an open cursor on
	 * the file. Open the backup checkpoint, confirming it exists.
	 */
	WT_ERR(__wt_scr_alloc(session, 0, &open_uri));
	WT_ERR(__wt_buf_fmt(session, open_uri, "file:%s", cb->incr_file));
	__wt_free(session, cb->incr_file);
	WT_ERR(__wt_strdup(session, open_uri->data, &cb->incr_file));

	WT_ERR(__wt_scr_alloc(session, 0, &open_checkpoint));
	WT_ERR(__wt_buf_fmt(session,
	    open_checkpoint, "checkpoint=%s", cb->incr_checkpoint_start));
	for (i = 0; cfg[i] != NULL; ++i)
		;
	WT_ERR(__wt_calloc_def(session, i + 2, &new_cfg));
	for (i = 0; cfg[i] != NULL; ++i)
		new_cfg[i] = cfg[i];
	new_cfg[i++] = open_checkpoint->data;
	new_cfg[i] = NULL;

	WT_ERR(__wt_curfile_open(
	    session, cb->incr_file, NULL, new_cfg, &cb->incr_cursor));

	WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

	/* XXX KEITH */
	WT_ERR(__wt_strdup(session,
	    cb->incr_cursor->internal_uri, &cb->incr_cursor->internal_uri));

err:
	__wt_scr_free(session, &open_checkpoint);
	__wt_scr_free(session, &open_uri);
	__wt_free(session, new_cfg);
	return (ret);
}
