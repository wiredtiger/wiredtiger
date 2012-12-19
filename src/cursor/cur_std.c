/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_cursor_notsup --
 *	Unsupported cursor actions.
 */
int
__wt_cursor_notsup(WT_CURSOR *cursor)
{
	WT_UNUSED(cursor);

	return (ENOTSUP);
}

/* 
 * __wt_cursor_noop --
 *	Cursor noop.
 */
int
__wt_cursor_noop(WT_CURSOR *cursor)
{
	WT_UNUSED(cursor);

	return (0);
}

/*
 * __wt_cursor_set_notsup --
 *	Reset the cursor methods to not-supported.
 */
void
__wt_cursor_set_notsup(WT_CURSOR *cursor)
{
	/*
	 * Set all of the cursor methods (except for close and reset), to fail.
	 * Close is unchanged so the cursor can be discarded, reset defaults to
	 * a no-op because session transactional operations reset all of the
	 * cursors in a session, and random cursors shouldn't block transactions
	 * or checkpoints.
	 */
	cursor->compare =
	    (int (*)(WT_CURSOR *, WT_CURSOR *, int *))__wt_cursor_notsup;
	cursor->next = __wt_cursor_notsup;
	cursor->prev = __wt_cursor_notsup;
	cursor->reset = __wt_cursor_noop;
	cursor->search = __wt_cursor_notsup;
	cursor->search_near = (int (*)(WT_CURSOR *, int *))__wt_cursor_notsup;
	cursor->insert = __wt_cursor_notsup;
	cursor->update = __wt_cursor_notsup;
	cursor->remove = __wt_cursor_notsup;
}

/*
 * __wt_cursor_kv_not_set --
 *	Standard error message for key/values not set.
 */
int
__wt_cursor_kv_not_set(WT_CURSOR *cursor, int key)
{
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cursor->session;

	WT_RET_MSG(session,
	    cursor->saved_err == 0 ? EINVAL : cursor->saved_err,
	    "requires %s be set", key ? "key" : "value");
}

/*
 * __wt_cursor_get_key --
 *	WT_CURSOR->get_key default implementation.
 */
int
__wt_cursor_get_key(WT_CURSOR *cursor, ...)
{
	WT_DECL_RET;
	va_list ap;

	va_start(ap, cursor);
	ret = __wt_cursor_get_keyv(cursor, cursor->flags, ap);
	va_end(ap);
	return (ret);
}

/*
 * __wt_cursor_set_key --
 *	WT_CURSOR->set_key default implementation.
 */
void
__wt_cursor_set_key(WT_CURSOR *cursor, ...)
{
	va_list ap;

	va_start(ap, cursor);
	__wt_cursor_set_keyv(cursor, cursor->flags, ap);
	va_end(ap);
}

/*
 * __wt_cursor_get_raw_key --
 *	Temporarily force raw mode in a cursor to get a canonical copy of
 * the key.
 */
int
__wt_cursor_get_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
{
	WT_DECL_RET;
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);
	ret = cursor->get_key(cursor, key);
	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);
	return (ret);
}

/*
 * __wt_cursor_set_raw_key --
 *	Temporarily force raw mode in a cursor to set a canonical copy of
 * the key.
 */
void
__wt_cursor_set_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
{
	int raw_set;

	raw_set = F_ISSET(cursor, WT_CURSTD_RAW) ? 1 : 0;
	if (!raw_set)
		F_SET(cursor, WT_CURSTD_RAW);
	cursor->set_key(cursor, key);
	if (!raw_set)
		F_CLR(cursor, WT_CURSTD_RAW);
}

/*
 * __wt_cursor_get_keyv --
 *	WT_CURSOR->get_key worker function.
 */
int
__wt_cursor_get_keyv(WT_CURSOR *cursor, uint32_t flags, va_list ap)
{
	WT_DECL_RET;
	WT_ITEM *key;
	WT_SESSION_IMPL *session;
	size_t size;
	const char *fmt;

	CURSOR_API_CALL(cursor, session, get_key, NULL);
	WT_CURSOR_NEEDKEY(cursor);

	if (WT_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(WT_CURSTD_RAW)) {
			key = va_arg(ap, WT_ITEM *);
			key->data = cursor->raw_recno_buf;
			WT_ERR(__wt_struct_size(
			    session, &size, "q", cursor->recno));
			key->size = (uint32_t)size;
			ret = __wt_struct_pack(session, cursor->raw_recno_buf,
			    sizeof(cursor->raw_recno_buf), "q", cursor->recno);
		} else
			*va_arg(ap, uint64_t *) = cursor->recno;
	} else {
		fmt = cursor->key_format;
		if (LF_ISSET(WT_CURSOR_RAW_OK))
			fmt = "u";
		ret = __wt_struct_unpackv(
		    session, cursor->key.data, cursor->key.size, fmt, ap);
	}

err:	API_END(session);
	return (ret);
}

/*
 * __wt_cursor_set_keyv --
 *	WT_CURSOR->set_key default implementation.
 */
void
__wt_cursor_set_keyv(WT_CURSOR *cursor, uint32_t flags, va_list ap)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_ITEM *buf, *item;
	size_t sz;
	va_list ap_copy;
	const char *fmt, *str;

	CURSOR_API_CALL(cursor, session, set_key, NULL);
	F_CLR(cursor, WT_CURSTD_KEY_SET);

	/* Fast path some common cases: single strings or byte arrays. */
	if (WT_CURSOR_RECNO(cursor)) {
		if (LF_ISSET(WT_CURSTD_RAW)) {
			item = va_arg(ap, WT_ITEM *);
			WT_ERR(__wt_struct_unpack(session,
			    item->data, item->size, "q", &cursor->recno));
		} else
			cursor->recno = va_arg(ap, uint64_t);
		if (cursor->recno == 0)
			WT_ERR_MSG(session, EINVAL,
			    "Record numbers must be greater than zero");
		cursor->key.data = &cursor->recno;
		sz = sizeof(cursor->recno);
	} else {
		fmt = cursor->key_format;
		if (LF_ISSET(WT_CURSOR_RAW_OK) || strcmp(fmt, "u") == 0) {
			item = va_arg(ap, WT_ITEM *);
			sz = item->size;
			cursor->key.data = item->data;
		} else if (strcmp(fmt, "S") == 0) {
			str = va_arg(ap, const char *);
			sz = strlen(str) + 1;
			cursor->key.data = (void *)str;
		} else {
			buf = &cursor->key;

			va_copy(ap_copy, ap);
			ret = __wt_struct_sizev(
			    session, &sz, cursor->key_format, ap_copy);
			va_end(ap_copy);
			WT_ERR(ret);

			WT_ERR(__wt_buf_initsize(session, buf, sz));
			WT_ERR(__wt_struct_packv(
			    session, buf->mem, sz, cursor->key_format, ap));
		}
	}
	if (sz == 0)
		WT_ERR_MSG(session, EINVAL, "Empty keys not permitted");
	else if ((uint32_t)sz != sz)
		WT_ERR_MSG(session, EINVAL,
		    "Key size (%" PRIu64 ") out of range", (uint64_t)sz);
	cursor->saved_err = 0;
	cursor->key.size = WT_STORE_SIZE(sz);
	F_SET(cursor, WT_CURSTD_KEY_APP);
	if (0) {
err:		cursor->saved_err = ret;
	}

	API_END(session);
}

/*
 * __wt_cursor_get_value --
 *	WT_CURSOR->get_value default implementation.
 */
int
__wt_cursor_get_value(WT_CURSOR *cursor, ...)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	const char *fmt;
	va_list ap;

	CURSOR_API_CALL(cursor, session, get_value, NULL);
	WT_CURSOR_NEEDVALUE(cursor);

	va_start(ap, cursor);
	fmt = F_ISSET(cursor, WT_CURSOR_RAW_OK) ? "u" : cursor->value_format;
	ret = __wt_struct_unpackv(session,
	    cursor->value.data, cursor->value.size, fmt, ap);
	va_end(ap);

err:	API_END(session);
	return (ret);
}

/*
 * __wt_cursor_set_value --
 *	WT_CURSOR->set_value default implementation.
 */
void
__wt_cursor_set_value(WT_CURSOR *cursor, ...)
{
	WT_DECL_RET;
	WT_ITEM *buf, *item;
	WT_SESSION_IMPL *session;
	const char *fmt, *str;
	size_t sz;
	va_list ap;

	va_start(ap, cursor);
	CURSOR_API_CALL(cursor, session, set_value, NULL);
	F_CLR(cursor, WT_CURSTD_VALUE_SET);

	fmt = F_ISSET(cursor, WT_CURSOR_RAW_OK) ? "u" : cursor->value_format;

	/* Fast path some common cases: single strings, byte arrays and bits. */
	if (strcmp(fmt, "S") == 0) {
		str = va_arg(ap, const char *);
		sz = strlen(str) + 1;
		cursor->value.data = str;
	} else if (F_ISSET(cursor, WT_CURSOR_RAW_OK) || strcmp(fmt, "u") == 0) {
		item = va_arg(ap, WT_ITEM *);
		sz = item->size;
		cursor->value.data = item->data;
	} else if (strcmp(fmt, "t") == 0 ||
	    (isdigit(fmt[0]) && strcmp(fmt + 1, "t") == 0)) {
		sz = 1;
		buf = &cursor->value;
		WT_ERR(__wt_buf_initsize(session, buf, sz));
		*(uint8_t *)buf->mem = (uint8_t)va_arg(ap, int);
	} else {
		WT_ERR(
		    __wt_struct_sizev(session, &sz, cursor->value_format, ap));
		va_end(ap);
		va_start(ap, cursor);
		buf = &cursor->value;
		WT_ERR(__wt_buf_initsize(session, buf, sz));
		WT_ERR(__wt_struct_packv(session, buf->mem, sz,
		    cursor->value_format, ap));
	}
	F_SET(cursor, WT_CURSTD_VALUE_APP);
	cursor->value.size = WT_STORE_SIZE(sz);

	if (0) {
err:		cursor->saved_err = ret;
	}
	va_end(ap);
	API_END(session);
}

/*
 * __cursor_search --
 *	WT_CURSOR->search default implementation.
 */
static int
__cursor_search(WT_CURSOR *cursor)
{
	int exact;

	WT_RET(cursor->search_near(cursor, &exact));
	return ((exact == 0) ? 0 : WT_NOTFOUND);
}

/*
 * __wt_cursor_close --
 *	WT_CURSOR->close default implementation.
 */
int
__wt_cursor_close(WT_CURSOR *cursor)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	CURSOR_API_CALL(cursor, session, close, NULL);

	__wt_buf_free(session, &cursor->key);
	__wt_buf_free(session, &cursor->value);

	if (F_ISSET(cursor, WT_CURSTD_OPEN))
		TAILQ_REMOVE(&session->cursors, cursor, q);

	__wt_free(session, cursor->uri);
	__wt_free(session, cursor);

err:	API_END(session);
	return (ret);
}

/*
 * __cursor_runtime_config --
 *	Set runtime-configurable settings.
 */
static int
__cursor_runtime_config(WT_CURSOR *cursor, const char *cfg[])
{
	WT_DECL_RET;
	WT_CONFIG_ITEM cval;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cursor->session;

	if ((ret =
	    __wt_config_gets_defno(session, cfg, "overwrite", &cval)) == 0) {
		if (cval.val)
			F_SET(cursor, WT_CURSTD_OVERWRITE);
		else
			F_CLR(cursor, WT_CURSTD_OVERWRITE);
	}
	WT_RET_NOTFOUND_OK(ret);

	return (0);
}

/*
 * __wt_cursor_dup --
 *	Duplicate a cursor.
 */
int
__wt_cursor_dup(WT_SESSION_IMPL *session,
    WT_CURSOR *to_dup, const char *cfg[], WT_CURSOR **cursorp)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_ITEM key;

	/* Open a new cursor with the same URI. */
	WT_ERR(__wt_open_cursor(session, to_dup->uri, NULL, cfg, &cursor));

	/*
	 * Get a copy of the cursor's raw key, and set it in the new cursor,
	 * then search for that key to position the cursor.
	 *
	 * Don't clear (or allocate memory for) the WT_ITEM structure because
	 * all that happens underneath is the data and size fields are reset
	 * to reference the cursor's key.
	 */
	WT_ERR(__wt_cursor_get_raw_key(to_dup, &key));
	__wt_cursor_set_raw_key(cursor, &key);
	WT_ERR(cursor->search(cursor));

	if (0) {
err:		if (cursor != NULL)
			WT_TRET(cursor->close(cursor));
		cursor = NULL;
	}

	*cursorp = cursor;
	return (ret);
}

/*
 * __wt_cursor_init --
 *	Default cursor initialization.
 *
 *	Most cursors are "public", and added to the list in the session
 *	to be closed when the cursor is closed.  However, some cursors are
 *	opened for internal use, or are opened inside another cursor (such
 *	as column groups or indices within a table cursor), and adding those
 *	cursors to the list introduces ordering dependencies into
 *	WT_SESSION->close that we prefer to avoid.
 */
int
__wt_cursor_init(WT_CURSOR *cursor,
    const char *uri, WT_CURSOR *owner, const char *cfg[], WT_CURSOR **cursorp)
{
	WT_CURSOR *cdump;
	WT_CONFIG_ITEM cval;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)cursor->session;

	/*
	 * Fill in unspecified cursor methods: get/set key/value, equality,
	 * search and reconfiguration are all standard.  Otherwise, if the
	 * method isn't set, assume it's unsupported.
	 */
	if (cursor->get_key == NULL)
		cursor->get_key = __wt_cursor_get_key;
	if (cursor->get_value == NULL)
		cursor->get_value = __wt_cursor_get_value;
	if (cursor->set_key == NULL)
		cursor->set_key = __wt_cursor_set_key;
	if (cursor->set_value == NULL)
		cursor->set_value = __wt_cursor_set_value;
	if (cursor->compare == NULL)
		cursor->compare = (int (*)
		    (WT_CURSOR *, WT_CURSOR *, int *))__wt_cursor_notsup;
	if (cursor->next == NULL)
		cursor->next = __wt_cursor_notsup;
	if (cursor->prev == NULL)
		cursor->prev = __wt_cursor_notsup;
	if (cursor->reset == NULL)
		cursor->reset = __wt_cursor_noop;
	if (cursor->search == NULL)
		cursor->search = __cursor_search;
	if (cursor->search_near == NULL)
		cursor->search_near =
		    (int (*)(WT_CURSOR *, int *))__wt_cursor_notsup;
	if (cursor->insert == NULL)
		cursor->insert = __wt_cursor_notsup;
	if (cursor->update == NULL)
		cursor->update = __wt_cursor_notsup;
	if (cursor->remove == NULL)
		cursor->remove = __wt_cursor_notsup;
	if (cursor->close == NULL)
		WT_RET_MSG(session, EINVAL, "cursor lacks a close method");
	if (cursor->compare == NULL)
		cursor->compare = (int (*)
		    (WT_CURSOR *, WT_CURSOR *, int *))__wt_cursor_notsup;

	if (cursor->uri == NULL)
		WT_RET(__wt_strdup(session, uri, &cursor->uri));

	WT_CLEAR(cursor->key);
	WT_CLEAR(cursor->value);

	/* Set runtime-configurable settings. */
	WT_RET(__cursor_runtime_config(cursor, cfg));

	/*
	 * append
	 * The append flag is only relevant to column stores.
	 */
	if (WT_CURSOR_RECNO(cursor)) {
		WT_RET(__wt_config_gets_defno(session, cfg, "append", &cval));
		if (cval.val != 0)
			F_SET(cursor, WT_CURSTD_APPEND);
	}

	/*
	 * checkpoint
	 * Checkpoint cursors are read-only.
	 */
	WT_RET(__wt_config_gets_defno(session, cfg, "checkpoint", &cval));
	if (cval.len != 0) {
		cursor->insert = __wt_cursor_notsup;
		cursor->update = __wt_cursor_notsup;
		cursor->remove = __wt_cursor_notsup;
	}

	/* dump */
	WT_RET(__wt_config_gets_defno(session, cfg, "dump", &cval));
	if (cval.len != 0) {
		/*
		 * Dump cursors should not have owners: only the top-level
		 * cursor should be wrapped in a dump cursor.
		 */
		WT_ASSERT(session, owner == NULL);

		F_SET(cursor,
		    WT_STRING_MATCH("print", cval.str, cval.len) ?
		    WT_CURSTD_DUMP_PRINT : WT_CURSTD_DUMP_HEX);
		WT_RET(__wt_curdump_create(cursor, owner, &cdump));
		owner = cdump;
	} else
		cdump = NULL;

	/* raw */
	WT_RET(__wt_config_gets_defno(session, cfg, "raw", &cval));
	if (cval.val != 0)
		F_SET(cursor, WT_CURSTD_RAW);

	/*
	 * Cursors that are internal to some other cursor (such as file cursors
	 * inside a table cursor) should be closed after the containing cursor.
	 * Arrange for that to happen by putting internal cursors after their
	 * owners on the queue.
	 */
	if (owner != NULL)
		TAILQ_INSERT_AFTER(&session->cursors, owner, cursor, q);
	else
		TAILQ_INSERT_HEAD(&session->cursors, cursor, q);

	F_SET(cursor, WT_CURSTD_OPEN);

	*cursorp = (cdump != NULL) ? cdump : cursor;
	return (0);
}
