/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curconfig_close --
 *	WT_CURSOR->close method for the config cursor type.
 */
static int
__curconfig_close(WT_CURSOR *cursor)
{
	return (__wt_cursor_close(cursor));
}

/*
 * __wt_curconfig_open --
 *	WT_SESSION->open_cursor method for config cursors.
 */
int
__wt_curconfig_open(WT_SESSION_IMPL *session,
    const char *uri, const char *cfg[], WT_CURSOR **cursorp)
{
	WT_CURSOR_STATIC_INIT(iface,
	    NULL,			/* get-key */
	    NULL,			/* get-value */
	    NULL,			/* set-key */
	    NULL,			/* set-value */
	    NULL,			/* compare */
	    __wt_cursor_notsup,		/* next */
	    __wt_cursor_notsup,		/* prev */
	    __wt_cursor_notsup,		/* reset */
	    __wt_cursor_notsup,		/* search */
	    __wt_cursor_notsup,		/* search-near */
	    __wt_cursor_notsup,		/* insert */
	    __wt_cursor_notsup,		/* update */
	    __wt_cursor_notsup,		/* remove */
	    __curconfig_close);
	WT_CURSOR_CONFIG *cconfig;
	WT_CURSOR *cursor;
	WT_DECL_RET;

	STATIC_ASSERT(offsetof(WT_CURSOR_CONFIG, iface) == 0);

	WT_UNUSED(uri);

	WT_RET(__wt_calloc_def(session, 1, &cconfig));

	cursor = &cconfig->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->key_format = cursor->value_format = "S";

	/* __wt_cursor_init is last so we don't have to clean up on error. */
	WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

	if (0) {
err:		__wt_free(session, cconfig);
	}
	return (ret);
}
