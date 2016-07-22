/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Custom NEED macros for metadata cursors - that copy the values into the
 * backing metadata table cursor.
 */
#define	WT_MD_CURSOR_NEEDKEY(cursor) do {				\
	WT_CURSOR_NEEDKEY(cursor);					\
	WT_ERR(__wt_buf_set(session,					\
	    &((WT_CURSOR_METADATA *)(cursor))->file_cursor->key,	\
	    cursor->key.data, cursor->key.size));			\
	F_SET(((WT_CURSOR_METADATA *)(cursor))->file_cursor,		\
	    WT_CURSTD_KEY_EXT);						\
} while (0)

#define	WT_MD_CURSOR_NEEDVALUE(cursor) do {				\
	WT_CURSOR_NEEDVALUE(cursor);					\
	WT_ERR(__wt_buf_set(session,					\
	    &((WT_CURSOR_METADATA *)(cursor))->file_cursor->value,	\
	    cursor->value.data, cursor->value.size));			\
	F_SET(((WT_CURSOR_METADATA *)(cursor))->file_cursor,		\
	    WT_CURSTD_VALUE_EXT);					\
} while (0)

/*
 * __wt_schema_create_final --
 *	Create a single configuration line from a set of configuration strings,
 * including all of the defaults declared for a session.create, and stripping
 * any configuration strings that don't belong in a session.create. Here for
 * the wt dump command utility, which reads a set of configuration strings and
 * needs to add in the defaults and then collapse them into single string for
 * a subsequent load.
 */
int
__wt_schema_create_final(
    WT_SESSION_IMPL *session, char *cfg_arg[], char **value_ret)
{
	WT_DECL_RET;
	u_int i;
	const char **cfg;

	/*
	 * Count the entries in the original,
	 * Allocate a copy with the defaults as the first entry,
	 * Collapse the whole thing into a single configuration string (which
	 * also strips any entries that don't appear in the first entry).
	 */
	for (i = 0; cfg_arg[i] != NULL; ++i)
		;
	WT_RET(__wt_calloc_def(session, i + 2, &cfg));
	cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_create);
	for (i = 0; cfg_arg[i] != NULL; ++i)
		cfg[i + 1] = cfg_arg[i];
	cfg[i + 1] = NULL;

	ret = __wt_config_collapse(session, cfg, value_ret);

	__wt_free(session, cfg);
	return (ret);
}

/*
 * __curmetadata_follow_source --
 *	The value of the "source" configuration variable is a URI;
 * return the value of this URI in the metadata.
 */
static int
__curmetadata_follow_source(WT_SESSION_IMPL *session, WT_CURSOR *srch,
    char *config, char **result)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	size_t len;
	char *v;
	void *p;

	p = NULL;
	WT_ERR(__wt_config_getones(session, config, "source", &cval));
	len = cval.len + 10;
	WT_ERR(__wt_malloc(session, len, &p));
	(void)snprintf(p, len, "%.*s", (int)cval.len, cval.str);
	srch->set_key(srch, p);
	if ((ret = srch->search(srch)) == WT_NOTFOUND)
		WT_ERR(EINVAL);
	WT_ERR(ret);
	WT_ERR(srch->get_value(srch, &v));
	WT_ERR(__wt_strdup(session, v, result));

err:	__wt_free(session, p);
	return (ret);
}

#define	TABLE_PFX_LEN		(strlen("table:"))
#define	COLGROUP_PFX_LEN	(strlen("colgroup:"))

/*
 * __schema_create_strip --
 *	Discard any configuration information from a schema entry that is
 * not applicable to an session.create call. Here for the metadata:create
 * URI.  For a table URI that contains no named column groups, fold in the
 * configuration from the implicit column group and its source. For a named
 * column group URI, fold in its source.
 */
static int
__schema_create_strip(WT_SESSION_IMPL *session, WT_CURSOR_METADATA *mdc,
    const char *key, char *value, char **value_ret)
{
	WT_CURSOR *c;
	WT_DECL_RET;
	size_t len;
	char **cfg, **finalcfg, *v, *_cfg[5] = {NULL, NULL, NULL, value, NULL};
	void *p;

	cfg = &_cfg[3];		/* position on value */
	p = NULL;
	c = NULL;
	if (key != NULL && WT_PREFIX_MATCH(key, "table:")) {
		c = mdc->create_cursor;
		len = strlen(key) - TABLE_PFX_LEN + COLGROUP_PFX_LEN + 1;
		WT_ERR(__wt_malloc(session, len, &p));
		/*
		 * When a table is created without column groups,
		 * we create one without a name.
		 */
		(void)snprintf(p, len, "colgroup:%.*s",
		    (int)(strlen(key) - TABLE_PFX_LEN), key + TABLE_PFX_LEN);
		c->set_key(c, p);
		if ((ret = c->search(c)) == 0) {
			WT_ERR(c->get_value(c, &v));
			WT_ERR(__wt_strdup(session, v, --cfg));
			WT_ERR(__curmetadata_follow_source(session, c, v,
			    --cfg));
		} else
			WT_ERR_NOTFOUND_OK(ret);
	} else if (key != NULL && WT_PREFIX_MATCH(key, "colgroup:")) {
		if (strchr(key + COLGROUP_PFX_LEN, ':') != NULL) {
			c = mdc->create_cursor;
			WT_ERR(__wt_strdup(session, value, --cfg));
			WT_ERR(__curmetadata_follow_source(session,
			    c, value, --cfg));
		}
	}
	finalcfg = cfg;
	*--finalcfg = (char *)WT_CONFIG_BASE(session, WT_SESSION_create);
	WT_ERR(__wt_config_collapse(session, (const char **)finalcfg,
	    value_ret));

err:	while (cfg < &_cfg[sizeof(_cfg)/sizeof(_cfg[0])] - 2)
		__wt_free(session, *cfg++);
	if (c != NULL)
		c->reset(c);
	__wt_free(session, p);
	return (ret);
}

/*
 * __curmetadata_setkv --
 *	Copy key/value into the public cursor, stripping internal metadata for
 *	"create-only" cursors.
 */
static int
__curmetadata_setkv(WT_CURSOR_METADATA *mdc, WT_CURSOR *fc)
{
	WT_CURSOR *c;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	char *value;

	value = NULL;
	c = &mdc->iface;
	session = (WT_SESSION_IMPL *)c->session;

	c->key.data = fc->key.data;
	c->key.size = fc->key.size;
	if (F_ISSET(mdc, WT_MDC_CREATEONLY)) {
		WT_ERR(__schema_create_strip(session, mdc, (char *)fc->key.data,
		    (char *)fc->value.data, &value));
		WT_ERR(__wt_buf_set(
		    session, &c->value, value, strlen(value) + 1));
	} else {
		c->value.data = fc->value.data;
		c->value.size = fc->value.size;
	}

	F_SET(c, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	F_CLR(mdc, WT_MDC_ONMETADATA);
	F_SET(mdc, WT_MDC_POSITIONED);

err:	__wt_free(session, value);
	return (ret);
}

/*
 * Check if a key matches the metadata.  The public value is "metadata:",
 * but also check for the internal version of the URI.
 */
#define	WT_KEY_IS_METADATA(key)						\
	(WT_STRING_MATCH(WT_METADATA_URI, (key)->data, (key)->size - 1) ||\
	 WT_STRING_MATCH(WT_METAFILE_URI, (key)->data, (key)->size - 1))

/*
 * __curmetadata_metadata_search --
 *	Retrieve the metadata for the metadata table
 */
static int
__curmetadata_metadata_search(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
{
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	char *value, *stripped;

	mdc = (WT_CURSOR_METADATA *)cursor;

	/* The metadata search interface allocates a new string in value. */
	WT_RET(__wt_metadata_search(session, WT_METAFILE_URI, &value));

	if (F_ISSET(mdc, WT_MDC_CREATEONLY)) {
		ret = __schema_create_strip(session, mdc, NULL, value,
		    &stripped);
		__wt_free(session, value);
		WT_RET(ret);
		value = stripped;
	}

	ret = __wt_buf_setstr(session, &cursor->value, value);
	__wt_free(session, value);
	WT_RET(ret);

	WT_RET(__wt_buf_setstr(session, &cursor->key, WT_METADATA_URI));

	F_SET(mdc, WT_MDC_ONMETADATA | WT_MDC_POSITIONED);
	F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	return (0);
}

/*
 * __curmetadata_compare --
 *	WT_CURSOR->compare method for the metadata cursor type.
 */
static int
__curmetadata_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
	WT_CURSOR *a_file_cursor, *b_file_cursor;
	WT_CURSOR_METADATA *a_mdc, *b_mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	a_mdc = ((WT_CURSOR_METADATA *)a);
	b_mdc = ((WT_CURSOR_METADATA *)b);
	a_file_cursor = a_mdc->file_cursor;
	b_file_cursor = b_mdc->file_cursor;

	CURSOR_API_CALL(a, session,
	    compare, ((WT_CURSOR_BTREE *)a_file_cursor)->btree);

	if (b->compare != __curmetadata_compare)
		WT_ERR_MSG(session, EINVAL,
		    "Can only compare cursors of the same type");

	WT_MD_CURSOR_NEEDKEY(a);
	WT_MD_CURSOR_NEEDKEY(b);

	if (F_ISSET(a_mdc, WT_MDC_ONMETADATA)) {
		if (F_ISSET(b_mdc, WT_MDC_ONMETADATA))
			*cmpp = 0;
		else
			*cmpp = 1;
	} else if (F_ISSET(b_mdc, WT_MDC_ONMETADATA))
		*cmpp = -1;
	else
		ret = a_file_cursor->compare(
		    a_file_cursor, b_file_cursor, cmpp);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_next --
 *	WT_CURSOR->next method for the metadata cursor type.
 */
static int
__curmetadata_next(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    next, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	if (!F_ISSET(mdc, WT_MDC_POSITIONED))
		WT_ERR(__curmetadata_metadata_search(session, cursor));
	else {
		/*
		 * When applications open metadata cursors, they expect to see
		 * all schema-level operations reflected in the results.  Query
		 * at read-uncommitted to avoid confusion caused by the current
		 * transaction state.
		 */
		WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->next(mdc->file_cursor));
		WT_ERR(ret);
		WT_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, WT_MDC_POSITIONED | WT_MDC_ONMETADATA);
		F_CLR(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_prev --
 *	WT_CURSOR->prev method for the metadata cursor type.
 */
static int
__curmetadata_prev(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    prev, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	if (F_ISSET(mdc, WT_MDC_ONMETADATA)) {
		ret = WT_NOTFOUND;
		goto err;
	}

	WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
	    ret = file_cursor->prev(file_cursor));
	if (ret == 0)
		WT_ERR(__curmetadata_setkv(mdc, file_cursor));
	else if (ret == WT_NOTFOUND)
		WT_ERR(__curmetadata_metadata_search(session, cursor));

err:	if (ret != 0) {
		F_CLR(mdc, WT_MDC_POSITIONED | WT_MDC_ONMETADATA);
		F_CLR(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_reset --
 *	WT_CURSOR->reset method for the metadata cursor type.
 */
static int
__curmetadata_reset(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    reset, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	if (F_ISSET(mdc, WT_MDC_POSITIONED) && !F_ISSET(mdc, WT_MDC_ONMETADATA))
		ret = file_cursor->reset(file_cursor);
	F_CLR(mdc, WT_MDC_POSITIONED | WT_MDC_ONMETADATA);
	F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_search --
 *	WT_CURSOR->search method for the metadata cursor type.
 */
static int
__curmetadata_search(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    search, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	WT_MD_CURSOR_NEEDKEY(cursor);

	if (WT_KEY_IS_METADATA(&cursor->key))
		WT_ERR(__curmetadata_metadata_search(session, cursor));
	else {
		WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->search(file_cursor));
		WT_ERR(ret);
		WT_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, WT_MDC_POSITIONED | WT_MDC_ONMETADATA);
		F_CLR(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_search_near --
 *	WT_CURSOR->search_near method for the metadata cursor type.
 */
static int
__curmetadata_search_near(WT_CURSOR *cursor, int *exact)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    search_near, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	WT_MD_CURSOR_NEEDKEY(cursor);

	if (WT_KEY_IS_METADATA(&cursor->key)) {
		WT_ERR(__curmetadata_metadata_search(session, cursor));
		*exact = 1;
	} else {
		WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
		    ret = file_cursor->search_near(file_cursor, exact));
		WT_ERR(ret);
		WT_ERR(__curmetadata_setkv(mdc, file_cursor));
	}

err:	if (ret != 0) {
		F_CLR(mdc, WT_MDC_POSITIONED | WT_MDC_ONMETADATA);
		F_CLR(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
	}
	API_END_RET(session, ret);
}

/*
 * __curmetadata_insert --
 *	WT_CURSOR->insert method for the metadata cursor type.
 */
static int
__curmetadata_insert(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    insert, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	WT_MD_CURSOR_NEEDKEY(cursor);
	WT_MD_CURSOR_NEEDVALUE(cursor);

	/*
	 * Since the key/value formats are 's' the WT_ITEMs must contain a
	 * NULL terminated string.
	 */
	ret =
	    __wt_metadata_insert(session, cursor->key.data, cursor->value.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_update --
 *	WT_CURSOR->update method for the metadata cursor type.
 */
static int
__curmetadata_update(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    update, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	WT_MD_CURSOR_NEEDKEY(cursor);
	WT_MD_CURSOR_NEEDVALUE(cursor);

	/*
	 * Since the key/value formats are 's' the WT_ITEMs must contain a
	 * NULL terminated string.
	 */
	ret =
	    __wt_metadata_update(session, cursor->key.data, cursor->value.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_remove --
 *	WT_CURSOR->remove method for the metadata cursor type.
 */
static int
__curmetadata_remove(WT_CURSOR *cursor)
{
	WT_CURSOR *file_cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	file_cursor = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session,
	    remove, ((WT_CURSOR_BTREE *)file_cursor)->btree);

	WT_MD_CURSOR_NEEDKEY(cursor);

	/*
	 * Since the key format is 's' the WT_ITEM must contain a NULL
	 * terminated string.
	 */
	ret = __wt_metadata_remove(session, cursor->key.data);

err:	API_END_RET(session, ret);
}

/*
 * __curmetadata_close --
 *	WT_CURSOR->close method for the metadata cursor type.
 */
static int
__curmetadata_close(WT_CURSOR *cursor)
{
	WT_CURSOR *c;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	mdc = (WT_CURSOR_METADATA *)cursor;
	c = mdc->file_cursor;
	CURSOR_API_CALL(cursor, session, close, c == NULL ?
	    NULL : ((WT_CURSOR_BTREE *)c)->btree);

	if (c != NULL)
		ret = c->close(c);
	if ((c = mdc->create_cursor) != NULL)
		WT_TRET(c->close(c));
	WT_TRET(__wt_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __wt_curmetadata_open --
 *	WT_SESSION->open_cursor method for metadata cursors.
 *
 * Metadata cursors are a similar to a file cursor on the special metadata
 * table, except that the metadata for the metadata table (which is stored
 * in the turtle file) can also be queried.
 *
 * Metadata cursors are read-only by default.
 */
int
__wt_curmetadata_open(WT_SESSION_IMPL *session,
    const char *uri, WT_CURSOR *owner, const char *cfg[], WT_CURSOR **cursorp)
{
	WT_CURSOR_STATIC_INIT(iface,
	    __wt_cursor_get_key,		/* get-key */
	    __wt_cursor_get_value,		/* get-value */
	    __wt_cursor_set_key,		/* set-key */
	    __wt_cursor_set_value,		/* set-value */
	    __curmetadata_compare,		/* compare */
	    __wt_cursor_equals,			/* equals */
	    __curmetadata_next,			/* next */
	    __curmetadata_prev,			/* prev */
	    __curmetadata_reset,		/* reset */
	    __curmetadata_search,		/* search */
	    __curmetadata_search_near,		/* search-near */
	    __curmetadata_insert,		/* insert */
	    __curmetadata_update,		/* update */
	    __curmetadata_remove,		/* remove */
	    __wt_cursor_reconfigure_notsup,	/* reconfigure */
	    __curmetadata_close);		/* close */
	WT_CURSOR *cursor;
	WT_CURSOR_METADATA *mdc;
	WT_DECL_RET;
	WT_CONFIG_ITEM cval;

	WT_RET(__wt_calloc_one(session, &mdc));

	cursor = &mdc->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	cursor->key_format = "S";
	cursor->value_format = "S";

	/*
	 * Open the file cursor for operations on the regular metadata; don't
	 * use the existing, cached session metadata cursor, the configuration
	 * may not be the same.
	 */
	WT_ERR(__wt_metadata_cursor_open(session, cfg[1], &mdc->file_cursor));

	/*
	 * If we are only returning create config, strip internal metadata.
	 * We'll need some extra cursors to pull out column group information
	 * and chase "source" entries.
	 */
	if (WT_STREQ(uri, "metadata:create")) {
		F_SET(mdc, WT_MDC_CREATEONLY);
		WT_ERR(__wt_metadata_cursor_open(session, cfg[1],
		    &mdc->create_cursor));
	}

	WT_ERR(__wt_cursor_init(cursor, uri, owner, cfg, cursorp));

	/*
	 * Metadata cursors default to readonly; if not set to not-readonly,
	 * they are permanently readonly and cannot be reconfigured.
	 */
	WT_ERR(__wt_config_gets_def(session, cfg, "readonly", 1, &cval));
	if (cval.val != 0) {
		cursor->insert = __wt_cursor_notsup;
		cursor->update = __wt_cursor_notsup;
		cursor->remove = __wt_cursor_notsup;
	}

	if (0) {
err:		WT_TRET(__curmetadata_close(cursor));
		*cursorp = NULL;
	}
	return (ret);
}
