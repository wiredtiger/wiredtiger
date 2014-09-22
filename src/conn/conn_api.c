/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define	WT_BASECONFIG	"WiredTiger.basecfg"
#define	WT_USERCONFIG	"WiredTiger.config"

static int __conn_statistics_config(WT_SESSION_IMPL *, const char *[]);

/*
 * ext_collate --
 *	Call the collation function (external API version).
 */
static int
ext_collate(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
    WT_COLLATOR *collator, WT_ITEM *first, WT_ITEM *second, int *cmpp)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *session;

	conn = (WT_CONNECTION_IMPL *)wt_api->conn;
	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = conn->default_session;

	WT_RET(__wt_compare(session, collator, first, second, cmpp));

	return (0);
}

/*
 * ext_collator_config --
 *	Given a configuration, configure the collator (external API version).
 */
static int
ext_collator_config(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
    WT_CONFIG_ARG *cfg_arg, WT_COLLATOR **collatorp, int *ownp)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *session;
	const char **cfg;

	conn = (WT_CONNECTION_IMPL *)wt_api->conn;
	if ((session = (WT_SESSION_IMPL *)wt_session) == NULL)
		session = conn->default_session;

	/* The default is a standard lexicographic comparison. */
	if ((cfg = (const char **)cfg_arg) == NULL)
		return (0);

	return (__wt_collator_config(session, cfg, collatorp, ownp));
}

/*
 * __wt_collator_config --
 *	Given a configuration, configure the collator.
 */
int
__wt_collator_config(WT_SESSION_IMPL *session, const char **cfg,
    WT_COLLATOR **collatorp, int *ownp)
{
	WT_CONNECTION_IMPL *conn;
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_NAMED_COLLATOR *ncoll;

	*collatorp = NULL;
	*ownp = 0;

	conn = S2C(session);

	if ((ret = __wt_config_gets(session, cfg, "collator", &cval)) != 0)
		return (ret == WT_NOTFOUND ? 0 : ret);

	if (cval.len > 0) {
		TAILQ_FOREACH(ncoll, &conn->collqh, q)
			if (WT_STRING_MATCH(ncoll->name, cval.str, cval.len))
				break;

		if (ncoll == NULL)
			WT_RET_MSG(session, EINVAL,
			    "unknown collator '%.*s'", (int)cval.len, cval.str);

		if (ncoll->collator->customize != NULL) {
			WT_RET(__wt_config_gets(session,
			    session->dhandle->cfg, "app_metadata", &cval));
			WT_RET(ncoll->collator->customize(
			    ncoll->collator, &session->iface,
			    session->dhandle->name, &cval, collatorp));
		}
		if (*collatorp == NULL)
			*collatorp = ncoll->collator;
		else
			*ownp = 1;
	}

	return (0);
}

/*
 * __conn_get_extension_api --
 *	WT_CONNECTION.get_extension_api method.
 */
static WT_EXTENSION_API *
__conn_get_extension_api(WT_CONNECTION *wt_conn)
{
	WT_CONNECTION_IMPL *conn;

	conn = (WT_CONNECTION_IMPL *)wt_conn;

	conn->extension_api.conn = wt_conn;
	conn->extension_api.err_printf = __wt_ext_err_printf;
	conn->extension_api.msg_printf = __wt_ext_msg_printf;
	conn->extension_api.strerror = wiredtiger_strerror;
	conn->extension_api.scr_alloc = __wt_ext_scr_alloc;
	conn->extension_api.scr_free = __wt_ext_scr_free;
	conn->extension_api.collator_config = ext_collator_config;
	conn->extension_api.collate = ext_collate;
	conn->extension_api.config_parser_open = __wt_ext_config_parser_open;
	conn->extension_api.config_get = __wt_ext_config_get;
	conn->extension_api.metadata_insert = __wt_ext_metadata_insert;
	conn->extension_api.metadata_remove = __wt_ext_metadata_remove;
	conn->extension_api.metadata_search = __wt_ext_metadata_search;
	conn->extension_api.metadata_update = __wt_ext_metadata_update;
	conn->extension_api.struct_pack = __wt_ext_struct_pack;
	conn->extension_api.struct_size = __wt_ext_struct_size;
	conn->extension_api.struct_unpack = __wt_ext_struct_unpack;
	conn->extension_api.transaction_id = __wt_ext_transaction_id;
	conn->extension_api.transaction_isolation_level =
	    __wt_ext_transaction_isolation_level;
	conn->extension_api.transaction_notify = __wt_ext_transaction_notify;
	conn->extension_api.transaction_oldest = __wt_ext_transaction_oldest;
	conn->extension_api.transaction_visible = __wt_ext_transaction_visible;
	conn->extension_api.version = wiredtiger_version;

	return (&conn->extension_api);
}

#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
	extern int snappy_extension_init(WT_CONNECTION *, WT_CONFIG_ARG *);
#endif
#ifdef HAVE_BUILTIN_EXTENSION_ZLIB
	extern int zlib_extension_init(WT_CONNECTION *, WT_CONFIG_ARG *);
#endif

/*
 * __conn_load_default_extensions --
 *	Load extensions that are enabled via --with-builtins
 */
static int
__conn_load_default_extensions(WT_CONNECTION_IMPL *conn)
{
	WT_UNUSED(conn);
#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
	WT_RET(snappy_extension_init(&conn->iface, NULL));
#endif
#ifdef HAVE_BUILTIN_EXTENSION_ZLIB
	WT_RET(zlib_extension_init(&conn->iface, NULL));
#endif
	return (0);
}

/*
 * __conn_load_extension --
 *	WT_CONNECTION->load_extension method.
 */
static int
__conn_load_extension(
    WT_CONNECTION *wt_conn, const char *path, const char *config)
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_DLH *dlh;
	WT_SESSION_IMPL *session;
	int (*load)(WT_CONNECTION *, WT_CONFIG_ARG *);
	int is_local;
	const char *init_name, *terminate_name;

	dlh = NULL;
	init_name = terminate_name = NULL;
	is_local = (strcmp(path, "local") == 0);

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, load_extension, config, cfg);

	/*
	 * This assumes the underlying shared libraries are reference counted,
	 * that is, that re-opening a shared library simply increments a ref
	 * count, and closing it simply decrements the ref count, and the last
	 * close discards the reference entirely -- in other words, we do not
	 * check to see if we've already opened this shared library.
	 */
	WT_ERR(__wt_dlopen(session, is_local ? NULL : path, &dlh));

	/*
	 * Find the load function, remember the unload function for when we
	 * close.
	 */
	WT_ERR(__wt_config_gets(session, cfg, "entry", &cval));
	WT_ERR(__wt_strndup(session, cval.str, cval.len, &init_name));
	WT_ERR(__wt_dlsym(session, dlh, init_name, 1, &load));

	WT_ERR(__wt_config_gets(session, cfg, "terminate", &cval));
	WT_ERR(__wt_strndup(session, cval.str, cval.len, &terminate_name));
	WT_ERR(__wt_dlsym(session, dlh, terminate_name, 0, &dlh->terminate));

	/* Call the load function last, it simplifies error handling. */
	WT_ERR(load(wt_conn, (WT_CONFIG_ARG *)cfg));

	/* Link onto the environment's list of open libraries. */
	__wt_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->dlhqh, dlh, q);
	__wt_spin_unlock(session, &conn->api_lock);
	dlh = NULL;

err:	if (dlh != NULL)
		WT_TRET(__wt_dlclose(session, dlh));
	__wt_free(session, init_name);
	__wt_free(session, terminate_name);

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_load_extensions --
 *	Load the list of application-configured extensions.
 */
static int
__conn_load_extensions(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG subconfig;
	WT_CONFIG_ITEM cval, skey, sval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_ITEM(exconfig);
	WT_DECL_ITEM(expath);
	WT_DECL_RET;

	conn = S2C(session);

	WT_ERR(__conn_load_default_extensions(conn));

	WT_ERR(__wt_config_gets(session, cfg, "extensions", &cval));
	WT_ERR(__wt_config_subinit(session, &subconfig, &cval));
	while ((ret = __wt_config_next(&subconfig, &skey, &sval)) == 0) {
		if (expath == NULL)
			WT_ERR(__wt_scr_alloc(session, 0, &expath));
		WT_ERR(__wt_buf_fmt(
		    session, expath, "%.*s", (int)skey.len, skey.str));
		if (sval.len > 0) {
			if (exconfig == NULL)
				WT_ERR(__wt_scr_alloc(session, 0, &exconfig));
			WT_ERR(__wt_buf_fmt(session,
			    exconfig, "%.*s", (int)sval.len, sval.str));
		}
		WT_ERR(conn->iface.load_extension(&conn->iface,
		    expath->data, (sval.len > 0) ? exconfig->data : NULL));
	}
	WT_ERR_NOTFOUND_OK(ret);

err:	__wt_scr_free(&expath);
	__wt_scr_free(&exconfig);

	return (ret);
}

/*
 * __conn_add_collator --
 *	WT_CONNECTION->add_collator method.
 */
static int
__conn_add_collator(WT_CONNECTION *wt_conn,
    const char *name, WT_COLLATOR *collator, const char *config)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_COLLATOR *ncoll;
	WT_SESSION_IMPL *session;

	ncoll = NULL;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, add_collator, config, cfg);
	WT_UNUSED(cfg);

	WT_ERR(__wt_calloc_def(session, 1, &ncoll));
	WT_ERR(__wt_strdup(session, name, &ncoll->name));
	ncoll->collator = collator;

	__wt_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->collqh, ncoll, q);
	ncoll = NULL;
	__wt_spin_unlock(session, &conn->api_lock);

err:	if (ncoll != NULL) {
		__wt_free(session, ncoll->name);
		__wt_free(session, ncoll);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __wt_conn_remove_collator --
 *	Remove collator added by WT_CONNECTION->add_collator, only used
 * internally.
 */
int
__wt_conn_remove_collator(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_COLLATOR *ncoll;

	conn = S2C(session);

	while ((ncoll = TAILQ_FIRST(&conn->collqh)) != NULL) {
		/* Call any termination method. */
		if (ncoll->collator->terminate != NULL)
			WT_TRET(ncoll->collator->terminate(
			    ncoll->collator, (WT_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->collqh, ncoll, q);
		__wt_free(session, ncoll->name);
		__wt_free(session, ncoll);
	}

	return (ret);
}

/*
 * __conn_add_compressor --
 *	WT_CONNECTION->add_compressor method.
 */
static int
__conn_add_compressor(WT_CONNECTION *wt_conn,
    const char *name, WT_COMPRESSOR *compressor, const char *config)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_COMPRESSOR *ncomp;
	WT_SESSION_IMPL *session;

	WT_UNUSED(name);
	WT_UNUSED(compressor);
	ncomp = NULL;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, add_compressor, config, cfg);
	WT_UNUSED(cfg);

	WT_ERR(__wt_calloc_def(session, 1, &ncomp));
	WT_ERR(__wt_strdup(session, name, &ncomp->name));
	ncomp->compressor = compressor;

	__wt_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->compqh, ncomp, q);
	ncomp = NULL;
	__wt_spin_unlock(session, &conn->api_lock);

err:	if (ncomp != NULL) {
		__wt_free(session, ncomp->name);
		__wt_free(session, ncomp);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __wt_conn_remove_compressor --
 *	remove compressor added by WT_CONNECTION->add_compressor, only used
 * internally.
 */
int
__wt_conn_remove_compressor(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_COMPRESSOR *ncomp;

	conn = S2C(session);

	while ((ncomp = TAILQ_FIRST(&conn->compqh)) != NULL) {
		/* Call any termination method. */
		if (ncomp->compressor->terminate != NULL)
			WT_TRET(ncomp->compressor->terminate(
			    ncomp->compressor, (WT_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->compqh, ncomp, q);
		__wt_free(session, ncomp->name);
		__wt_free(session, ncomp);
	}

	return (ret);
}

/*
 * __conn_add_data_source --
 *	WT_CONNECTION->add_data_source method.
 */
static int
__conn_add_data_source(WT_CONNECTION *wt_conn,
    const char *prefix, WT_DATA_SOURCE *dsrc, const char *config)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_DATA_SOURCE *ndsrc;
	WT_SESSION_IMPL *session;

	ndsrc = NULL;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, add_data_source, config, cfg);
	WT_UNUSED(cfg);

	WT_ERR(__wt_calloc_def(session, 1, &ndsrc));
	WT_ERR(__wt_strdup(session, prefix, &ndsrc->prefix));
	ndsrc->dsrc = dsrc;

	/* Link onto the environment's list of data sources. */
	__wt_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->dsrcqh, ndsrc, q);
	ndsrc = NULL;
	__wt_spin_unlock(session, &conn->api_lock);

err:	if (ndsrc != NULL) {
		__wt_free(session, ndsrc->prefix);
		__wt_free(session, ndsrc);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __wt_conn_remove_data_source --
 *	Remove data source added by WT_CONNECTION->add_data_source.
 */
int
__wt_conn_remove_data_source(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_NAMED_DATA_SOURCE *ndsrc;

	conn = S2C(session);

	while ((ndsrc = TAILQ_FIRST(&conn->dsrcqh)) != NULL) {
		/* Call any termination method. */
		if (ndsrc->dsrc->terminate != NULL)
			WT_TRET(ndsrc->dsrc->terminate(
			    ndsrc->dsrc, (WT_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->dsrcqh, ndsrc, q);
		__wt_free(session, ndsrc->prefix);
		__wt_free(session, ndsrc);
	}

	return (ret);
}

/*
 * __conn_add_extractor --
 *	WT_CONNECTION->add_extractor method.
 */
static int
__conn_add_extractor(WT_CONNECTION *wt_conn,
    const char *name, WT_EXTRACTOR *extractor, const char *config)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(name);
	WT_UNUSED(extractor);
	ret = ENOTSUP;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, add_extractor, config, cfg);
	WT_UNUSED(cfg);

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_async_flush --
 *	WT_CONNECTION.async_flush method.
 */
static int
__conn_async_flush(WT_CONNECTION *wt_conn)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL_NOCONF(conn, session, async_flush);
	WT_ERR(__wt_async_flush(session));

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_async_new_op --
 *	WT_CONNECTION.async_new_op method.
 */
static int
__conn_async_new_op(WT_CONNECTION *wt_conn, const char *uri, const char *config,
    WT_ASYNC_CALLBACK *callback, WT_ASYNC_OP **asyncopp)
{
	WT_ASYNC_OP_IMPL *op;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL(conn, session, async_new_op, config, cfg);
	WT_ERR(__wt_async_new_op(session, uri, config, cfg, callback, &op));

	*asyncopp = &op->iface;

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_get_home --
 *	WT_CONNECTION.get_home method.
 */
static const char *
__conn_get_home(WT_CONNECTION *wt_conn)
{
	return (((WT_CONNECTION_IMPL *)wt_conn)->home);
}

/*
 * __conn_configure_method --
 *	WT_CONNECTION.configure_method method.
 */
static int
__conn_configure_method(WT_CONNECTION *wt_conn, const char *method,
    const char *uri, const char *config, const char *type, const char *check)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	CONNECTION_API_CALL_NOCONF(conn, session, configure_method);

	ret = __wt_configure_method(session, method, uri, config, type, check);

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_is_new --
 *	WT_CONNECTION->is_new method.
 */
static int
__conn_is_new(WT_CONNECTION *wt_conn)
{
	return (((WT_CONNECTION_IMPL *)wt_conn)->is_new);
}

/*
 * __conn_close --
 *	WT_CONNECTION->close method.
 */
static int
__conn_close(WT_CONNECTION *wt_conn, const char *config)
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	WT_SESSION_IMPL *s, *session;
	uint32_t i;

	conn = (WT_CONNECTION_IMPL *)wt_conn;

	CONNECTION_API_CALL(conn, session, close, config, cfg);

	WT_TRET(__wt_config_gets(session, cfg, "leak_memory", &cval));
	if (cval.val != 0)
		F_SET(conn, WT_CONN_LEAK_MEMORY);

err:	/*
	 * Rollback all running transactions.
	 * We do this as a separate pass because an active transaction in one
	 * session could cause trouble when closing a file, even if that
	 * session never referenced that file.
	 */
	for (s = conn->sessions, i = 0; i < conn->session_cnt; ++s, ++i)
		if (s->active && !F_ISSET(s, WT_SESSION_INTERNAL) &&
		    F_ISSET(&s->txn, TXN_RUNNING)) {
			wt_session = &s->iface;
			WT_TRET(wt_session->rollback_transaction(
			    wt_session, NULL));
		}

	/* Close open, external sessions. */
	for (s = conn->sessions, i = 0; i < conn->session_cnt; ++s, ++i)
		if (s->active && !F_ISSET(s, WT_SESSION_INTERNAL)) {
			wt_session = &s->iface;
			/*
			 * Notify the user that we are closing the session
			 * handle via the registered close callback.
			 */
			if (s->event_handler->handle_close != NULL)
				WT_TRET(s->event_handler->handle_close(
				    s->event_handler, wt_session, NULL));
			WT_TRET(wt_session->close(wt_session, config));
		}

	WT_TRET(__wt_connection_close(conn));

	/* We no longer have a session, don't try to update it. */
	session = NULL;

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_reconfigure --
 *	WT_CONNECTION->reconfigure method.
 */
static int
__conn_reconfigure(WT_CONNECTION *wt_conn, const char *config)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	const char *p, *config_cfg[] = { NULL, NULL, NULL };

	conn = (WT_CONNECTION_IMPL *)wt_conn;

	CONNECTION_API_CALL(conn, session, reconfigure, config, cfg);
	WT_UNUSED(cfg);

	/* Serialize reconfiguration. */
	__wt_spin_lock(session, &conn->reconfig_lock);

	/*
	 * The configuration argument has been checked for validity, replace the
	 * previous connection configuration.
	 *
	 * DO NOT merge the configuration before the reconfigure calls.  Some
	 * of the underlying reconfiguration functions do explicit checks with
	 * the second element of the configuration array, knowing the defaults
	 * are in slot #1 and the application's modifications are in slot #2.
	 */
	config_cfg[0] = conn->cfg;
	config_cfg[1] = config;

	WT_ERR(__conn_statistics_config(session, config_cfg));
	WT_ERR(__wt_async_reconfig(session, config_cfg));
	WT_ERR(__wt_cache_config(session, config_cfg));
	WT_ERR(__wt_cache_pool_config(session, config_cfg));
	WT_ERR(__wt_checkpoint_server_create(session, config_cfg));
	WT_ERR(__wt_lsm_manager_config(session, config_cfg));
	WT_ERR(__wt_statlog_create(session, config_cfg));
	WT_ERR(__wt_verbose_config(session, config_cfg));

	WT_ERR(__wt_config_merge(session, config_cfg, &p));
	__wt_free(session, conn->cfg);
	conn->cfg = p;

err:	__wt_spin_unlock(session, &conn->reconfig_lock);

	API_END_RET(session, ret);
}

/*
 * __conn_open_session --
 *	WT_CONNECTION->open_session method.
 */
static int
__conn_open_session(WT_CONNECTION *wt_conn,
    WT_EVENT_HANDLER *event_handler, const char *config,
    WT_SESSION **wt_sessionp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session, *session_ret;

	*wt_sessionp = NULL;

	conn = (WT_CONNECTION_IMPL *)wt_conn;
	session_ret = NULL;

	CONNECTION_API_CALL(conn, session, open_session, config, cfg);
	WT_UNUSED(cfg);

	WT_ERR(__wt_open_session(conn, event_handler, config, &session_ret));

	*wt_sessionp = &session_ret->iface;

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_config_append --
 *	Append an entry to a config stack.
 */
static inline void
__conn_config_append(const char *cfg[], const char *config)
{
	while (*cfg != NULL)
		++cfg;
	*cfg = config;
}

/*
 * __conn_config_file --
 *	Read in any WiredTiger_config file in the home directory.
 */
static int
__conn_config_file(WT_SESSION_IMPL *session,
    const char *filename, const char **cfg, WT_ITEM *cbuf)
{
	WT_DECL_RET;
	WT_FH *fh;
	off_t size;
	size_t len;
	int exist, quoted;
	uint8_t *p, *t;

	fh = NULL;

	/* Check for an optional configuration file. */
	WT_RET(__wt_exist(session, filename, &exist));
	if (!exist)
		return (0);

	/* Open the configuration file. */
	WT_RET(__wt_open(session, filename, 0, 0, 0, &fh));
	WT_ERR(__wt_filesize(session, fh, &size));
	if (size == 0)
		goto err;

	/*
	 * Sanity test: a 100KB configuration file would be insane.  (There's
	 * no practical reason to limit the file size, but I can either limit
	 * the file size to something rational, or I can add code to test if
	 * the off_t size is larger than a uint32_t, which is more complicated
	 * and a waste of time.)
	 */
	if (size > 100 * 1024)
		WT_ERR_MSG(
		    session, EFBIG, "Configuration file too big: %s", filename);
	len = (size_t)size;

	/*
	 * Copy the configuration file into memory, with a little slop, I'm not
	 * interested in debugging off-by-ones.
	 *
	 * The beginning of a file is the same as if we run into an unquoted
	 * newline character, simplify the parsing loop by pretending that's
	 * what we're doing.
	 */
	WT_ERR(__wt_buf_init(session, cbuf, len + 10));
	WT_ERR(
	    __wt_read(session, fh, (off_t)0, len, ((uint8_t *)cbuf->mem) + 1));
	((uint8_t *)cbuf->mem)[0] = '\n';
	cbuf->size = len + 1;

	/*
	 * Collapse the file's lines into a single string: newline characters
	 * are replaced with commas unless the newline is quoted or backslash
	 * escaped.  Comment lines (an unescaped newline where the next non-
	 * white-space character is a hash), are discarded.
	 */
	for (quoted = 0, p = t = cbuf->mem; len > 0;) {
		/*
		 * Backslash pairs pass through untouched, unless immediately
		 * preceding a newline, in which case both the backslash and
		 * the newline are discarded.  Backslash characters escape
		 * quoted characters, too, that is, a backslash followed by a
		 * quote doesn't start or end a quoted string.
		 */
		if (*p == '\\' && len > 1) {
			if (p[1] != '\n') {
				*t++ = p[0];
				*t++ = p[1];
			}
			p += 2;
			len -= 2;
			continue;
		}

		/*
		 * If we're in a quoted string, or starting a quoted string,
		 * take all characters, including white-space and newlines.
		 */
		if (quoted || *p == '"') {
			if (*p == '"')
				quoted = !quoted;
			*t++ = *p++;
			--len;
			continue;
		}

		/* Everything else gets taken, except for newline characters. */
		if (*p != '\n') {
			*t++ = *p++;
			--len;
			continue;
		}

		/*
		 * Replace any newline characters with commas (and strings of
		 * commas are safe).
		 *
		 * After any newline, skip to a non-white-space character; if
		 * the next character is a hash mark, skip to the next newline.
		 */
		for (;;) {
			for (*t++ = ','; --len > 0 && isspace(*++p);)
				;
			if (len == 0)
				break;
			if (*p != '#')
				break;
			while (--len > 0 && *++p != '\n')
				;
			if (len == 0)
				break;
		}
	}
	*t = '\0';

#if 0
	fprintf(stderr, "file config: {%s}\n", (const char *)cbuf->data);
#endif

	/* Check the configuration string. */
	WT_ERR(__wt_config_check(session,
	    WT_CONFIG_REF(session, wiredtiger_open), cbuf->data, 0));

	/* Append it to the stack. */
	__conn_config_append(cfg, cbuf->data);

err:	if (fh != NULL)
		WT_TRET(__wt_close(session, fh));
	return (ret);
}

/*
 * __conn_config_env --
 *	Read configuration from an environment variable, if set.
 */
static int
__conn_config_env(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	const char *env_config;

	if ((env_config = getenv("WIREDTIGER_CONFIG")) == NULL ||
	    strlen(env_config) == 0)
		return (0);

	/*
	 * Security stuff:
	 *
	 * If the "use_environment_priv" configuration string is set, use the
	 * environment variable if the process has appropriate privileges.
	 */
	WT_RET(__wt_config_gets(session, cfg, "use_environment_priv", &cval));
	if (cval.val == 0 && __wt_has_priv())
		WT_RET_MSG(session, WT_ERROR, "%s",
		    "WIREDTIGER_CONFIG environment variable set but process "
		    "lacks privileges to use that environment variable");

	/* Check the configuration string. */
	WT_RET(__wt_config_check(session,
	    WT_CONFIG_REF(session, wiredtiger_open), env_config, 0));

	__conn_config_append(cfg, env_config);
	return (0);
}

/*
 * __conn_home --
 *	Set the database home directory.
 */
static int
__conn_home(WT_SESSION_IMPL *session, const char *home, const char *cfg[])
{
	WT_CONFIG_ITEM cval;

	/* If the application specifies a home directory, use it. */
	if (home != NULL)
		goto copy;

	/* If there's no WIREDTIGER_HOME environment variable, use ".". */
	if ((home = getenv("WIREDTIGER_HOME")) == NULL || strlen(home) == 0) {
		home = ".";
		goto copy;
	}

	/*
	 * Security stuff:
	 *
	 * Unless the "use_environment_priv" configuration string is set,
	 * fail if the process is running with special privileges.
	 */
	WT_RET(__wt_config_gets(session, cfg, "use_environment_priv", &cval));
	if (cval.val == 0 && __wt_has_priv())
		WT_RET_MSG(session, WT_ERROR, "%s",
		    "WIREDTIGER_HOME environment variable set but process "
		    "lacks privileges to use that environment variable");

copy:	return (__wt_strdup(session, home, &S2C(session)->home));
}

/*
 * __conn_single --
 *	Confirm that no other thread of control is using this database.
 */
static int
__conn_single(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn, *t;
	WT_DECL_RET;
	off_t size;
	size_t len;
	int created;
	char buf[256];

	conn = S2C(session);

	/*
	 * Optionally create the wiredtiger flag file if it doesn't already
	 * exist.  We don't actually care if we create it or not, the "am I the
	 * only locker" tests are all that matter.
	 */
	WT_RET(__wt_config_gets(session, cfg, "create", &cval));
	WT_RET(__wt_open(session,
	    WT_SINGLETHREAD, cval.val == 0 ? 0 : 1, 0, 0, &conn->lock_fh));

	/*
	 * Lock a byte of the file: if we don't get the lock, some other process
	 * is holding it, we're done.  Note the file may be zero-length length,
	 * and that's OK, the underlying call supports acquisition of locks past
	 * the end-of-file.
	 */
	if (__wt_bytelock(conn->lock_fh, (off_t)0, 1) != 0)
		WT_ERR_MSG(session, EBUSY,
		    "WiredTiger database is already being managed by another "
		    "process");

	/* Check to see if another thread of control has this database open. */
	__wt_spin_lock(session, &__wt_process.spinlock);
	TAILQ_FOREACH(t, &__wt_process.connqh, q)
		if (t->home != NULL &&
		    t != conn && strcmp(t->home, conn->home) == 0) {
			ret = EBUSY;
			break;
		}
	__wt_spin_unlock(session, &__wt_process.spinlock);
	if (ret != 0)
		WT_ERR_MSG(session, EBUSY,
		    "WiredTiger database is already being managed by another "
		    "thread in this process");

	/*
	 * If the size of the file is 0, we created it (or we're racing with
	 * the thread that created it, it doesn't matter), write some bytes
	 * into the file.  Strictly speaking, this isn't even necessary, but
	 * zero-length files always make me nervous.
	 */
	WT_ERR(__wt_filesize(session, conn->lock_fh, &size));
	if (size == 0) {
		len = (size_t)snprintf(buf, sizeof(buf), "%s\n%s\n",
		    WT_SINGLETHREAD, WIREDTIGER_VERSION_STRING);
		WT_ERR(__wt_write(session, conn->lock_fh, (off_t)0, len, buf));
		created = 1;
	} else {
		WT_ERR(__wt_config_gets(session, cfg, "exclusive", &cval));
		if (cval.val != 0)
			WT_ERR_MSG(session, EEXIST,
			    "WiredTiger database already exists and exclusive "
			    "option configured");
		created = 0;
	}

	/*
	 * If we found a zero-length WiredTiger lock file, and eventually ended
	 * as the database owner, return that we created the database.  (There
	 * is a theoretical chance that another process created the WiredTiger
	 * lock file but we won the race to add the WT_CONNECTION_IMPL structure
	 * to the process' list.  It doesn't much matter, only one thread will
	 * be told it created the database.)
	 */
	conn->is_new = created;

	return (0);

err:	if (conn->lock_fh != NULL) {
		WT_TRET(__wt_close(session, conn->lock_fh));
		conn->lock_fh = NULL;
	}
	return (ret);
}

/*
 * __conn_statistics_config --
 *	Set statistics configuration.
 */
static int
__conn_statistics_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval, sval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint32_t flags;
	int set;

	conn = S2C(session);

	WT_RET(__wt_config_gets(session, cfg, "statistics", &cval));

	if (cval.len == 0) {
		conn->stat_flags = 0;
		return (0);
	}

	flags = 0;
	set = 0;
	if ((ret = __wt_config_subgets(
	    session, &cval, "none", &sval)) == 0 && sval.val != 0) {
		LF_SET(WT_CONN_STAT_NONE);
		++set;
	}
	WT_RET_NOTFOUND_OK(ret);

	if ((ret = __wt_config_subgets(
	    session, &cval, "fast", &sval)) == 0 && sval.val != 0) {
		LF_SET(WT_CONN_STAT_FAST);
		++set;
	}
	WT_RET_NOTFOUND_OK(ret);

	if ((ret = __wt_config_subgets(
	    session, &cval, "all", &sval)) == 0 && sval.val != 0) {
		LF_SET(WT_CONN_STAT_ALL | WT_CONN_STAT_FAST);
		++set;
	}
	WT_RET_NOTFOUND_OK(ret);

	if ((ret = __wt_config_subgets(
	    session, &cval, "clear", &sval)) == 0 && sval.val != 0)
		LF_SET(WT_CONN_STAT_CLEAR);
	WT_RET_NOTFOUND_OK(ret);

	if (set > 1)
		WT_RET_MSG(session, EINVAL,
		    "only one statistics configuration value may be specified");

	/* Configuring statistics clears any existing values. */
	conn->stat_flags = flags;

	return (0);
}

/*
 * __wt_verbose_config --
 *	Set verbose configuration.
 */
int
__wt_verbose_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval, sval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	uint32_t flags;
	static const struct {
		const char *name;
		uint32_t flag;
	} *ft, verbtypes[] = {
		{ "api",		WT_VERB_API },
		{ "block",		WT_VERB_BLOCK },
		{ "checkpoint",		WT_VERB_CHECKPOINT },
		{ "compact",		WT_VERB_COMPACT },
		{ "evict",		WT_VERB_EVICT },
		{ "evictserver",	WT_VERB_EVICTSERVER },
		{ "fileops",		WT_VERB_FILEOPS },
		{ "log",		WT_VERB_LOG },
		{ "lsm",		WT_VERB_LSM },
		{ "metadata",		WT_VERB_METADATA },
		{ "mutex",		WT_VERB_MUTEX },
		{ "overflow",		WT_VERB_OVERFLOW },
		{ "read",		WT_VERB_READ },
		{ "reconcile",		WT_VERB_RECONCILE },
		{ "recovery",		WT_VERB_RECOVERY },
		{ "salvage",		WT_VERB_SALVAGE },
		{ "shared_cache",	WT_VERB_SHARED_CACHE },
		{ "split",		WT_VERB_SPLIT },
		{ "verify",		WT_VERB_VERIFY },
		{ "version",		WT_VERB_VERSION },
		{ "write",		WT_VERB_WRITE },
		{ NULL, 0 }
	};

	conn = S2C(session);

	WT_RET(__wt_config_gets(session, cfg, "verbose", &cval));

	flags = 0;
	for (ft = verbtypes; cval.len != 0 && ft->name != NULL; ft++) {
		if ((ret = __wt_config_subgets(
		    session, &cval, ft->name, &sval)) == 0 && sval.val != 0) {
#ifdef HAVE_VERBOSE
			LF_SET(ft->flag);
#else
			WT_RET_MSG(session, EINVAL,
			    "Verbose option specified when WiredTiger built "
			    "without verbose support. Add --enable-verbose to "
			    "configure command and rebuild to include support "
			    "for verbose messages");
#endif
		}
		WT_RET_NOTFOUND_OK(ret);
	}

	conn->verbose = flags;
	return (0);
}

/*
 * __conn_write_config --
 *	Save the configuration used to create a database.
 */
static int
__conn_write_config(
    WT_SESSION_IMPL *session, const char *filename, const char *config)
{
	FILE *fp;
	WT_CONFIG parser;
	WT_CONFIG_ITEM ckey, cval;
	WT_DECL_RET;
	char *path;

	/* If there is no configuration, don't bother creating an empty file. */
	if (config == NULL)
		return (0);

	WT_RET(__wt_filename(session, filename, &path));
	if ((fp = fopen(path, "w")) == NULL)
		ret = __wt_errno();
	__wt_free(session, path);
	if (fp == NULL)
		return (ret);

	fprintf(fp, "%s\n\n",
	    "# Do not modify this file.\n"
	    "#\n"
	    "# WiredTiger created this file when the database was created,\n"
	    "# to store persistent database settings.  Instead of changing\n"
	    "# these settings, set a WIREDTIGER_CONFIG environment variable\n"
	    "# or create a WiredTiger.config file to override them.");
	WT_ERR(__wt_config_init(session, &parser, config));
	while ((ret = __wt_config_next(&parser, &ckey, &cval)) == 0) {
		/* Skip "create". */
		if (WT_STRING_MATCH("create", ckey.str, ckey.len))
			continue;

		/* Fix quoting for non-trivial settings. */
		if (cval.type == WT_CONFIG_ITEM_STRING) {
			--cval.str;
			cval.len += 2;
		}
		fprintf(fp, "%.*s=%.*s\n",
		    (int)ckey.len, ckey.str, (int)cval.len, cval.str);
	}
	WT_ERR_NOTFOUND_OK(ret);

err:	WT_TRET(fclose(fp));
	return (ret);
}

/*
 * wiredtiger_open --
 *	Main library entry point: open a new connection to a WiredTiger
 *	database.
 */
int
wiredtiger_open(const char *home, WT_EVENT_HANDLER *event_handler,
    const char *config, WT_CONNECTION **wt_connp)
{
	static const WT_CONNECTION stdc = {
		__conn_async_flush,
		__conn_async_new_op,
		__conn_close,
		__conn_reconfigure,
		__conn_get_home,
		__conn_configure_method,
		__conn_is_new,
		__conn_open_session,
		__conn_load_extension,
		__conn_add_data_source,
		__conn_add_collator,
		__conn_add_compressor,
		__conn_add_extractor,
		__conn_get_extension_api
	};
	static const struct {
		const char *name;
		uint32_t flag;
	} *ft, file_types[] = {
		{ "checkpoint",	WT_FILE_TYPE_CHECKPOINT },
		{ "data",	WT_FILE_TYPE_DATA },
		{ "log",	WT_FILE_TYPE_LOG },
		{ NULL, 0 }
	};
	WT_CONFIG_ITEM cval, sval;
	WT_CONNECTION_IMPL *conn;
	WT_ITEM cbbuf, cubuf;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	int exist;
	/* Leave space for optional additional configuration. */
	const char *cfg[] = { NULL, NULL, NULL, NULL, NULL, NULL };

	*wt_connp = NULL;

	conn = NULL;
	session = NULL;
	WT_CLEAR(cbbuf);
	WT_CLEAR(cubuf);

	WT_RET(__wt_library_init());

	WT_RET(__wt_calloc_def(NULL, 1, &conn));
	conn->iface = stdc;

	/*
	 * Immediately link the structure into the connection structure list:
	 * the only thing ever looked at on that list is the database name,
	 * and a NULL value is fine.
	 */
	__wt_spin_lock(NULL, &__wt_process.spinlock);
	TAILQ_INSERT_TAIL(&__wt_process.connqh, conn, q);
	__wt_spin_unlock(NULL, &__wt_process.spinlock);

	session = conn->default_session = &conn->dummy_session;
	session->iface.connection = &conn->iface;
	session->name = "wiredtiger_open";
	__wt_random_init(session->rnd);
	__wt_event_handler_set(session, event_handler);

	/* Remaining basic initialization of the connection structure. */
	WT_ERR(__wt_connection_init(conn));

	/* Check/set the configuration strings. */
	WT_ERR(__wt_config_check(session,
	    WT_CONFIG_REF(session, wiredtiger_open), config, 0));
	cfg[0] = WT_CONFIG_BASE(session, wiredtiger_open);
	cfg[1] = config;

	/* Finish configuring error messages so we get them right early. */
	WT_ERR(__wt_config_gets(session, cfg, "error_prefix", &cval));
	if (cval.len != 0)
		WT_ERR(__wt_strndup(
		    session, cval.str, cval.len, &conn->error_prefix));

	/* Get the database home. */
	WT_ERR(__conn_home(session, home, cfg));

	/* Make sure no other thread of control already owns this database. */
	WT_ERR(__conn_single(session, cfg));

	/*
	 * Build the configuration stack, in the following order (where later
	 * entries override earlier entries):
	 *
	 * 1. default wiredtiger_open configuration
	 * 2. base configuration file, created with the database (optional)
	 * 3. the config passed in by the application.
	 * 4. user configuration file (optional)
	 * 5. environment variable settings (optional)
	 *
	 * Clear the entry we added to the stack, we're going to build it in
	 * order.
	 */
	cfg[1] = NULL;

	/*
	 * The base configuration should not exist if we are creating this
	 * database.
	 */
	exist = 0;
	if (conn->is_new) {
		WT_ERR(__wt_exist(session, WT_BASECONFIG, &exist));
		if (exist)
			WT_ERR_MSG(session, EINVAL,
			    "%s exists on creation", WT_BASECONFIG);
	} else
		WT_ERR(__conn_config_file(session, WT_BASECONFIG, cfg, &cbbuf));

	/* Add the config string passed in by the application. */
	__conn_config_append(cfg, config);

	/*
	 * Read in user's config file and the config environment variable.
	 */
	WT_ERR(__conn_config_file(session, WT_USERCONFIG, cfg, &cubuf));
	WT_ERR(__conn_config_env(session, cfg));

	/*
	 * Configuration ...
	 *
	 * We can't open sessions yet, so any configurations that cause
	 * sessions to be opened must be handled inside __wt_connection_open.
	 */
	WT_ERR(__wt_config_gets(session, cfg, "hazard_max", &cval));
	conn->hazard_max = (uint32_t)cval.val;

	WT_ERR(__wt_config_gets(session, cfg, "session_max", &cval));
	conn->session_size = (uint32_t)cval.val + WT_NUM_INTERNAL_SESSIONS;

	WT_ERR(__wt_config_gets(session, cfg, "lsm_manager.merge", &cval));
	if (cval.val)
		F_SET(conn, WT_CONN_LSM_MERGE);

	WT_ERR(__wt_config_gets(
	    session, cfg, "lsm_manager.worker_thread_max", &cval));
	if (cval.val)
		conn->lsm_manager.lsm_workers_max = (uint32_t)cval.val;

	WT_ERR(__wt_config_gets(session, cfg, "checkpoint_sync", &cval));
	if (cval.val)
		F_SET(conn, WT_CONN_CKPT_SYNC);

	WT_ERR(__wt_verbose_config(session, cfg));

	WT_ERR(__wt_config_gets(session, cfg, "buffer_alignment", &cval));
	if (cval.val == -1)
		conn->buffer_alignment = WT_BUFFER_ALIGNMENT_DEFAULT;
	else
		conn->buffer_alignment = (size_t)cval.val;
#ifndef HAVE_POSIX_MEMALIGN
	if (conn->buffer_alignment != 0)
		WT_ERR_MSG(session, EINVAL,
		    "buffer_alignment requires posix_memalign");
#endif

	WT_ERR(__wt_config_gets(session, cfg, "direct_io", &cval));
	for (ft = file_types; cval.len != 0 && ft->name != NULL; ft++) {
		ret = __wt_config_subgets(session, &cval, ft->name, &sval);
		if (ret == 0) {
			if (sval.val)
				FLD_SET(conn->direct_io, ft->flag);
		} else if (ret != WT_NOTFOUND)
			goto err;
	}

	WT_ERR(__wt_config_gets(session, cfg, "file_extend", &cval));
	for (ft = file_types; cval.len != 0 && ft->name != NULL; ft++) {
		ret = __wt_config_subgets(session, &cval, ft->name, &sval);
		if (ret == 0) {
			switch (ft->flag) {
			case WT_FILE_TYPE_DATA:
				conn->data_extend_len = sval.val;
				break;
			case WT_FILE_TYPE_LOG:
				conn->log_extend_len = sval.val;
				break;
			}
		} else if (ret != WT_NOTFOUND)
			goto err;
	}

	WT_ERR(__wt_config_gets(session, cfg, "mmap", &cval));
	conn->mmap = cval.val == 0 ? 0 : 1;

	WT_ERR(__conn_statistics_config(session, cfg));

	/* Write the base configuration file, if we're creating the database. */
	if (conn->is_new)
		WT_ERR(__conn_write_config(session, WT_BASECONFIG, config));

	/* Now that we know if verbose is configured, output the version. */
	WT_ERR(__wt_verbose(
	    session, WT_VERB_VERSION, "%s", WIREDTIGER_VERSION_STRING));

	/*
	 * Open the connection, then reset the local session as the real one
	 * was allocated in __wt_connection_open.
	 */
	WT_ERR(__wt_connection_open(conn, cfg));
	session = conn->default_session;

	/*
	 * Check on the turtle and metadata files, creating them if necessary
	 * (which avoids application threads racing to create the metadata file
	 * later).  Once the metadata file exists, get a reference to it in
	 * the connection's session.
	 */
	WT_ERR(__wt_turtle_init(session));
	WT_ERR(__wt_metadata_open(session));

	/*
	 * Load the extensions after initialization completes; extensions expect
	 * everything else to be in place, and the extensions call back into the
	 * library.
	 */
	WT_ERR(__conn_load_extensions(session, cfg));

	/*
	 * Start the worker threads last.
	 */
	WT_ERR(__wt_connection_workers(session, cfg));

	/* Merge the final configuration for later reconfiguration. */
	WT_ERR(__wt_config_merge(session, cfg, &conn->cfg));

	STATIC_ASSERT(offsetof(WT_CONNECTION_IMPL, iface) == 0);
	*wt_connp = &conn->iface;

err:	__wt_buf_free(session, &cbbuf);
	__wt_buf_free(session, &cubuf);

	if (ret != 0 && conn != NULL)
		WT_TRET(__wt_connection_close(conn));

	return (ret);
}
