/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_prepare_notsup --
 *	Unsupported session method; prepare notsupport version.
 */
static int
__wt_session_prepare_notsup(WT_SESSION_IMPL *session)
{
	WT_RET_MSG(session,
	    ENOTSUP, "Unsupported session method in prepare transaction state");
}

/*
 * __wt_session_open_cursor_prepare --
 *	WT_SESSION->open_cursor method.
 */
int
__wt_session_open_cursor_prepare(WT_SESSION *wt_session,
    const char *uri, WT_CURSOR *to_dup, const char *config, WT_CURSOR **cursorp)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(to_dup);
	WT_UNUSED(config);
	WT_UNUSED(cursorp);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, open_cursor);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_alter_prepare --
 *	WT_SESSION->alter method; prepare notsupport version.
 */
int
__wt_session_alter_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, alter);

	WT_STAT_CONN_INCR(session, session_table_alter_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_create_prepare --
 *	WT_SESSION->create method; prepare notsupport version.
 */
int
__wt_session_create_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, create);

	WT_STAT_CONN_INCR(session, session_table_create_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_log_flush_prepare --
 *	WT_SESSION->log_flush method; prepare notsupport version.
 */
int
__wt_session_log_flush_prepare(WT_SESSION *wt_session, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, log_flush);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_log_printf_prepare --
 *	WT_SESSION->log_printf method; prepare notsupport version.
 */
int
__wt_session_log_printf_prepare(WT_SESSION *wt_session, const char *fmt, ...)
    WT_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(fmt);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, log_printf);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_rebalance_prepare --
 *	WT_SESSION->rebalance method; prepare notsupport version.
 */
int
__wt_session_rebalance_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, rebalance);

	WT_STAT_CONN_INCR(session, session_table_rebalance_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_rename_prepare --
 *	WT_SESSION->rename method; prepare notsupport version.
 */
int
__wt_session_rename_prepare(WT_SESSION *wt_session,
    const char *uri, const char *newuri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(newuri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, rename);

	WT_STAT_CONN_INCR(session, session_table_rename_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_drop_prepare --
 *	WT_SESSION->drop method; prepare notsupport version.
 */
int
__wt_session_drop_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, drop);

	WT_STAT_CONN_INCR(session, session_table_drop_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_join_prepare --
 *	WT_SESSION->join method; prepare notsupport version.
 */
int
__wt_session_join_prepare(WT_SESSION *wt_session, WT_CURSOR *join_cursor,
    WT_CURSOR *ref_cursor, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(join_cursor);
	WT_UNUSED(ref_cursor);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, join);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_salvage_prepare --
 *	WT_SESSION->salvage method; prepare notsupport version.
 */
int
__wt_session_salvage_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, salvage);

	WT_STAT_CONN_INCR(session, session_table_salvage_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_truncate_prepare --
 *	WT_SESSION->truncate method; prepare notsupport version.
 */
int
__wt_session_truncate_prepare(WT_SESSION *wt_session,
    const char *uri, WT_CURSOR *start, WT_CURSOR *stop, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(start);
	WT_UNUSED(stop);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, truncate);

	WT_STAT_CONN_INCR(session, session_table_truncate_fail);
	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_upgrade_prepare --
 *	WT_SESSION->upgrade method; prepare notsupport version.
 */
int
__wt_session_upgrade_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, upgrade);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_verify_prepare --
 *	WT_SESSION->verify method; prepare notsupport version.
 */
int
__wt_session_verify_prepare(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, verify);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_prepare_transaction_prepare --
 *	WT_SESSION->prepare_transaction method; prepare notsupport version.
 */
int
__wt_session_prepare_transaction_prepare(
    WT_SESSION *wt_session, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, prepare_transaction);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_timestamp_transaction_prepare --
 *	WT_SESSION->timestamp_transaction method; prepare notsupport version.
 */
int
__wt_session_timestamp_transaction_prepare(
    WT_SESSION *wt_session, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, timestamp_transaction);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_transaction_pinned_range_prepare --
 *	WT_SESSION->transaction_pinned_range method; prepare notsupport version.
 */
int
__wt_session_transaction_pinned_range_prepare(
    WT_SESSION *wt_session, uint64_t *prange)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(prange);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, pinned_range);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}

/*
 * __wt_session_snapshot_prepare --
 *	WT_SESSION->snapshot method; prepare notsupport version.
 */
int
__wt_session_snapshot_prepare(WT_SESSION *wt_session, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, snapshot);

	ret = __wt_session_prepare_notsup(session);
err:	API_END_RET(session, ret);
}
