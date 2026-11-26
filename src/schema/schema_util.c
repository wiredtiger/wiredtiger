/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __schema_backup_check_int --
 *     Helper for __wt_schema_backup_check. Intended to be called while holding the hot backup read
 *     lock.
 */
static int
__schema_backup_check_int(WT_SESSION_IMPL *session, const char *name)
{
    WT_CONNECTION_IMPL *conn;
    int i;
    char **backup_list;

    conn = S2C(session);

    /*
     * There is a window at the end of a backup where the list has been cleared from the connection
     * but the flag is still set. It is safe to drop at that point.
     */
    if (__wt_atomic_load_uint64_relaxed(&conn->backup.start) == 0 ||
      (backup_list = conn->backup.list) == NULL) {
        return (0);
    }
    for (i = 0; backup_list[i] != NULL; ++i) {
        if (strcmp(backup_list[i], name) == 0)
            return (__wt_set_return(session, EBUSY));
    }

    return (0);
}

/*
 * __wti_schema_backup_check --
 *     Check if a backup cursor is open and give an error if the schema operation will conflict.
 *     This is called after the schema operations have taken the schema lock so no hot backup cursor
 *     can be created until this is done.
 */
int
__wti_schema_backup_check(WT_SESSION_IMPL *session, const char *name)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);
    if (__wt_atomic_load_uint64_relaxed(&conn->backup.start) == 0)
        return (0);
    WT_WITH_HOTBACKUP_READ_LOCK_UNCOND(session, ret = __schema_backup_check_int(session, name));
    return (ret);
}

/*
 * __wt_schema_get_source --
 *     Find a matching data source or report an error.
 */
WT_DATA_SOURCE *
__wt_schema_get_source(WT_SESSION_IMPL *session, const char *name)
{
    WT_NAMED_DATA_SOURCE *ndsrc;

    TAILQ_FOREACH (ndsrc, &S2C(session)->dsrcqh, q)
        if (WT_PREFIX_MATCH(name, ndsrc->prefix))
            return (ndsrc->dsrc);
    return (NULL);
}

/*
 * __wti_schema_internal_session --
 *     Create and return an internal schema session if necessary.
 */
int
__wti_schema_internal_session(WT_SESSION_IMPL *session, WT_SESSION_IMPL **int_sessionp)
{
    /*
     * Open an internal session if a transaction is running so that the schema operations are not
     * logged and buffered with any log records in the transaction. The new session inherits its
     * flags from the original.
     */
    *int_sessionp = session;
    if (F_ISSET(session->txn, WT_TXN_RUNNING)) {
        /* We should not have a schema txn running now. */
        WT_ASSERT(session, !F_ISSET(session, WT_SESSION_SCHEMA_TXN));
        WT_RET(__wt_open_internal_session(
          S2C(session), "schema", true, session->flags, session->lock_flags, int_sessionp));
    }
    return (0);
}

/*
 * __wti_schema_session_release --
 *     Release an internal schema session if needed.
 */
int
__wti_schema_session_release(WT_SESSION_IMPL *session, WT_SESSION_IMPL *int_session)
{
    if (session != int_session) {
        WT_ERROR_INFO int_error_info = int_session->err_info;
        if (int_error_info.err != 0)
            __wt_session_set_last_error(
              session, int_error_info.err, int_error_info.sub_level_err, int_error_info.err_msg);
        WT_RET(__wt_session_close_internal(int_session));
    }
    return (0);
}

/*
 * __schema_uri_name --
 *     Return the object name after the URI prefixes, skipping at most two of them.
 */
static const char *
__schema_uri_name(const char *uri)
{
    int skipped;
    const char *name, *sep;

    name = uri;
    for (skipped = 0; skipped < 2 && (sep = strchr(name, ':')) != NULL; skipped++)
        name = sep + 1;
    return (name);
}

/*
 * __schema_internal_access_bypass --
 *     Return whether the session was configured to bypass the internal access checks; only for
 *     debugging and repair.
 */
static bool
__schema_internal_access_bypass(WT_SESSION_IMPL *session)
{
    return (F_ISSET(session, WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS));
}

/*
 * __schema_internals_check_skip --
 *     Return whether the internal table and WiredTiger name space checks are skipped; they only
 *     apply to direct application calls in diagnostic builds.
 */
static bool
__schema_internals_check_skip(WT_SESSION_IMPL *session)
{
#ifdef HAVE_DIAGNOSTIC
    /* Nested API calls may access internal files on behalf of a logical table operation. */
    return (__schema_internal_access_bypass(session) || !(API_USER_ENTRY(session)) ||
      F_ISSET(session, WT_SESSION_INTERNAL));
#else
    WT_UNUSED(session);
    return (true);
#endif
}

/*
 * __str_name_check --
 *     Internal function to disallow use of the WiredTiger name space and other internal entities.
 */
static int
__str_name_check(WT_SESSION_IMPL *session, const char *name, bool allow_wt)
{
    bool is_wt;

    if (__schema_internal_access_bypass(session))
        return (0);

    /*
     * Disallow JSON quoting characters -- the config string parsing code supports quoted strings,
     * but there's no good reason to use them in names and we're not going to do the testing.
     */
    if (strpbrk(name, "{},:[]\\\"'") != NULL)
        WT_RET_MSG(session, EINVAL,
          "%s: WiredTiger objects should not include grouping characters in their names", name);

    if (__schema_internals_check_skip(session))
        return (0);

    is_wt = WT_PREFIX_MATCH(name, "WiredTiger");
    if (is_wt && !allow_wt)
        WT_RET_MSG(session, EINVAL,
          "%s: the \"WiredTiger\" name space may not be used by applications", name);

    /*
     * When WT namespace entries are allowed, it's also an exception for the constituent check as
     * they don't have layered: entry that could be accessed.
     */
    if (!is_wt && __wt_session_is_layered_internal(session, name))
        WT_RET_MSG(session, EINVAL,
          "%s: direct access to an internal table is not allowed; configure the session with "
          "debug=(allow_internal_access=true) for debugging/repairing.",
          name);

    return (0);
}

/*
 * __wt_uri_name_check --
 *     Check the object name of a URI.
 */
int
__wt_uri_name_check(WT_SESSION_IMPL *session, const char *uri, bool allow_wt)
{
    const char *name;

    if (uri == NULL)
        return (0);

    name = __schema_uri_name(uri);
    if (name == uri)
        WT_RET_MSG(
          session, EINVAL, "%s: URI is expected to be passed; no colon separator detected", uri);

    return (__str_name_check(session, name, allow_wt));
}

/*
 * __wt_name_check --
 *     Check a name or URI that is not nul-terminated.
 */
int
__wt_name_check(
  WT_SESSION_IMPL *session, const char *str, size_t len, bool check_uri, bool allow_wt)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;

    WT_RET(__wt_scr_alloc(session, len, &tmp));

    WT_ERR(__wt_buf_fmt(session, tmp, "%.*s", (int)len, str));

    ret = check_uri ? __wt_uri_name_check(session, tmp->data, allow_wt) :
                      __str_name_check(session, tmp->data, allow_wt);

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_is_simple_table --
 *     Check whether the given table is simple.
 */
int
__wt_is_simple_table(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *colconf, bool *is_simplep)
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM ckey, cval;
    WT_DECL_RET;

    __wt_config_subinit(session, &cparser, colconf);
    *is_simplep = true;
    /* Count the number of columns: tables are "simple" if the columns are not named. */
    while ((ret = __wt_config_next(&cparser, &ckey, &cval)) == 0) {
        *is_simplep = false;
        break;
    }
    WT_RET_NOTFOUND_OK(ret);

    return (0);
}

/*
 * __wti_debug_crash_if_flag_set --
 *     Crash during schema operations for debugging purposes.
 */
void
__wti_debug_crash_if_flag_set(
  WT_SESSION_IMPL *session, uint32_t flag, const char *msg, const char *uri)
{
    /* FIXME-WT-12021: Replace this function and its call sites with a proper failpoint once the
     * framework is available. */
    if (FLD_ISSET(S2C(session)->debug.flags, flag)) {
        __wt_verbose_warning(session, WT_VERB_DEFAULT, "Simulating a crash %s '%s'", msg, uri);
        /* Wait for the previous metadata change to be persisted. */
        __wt_sleep(2, 0);
        __wt_abort(session);
    }
}
