/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef __GNUC__
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ > 1)
/*
 * !!!
 * GCC with -Wformat-nonliteral complains about calls to strftime in this file.
 * There's nothing wrong, this makes the warning go away.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif
#endif

/*
 * __share_storage_once --
 *     Perform one iteration of shared storage maintenance.
 */
static int
__share_storage_once(WT_SESSION_IMPL *session, bool force)
{
    WT_UNUSED(session);
    WT_UNUSED(force);
    /*
     * - See if there is any "merging" work to do to prepare and create an object that is
     *   suitable for placing onto shared storage.
     * - Do the work to create said objects.
     * - Move the objects.
     */
    return (0);
}

/*
 * __share_storage_remove --
 *     Perform one iteration of shared storage local tier removal.
 */
static int
__share_storage_remove_tree(WT_SESSION_IMPL *session, const char *uri, bool force)
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    size_t len;
    uint64_t now;
    char *config, *newfile;
    const char *cfg[2], *filename;

    if (uri == NULL)
        return (0);
    __wt_verbose(session, WT_VERB_TIERED, "Removing tree %s", uri);
    WT_ASSERT(session, WT_PREFIX_MATCH(uri, "shared:"));
    filename = uri;
    WT_PREFIX_SKIP_REQUIRED(session, filename, "shared:");
    len = strlen("file:") + strlen(filename) + 1;
    WT_ERR(__wt_calloc_def(session, len, &newfile));
    WT_ERR(__wt_snprintf(newfile, len, "file:%s", filename));

    /*
     * If the file version of the shared object does not exist there is nothing to do.
     */
    WT_ERR(__wt_metadata_search(session, newfile, &config));

    /*
     * We have a local version of this shared data. Check its metadata for when it expires and
     * remove if necessary.
     */
    cfg[0] = config;
    cfg[1] = NULL;
    WT_ERR(__wt_config_gets(session, cfg, "local_retain", &cval));
    __wt_seconds(session, &now);
    if (force || (uint64_t)cval.val + S2C(session)->storage_retain_secs >= now)
        /*
         * We want to remove the entry and the file. Probably do a schema_drop on the file:uri.
         */
        ;

err:
    __wt_free(session, config);
    __wt_free(session, newfile);
    return (ret);
}

/*
 * __share_storage_remove --
 *     Perform one iteration of shared storage local tier removal.
 */
static int
__share_storage_remove(WT_SESSION_IMPL *session, bool force)
{
    WT_UNUSED(session);
    WT_UNUSED(force);

    /*
     * We want to walk the metadata perhaps and for each shared URI, call remove on its file:URI
     * version.
     */
    WT_RET(__share_storage_remove_tree(session, NULL, force));
    return (0);
}

/*
 * __wt_share_storage --
 *     Entry function for share_storage method.
 */
int
__wt_share_storage(WT_SESSION_IMPL *session, const char *config)
{
    WT_CONFIG_ITEM cval;
    const char *cfg[2];
    bool force;

    WT_STAT_CONN_INCR(session, share_storage);
    cfg[0] = (char *)config;
    cfg[1] = NULL;
    WT_RET(__wt_config_gets(session, cfg, "force", &cval));
    force = cval.val != 0;

    WT_RET(__share_storage_once(session, force));
    return (0);
}

/*
 * __wt_storage_config --
 *     Parse and setup the storage server options.
 */
int
__wt_storage_config(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    bool enabled;

    /*
     * A note on reconfiguration: the standard "is this configuration string allowed" checks should
     * fail if reconfiguration has invalid strings, for example, "log=(enabled)", or
     * "statistics_log=(path=XXX)", because the connection reconfiguration method doesn't allow
     * those strings. Additionally, the base configuration values during reconfiguration are the
     * currently configured values (so we don't revert to default values when repeatedly
     * reconfiguring), and configuration processing of a currently set value should not change the
     * currently set value.
     *
     * In this code path, a previous storage log server reconfiguration may have stopped the
     * server (and we're about to restart it). Because stopping the server discarded the configured
     * information stored in the connection structure, we have to re-evaluate all configuration
     * values, reconfiguration can't skip any of them.
     */
    conn = S2C(session);

    WT_RET(__wt_config_gets(session, cfg, "shared_storage.enabled", &cval));
    enabled = cval.val != 0;

    if (enabled)
        FLD_SET(conn->storage_flags, WT_CONN_STORAGE_ENABLED);
    else
        FLD_CLR(conn->storage_flags, WT_CONN_STORAGE_ENABLED);

    WT_RET(__wt_config_gets(session, cfg, "shared_storage.auth_timeout", &cval));
    conn->storage_auth_timeout = (uint64_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "shared_storage.auth_token", &cval));
    conn->storage_auth_token = cval.str;

    WT_RET(__wt_config_gets(session, cfg, "shared_storage.local_retention", &cval));
    conn->storage_retain_secs = (uint64_t)cval.val * WT_MINUTE;
    WT_STAT_CONN_SET(session, storage_retention, conn->storage_retain_secs);

    return (__wt_storage_manager_config(session, cfg));
}

/*
 * __wt_storage_manager_config --
 *     Parse and setup the storage manager options.
 */
int
__wt_storage_manager_config(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_STORAGE_MANAGER *mgr;

    conn = S2C(session);
    mgr = &conn->storage_manager;

    /* Only start the server if wait time is non-zero */
    WT_RET(__wt_config_gets(session, cfg, "shared_storage_manager.wait", &cval));
    mgr->wait_usecs = (uint64_t)cval.val * WT_MILLION;

    WT_RET(__wt_config_gets(session, cfg, "shared_storage_manager.threads_max", &cval));
    if (cval.val > WT_STORAGE_MAX_WORKERS)
        WT_RET_MSG(session, EINVAL, "Maximum storage workers of %" PRIu32 " larger than %d",
          (uint32_t)cval.val, WT_STORAGE_MAX_WORKERS);
    mgr->workers_max = (uint32_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "shared_storage_manager.threads_min", &cval));
    if (cval.val < WT_STORAGE_MIN_WORKERS)
        WT_RET_MSG(session, EINVAL, "Minimum storage workers of %" PRIu32 " less than %d",
          (uint32_t)cval.val, WT_STORAGE_MIN_WORKERS);
    mgr->workers_min = (uint32_t)cval.val;
    WT_ASSERT(session, mgr->workers_min <= mgr->workers_max);
    return (0);
}

/*
 * __storage_server_run_chk --
 *     Check to decide if the statistics log server should continue running.
 */
static bool
__storage_server_run_chk(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    return ((FLD_ISSET(conn->server_flags, WT_CONN_SERVER_STORAGE)) &&
      !F_ISSET(&conn->storage_manager, WT_STORAGE_MANAGER_SHUTDOWN));
}

/*
 * __storage_server --
 *     The statistics server thread.
 */
static WT_THREAD_RET
__storage_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_ITEM path, tmp;
    WT_SESSION_IMPL *session;
    WT_STORAGE_MANAGER *mgr;

    session = arg;
    conn = S2C(session);
    mgr = &conn->storage_manager;

    WT_CLEAR(path);
    WT_CLEAR(tmp);

    /*
     * We need a temporary place to build a path and an entry prefix. The length of the path plus
     * 128 should be more than enough.
     *
     * We also need a place to store the current path, because that's how we know when to
     * close/re-open the file.
     */
    for (;;) {
        /* Wait until the next event. */
        __wt_cond_wait(session, conn->storage_cond, mgr->wait_usecs, __storage_server_run_chk);

        /* Check if we're quitting or being reconfigured. */
        if (!__storage_server_run_chk(session))
            break;

        /*
         * Here is where we do work. Work we expect to do:
         *
         */
        WT_ERR(__share_storage_once(session, false));
        WT_ERR(__share_storage_remove(session, false));
    }

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "storage server error"));
    }
    __wt_buf_free(session, &path);
    __wt_buf_free(session, &tmp);
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_storage_create --
 *     Start the statistics server thread.
 */
int
__wt_storage_create(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    /* Set first, the thread might run before we finish up. */
    WT_RET(__wt_storage_config(session, cfg));
    if (!F_ISSET(conn, WT_CONN_STORAGE_ENABLED))
        return (0);

    FLD_SET(conn->server_flags, WT_CONN_SERVER_STORAGE);

    WT_ERR(__wt_open_internal_session(conn, "storage-server", true, 0, &conn->storage_session));
    session = conn->storage_session;

    WT_ERR(__wt_cond_alloc(session, "storage server", &conn->storage_cond));

    /* Start the thread. */
    WT_ERR(__wt_thread_create(session, &conn->storage_tid, __storage_server, session));
    conn->storage_tid_set = true;

    if (0) {
err:
        WT_TRET(__wt_storage_destroy(session));
    }
    return (ret);
}

/*
 * __wt_storage_destroy --
 *     Destroy the storage server thread.
 */
int
__wt_storage_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);
    /*
     * This may look a lot more like __wt_lsm_manager_destroy instead. It depends on what the final
     * API looks like. For now handle it like a single internal worker thread.
     */

    /* Stop the server thread. */
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_STORAGE);
    if (conn->storage_tid_set) {
        __wt_cond_signal(session, conn->storage_cond);
        WT_TRET(__wt_thread_join(session, &conn->storage_tid));
        conn->storage_tid_set = false;
    }
    __wt_cond_destroy(session, &conn->storage_cond);

    /* Close the server thread's session. */
    if (conn->storage_session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->storage_session));
        conn->storage_session = NULL;
    }

    return (ret);
}
