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

    WT_RET(__wt_config_gets(session, cfg, "shared_storage.local_retention", &cval));
    conn->storage_retain_secs = (uint64_t)cval.val * WT_MINUTE;

    return (__wt_storage_manager_config(session, cfg));
}

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
    mgr->workers_max = (uint32_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "shared_storage_manager.threads_min", &cval));
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
    return (F_ISSET(S2C(session), WT_CONN_SERVER_STORAGE));
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
         * - See if there is any "merging" work to do to prepare and create an object that is
         *   suitable for placing onto shared storage.
         * - Do the work to create said objects.
         * - Move the objects.
         * - See if there is any "overlapping" data that needs to be removed from local tier.
         * - Remove the local objects.
         */
    }

    if (0) {
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

    F_SET(conn, WT_CONN_SERVER_STORAGE);

    WT_ERR(__wt_open_internal_session(conn, "storage-server", true, 0, &conn->storage_session));
    session = conn->storage_session;

    WT_ERR(__wt_cond_alloc(session, "storage server", &conn->storage_cond));

    /* Start the thread. */
    WT_ERR(__wt_thread_create(session, &conn->storage_tid, __storage_server, session));
    conn->storage_tid_set = true;

    if (0) {
err:
        (void)__wt_storage_destroy(session);
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
    F_CLR(conn, WT_CONN_SERVER_STORAGE);
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
