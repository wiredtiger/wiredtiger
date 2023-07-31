/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __compact_server_run_chk --
 *     Check to decide if the compact server should continue running.
 */
static bool
__compact_server_run_chk(WT_SESSION_IMPL *session)
{
    return (FLD_ISSET(S2C(session)->server_flags, WT_CONN_SERVER_COMPACT));
}

/*
 * __compact_server --
 *     The compact server thread.
 */
static WT_THREAD_RET
__compact_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION *wt_session;
    WT_SESSION_IMPL *session;
    int exact;
    const char *config, *key;
    bool full_iteration;

    session = arg;
    conn = S2C(session);
    wt_session = (WT_SESSION *)session;
    cursor = NULL;
    config = NULL;
    key = NULL;
    exact = 0;
    full_iteration = false;

    for (;;) {

        /* When the entire metadata file has been parsed, take a break. */
        if (full_iteration) {
            full_iteration = false;
            /*
             * TODO: We may have processed just a few files, might as well sleep to give the system
             * a break.
             */
            __wt_sleep(10, 0);
        }

        /* Only an explicit call to the compact API should signal the thread to wake up. */
        if (!conn->background_compact.running) {
            /*
             * TODO: Depending on the previous state, we may not want to clear out the last key
             * used. This could be useful if the server was paused to be resumed later.
             */
            key = NULL;
            __wt_cond_wait(session, conn->background_compact.cond, 0, NULL);
        }

        /*
         * Now the server has been requested to run, check if it is supposed to, we could be
         * quitting or reconfiguring the connection too.
         */
        if (!__compact_server_run_chk(session))
            break;

        /* Open a metadata cursor. */
        WT_ERR(__wt_metadata_cursor(session, &cursor));

        /*
         * If we are in the middle of an iteration, position the cursor on the previously selected
         * key.
         */
        if (key != NULL) {
            cursor->set_key(cursor, key);
            WT_ERR(cursor->search_near(cursor, &exact));
        }

        /*
         * If no key has been set yet, move to the first record. If the previously used key could
         * not be found, make sure not to go backwards.
         */
        if (key == NULL || exact <= 0)
            WT_ERR_NOTFOUND_OK(cursor->next(cursor), true);

        /* Find a table to compact. */
        while (ret == 0) {
            WT_ERR(cursor->get_key(cursor, &key));
            if (WT_PREFIX_MATCH(key, "file:") && !WT_STREQ(key, WT_HS_URI))
                /* FIXME-WT-11343: check if the table is supposed to be compacted. */
                break;
            WT_ERR_NOTFOUND_OK(cursor->next(cursor), true);
        }

        /* We may have reached the end of the file. */
        if (ret == WT_NOTFOUND) {
            WT_ERR(__wt_metadata_cursor_release(session, &cursor));
            full_iteration = true;
            key = NULL;
            continue;
        }

        /* Always close the metadata cursor. */
        WT_ERR(__wt_metadata_cursor_release(session, &cursor));

        /* Compact the file with the latest configuration. */
        __wt_spin_lock(session, &conn->background_compact.cfg_lock);
        WT_ERR(__wt_strndup(
          session, conn->background_compact.cfg, strlen(conn->background_compact.cfg), &config));
        __wt_spin_unlock(session, &conn->background_compact.cfg_lock);
        ret = wt_session->compact(wt_session, key, config);
        /* FIXME-WT-11343: compaction is done, update the data structure for this table. */
        /*
         * Compact may return:
         * - EBUSY for various reasons.
         * - ETIMEDOUT if the timer has been configured and compaction took too long.
         * - WT_NOTFOUND if the underlying file has been deleted.
         */
        if (ret == EBUSY || ret == WT_NOTFOUND || ret == ETIMEDOUT)
            ret = 0;
        WT_ERR(ret);
    }

    WT_ERR(__wt_metadata_cursor_close(session));
    __wt_free(session, config);
    __wt_free(session, key);

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "compact server error"));
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_compact_server_create --
 *     Start the compact thread.
 */
int
__wt_compact_server_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint32_t session_flags;

    conn = S2C(session);

    /* Set first, the thread might run before we finish up. */
    FLD_SET(conn->server_flags, WT_CONN_SERVER_COMPACT);

    /*
     * Compaction does enough I/O it may be called upon to perform slow operations for the block
     * manager.
     */
    session_flags = WT_SESSION_CAN_WAIT;
    WT_RET(__wt_open_internal_session(
      conn, "compact-server", true, session_flags, 0, &conn->background_compact.session));
    session = conn->background_compact.session;

    WT_RET(__wt_cond_alloc(session, "compact server", &conn->background_compact.cond));

    WT_RET(__wt_thread_create(session, &conn->background_compact.tid, __compact_server, session));
    conn->background_compact.tid_set = true;

    return (0);
}

/*
 * __wt_compact_server_destroy --
 *     Destroy the background compaction server thread.
 */
int
__wt_compact_server_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    FLD_CLR(conn->server_flags, WT_CONN_SERVER_COMPACT);
    if (conn->background_compact.tid_set) {
        __wt_cond_signal(session, conn->background_compact.cond);
        WT_TRET(__wt_thread_join(session, &conn->background_compact.tid));
        conn->background_compact.tid_set = false;
    }
    __wt_cond_destroy(session, &conn->background_compact.cond);

    /* Close the server thread's session. */
    if (conn->background_compact.session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->background_compact.session));
        conn->background_compact.session = NULL;
    }

    return (ret);
}
