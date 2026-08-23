/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "test_util.h"

#include <sys/wait.h>

#define GETOPTS "b:Gh:p"
#define TABLE_URI "file:stepdown_history_store_deadlock.wt_stable"
#define TABLE_CONFIG "key_format=S,value_format=S,block_manager=disagg,log=(enabled=false)"
#define TIMEOUT_SECONDS 60

typedef struct {
    WT_CONNECTION_IMPL *conn;
    WT_BTREE *btree;
    WT_SESSION_IMPL *session;
    wt_shared bool started, writer_acquired;
} THREAD_DATA;

static TEST_OPTS _opts;

static WT_DATA_HANDLE *find_dhandle(WT_SESSION_IMPL *, const char *);
static uint32_t find_session_id(WT_SESSION_IMPL *, const char *);
static WT_THREAD_RET open_history_store(void *);
static WT_THREAD_RET queue_handle_list_writer(void *);
static WT_THREAD_RET step_down(void *);
static void subtest_run(TEST_OPTS *) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * find_dhandle --
 *     Return a referenced live dhandle for a URI.
 */
static WT_DATA_HANDLE *
find_dhandle(WT_SESSION_IMPL *session, const char *uri)
{
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    dhandle = NULL;
    WT_WITH_HANDLE_LIST_READ_LOCK(session, ret = __wt_conn_dhandle_find(session, uri, NULL);
      if (ret == 0) WT_DHANDLE_ACQUIRE(session->dhandle));
    testutil_check(ret);
    dhandle = session->dhandle;
    WT_DHANDLE_CLEAR(session);
    return (dhandle);
}

/*
 * find_session_id --
 *     Find an internal session by name.
 */
static uint32_t
find_session_id(WT_SESSION_IMPL *session, const char *name)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *array_session;
    uint32_t i, session_count;
    u_int active;
    const char *session_name;

    conn = S2C(session);
    WT_READ_ONCE(session_count, conn->session_array.cnt);
    array_session = WT_CONN_SESSIONS_GET(conn);

    for (i = 0; i < session_count; ++i, ++array_session) {
        WT_ACQUIRE_READ_WITH_BARRIER(active, array_session->active);
        if (!active)
            continue;

        session_name = __wt_atomic_load_ptr_relaxed(&array_session->name);
        if (session_name != NULL && strcmp(session_name, name) == 0)
            return (array_session->id);
    }
    return (WT_SESSION_ID_INVALID);
}

/*
 * step_down --
 *     Reconfigure the connection to follower mode.
 */
static WT_THREAD_RET
step_down(void *arg)
{
    THREAD_DATA *td;

    td = arg;
    __wt_atomic_store_bool_relaxed(&td->started, true);
    testutil_check(td->conn->iface.reconfigure(&td->conn->iface, "disaggregated=(role=follower)"));
    return (WT_THREAD_RET_VALUE);
}

/*
 * queue_handle_list_writer --
 *     Queue a handle-list writer, which prevents later readers from joining the active read group.
 */
static WT_THREAD_RET
queue_handle_list_writer(void *arg)
{
    THREAD_DATA *td;

    td = arg;
    __wt_writelock(td->session, &td->conn->dhandle_lock);
    __wt_atomic_store_bool_relaxed(&td->writer_acquired, true);
    __wt_writeunlock(td->session, &td->conn->dhandle_lock);
    return (WT_THREAD_RET_VALUE);
}

/*
 * open_history_store --
 *     Model the history-store open performed by an eviction worker after it increments evict_busy.
 */
static WT_THREAD_RET
open_history_store(void *arg)
{
    THREAD_DATA *td;
    WT_CURSOR *cursor;

    td = arg;
    cursor = NULL;
    testutil_check(__wt_curhs_open(td->session, td->btree->id, NULL, NULL, &cursor));
    testutil_check(cursor->close(cursor));
    (void)__wt_atomic_sub_uint32_v(&td->btree->evict_busy, 1);
    return (WT_THREAD_RET_VALUE);
}

/*
 * subtest_run --
 *     Reproduce the handle-list lock and history-store eviction ordering in a bounded child.
 */
static void
subtest_run(TEST_OPTS *opts)
{
    THREAD_DATA hs_td, stepdown_td, writer_td;
    WT_CONNECTION *conn;
    WT_CONNECTION_IMPL *conn_impl;
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle, *shared_hs_dhandle, *shared_metadata_dhandle;
    WT_SESSION *session;
    WT_SESSION_IMPL *hs_session, *writer_session;
    pthread_t hs_thread, stepdown_thread, writer_thread;
    uint64_t start;
    uint32_t stepdown_session_id;

    testutil_wiredtiger_open(
      opts, opts->home, "create,cache_cursors=false,statistics=(all)", NULL, &conn, false, false);
    conn_impl = (WT_CONNECTION_IMPL *)conn;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    testutil_check(session->create(session, TABLE_URI, TABLE_CONFIG));
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    cursor->set_key(cursor, "key");
    cursor->set_value(cursor, "value");
    testutil_check(cursor->insert(cursor));
    testutil_check(cursor->close(cursor));
    testutil_check(conn->set_timestamp(conn, "stable_timestamp=1"));
    testutil_check(session->checkpoint(session, NULL));

    dhandle = find_dhandle((WT_SESSION_IMPL *)session, TABLE_URI);
    shared_hs_dhandle = find_dhandle((WT_SESSION_IMPL *)session, WT_HS_URI_SHARED);
    shared_metadata_dhandle = find_dhandle((WT_SESSION_IMPL *)session, WT_DISAGG_METADATA_URI);
    testutil_assert(WT_DHANDLE_BTREE(dhandle));
    testutil_assert(WT_DHANDLE_BTREE(shared_hs_dhandle));
    testutil_assert(WT_DHANDLE_BTREE(shared_metadata_dhandle));

    testutil_check(
      __wt_open_internal_session(conn_impl, "test-list-writer", false, 0, 0, &writer_session));
    testutil_check(
      __wt_open_internal_session(conn_impl, "test-history-store", false, 0, 0, &hs_session));

    (void)__wt_atomic_add_uint32_v(&((WT_BTREE *)dhandle->handle)->evict_busy, 1);

    WT_CLEAR(stepdown_td);
    stepdown_td.conn = conn_impl;
    testutil_check(pthread_create(&stepdown_thread, NULL, step_down, &stepdown_td));

    start = __wt_clock((WT_SESSION_IMPL *)session);
    while (!__wt_atomic_load_bool_relaxed(&stepdown_td.started)) {
        testutil_assert(WT_CLOCKDIFF_SEC(__wt_clock((WT_SESSION_IMPL *)session), start) < 10);
        __wt_yield();
    }

    stepdown_session_id = WT_SESSION_ID_INVALID;
    while (stepdown_session_id == WT_SESSION_ID_INVALID) {
        stepdown_session_id = find_session_id((WT_SESSION_IMPL *)session, "disagg-step-down");
        testutil_assert(WT_CLOCKDIFF_SEC(__wt_clock((WT_SESSION_IMPL *)session), start) < 10);
        __wt_yield();
    }

    /* The step-down session owns the walk lock while waiting for the synthetic eviction. */
    while (__wt_atomic_load_uint32_relaxed(&conn_impl->evict->evict_walk_lock.session_id) !=
      stepdown_session_id) {
        testutil_assert(WT_CLOCKDIFF_SEC(__wt_clock((WT_SESSION_IMPL *)session), start) < 10);
        __wt_yield();
    }

    WT_CLEAR(writer_td);
    writer_td.conn = conn_impl;
    writer_td.session = writer_session;
    testutil_check(pthread_create(&writer_thread, NULL, queue_handle_list_writer, &writer_td));

    start = __wt_clock((WT_SESSION_IMPL *)session);
    while (!__wt_atomic_load_bool_relaxed(&writer_td.writer_acquired) &&
      __wt_atomic_load_uint8_v_relaxed(&conn_impl->dhandle_lock.u.s.next) ==
        __wt_atomic_load_uint8_v_relaxed(&conn_impl->dhandle_lock.u.s.current)) {
        testutil_assert(WT_CLOCKDIFF_SEC(__wt_clock((WT_SESSION_IMPL *)session), start) < 10);
        __wt_yield();
    }

    WT_CLEAR(hs_td);
    hs_td.btree = dhandle->handle;
    hs_td.conn = conn_impl;
    hs_td.session = hs_session;
    testutil_check(pthread_create(&hs_thread, NULL, open_history_store, &hs_td));

    testutil_check(pthread_join(hs_thread, NULL));
    testutil_check(pthread_join(stepdown_thread, NULL));
    testutil_check(pthread_join(writer_thread, NULL));
    testutil_assert(__wt_atomic_load_bool_relaxed(&writer_td.writer_acquired));

    testutil_assert(F_ISSET(dhandle, WT_DHANDLE_OUTDATED));
    testutil_assert(F_ISSET(shared_hs_dhandle, WT_DHANDLE_OUTDATED));
    testutil_assert(F_ISSET(shared_metadata_dhandle, WT_DHANDLE_OUTDATED));
    testutil_check(conn->reconfigure(conn, "disaggregated=(role=leader)"));

    cursor = NULL;
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    testutil_assert(((WT_CURSOR_BTREE *)cursor)->dhandle != dhandle);
    testutil_check(cursor->close(cursor));

    cursor = NULL;
    testutil_check(session->open_cursor(session, WT_HS_URI_SHARED, NULL, NULL, &cursor));
    testutil_assert(((WT_CURSOR_BTREE *)cursor)->dhandle != shared_hs_dhandle);
    testutil_check(cursor->close(cursor));

    cursor = NULL;
    testutil_check(session->open_cursor(session, WT_DISAGG_METADATA_URI, NULL, NULL, &cursor));
    testutil_assert(((WT_CURSOR_BTREE *)cursor)->dhandle != shared_metadata_dhandle);
    testutil_check(cursor->close(cursor));

    WT_DHANDLE_RELEASE(shared_metadata_dhandle);
    WT_DHANDLE_RELEASE(shared_hs_dhandle);
    WT_DHANDLE_RELEASE(dhandle);
    testutil_check(__wt_session_close_internal(hs_session));
    testutil_check(__wt_session_close_internal(writer_session));
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));
    _exit(EXIT_SUCCESS);
}

/*
 * main --
 *     Run the deadlock regression in a child so a failure has a hard deadline.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts;
    pid_t pid;
    int ch;

    opts = &_opts;
    opts->table_type = TABLE_ROW;

    testutil_parse_begin_opt(argc, argv, GETOPTS, opts);
    while ((ch = __wt_getopt(opts->progname, argc, argv, GETOPTS)) != EOF)
        if (testutil_parse_single_opt(opts, ch) != 0)
            testutil_die(EINVAL, "unexpected option");
    testutil_parse_end_opt(opts);

    opts->disagg.page_log_home = opts->home;
    testutil_recreate_dir(opts->home);

    pid = fork();
    testutil_assert(pid >= 0);
    if (pid == 0)
        subtest_run(opts);

    testutil_timeout_wait(TIMEOUT_SECONDS, pid);

    if (!opts->preserve)
        testutil_remove(opts->home);
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
