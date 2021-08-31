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

#define NR_RECORDS 3000000

/* Constants and variables declaration. */
static const char conn_config[] =
  "create,cache_size=2GB,statistics=(all),verbose=[compact,compact_progress]";//,checkpoint=(wait=180)";
static const char table_config[] =
  "allocation_size=4KB,leaf_page_max=4KB,key_format=i,value_format=QQQS";
static char data_str[1024] = "";

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
};

/* Forward declarations. */
static void run_test(const char *home, const char *uri);
static void populate(WT_SESSION *session, const char *uri);
static void remove_records(WT_SESSION *session, const char *uri);
static uint64_t get_file_size(WT_SESSION *session, const char *uri);
static void *thread_func_compact(void *arg);
static void *thread_func_checkpoint(void *arg);
//static void set_timing_stress_checkpoint(WT_CONNECTION *conn);

/* Methods implementation. */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    run_test(opts->home, opts->uri);

    opts->conn = NULL;
    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

static void
run_test(const char *home, const char *uri)
{
    struct thread_data td;
    WT_CONNECTION *conn;
    WT_CONNECTION_IMPL *conn_impl;
    WT_SESSION *session;
    pthread_t thread_compact, thread_checkpoint;
    uint64_t file_sz_after, file_sz_before;

    testutil_make_work_dir(home);
    testutil_check(wiredtiger_open(home, NULL, conn_config, &conn));

    /* Set WT_TIMING_STRESS_CHECKPOINT_SLOW flag. It adds 10 seconds sleep before each checkpoint.*/
    //set_timing_stress_checkpoint(conn);
    
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and initialize conditional variable. */
    conn_impl = (WT_CONNECTION_IMPL *)conn;
    conn_impl->compact_session = (WT_SESSION_IMPL *)session;
    testutil_check(__wt_cond_alloc(conn_impl->compact_session, "compact operation", &conn_impl->compact_cond));

    /* 1. Create and populate dataset. Checkpoint the data after that. */
    testutil_check(session->create(session, uri, table_config));

    populate(session, uri);
    testutil_check(session->checkpoint(session, NULL));

    /*
     * 2. Remove 1/3 of data from the middle of the key range. To let compact relocate blocks
     * from the end of the file.
     */
    remove_records(session, uri);

    file_sz_before = get_file_size(session, uri);

    /* 3. Run checkpoint and compact. */
    td.conn = conn;
    td.uri = uri;
    testutil_check(pthread_create(&thread_checkpoint, NULL, thread_func_checkpoint, &td));
    testutil_check(pthread_create(&thread_compact, NULL, thread_func_compact, &td));
    (void)pthread_join(thread_checkpoint, NULL);
    (void)pthread_join(thread_compact, NULL);

    file_sz_after = get_file_size(session, uri);

    __wt_cond_destroy(conn_impl->compact_session, &conn_impl->compact_cond);
    conn_impl->compact_session = NULL;
    conn_impl->compact_cond = NULL;

    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));

    printf("Compressed file size MB: %f\n", file_sz_after / (1024.0 * 1024));
    printf("Original file size MB: %f\n", file_sz_before / (1024.0 * 1024));

    /* Check if there's at least 10% compaction. */
    testutil_assert(file_sz_before * 0.9 > file_sz_after);
}

static void
populate(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    time_t t;
    uint64_t val;
    int i, str_len;

    srand((u_int)time(&t));

    str_len = sizeof(data_str) / sizeof(data_str[0]);
    for (i = 0; i < str_len - 1; i++) {
        data_str[i] = 'a' + rand() % 26;
    }
    data_str[str_len - 1] = '\0';

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < NR_RECORDS; i++) {
        cursor->set_key(cursor, i);
        val = (uint64_t)rand();
        cursor->set_value(cursor, val, val, val, data_str);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
}

static void
remove_records(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    int i;

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    for (i = NR_RECORDS / 3; i < (NR_RECORDS * 2) / 3; i++) {
        cursor->set_key(cursor, i);
        testutil_check(cursor->remove(cursor));
    }

    testutil_check(cursor->close(cursor));
}

static uint64_t
get_file_size(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cur_stat;
    uint64_t val;
    char *descr, *str_val;
    char stat_uri[128];

    sprintf(stat_uri, "statistics:%s", uri);
    testutil_check(session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cur_stat));
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BLOCK_SIZE);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, &val));
    testutil_check(cur_stat->close(cur_stat));

    return val;
}

static void *
thread_func_compact(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    testutil_check(session->compact(session, td->uri, NULL));
    testutil_check(session->close(session, NULL));

    return (NULL);
}

static void *
thread_func_checkpoint(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;
    WT_CONNECTION_IMPL *conn_impl;
    bool signalled;

    td = (struct thread_data *)arg;
    conn_impl = (WT_CONNECTION_IMPL *)td->conn;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    printf("AAA: wait for the signal.\n");
    __wt_cond_wait_signal(conn_impl->compact_session, conn_impl->compact_cond, 0, NULL, &signalled);
    printf("AAA: received the signal. Starting checkpoint.\n");

    testutil_check(session->checkpoint(session, NULL));
    testutil_check(session->close(session, NULL));

    return (NULL);
}

/*static void
set_timing_stress_checkpoint(WT_CONNECTION *conn)
{
    WT_CONNECTION_IMPL *conn_impl;

    conn_impl = (WT_CONNECTION_IMPL *)conn;
    conn_impl->timing_stress_flags |= WT_TIMING_STRESS_CHECKPOINT_SLOW;
}*/
