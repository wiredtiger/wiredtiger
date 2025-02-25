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

/*
 * This test executes two test cases:
 * - One with WT_TIMING_STRESS_CHECKPOINT_SLOW flag. It adds 10 seconds sleep before each
 * checkpoint.
 * - Another test case synchronizes compact and checkpoint threads by forcing them to wait
 * until both threads have started.
 * The reason we have two tests here is that they give different output when configured
 * with "verbose=[compact,compact_progress]". There's a chance these two cases are different.
 */

// #define NUM_RECORDS WT_MILLION
#define NUM_RECORDS 50 * WT_THOUSAND
// #define NUM_UPDATES 1000
#define HOME_BUF_SIZE 512
// #define KB 1024
#define MB 1024 * 1024
#define MIN_SIZE 512
#define MAX_SIZE 1 * MB

/* Constants and variables declaration. */
/*
 * You may want to add "verbose=[compact,compact_progress]" to the connection config string to get
 * better view on what is happening.
 */
static const char conn_config[] =
  "create,cache_size=2GB,statistics=(all),statistics_log=(json,on_close,wait=1),verbose=[compact:2,"
  "compact_progress]";
static const char table_config_row[] =
  "allocation_size=512B,leaf_page_max=512B,leaf_value_max=64MB,memory_page_max=10m,split_pct=90,"
  "key_format=Q,value_format=u";
static char data_str[1024] = "";

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
};

/* Forward declarations. */
static void run_test_clean(bool, bool, bool, const char *, const char *, const char *ri);
static void run_test(const char *, const char *);
static void populate(WT_SESSION *, const char *);
// static void remove_records(WT_SESSION *, const char *);

/*
 * main --
 *     Methods implementation.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    /*
     * Now, run test where compact and checkpoint threads are synchronized using global thread
     * counter. Row store case.
     */
    run_test_clean(false, false, opts->preserve, opts->home, "NR", opts->uri);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * run_test_clean --
 *     TODO: Add a comment describing this function.
 */
static void
run_test_clean(bool stress_test, bool column_store, bool preserve, const char *home,
  const char *suffix, const char *uri)
{
    char home_full[HOME_BUF_SIZE];

    printf("\n");
    printf("Running %s test with %s store...\n", stress_test ? "stress" : "normal",
      column_store ? "column" : "row");
    testutil_assert(sizeof(home_full) > strlen(home) + strlen(suffix) + 2);
    testutil_snprintf(home_full, HOME_BUF_SIZE, "%s.%s", home, suffix);
    run_test(home_full, uri);

    /* Cleanup */
    if (!preserve)
        testutil_remove(home_full);
}

/*
 * run_test --
 *     TODO: Add a comment describing this function.
 */
static void
run_test(const char *home, const char *uri)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;

    testutil_recreate_dir(home);
    testutil_check(wiredtiger_open(home, NULL, conn_config, &conn));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and populate table. Checkpoint the data after that. */
    testutil_check(session->create(session, uri, table_config_row));

    populate(session, uri);
    testutil_check(session->checkpoint(session, NULL));

    testutil_check(session->close(session, NULL));
    session = NULL;

    testutil_check(conn->close(conn, NULL));
    conn = NULL;
}

/*
 * populate --
 *     TODO: Add a comment describing this function.
 */
static void
populate(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_ITEM value;
    WT_RAND_STATE rnd;
    uint64_t i, str_len;
    char *val_str;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    str_len = sizeof(data_str) / sizeof(data_str[0]);
    for (i = 0; i < str_len - 1; i++)
        data_str[i] = 'a' + (uint32_t)__wt_random(&rnd) % 26;

    data_str[str_len - 1] = '\0';

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < NUM_RECORDS; i++) {
        // if ((NUM_RECORDS / 10) % i == 0) {
        //     printf("i: %" PRIu64, i);
        // }
        uint64_t val_size = (uint64_t)__wt_random(&rnd) % (MAX_SIZE - MIN_SIZE + 1) + MIN_SIZE;
        val_str = dmalloc(val_size);
        memset(val_str, 'a', val_size);
        value.data = val_str;
        value.size = val_size;

        cursor->set_key(cursor, i + 1);
        // val = (uint64_t)__wt_random(&rnd);
        cursor->set_value(cursor, &value);
        testutil_check(cursor->insert(cursor));
        free(val_str);
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

// /*
//  * remove_records --
//  *     TODO: Add a comment describing this function.
//  */
// static void
// remove_records(WT_SESSION *session, const char *uri)
// {
//     WT_CURSOR *cursor;
//     WT_RAND_STATE rnd;
//     uint64_t i;

//     __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
//     testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

//     /* Remove 1/3 of the records from the middle of the key range. */
//     // for (i = NUM_RECORDS / 3; i < (NUM_RECORDS * 2) / 3; i++) {
//     //     cursor->set_key(cursor, i + 1);
//     //     testutil_check(cursor->remove(cursor));
//     // }

//     // Remove 1/3 of the keys randomly.
//     for (i = 0; i < NUM_RECORDS; i++) {
//         if (i % 3 != 0) {
//             continue;
//         }
//         // if ((uint64_t)__wt_random(&rnd) % 3 != 0)
//         //     continue;
//         cursor->set_key(cursor, i + 1);
//         testutil_check(cursor->remove(cursor));
//     }

//     testutil_check(cursor->close(cursor));
//     cursor = NULL;
// }
