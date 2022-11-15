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

#define NUM_RECORDS 500
#define HOME_BUF_SIZE 512
#define MB (1024 * 1024)

/* Constants and variables declaration. */
static const char conn_config[] =
  "create,cache_size=2GB,statistics=(all),statistics_log=(json,on_close,wait=1)";
static const char table_config_row[] = "leaf_page_max=64KB,key_format=i,value_format=S";
static char data_str[202] = "";
static uint64_t ready_counter;

static TEST_OPTS *opts, _opts;

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
    bool stress_test;
};

/* Forward declarations. */
static void get_file_size(const char *, int64_t *);
static void run_test_clean(const char *, uint64_t, bool);
static void run_test(const char *, uint64_t, bool);
static void populate(WT_SESSION *, uint64_t);

/*
 * main --
 *     Methods implementation.
 */
int
main(int argc, char *argv[])
{
    bool flush;
    int i;
    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    flush = false;
    for (i = 0; i < 2; ++i) {

        printf("Flush === %d\n", flush);

        /*
         * First, run test with 100K file size. Row store case.
         */
        run_test_clean("100K", NUM_RECORDS, flush);

        /*
         * First, run test with 1Mb file size. Row store case.
         */
        run_test_clean("1M", NUM_RECORDS * 10, flush);

        /*
         * First, run test with 10 Mb file size. Row store case.
         */
        run_test_clean("10M", NUM_RECORDS * 100, flush);

        /*
         * First, run test with 100 Mb file size. Row store case.
         */
        run_test_clean("100M", NUM_RECORDS * 1000, flush);
        flush = true;
    }

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

static double
difftime_msec(struct timeval t0, struct timeval t1)
{
    return (t1.tv_sec - t0.tv_sec) * (double)WT_THOUSAND +
      (t1.tv_usec - t0.tv_usec) / (double)WT_THOUSAND;
}

static double
difftime_sec(struct timeval t0, struct timeval t1)
{
    // return (t1.tv_sec - t0.tv_sec) * (double)WT_MILLION + (t1.tv_usec - t0.tv_usec) /
    // (double)WT_MILLION;
    return difftime_msec(t0, t1) / (double)WT_THOUSAND;
}

/*
 * run_test_clean --
 *     TODO: Add a comment describing this function.
 */
static void
run_test_clean(const char *suffix, uint64_t num_records, bool flush)
{
    char home_full[HOME_BUF_SIZE];

    ready_counter = 0;

    printf("\n");
    printf("Running %s test \n", suffix);
    testutil_assert(sizeof(home_full) > strlen(opts->home) + strlen(suffix) + 2);
    testutil_check(__wt_snprintf(home_full, HOME_BUF_SIZE, "%s.%s.%d", opts->home, suffix, flush));
    run_test(home_full, num_records, flush);

    /* Cleanup */
    if (!opts->preserve)
        testutil_clean_work_dir(home_full);
}

/*
 * run_test --
 *     TODO: Add a comment describing this function.
 */
static void
run_test(const char *home, uint64_t num_records, bool flush)
{
    struct timeval start, end;

    char buf[1024];
    double diff_sec;
    int64_t file_size;

    WT_CONNECTION *conn;
    WT_SESSION *session;

    testutil_make_work_dir(home);
    if (opts->tiered_storage) {
        testutil_check(__wt_snprintf(buf, sizeof(buf), "%s/bucket", home));
        testutil_make_work_dir(buf);
    }

    testutil_wiredtiger_open(opts, home, conn_config, NULL, &conn, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and populate table. Checkpoint the data after that. */
    testutil_check(session->create(session, opts->uri, table_config_row));

    testutil_check(__wt_snprintf(buf, sizeof(buf), flush ? "flush_tier=(enabled,force=true)" : ""));

    gettimeofday(&start, 0);
    populate(session, num_records);
    printf("Checkpoint buf : %s\n", buf);

    testutil_check(session->checkpoint(session, buf));
    gettimeofday(&end, 0);

    diff_sec = difftime_sec(start, end);
    printf("Code executed in %f ms, %f s\n", difftime_msec(start, end), diff_sec);

    sleep(2);

    get_file_size(home, &file_size);
    printf("File Size - %" PRIi64 ", Throughput - %f MB/second\n", file_size,
      ((file_size / diff_sec) / MB));
}

/*
 * populate --
 *     TODO: Add a comment describing this function.
 */
static void
populate(WT_SESSION *session, uint64_t num_records)
{
    WT_CURSOR *cursor;
    WT_RAND_STATE rnd;
    uint64_t i, str_len;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    str_len = sizeof(data_str) / sizeof(data_str[0]);
    for (i = 0; i < str_len - 1; i++)
        data_str[i] = 'a' + (uint32_t)__wt_random(&rnd) % 26;

    data_str[str_len - 1] = '\0';

    testutil_check(session->open_cursor(session, opts->uri, NULL, NULL, &cursor));
    for (i = 0; i < num_records; i++) {
        cursor->set_key(cursor, i + 1);
        cursor->set_value(cursor, data_str);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

/*
 * get_file_size --
 *     TODO: Retrieve the file size of the table.
 */
static void
get_file_size(const char *home, int64_t *file_size)
{
    struct stat stats;

    char path[512], pwd[512], token_path[512], stat_path[1512];
    char *token;
    const char *tablename;

    size_t len;

    token_path[0] = '\0';
    tablename = strchr(opts->uri, ':');
    testutil_assert(tablename != NULL);
    tablename++;

    if (getcwd(pwd, sizeof(pwd)) == NULL)
        testutil_die(ENOENT, "No such directory");

    testutil_check(__wt_snprintf(path, sizeof(path), "%s/%s", pwd, opts->argv0));

    token = strtok(path, "/");

    while (token != NULL) {
        len = strlen(token);
        strncat(token_path, "/", len);
        strncat(token_path, token, len);

        if (opts->tiered_storage)
            testutil_check(__wt_snprintf(stat_path, sizeof(stat_path), "%s/%s/%s-0000000001.wtobj",
              token_path, home, tablename));
        else
            testutil_check(__wt_snprintf(
              stat_path, sizeof(stat_path), "%s/%s/%s.wt", token_path, home, tablename));

        if (stat(stat_path, &stats) == 0) {
            *file_size = stats.st_size;
            return;
        }
        token = strtok(NULL, "/");
    }
    return;
}
