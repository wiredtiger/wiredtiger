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

#define NUM_RECORDS WT_MILLION
// #define NUM_RECORDS 100 * WT_THOUSAND

#define CHECKPOINT_NUM 3
#define STAT_BUF_SIZE 128
#define KB 1024
// #define MB 1024 * 1024
#define MIN_SIZE 512
#define MAX_SIZE 4 * KB

/*
 * You may want to add "verbose=[compact,compact_progress]" to the connection config string to get
 * better view on what is happening.
 */
// Default eviction config
// static const char conn_config[] =
//   "create,cache_size=20GB,statistics=(all),statistics_log=(json,on_close,wait=1,sources=[file:]),"
//   "verbose=[compact:"
//   "2,"
//   "compact_progress]";

// static const char conn_config[] =
//   "create,cache_size=20GB,statistics=(all),statistics_log=(json,on_close,wait=1),verbose=[compact_"
//   "progress]";

// Eviction parameter tuning
static const char conn_config[] =
  "create,cache_size=20GB,statistics=(all),statistics_log=(json,on_close,wait=1),verbose=[compact:"
  "2,"
  "compact_progress]";

// Default block/page size values.
// A smaller allocation size with result in more fragmentation and slow things down further.
static const char table_config_row[] =
  "allocation_size=4KB,leaf_page_max=32KB,leaf_value_max=64MB,memory_page_max=10m,split_pct=90,"
  "key_format=Q,value_format=u";

// // Low leaf_page_max
// static const char table_config_row[] =
//   "allocation_size=512B,leaf_page_max=512B,leaf_value_max=64MB,memory_page_max=10m,split_pct=90,"
//   "key_format=Q,value_format=u";

static pthread_t thread_compact;
static uint64_t ready_counter;
static bool compact_finished = false;

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
};

/* Forward declarations. */
static void run_test_clean(TEST_OPTS *);
static void run_test(TEST_OPTS *);
static void *thread_func_compact(void *);
static void *thread_func_checkpoint(void *);
static void *thread_func_updates(void *);
static void populate(WT_SESSION *, const char *);
static void update_records(WT_SESSION *, const char *);
static void remove_records(WT_SESSION *, const char *);
static void get_file_stats(WT_SESSION *, const char *, uint64_t *, uint64_t *);
static void set_timing_stress_checkpoint(WT_CONNECTION *);
static void get_compact_progress(
  WT_SESSION *session, const char *, uint64_t *, uint64_t *, uint64_t *);
static void thread_wait(void);

/*
 * main --
 *     Test entry point.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    run_test_clean(opts);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * run_test_clean --
 *     Initialise global variables, call the test runner and then cleanup.
 */
static void
run_test_clean(TEST_OPTS *opts)
{
    ready_counter = 0;

    printf("\n");
    printf("Running compact test...\n");
    run_test(opts);

    /* Cleanup */
    if (!opts->preserve)
        testutil_remove(opts->home);
}

/*
 * run_test --
 *     Test runner.
 */
static void
run_test(TEST_OPTS *opts)
{
    struct thread_data td;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    pthread_t thread_checkpoint;
    pthread_t thread_updates;
    uint64_t pages_reviewed, pages_rewritten, pages_skipped;
    char *home, *uri;

    home = opts->home;
    uri = opts->uri;

    testutil_recreate_dir(home);

    testutil_check(wiredtiger_open(home, NULL, conn_config, &conn));

    /*
     * Set WT_TIMING_STRESS_CHECKPOINT_SLOW flag for stress test. It adds 10 seconds sleep before
     * each checkpoint.
     */
    // set_timing_stress_checkpoint(conn);

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and populate table. Checkpoint the data after that. */
    testutil_check(session->create(session, uri, table_config_row));

    populate(session, uri);
    testutil_check(session->checkpoint(session, NULL));

    /*
     * Remove 1/3 of data randomly to create fragmentation.
     */
    remove_records(session, uri);

    td.conn = conn;
    td.uri = uri;

    /* Spawn checkpoint, compact and updates threads. */
    testutil_check(pthread_create(&thread_compact, NULL, thread_func_compact, &td));
    testutil_check(pthread_create(&thread_checkpoint, NULL, thread_func_checkpoint, &td));
    testutil_check(pthread_create(&thread_updates, NULL, thread_func_updates, &td));

    /* Wait for the threads to finish the work. */
    (void)pthread_join(thread_checkpoint, NULL);
    (void)pthread_join(thread_compact, NULL);
    (void)pthread_join(thread_updates, NULL);

    /* Collect compact progress stats. */
    get_compact_progress(session, uri, &pages_reviewed, &pages_skipped, &pages_rewritten);

    testutil_check(session->close(session, NULL));
    session = NULL;

    testutil_check(conn->close(conn, NULL));
    conn = NULL;

    printf(" - Pages reviewed: %" PRIu64 "\n", pages_reviewed);
    printf(" - Pages selected for being rewritten: %" PRIu64 "\n", pages_rewritten);
    printf(" - Pages skipped: %" PRIu64 "\n", pages_skipped);
}

/*
 * thread_func_compact --
 *     Call session->compact API.
 */
static void *
thread_func_compact(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    /* Wait until all threads are ready to go. */
    printf("Waiting for other threads before starting compaction.\n");
    thread_wait();
    printf("Threads ready, starting compaction\n");

    /* Perform compact operation. */
    session->compact(session, td->uri, NULL);
    // __wt_sleep(5, 0);

    compact_finished = true;

    testutil_check(session->close(session, NULL));
    session = NULL;

    return (NULL);
}

/*
 * thread_func_checkpoint --
 *     Trigger some number of checkpoints, waiting for a random interval between calls.
 */
static void *
thread_func_checkpoint(void *arg)
{
    struct thread_data *td;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    uint64_t sleep_sec;
    int i;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    /* Wait until both checkpoint and compact threads are ready to go. */
    printf("Waiting for other threads before starting checkpoint.\n");
    thread_wait();
    printf("Threads ready, starting checkpoint\n");

    /*
     * Run several checkpoints. First one without any delay. Others will have a random delay before
     * start.
     */
    for (i = 0; i < CHECKPOINT_NUM; i++) {
        testutil_check(session->checkpoint(session, NULL));

        if (i < CHECKPOINT_NUM - 1) {
            sleep_sec = (uint64_t)__wt_random(&rnd) % 15 + 1;
            printf("Sleep %" PRIu64 " sec before next checkpoint.\n", sleep_sec);
            // __wt_sleep(sleep_sec, 0);
        }
    }

    testutil_check(session->close(session, NULL));
    session = NULL;

    return (NULL);
}

/*
 * thread_wait --
 *     Loop to constantly yield the calling thread until all threads are ready.
 */
static void
thread_wait(void)
{
    uint64_t ready_counter_local;

    (void)__wt_atomic_add64(&ready_counter, 1);
    for (;; __wt_yield()) {
        WT_ACQUIRE_READ_WITH_BARRIER(ready_counter_local, ready_counter);
        if (ready_counter_local >= 3) {
            break;
        }
    }
}

/*
 * populate --
 *     Populate the database with k/v pairs of varied sizes.
 */
static void
populate(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_ITEM value;
    WT_RAND_STATE rnd;
    uint64_t i;
    char *val_str;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < NUM_RECORDS; i++) {
        uint64_t val_size = (uint64_t)__wt_random(&rnd) % (MAX_SIZE - MIN_SIZE + 1) + MIN_SIZE;
        val_str = dmalloc(val_size);
        memset(val_str, 'a', val_size);
        value.data = val_str;
        value.size = val_size;

        cursor->set_key(cursor, i + 1);
        cursor->set_value(cursor, &value);
        testutil_check(cursor->insert(cursor));
        free(val_str);
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

/*
 * thread_func_updates --
 *     Wait for all threads to be ready, then start applying updates.
 */
static void *
thread_func_updates(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    printf("Waiting for other threads before starting updates thread.\n");
    thread_wait();
    printf("Threads ready, starting updates thread\n");
    update_records(session, td->uri);

    testutil_check(session->close(session, NULL));

    return (NULL);
}

/*
 * update_records --
 *     While compact is running, pick keys at random to apply an update to.
 */
static void
update_records(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_ITEM update;
    WT_RAND_STATE rnd;
    uint64_t key;
    char *val_str;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    while (!compact_finished) {
        // Generate a key in they keyspace [1, NUM_RECORDS]
        key = (uint64_t)__wt_random(&rnd) % NUM_RECORDS + 1;

        // Skip deleted keys to avoid filling the free space.
        if (key % 3 == 0) {
            continue;
        }

        // Pick a letter
        char letter = 'a' + (uint32_t)__wt_random(&rnd) % 26;

        // Set the key
        cursor->set_key(cursor, key);

        // Create the update entry
        uint64_t val_size = (uint64_t)__wt_random(&rnd) % (MAX_SIZE - MIN_SIZE + 1) + MIN_SIZE;
        val_str = dmalloc(val_size);
        memset(val_str, letter, val_size);
        update.data = val_str;
        update.size = val_size;

        cursor->set_value(cursor, &update);

        testutil_check(cursor->update(cursor));

        free(val_str);

        // Throttle the updates thread.
        // __wt_sleep(0, 1000);
    }

    cursor->close(cursor);
}

/*
 * remove_records --
 *     Remove a portion of the k/v pairs to create fragmentation.
 */
static void
remove_records(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_RAND_STATE rnd;
    uint64_t i;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    // Remove 1/3 of the keys.
    for (i = 0; i < NUM_RECORDS; i++) {
        if (i % 3 != 0) {
            continue;
        }
        cursor->set_key(cursor, i + 1);
        testutil_check(cursor->remove(cursor));
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

/*
 * get_file_stats --
 *     TODO: Add a comment describing this function.
 */
static void
get_file_stats(WT_SESSION *session, const char *uri, uint64_t *file_sz, uint64_t *avail_bytes)
{
    WT_CURSOR *cur_stat;
    char *descr, *str_val, stat_uri[STAT_BUF_SIZE];

    testutil_snprintf(stat_uri, STAT_BUF_SIZE, "statistics:%s", uri);
    testutil_check(session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cur_stat));

    /* Get file size. */
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BLOCK_SIZE);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, file_sz));

    /* Get bytes available for reuse. */
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BLOCK_REUSE_BYTES);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, avail_bytes));

    testutil_check(cur_stat->close(cur_stat));
    cur_stat = NULL;
}

/*
 * set_timing_stress_checkpoint --
 *     TODO: Add a comment describing this function.
 */
static void
set_timing_stress_checkpoint(WT_CONNECTION *conn)
{
    WT_CONNECTION_IMPL *conn_impl;

    conn_impl = (WT_CONNECTION_IMPL *)conn;
    conn_impl->timing_stress_flags |= WT_TIMING_STRESS_CHECKPOINT_SLOW;
}

/*
 * get_compact_progress --
 *     TODO: Add a comment describing this function.
 */
static void
get_compact_progress(WT_SESSION *session, const char *uri, uint64_t *pages_reviewed,
  uint64_t *pages_skipped, uint64_t *pages_rewritten)
{

    WT_CURSOR *cur_stat;
    char *descr, *str_val;
    char stat_uri[STAT_BUF_SIZE];

    testutil_snprintf(stat_uri, STAT_BUF_SIZE, "statistics:%s", uri);
    testutil_check(session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cur_stat));

    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, pages_reviewed));
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BTREE_COMPACT_PAGES_SKIPPED);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, pages_skipped));
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REWRITTEN);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, pages_rewritten));

    testutil_check(cur_stat->close(cur_stat));
}
