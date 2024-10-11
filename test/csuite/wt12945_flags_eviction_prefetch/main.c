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
 * JIRA ticket reference: WT-12945 Test case description: This is a test case that looks for crashes
 * when pre-fetch and eviction of the same page happens at the same time.
 *
 * This variant tests dirty eviction.
 */

/* Constants */

/* From wt verify -d dump_tree_shape file:test_wt12945_flags_eviction_prefetch.wt */
#define RECORDS_PER_PAGE 3579

/* Warm-up loop: [0, NUM_WARM_UP_RECORDS - 1] 3 pages full */
#define NUM_WARM_UP_RECORDS (3 * RECORDS_PER_PAGE) /* How many records to insert during warmup. */

/* Update loop: [FIRST_RECORD_TO_CHANGE, FIRST_RECORD_TO_CHANGE + NUM_EVICTION - 1] */
/* First record to change to give pre-fetch thread time to begin pre-fetching. */
#define FIRST_RECORD_TO_CHANGE (RECORDS_PER_PAGE + 1)
#define NUM_EVICTION RECORDS_PER_PAGE /* How many times to force eviction. */

/* Prefetch loop: [0, NUM_WARM_UP_RECORDS - 1] But stop when pages are queued. */

void *thread_do_prefetch(void *);

static uint64_t ready_counter;
static const char *const session_open_config = "prefetch=(enabled=true)";

/*
 * get_stat --
 *     Get one statistic.
 */
static int64_t
get_stat(TEST_OPTS *opts, WT_SESSION *wt_session, int which_stat)
{
    WT_CURSOR *stat_cursor;
    int64_t value;
    const char *desc, *pvalue;
    WT_UNUSED(opts);

    testutil_check(wt_session->open_cursor(wt_session, "statistics:", NULL, NULL, &stat_cursor));
    stat_cursor->set_key(stat_cursor, which_stat);
    testutil_check(stat_cursor->search(stat_cursor));
    testutil_check(stat_cursor->get_value(stat_cursor, &desc, &pvalue, &value));
    testutil_check(stat_cursor->close(stat_cursor));

    return (value);
}

/*
 * get_key --
 *     Wrapper providing the correct typing for the WT_CURSOR::get_key variadic argument.
 */
static uint64_t
get_key(TEST_OPTS *opts, WT_CURSOR *cursor)
{
    uint64_t value64;
    WT_UNUSED(opts);

    testutil_check(cursor->get_key(cursor, &value64));
    return (value64);
}

/*
 * set_key --
 *     Wrapper providing the correct typing for the WT_CURSOR::set_key variadic argument.
 */
static void
set_key(WT_CURSOR *cursor, uint64_t value)
{
    cursor->set_key(cursor, value);
}

/*
 * set_value --
 *     Wrapper providing the correct typing for the WT_CURSOR::set_value variadic argument.
 */
static void
set_value(TEST_OPTS *opts, WT_CURSOR *cursor, uint64_t value)
{
    WT_UNUSED(opts);
    cursor->set_value(cursor, value);
}

/*
 * get_value --
 *     Wrapper providing the correct typing for the WT_CURSOR::get_value variadic argument.
 */
static uint64_t
get_value(TEST_OPTS *opts, WT_CURSOR *cursor)
{
    uint64_t value64;
    WT_UNUSED(opts);

    testutil_check(cursor->get_value(cursor, &value64));
    return (value64);
}

/*
 * print_eviction_stats --
 *     Print some eviction stats.
 */
static void
print_eviction_stats(
  WT_SESSION *wt_session, TEST_OPTS *opts, const char *where, uint64_t idx, bool no_duplicates)
{
    char separator;
    int64_t eviction_clean;                /* cache: unmodified pages evicted */
    int64_t eviction_dirty;                /* cache: modified pages evicted */
    int64_t eviction_force;                /* evict: forced eviction - pages selected count */
    int64_t eviction_pages_seen;           /* cache: pages seen by eviction walk */
    int64_t eviction_server_evict_attempt; /* evict: evict page attempts by eviction server */
    int64_t eviction_walk_passes;          /* evict: eviction passes of a file */
    int64_t
      eviction_worker_evict_attempt; /* evict: evict page attempts by eviction worker threads */

    static int64_t last_eviction_clean = 0;
    static int64_t last_eviction_dirty = 0;
    static int64_t last_eviction_force = 0;
    static int64_t last_eviction_pages_seen = 0;
    static int64_t last_eviction_server_evict_attempt = 0;
    static int64_t last_eviction_walk_passes = 0;
    static int64_t last_eviction_worker_evict_attempt = 0;

    eviction_clean = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_CLEAN);
    eviction_dirty = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_DIRTY);
    eviction_force = get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_FORCE);
    eviction_pages_seen = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_PAGES_SEEN);
    eviction_server_evict_attempt =
      get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_SERVER_EVICT_ATTEMPT);
    eviction_walk_passes = get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_WALK_PASSES);
    eviction_worker_evict_attempt =
      get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_WORKER_EVICT_ATTEMPT);

    if (no_duplicates && (eviction_clean == last_eviction_clean) &&
      (eviction_dirty == last_eviction_dirty) && (eviction_force == last_eviction_force) &&
      (eviction_pages_seen == last_eviction_pages_seen) &&
      (eviction_server_evict_attempt == last_eviction_server_evict_attempt) &&
      (eviction_walk_passes == last_eviction_walk_passes) &&
      (eviction_worker_evict_attempt == last_eviction_worker_evict_attempt))
        return;

    last_eviction_clean = eviction_clean;
    last_eviction_dirty = eviction_dirty;
    last_eviction_force = eviction_force;
    last_eviction_pages_seen = eviction_pages_seen;
    last_eviction_server_evict_attempt = eviction_server_evict_attempt;
    last_eviction_worker_evict_attempt = eviction_worker_evict_attempt;
    eviction_walk_passes = last_eviction_walk_passes;

    separator = '.';
    printf("%s: %" PRIu64, where, idx);
    if (eviction_clean != 0) {
        printf("%c eviction_clean=%" PRId64, separator, eviction_clean);
        separator = ',';
    }
    if (eviction_dirty != 0) {
        printf("%c eviction_dirty=%" PRId64, separator, eviction_dirty);
        separator = ',';
    }
    if (eviction_force != 0) {
        printf("%c eviction_force=%" PRId64, separator, eviction_force);
        separator = ',';
    }
    if (eviction_pages_seen != 0) {
        printf("%c eviction_pages_seen=%" PRId64, separator, eviction_pages_seen);
        separator = ',';
    }
    if (eviction_server_evict_attempt != 0) {
        printf(
          "%c eviction_server_evict_attempt=%" PRId64, separator, eviction_server_evict_attempt);
        separator = ',';
    }
    if (eviction_worker_evict_attempt != 0) {
        printf(
          "%c eviction_worker_evict_attempt=%" PRId64, separator, eviction_worker_evict_attempt);
        separator = ',';
    }
    if (eviction_walk_passes != 0) {
        printf("%c eviction_walk_passes=%" PRId64, separator, eviction_walk_passes);
        separator = ',';
    }
    printf("\n");

    fflush(stdout);
}

/*
 * print_prefetch_stats --
 *     Print some pre-fetch stats.
 */
static void
print_prefetch_stats(
  WT_SESSION *wt_session, TEST_OPTS *opts, const char *where, uint64_t idx, bool no_duplicates)
{
    char separator;
    int64_t prefetch_attempts;     /* prefetch: pre-fetch triggered by page read */
    int64_t prefetch_pages_queued; /* prefetch: pre-fetch pages queued */
    int64_t prefetch_pages_read;   /* prefetch: pre-fetch pages read in background */
    int64_t cache_pages_prefetch;  /* cache: pages requested from the cache due to pre-fetch */

    static int64_t last_prefetch_attempts = 0;
    static int64_t last_prefetch_pages_queued = 0;
    static int64_t last_prefetch_pages_read = 0;
    static int64_t last_cache_pages_prefetch = 0;

    prefetch_attempts = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_ATTEMPTS);
    prefetch_pages_queued = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_PAGES_QUEUED);
    prefetch_pages_read = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_PAGES_READ);
    cache_pages_prefetch = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_PAGES_PREFETCH);

    if (no_duplicates && (prefetch_attempts == last_prefetch_attempts) &&
      (prefetch_pages_queued == last_prefetch_pages_queued) &&
      (prefetch_pages_read == last_prefetch_pages_read) &&
      (cache_pages_prefetch == last_cache_pages_prefetch))
        return;

    last_prefetch_attempts = prefetch_attempts;
    last_prefetch_pages_queued = prefetch_pages_queued;
    last_prefetch_pages_read = prefetch_pages_read;
    last_cache_pages_prefetch = cache_pages_prefetch;

    separator = '.';
    printf("%s: %" PRIu64, where, idx);
    if (prefetch_attempts != 0) {
        printf("%c prefetch_attempts=%" PRId64, separator, prefetch_attempts);
        separator = ',';
    }
    if (prefetch_pages_queued != 0) {
        printf("%c prefetch_pages_queued=%" PRId64, separator, prefetch_pages_queued);
        separator = ',';
    }
    if (prefetch_pages_read != 0) {
        printf("%c prefetch_pages_read=%" PRId64, separator, prefetch_pages_read);
        separator = ',';
    }
    if (cache_pages_prefetch != 0) {
        printf("%c cache_pages_prefetch=%" PRId64, separator, cache_pages_prefetch);
        separator = ',';
    }
    printf("\n");

    fflush(stdout);
}

/*
 * main --
 *     TODO: Add a comment describing this function.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *cursor;
    WT_SESSION *wt_session;
    pthread_t prefetch_thread_id;
    uint64_t ready_counter_local;
    uint64_t record_idx;
    int ret;
    const char *wiredtiger_open_config =
      "create,cache_size=2G,eviction=(threads_min=1,threads_max=1),"
      "prefetch=(available=true,default=true),"
#if 1 /* Include if needed */
      "verbose=["
      "control_point=5,"
      "prefetch=1,"
      "],"
#endif
      "statistics=(all),statistics_log=(json,on_close,wait=1)";

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    opts->nthreads = 1;                   /* Value ignored */
    opts->nrecords = NUM_WARM_UP_RECORDS; /* Use: How many records to insert during warmup. */
    opts->table_type = TABLE_ROW;         /* Value ignored */
    testutil_check(testutil_parse_opts(argc, argv, opts));
    opts->nthreads = 1;           /* Force desired value */
    opts->table_type = TABLE_ROW; /* Force desired value */
    testutil_recreate_dir(opts->home);

    testutil_check(wiredtiger_open(opts->home, NULL, wiredtiger_open_config, &opts->conn));

    /* Create the session for eviction. */
    testutil_check(opts->conn->open_session(opts->conn, NULL, session_open_config, &wt_session));
    testutil_check(
      wt_session->create(wt_session, opts->uri, "key_format=Q,value_format=Q,leaf_page_max=32k"));

    /* Warm-up: Insert some documents */
    testutil_check((ret = wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor)));
    for (record_idx = 0; record_idx < opts->nrecords; ++record_idx) {
        print_eviction_stats(wt_session, opts, "Warm up", record_idx, true);
        /* Do one insertion */
        set_key(cursor, record_idx);
        set_value(opts, cursor, record_idx);
        testutil_check(wt_session->begin_transaction(wt_session, "isolation=snapshot"));
        testutil_check(cursor->insert(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, NULL));
        /* Print progress. */
        if ((record_idx % WT_THOUSAND) == 0) {
            printf("eviction thread: Warm-up: insert key=%" PRIu64 ", value=%" PRIu64 "\n",
              record_idx, record_idx);
            fflush(stdout);
        }
    }
    print_eviction_stats(wt_session, opts, "After Warm up", record_idx, false);
    testutil_check(cursor->close(cursor));

    /* Close and reopen the connection to force the warm-up documents out of the cache. */
    testutil_check(wt_session->close(wt_session, NULL));
    testutil_check(opts->conn->close(opts->conn, ""));

    testutil_check(wiredtiger_open(opts->home, NULL, wiredtiger_open_config, &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, session_open_config, &wt_session));

    opts->conn->enable_control_point(opts->conn, WT_CONN_CONTROL_POINT_ID_WT_12945, NULL);

    /* Create the thread for pre-fetch and wait for it to be ready. */
    testutil_check(pthread_create(&prefetch_thread_id, NULL, thread_do_prefetch, opts));

    for (;; __wt_yield()) {
        WT_ACQUIRE_READ_WITH_BARRIER(ready_counter_local, ready_counter);
        if (ready_counter_local >= 1)
            break;
    }

    /* Loop updating documents and triggering eviction. */
    testutil_check(
      wt_session->open_cursor(wt_session, opts->uri, NULL, "debug=release_evict", &cursor));
    for (record_idx = FIRST_RECORD_TO_CHANGE; record_idx < FIRST_RECORD_TO_CHANGE + NUM_EVICTION;
         ++record_idx) {
        /* print_eviction_stats(wt_session, opts, "Update", record_idx, true); Too many. */
        /* Do one update */
        set_key(cursor, record_idx);
        set_value(opts, cursor, 2 * record_idx);
        testutil_check(wt_session->begin_transaction(wt_session, NULL));
        testutil_check(cursor->update(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, NULL));
        /* Print progress. */
        if ((record_idx % 100) == 0) {
            printf("eviction thread: Updates: update key=%" PRIu64 ", value=%" PRIu64 "\n",
              record_idx, 2 * record_idx);
            fflush(stdout);
        }
        /* Force eviction: Makes use of debug.release_evict. */
        cursor->reset(cursor);
    }
    print_eviction_stats(wt_session, opts, "After Update", record_idx, false);

    testutil_check(pthread_join(prefetch_thread_id, NULL));

    print_eviction_stats(wt_session, opts, "After pthread_join", record_idx, false);
    print_prefetch_stats(wt_session, opts, "After pthread_join", record_idx, false);

    opts->conn->disable_control_point(opts->conn, WT_CONN_CONTROL_POINT_ID_WT_12945);

    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}

/*
 * thread_do_prefetch --
 *     Read to trigger pre-fetch.
 */
void *
thread_do_prefetch(void *arg)
{
    TEST_OPTS *opts;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *wt_session;
    uint64_t idx;
    uint64_t key;
    uint64_t value;
    int64_t current_prefetch_pages_queued;
    int64_t previous_prefetch_pages_queued;
    int ret;

    opts = (TEST_OPTS *)arg;
    conn = opts->conn;

    printf("Running pre-fetch thread\n");

    testutil_check(conn->open_session(conn, NULL, session_open_config, &wt_session));
    testutil_check(wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor));

    /* Tell the eviction thread that the pre-fetch thread is ready. */
    (void)__wt_atomic_add64(&ready_counter, 1);

    /* Read to trigger pre-fetch */
    previous_prefetch_pages_queued = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_PAGES_QUEUED);
    idx = 0;
    while ((ret = cursor->next(cursor)) != WT_NOTFOUND) {
        WT_ERR(ret);
        current_prefetch_pages_queued =
          get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_PAGES_QUEUED);
        if (current_prefetch_pages_queued > previous_prefetch_pages_queued) {
            printf("%" PRIu64 ". prefetch_pages_queued increased from %" PRId64 " to %" PRId64
                   ". Exit loop.\n",
              idx, previous_prefetch_pages_queued, current_prefetch_pages_queued);
            break;
        }
        previous_prefetch_pages_queued = current_prefetch_pages_queued;
        print_prefetch_stats(wt_session, opts, "Prefix", idx, true);
        /* Read */
        key = get_key(opts, cursor);
        value = get_value(opts, cursor);
        /* Print progress. */
        if ((idx % 100) == 0) {
            printf("pre-fetch thread: read key=%" PRIu64 ", value=%" PRIu64 "\n", key, value);
            fflush(stdout);
        }
        __wt_sleep(0, WT_THOUSAND); /* 1 millisecond */
        ++idx;
    }
    print_prefetch_stats(wt_session, opts, "After Prefix", idx, false);

err:
    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));

    opts->running = false;

    printf("End pre-fetch thread\n");

    return (NULL);
}
