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
 * when prefetch and eviction of the same page happens at the same time.
 *
 * This variant tests dirty eviction.
 */

/* TEMPORARY: NOTE: This is loosely based on wt-2535_insert_race. */

/* Constants */

#define NUM_WARM_UP_RECORDS 100000 /* How many records to insert during warmup. */
/* First record to change to give pre-fetch thread time to begin prefetching */
#define FIRST_RECORD_TO_CHANGE 2000
#define NUM_EVICTION 1 /* How many times to force eviction */

void *thread_do_prefetch(void *);

static uint64_t ready_counter;

/*
 * get_stat --
 *     Get one statistic scaning to find it.
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
 * get_value --
 *     Wrapper providing the correct typing for the WT_CURSOR::get_value variadic argument.
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
print_eviction_stats(WT_SESSION *wt_session, TEST_OPTS *opts, const char *where, uint64_t idx)
{
    char separator;
    int64_t eviction_clean;                /* cache: unmodified pages evicted */
    int64_t eviction_dirty;                /* cache: modified pages evicted */
    int64_t eviction_force;                /* forced eviction - pages selected count */
    int64_t eviction_pages_seen;           /* cache: pages seen by eviction walk */
    int64_t eviction_server_evict_attempt; /* evict page attempts by eviction server */
    int64_t eviction_walk_passes;          /* eviction: eviction passes of a file */
    int64_t eviction_worker_evict_attempt; /* evict page attempts by eviction worker */
    eviction_clean = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_CLEAN);
    eviction_dirty = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_DIRTY);
    eviction_force = get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_FORCE);
    eviction_pages_seen = get_stat(opts, wt_session, WT_STAT_CONN_CACHE_EVICTION_PAGES_SEEN);
    eviction_server_evict_attempt =
      get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_SERVER_EVICT_ATTEMPT);
    eviction_walk_passes = get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_WALK_PASSES);
    eviction_worker_evict_attempt =
      get_stat(opts, wt_session, WT_STAT_CONN_EVICTION_WORKER_EVICT_ATTEMPT);

    if ((eviction_clean == 0) && (eviction_dirty == 0) && (eviction_force == 0) &&
      (eviction_pages_seen == 0) && (eviction_server_evict_attempt == 0) &&
      (eviction_walk_passes == 0) && (eviction_worker_evict_attempt == 0))
        return;

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
 *     Print some prefetch stats.
 */
static void
print_prefetch_stats(WT_SESSION *wt_session, TEST_OPTS *opts, const char *where, uint64_t idx)
{
    int64_t prefetch_attempts;
    int64_t prefetch_pages_queued;
    char separator;

    prefetch_attempts = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_ATTEMPTS);
    prefetch_pages_queued = get_stat(opts, wt_session, WT_STAT_CONN_PREFETCH_PAGES_QUEUED);

    if ((prefetch_attempts == 0) && (prefetch_pages_queued == 0))
        return;

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
      "create,cache_size=2G,eviction=(threads_max=5),"
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
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &wt_session));
    testutil_check(
      wt_session->create(wt_session, opts->uri, "key_format=Q,value_format=Q,leaf_page_max=32k"));

    /* Warm-up: Insert some documents */
    testutil_check((ret = wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor)));
    for (record_idx = 0; record_idx < opts->nrecords; ++record_idx) {
        print_eviction_stats(wt_session, opts, "Warm up", record_idx);
        /* Do one insertion */
        set_key(cursor, record_idx);
        set_value(opts, cursor, record_idx);
        testutil_check(wt_session->begin_transaction(wt_session, "isolation=snapshot"));
        testutil_check(cursor->insert(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, NULL));
        if (record_idx % (10 * WT_THOUSAND) == 0) {
            printf("eviction thread: Warm-up: insert key=%" PRIu64 ", value=%" PRIu64 "\n",
              record_idx, record_idx);
            fflush(stdout);
        }
    }
    print_eviction_stats(wt_session, opts, "After Warm up", record_idx);
    testutil_check(cursor->close(cursor));

    /* Close and reopen the connection to force the warm-up documents out of the cache. */
    testutil_check(wt_session->close(wt_session, NULL));
    testutil_check(opts->conn->close(opts->conn, ""));

    testutil_check(wiredtiger_open(opts->home, NULL, wiredtiger_open_config, &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &wt_session));

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
        print_eviction_stats(wt_session, opts, "Update", record_idx);
        /* Do one update */
        set_key(cursor, record_idx);
        set_value(opts, cursor, 2 * record_idx);
        testutil_check(wt_session->begin_transaction(wt_session, NULL));
        testutil_check(cursor->update(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, NULL));
        if (record_idx % (10 * WT_THOUSAND) == 0) {
            printf("eviction thread: Updates: update key=%" PRIu64 ", value=%" PRIu64 "\n",
              record_idx, 2 * record_idx);
            fflush(stdout);
        }
        /* Force eviction: Makes use of debug.release_evict. */
        cursor->reset(cursor);
    }
    print_eviction_stats(wt_session, opts, "After Update", record_idx);

    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));

    testutil_check(pthread_join(prefetch_thread_id, NULL));

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
    int ret;

    opts = (TEST_OPTS *)arg;
    conn = opts->conn;

    printf("Running prefetch thread\n");

    testutil_check(conn->open_session(conn, NULL, NULL, &wt_session));
    testutil_check(wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor));

    /* Tell the eviction thread that the prefetch thread is ready. */
    (void)__wt_atomic_add64(&ready_counter, 1);

    /* Read to trigger prefetch */
    idx = 0;
    while ((ret = cursor->next(cursor)) != WT_NOTFOUND) {
        WT_ERR(ret);
        print_prefetch_stats(wt_session, opts, "Prefix", idx);
        /* Read */
        key = get_key(opts, cursor);
        value = get_value(opts, cursor);
        if (idx % (10 * WT_THOUSAND) == 0) {
            printf("prefetch thread: read key=%" PRIu64 ", value=%" PRIu64 "\n", idx, value);
            fflush(stdout);
        }
        if (key == (FIRST_RECORD_TO_CHANGE - 30))
            break;                  /* Close enough for prefetch to do the rest. */
        __wt_sleep(0, WT_THOUSAND); /* 1 millisecond */
        ++idx;
    }
    print_prefetch_stats(wt_session, opts, "After Prefix", idx);

err:
    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));

    opts->running = false;

    return (NULL);
}
