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
 * Time stepping down from leader to follower with many layered tables and a dirty cache. The test
 * creates a large number of layered tables (logging is enabled so table creation is not bound on
 * fsync), writes until the cache is full so that eviction is running in the background, and then
 * times the reconfigure that steps the connection down to the follower role.
 */

/*
 * Command-line flags for testutil_parse_single_opt.
 *   b: build directory override
 *   c: cache size in MB (default 100)
 *   G: enable disaggregated storage (required)
 *   h: home directory for the WiredTiger database
 *   n: number of layered tables to create (default 20,000)
 *   p: preserve the home directory after the test completes
 *   r: reopen an existing database instead of recreating the tables (implies -p)
 *   L: linger this many seconds as a follower after the step-down before closing
 *   l: linger this many seconds as leader, writes still running, before the step-down
 *   v: verbose mode
 *   W: number of worker threads (default 4)
 */
#define GETOPTS "b:c:Gh:n:prvW:L:l:"

extern char *__wt_optarg; /* argument associated with option */

#define TABLE_URI_PREFIX "layered:test_"
#define TABLE_CONFIG "key_format=S,value_format=S"
#define VALUE_SIZE 2048
#define DEFAULT_NTABLES 20000
#define DEFAULT_CACHE_SIZE_MB 100
/*
 * The table handles alone far exceed the target cache size, so create under a large cache and
 * shrink to the target size once every table exists: the shrink alone leaves eviction with plenty
 * of background work.
 */
#define CREATE_CACHE_SIZE_MB 2048

/*
 * Stop filling once the cache is 80% full, or after writing ten times the cache size. The stop
 * threshold sits just below the eviction trigger: at equilibrium eviction holds the cache at the
 * trigger, and the dirty content keeps eviction busy long after the writers stop.
 */
#define CACHE_FULL_PCT 80

typedef struct {
    WT_CONNECTION *conn;
    uint64_t ntables;         /* Total number of tables */
    uint64_t nthreads;        /* Total number of worker threads */
    uint64_t tid;             /* This thread's ID */
    uint64_t ops;             /* Creates or writes this thread completed */
    uint64_t inserts;         /* Inserts this thread completed */
    uint64_t updates;         /* Updates this thread completed */
    int err;                  /* First unexpected error seen by this thread */
    volatile bool *running;   /* Cleared when the cache is full */
    uint64_t *bytes_inserted; /* Shared counter of payload bytes written */
} WORKER_ARG;

/*
 * stat_get --
 *     Read a single connection statistic.
 */
static int64_t
stat_get(WT_SESSION *session, int stat_key)
{
    WT_CURSOR *stat;
    int64_t value;
    const char *desc, *pvalue;

    testutil_check(session->open_cursor(session, "statistics:", NULL, NULL, &stat));
    stat->set_key(stat, stat_key);
    testutil_check(stat->search(stat));
    testutil_check(stat->get_value(stat, &desc, &pvalue, &value));
    testutil_check(stat->close(stat));
    return (value);
}

/*
 * create_thread --
 *     Create every (strided) table.
 */
static WT_THREAD_RET
create_thread(void *arg)
{
    WORKER_ARG *w;
    WT_SESSION *session;
    uint64_t i;
    char uri[128];

    w = (WORKER_ARG *)arg;
    testutil_check(w->conn->open_session(w->conn, NULL, NULL, &session));

    for (i = w->tid; i < w->ntables; i += w->nthreads) {
        testutil_snprintf(uri, sizeof(uri), "%s%05" PRIu64, TABLE_URI_PREFIX, i);
        testutil_check(session->create(session, uri, TABLE_CONFIG));
        ++w->ops;
    }

    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/* Updates hit a small hot keyspace repeatedly; inserts always land on a fresh key. */
#define UPDATE_KEYSPACE 1000

/*
 * writer_thread --
 *     Hammer random tables with a 50/50 mix of inserts and updates until told to stop, dirtying the
 *     cache across all the tables.
 */
static WT_THREAD_RET
writer_thread(void *arg)
{
    WORKER_ARG *w;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    uint64_t seq;
    char key[64], uri[128], value[VALUE_SIZE];
    bool do_insert;

    w = (WORKER_ARG *)arg;
    memset(value, (int)('a' + (w->tid % 26)), sizeof(value) - 1);
    value[sizeof(value) - 1] = '\0';
    __wt_random_init_seed(&rnd, w->tid + 1);
    seq = 0;

    testutil_check(w->conn->open_session(w->conn, NULL, NULL, &session));

    while (*w->running) {
        testutil_snprintf(
          uri, sizeof(uri), "%s%05" PRIu64, TABLE_URI_PREFIX, __wt_random(&rnd) % w->ntables);
        testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

        do_insert = __wt_random(&rnd) % 2 == 0;
        if (do_insert)
            testutil_snprintf(key, sizeof(key), "i%06" PRIu64 "-%09" PRIu64, w->tid, seq++);
        else
            testutil_snprintf(
              key, sizeof(key), "u%09" PRIu64, (uint64_t)(__wt_random(&rnd) % UPDATE_KEYSPACE));
        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);
        /* Cache pressure can roll the operation back; retry until it commits. */
        while ((ret = do_insert ? cursor->insert(cursor) : cursor->update(cursor)) == WT_ROLLBACK &&
          *w->running)
            testutil_check(cursor->reset(cursor));
        if (ret != 0) {
            w->err = ret;
            fprintf(stderr, "writer %" PRIu64 ": %s into %s failed: %s\n", w->tid,
              do_insert ? "insert" : "update", uri, session->strerror(session, ret));
            testutil_check(cursor->close(cursor));
            break;
        }
        testutil_check(cursor->close(cursor));
        ++w->ops;
        if (do_insert)
            ++w->inserts;
        else
            ++w->updates;
        __wt_atomic_add_uint64(w->bytes_inserted, VALUE_SIZE);
    }

    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*
 * run_workers --
 *     Run a worker function over the thread set and join them all.
 */
static void
run_workers(WT_CONNECTION *conn, uint64_t ntables, uint64_t nthreads, volatile bool *running,
  uint64_t *bytes_inserted, WT_THREAD_RET (*func)(void *), WORKER_ARG *args_out)
{
    wt_thread_t *threads;
    uint64_t i;

    threads = dmalloc(nthreads * sizeof(wt_thread_t));
    memset(args_out, 0, nthreads * sizeof(WORKER_ARG));
    for (i = 0; i < nthreads; ++i) {
        args_out[i].conn = conn;
        args_out[i].ntables = ntables;
        args_out[i].nthreads = nthreads;
        args_out[i].tid = i;
        args_out[i].running = running;
        args_out[i].bytes_inserted = bytes_inserted;
        testutil_check(__wt_thread_create(NULL, &threads[i], func, &args_out[i]));
    }
    for (i = 0; i < nthreads; ++i)
        testutil_check(__wt_thread_join(NULL, &threads[i]));
    free(threads);
}

/*
 * main --
 *     Create many layered tables, fill the cache, and time the step-down to follower.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WORKER_ARG *worker_args;
    WT_SESSION *session;
    wt_thread_t *threads;
    uint64_t bytes_inserted, cache_full_bytes, cache_size_mb, i, inserts_total, updates_total;
    uint64_t leader_linger_sec, linger_sec, max_fill_bytes;
    uint64_t ntables, nthreads;
    uint64_t time_start, time_stop;
    int ch;
    char conn_config[512];
    bool reopen, running;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    opts->table_type = TABLE_ROW;

    reopen = false;
    linger_sec = leader_linger_sec = 0;
    cache_size_mb = DEFAULT_CACHE_SIZE_MB;
    testutil_parse_begin_opt(argc, argv, GETOPTS, opts);
    while ((ch = __wt_getopt(opts->progname, argc, argv, GETOPTS)) != EOF)
        switch (ch) {
        case 'r': /* Reopen an existing database. */
            reopen = true;
            break;
        case 'c': /* Cache size in MB. */
            cache_size_mb = (uint64_t)atoll(__wt_optarg);
            break;
        case 'L': /* Linger as a follower after the step-down. */
            linger_sec = (uint64_t)atoll(__wt_optarg);
            break;
        case 'l': /* Linger as leader, writes still running, before the step-down. */
            leader_linger_sec = (uint64_t)atoll(__wt_optarg);
            break;
        default:
            if (testutil_parse_single_opt(opts, ch) != 0)
                testutil_die(EINVAL, "unexpected option");
        }
    testutil_parse_end_opt(opts);

    if (!opts->disagg.is_enabled)
        testutil_die(EINVAL, "test requires disaggregated storage (-G)");
    opts->disagg.page_log_home = opts->home; /* Set home directory for page log. */

    /* A reopened database is always preserved for the next run. */
    if (reopen)
        opts->preserve = true;

    ntables = opts->nrecords == 0 ? DEFAULT_NTABLES : opts->nrecords;
    nthreads = opts->n_write_threads == 0 ? 4 : opts->n_write_threads;
    max_fill_bytes = 10ULL * cache_size_mb * WT_MEGABYTE;

    if (!reopen)
        testutil_recreate_dir(opts->home);
    else
        testutil_assert_errno(testutil_exists(NULL, opts->home));

    /*
     * Open the connection. Logging keeps table creation fast; the idle handle close is disabled so
     * every table stays open for the step-down to walk. Eviction is configured aggressively: low
     * targets and triggers keep the eviction workers busy well before the cache fills.
     */
    testutil_snprintf(conn_config, sizeof(conn_config),
      "%s"
      ",cache_size=%dMB"
      ",log=(enabled=true)"
      ",statistics=(all)"
      ",statistics_log=(wait=1,json=true,on_close=true)"
      ",file_manager=(close_idle_time=0)"
      ",eviction=(threads_min=8,threads_max=8)"
      ",eviction_target=70,eviction_trigger=85"
      ",eviction_dirty_target=5,eviction_dirty_trigger=30",
      reopen ? "" : "create", CREATE_CACHE_SIZE_MB);
    testutil_wiredtiger_open(opts, opts->home, conn_config, NULL, &opts->conn, false, false);

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    worker_args = dmalloc(nthreads * sizeof(WORKER_ARG));
    threads = dmalloc(nthreads * sizeof(wt_thread_t));
    bytes_inserted = 0;
    running = true;

    if (!reopen) {
        /* Create the layered tables. */
        printf(
          "Creating %" PRIu64 " layered tables using %" PRIu64 " threads...\n", ntables, nthreads);
        time_start = __wt_clock((WT_SESSION_IMPL *)session);
        run_workers(
          opts->conn, ntables, nthreads, &running, &bytes_inserted, create_thread, worker_args);
        time_stop = __wt_clock((WT_SESSION_IMPL *)session);
        printf("Created %" PRIu64 " layered tables in %" PRIu64 " ms\n", ntables,
          WT_CLOCKDIFF_MS(time_stop, time_start));

        /*
         * Checkpoint the empty tables so a later reopen (-r) finds them in shared storage. The
         * checkpoint runs before the fill, so the cache is still dirty when the step-down is timed
         * below.
         */
        testutil_check(
          opts->conn->set_timestamp(opts->conn, "oldest_timestamp=1,stable_timestamp=1"));
        time_start = __wt_clock((WT_SESSION_IMPL *)session);
        testutil_check(session->checkpoint(session, NULL));
        time_stop = __wt_clock((WT_SESSION_IMPL *)session);
        printf("Checkpointed %" PRIu64 " tables in %" PRIu64 " ms\n", ntables,
          WT_CLOCKDIFF_MS(time_stop, time_start));
    } else
        printf("Reopened existing database, skipping table creation\n");

    /* Shrink the cache to the target size; eviction starts draining the resident handles. */
    testutil_snprintf(conn_config, sizeof(conn_config), "cache_size=%" PRIu64 "MB", cache_size_mb);
    testutil_check(opts->conn->reconfigure(opts->conn, conn_config));

    /*
     * Hammer random tables with inserts and updates until the cache is full, so eviction keeps
     * running in the background. The random spread opens every table's handles for the step-down to
     * walk.
     */
    printf("Writing to fill the %" PRIu64 "MB cache using %" PRIu64 " threads...\n", cache_size_mb,
      nthreads);
    cache_full_bytes = (cache_size_mb * WT_MEGABYTE * CACHE_FULL_PCT) / 100;
    time_start = __wt_clock((WT_SESSION_IMPL *)session);
    memset(worker_args, 0, nthreads * sizeof(WORKER_ARG));
    for (i = 0; i < nthreads; ++i) {
        worker_args[i].conn = opts->conn;
        worker_args[i].ntables = ntables;
        worker_args[i].nthreads = nthreads;
        worker_args[i].tid = i;
        worker_args[i].running = &running;
        worker_args[i].bytes_inserted = &bytes_inserted;
        testutil_check(__wt_thread_create(NULL, &threads[i], writer_thread, &worker_args[i]));
    }
    /*
     * Wait until enough has been written to cover the tables and the cache has filled, then stop
     * the writers. The dirty content keeps eviction busy long after the writers stop.
     */
    for (;;) {
        if (__wt_atomic_load_uint64_relaxed(&bytes_inserted) >= ntables * (uint64_t)VALUE_SIZE &&
          stat_get(session, WT_STAT_CONN_CACHE_BYTES_INUSE) >= (int64_t)cache_full_bytes)
            break;
        if (__wt_atomic_load_uint64_relaxed(&bytes_inserted) >= max_fill_bytes) {
            fprintf(stderr,
              "warning: wrote %" PRIu64 "MB without filling the cache, stopping writes\n",
              max_fill_bytes / WT_MEGABYTE);
            break;
        }
        __wt_sleep(0, 20 * WT_THOUSAND);
    }

    /* Keep writing as leader for a while: the cache keeps churning with eviction fully engaged. */
    if (leader_linger_sec > 0) {
        printf("Continuing writes as leader for %" PRIu64 " seconds...\n", leader_linger_sec);
        for (i = 0; i < leader_linger_sec; ++i)
            __wt_sleep(1, 0);
    }

    running = false;
    for (i = 0; i < nthreads; ++i)
        testutil_check(__wt_thread_join(NULL, &threads[i]));
    time_stop = __wt_clock((WT_SESSION_IMPL *)session);

    inserts_total = updates_total = 0;
    for (i = 0; i < nthreads; ++i) {
        testutil_check(worker_args[i].err);
        inserts_total += worker_args[i].ops;
        updates_total += worker_args[i].updates;
    }
    printf("Wrote %" PRIu64 " records (%" PRIu64 " inserts, %" PRIu64 " updates) in %" PRIu64
           " ms\n",
      inserts_total, inserts_total - updates_total, updates_total,
      WT_CLOCKDIFF_MS(time_stop, time_start));

    /* Eviction should be active against the dirty cache when the step-down begins. */
    printf("Cache bytes in use: %" PRId64 ", dirty: %" PRId64 ", modified pages evicted: %" PRId64
           "\n",
      stat_get(session, WT_STAT_CONN_CACHE_BYTES_INUSE),
      stat_get(session, WT_STAT_CONN_CACHE_BYTES_DIRTY_TOTAL),
      stat_get(session, WT_STAT_CONN_CACHE_EVICTION_DIRTY));

    /* Time the step-down to the follower role. */
    time_start = __wt_clock((WT_SESSION_IMPL *)session);
    testutil_check(opts->conn->reconfigure(opts->conn, "disaggregated=(role=follower)"));
    time_stop = __wt_clock((WT_SESSION_IMPL *)session);
    printf("Reconfigure to follower took %" PRIu64 " ms\n", WT_CLOCKDIFF_MS(time_stop, time_start));
    printf("Step-down time reported by statistics: %" PRId64 " ms\n",
      stat_get(session, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME));

    /* Linger as a follower so the per-second statistics log covers the follower phase. */
    if (linger_sec > 0) {
        printf("Lingering as a follower for %" PRIu64 " seconds...\n", linger_sec);
        for (i = 0; i < linger_sec; ++i)
            __wt_sleep(1, 0);
    }

    free(threads);
    free(worker_args);
    testutil_check(session->close(session, NULL));

    /* Close the connection before testutil_cleanup removes the home directory. */
    testutil_check(opts->conn->close(opts->conn, NULL));
    opts->conn = NULL;

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
