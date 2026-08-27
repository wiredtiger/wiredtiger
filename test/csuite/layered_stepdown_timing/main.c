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
 *   e: mild eviction (4-8 threads, default targets) instead of aggressive eviction
 *   G: enable disaggregated storage (required)
 *   h: home directory for the WiredTiger database
 *   n: number of layered tables to create (default 6,666, i.e. ~20k active dhandles)
 *   p: preserve the home directory after the test completes
 *   r: reopen an existing database instead of recreating the tables (implies -p)
 *   L: linger this many seconds as a follower after the step-down before closing
 *   l: linger this many seconds as leader, writes still running, before the step-down
 *   v: verbose mode
 *   W: number of worker threads (default 4)
 *   x: transactional writes with commit timestamps; set stable to the last committed timestamp
 *      and take a precise checkpoint before stepping down
 */
#define GETOPTS "b:c:eGh:n:prvW:L:l:x"

extern char *__wt_optarg; /* argument associated with option */

#define TABLE_URI_PREFIX "layered:test_"
#define TABLE_CONFIG "key_format=S,value_format=S"
#define VALUE_SIZE 2048
/*
 * Every layered table carries three dhandles (layered, ingest, stable), so 6,666 tables make for
 * ~20k active dhandles on the step-down walk.
 */
#define DEFAULT_NTABLES 6666
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
    uint64_t *next_ts;        /* Shared commit timestamp allocator, NULL for untimestamped */
    uint64_t commit_ts;       /* Last commit timestamp this thread committed */
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

/*
 * Updates hit a small hot keyspace repeatedly; inserts always land on a fresh key. The keyspace is
 * private to each thread: cross-thread write conflicts would roll the whole batch back.
 */
#define UPDATE_KEYSPACE 1000

/* Operations per transaction in timestamp mode; one commit per operation is too slow. */
#define TS_OPS_PER_TXN 100

/*
 * How far behind the timestamp allocator the oldest/stable timestamps trail during the write phase.
 * Without advancing them, every update version stays pinned forever and eviction cannot make
 * progress.
 */
#define TS_ADVANCE_MARGIN 10000

/*
 * writer_op --
 *     A single insert or update against a random table.
 */
static int
writer_op(WORKER_ARG *w, WT_SESSION *session, WT_RAND_STATE *rnd, uint64_t *seqp, const char *value,
  bool *was_insert)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    char key[64], uri[128];

    testutil_snprintf(
      uri, sizeof(uri), "%s%05" PRIu64, TABLE_URI_PREFIX, __wt_random(rnd) % w->ntables);
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    *was_insert = __wt_random(rnd) % 2 == 0;
    if (*was_insert)
        testutil_snprintf(key, sizeof(key), "i%06" PRIu64 "-%09" PRIu64, w->tid, (*seqp)++);
    else
        testutil_snprintf(key, sizeof(key), "u%06" PRIu64 "-%09" PRIu64, w->tid,
          (uint64_t)(__wt_random(rnd) % UPDATE_KEYSPACE));
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, value);
    ret = *was_insert ? cursor->insert(cursor) : cursor->update(cursor);
    if (ret != 0 && ret != WT_ROLLBACK)
        fprintf(stderr, "writer %" PRIu64 ": %s into %s failed: %s\n", w->tid,
          *was_insert ? "insert" : "update", uri, session->strerror(session, ret));
    testutil_check(cursor->close(cursor));
    return (ret);
}

/*
 * writer_thread --
 *     Hammer random tables with a 50/50 mix of inserts and updates until told to stop, dirtying the
 *     cache across all the tables. Timestamp mode batches operations into transactions committed
 *     with monotonically increasing commit timestamps.
 */
static WT_THREAD_RET
writer_thread(void *arg)
{
    WORKER_ARG *w;
    WT_DECL_RET;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    uint64_t batch_ins, batch_ops, batch_upd, j, seq, ts;
    char ts_config[64], value[VALUE_SIZE];
    bool was_insert;

    w = (WORKER_ARG *)arg;
    memset(value, (int)('a' + (w->tid % 26)), sizeof(value) - 1);
    value[sizeof(value) - 1] = '\0';
    __wt_random_init_seed(&rnd, w->tid + 1);
    seq = 0;

    testutil_check(w->conn->open_session(w->conn, NULL, NULL, &session));

    while (*w->running) {
        if (w->next_ts != NULL)
            testutil_check(session->begin_transaction(session, NULL));

        batch_ops = batch_ins = batch_upd = 0;
        for (j = 0; j < TS_OPS_PER_TXN && *w->running; ++j) {
            ret = writer_op(w, session, &rnd, &seq, value, &was_insert);
            /* Cache pressure can roll the unit back; abandon it and start a fresh one. */
            if (ret == WT_ROLLBACK)
                break;
            if (ret != 0) {
                w->err = ret;
                goto done;
            }
            ++batch_ops;
            if (was_insert)
                ++batch_ins;
            else
                ++batch_upd;
        }

        if (w->next_ts != NULL) {
            if (ret == WT_ROLLBACK) {
                /* The unit can only roll back; replay it in full with fresh insert keys. */
                testutil_check(session->rollback_transaction(session, NULL));
                continue;
            }
            ts = __wt_atomic_fetch_add_uint64(w->next_ts, 1);
            testutil_snprintf(ts_config, sizeof(ts_config), "commit_timestamp=%" PRIx64, ts);
            ret = session->commit_transaction(session, ts_config);
            if (ret == WT_ROLLBACK) {
                testutil_check(session->rollback_transaction(session, NULL));
                continue;
            }
            if (ret != 0) {
                w->err = ret;
                fprintf(stderr, "writer %" PRIu64 ": commit failed: %s\n", w->tid,
                  session->strerror(session, ret));
                break;
            }
            w->commit_ts = ts;
        }

        /* Only count the batch once it is committed (in autocommit mode, each op committed). */
        w->ops += batch_ops;
        w->inserts += batch_ins;
        w->updates += batch_upd;
        __wt_atomic_add_uint64(w->bytes_inserted, batch_ops * VALUE_SIZE);
    }

done:
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
    uint64_t last_commit, leader_linger_sec, linger_sec, max_fill_bytes, next_ts;
    uint64_t ntables, nthreads;
    uint64_t time_start, time_stop;
    int ch;
    char conn_config[512];
    const char *eviction_config;
    bool reopen, running, ts_mode;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    opts->table_type = TABLE_ROW;

    reopen = false;
    ts_mode = false;
    linger_sec = leader_linger_sec = 0;
    cache_size_mb = DEFAULT_CACHE_SIZE_MB;
    eviction_config =
      ",eviction=(threads_min=8,threads_max=8)"
      ",eviction_target=70,eviction_trigger=85"
      ",eviction_dirty_target=5,eviction_dirty_trigger=30";
    testutil_parse_begin_opt(argc, argv, GETOPTS, opts);
    while ((ch = __wt_getopt(opts->progname, argc, argv, GETOPTS)) != EOF)
        switch (ch) {
        case 'r': /* Reopen an existing database. */
            reopen = true;
            break;
        case 'c': /* Cache size in MB. */
            cache_size_mb = (uint64_t)atoll(__wt_optarg);
            break;
        case 'e': /* Mild eviction instead of aggressive. */
            eviction_config = ",eviction=(threads_min=4,threads_max=8)";
            break;
        case 'x': /* Timestamped transactional writes + precise checkpoint before step-down. */
            ts_mode = true;
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
     * every table stays open for the step-down to walk. Eviction defaults to aggressive: low
     * targets and triggers keep the eviction workers busy well before the cache fills.
     */
    testutil_snprintf(conn_config, sizeof(conn_config),
      "%s"
      ",cache_size=%dMB"
      ",log=(enabled=true)"
      ",statistics=(all)"
      ",statistics_log=(wait=1,json=true,on_close=true)"
      ",file_manager=(close_idle_time=0)"
      "%s",
      reopen ? "" : "create", CREATE_CACHE_SIZE_MB, eviction_config);
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
    next_ts = 2; /* Timestamp 1 is used by the create-mode checkpoint. */
    time_start = __wt_clock((WT_SESSION_IMPL *)session);
    memset(worker_args, 0, nthreads * sizeof(WORKER_ARG));
    for (i = 0; i < nthreads; ++i) {
        worker_args[i].conn = opts->conn;
        worker_args[i].ntables = ntables;
        worker_args[i].nthreads = nthreads;
        worker_args[i].tid = i;
        worker_args[i].running = &running;
        worker_args[i].bytes_inserted = &bytes_inserted;
        worker_args[i].next_ts = ts_mode ? &next_ts : NULL;
        testutil_check(__wt_thread_create(NULL, &threads[i], writer_thread, &worker_args[i]));
    }
    /*
     * Wait until enough has been written to cover the tables and the cache has filled, then stop
     * the writers. The dirty content keeps eviction busy long after the writers stop. Timestamp
     * mode only waits for the coverage pass: it ends with a precise checkpoint that cleans the
     * cache anyway, and timestamped writes throttle at the dirty trigger well before the cache
     * fills.
     */
    for (uint64_t polls = 0;; ++polls) {
        /* A dead writer means the cache may never fill; bail and report its error below. */
        for (i = 0; i < nthreads; ++i)
            if (worker_args[i].err != 0)
                goto fill_done;
        if (__wt_atomic_load_uint64_relaxed(&bytes_inserted) >= ntables * (uint64_t)VALUE_SIZE &&
          (ts_mode ||
            stat_get(session, WT_STAT_CONN_CACHE_BYTES_INUSE) >= (int64_t)cache_full_bytes))
            break;
        if (__wt_atomic_load_uint64_relaxed(&bytes_inserted) >= max_fill_bytes) {
            fprintf(stderr,
              "warning: wrote %" PRIu64 "MB without filling the cache, stopping writes\n",
              max_fill_bytes / WT_MEGABYTE);
            break;
        }
        /* Advance oldest/stable once a second so update versions do not stay pinned forever. */
        if (ts_mode && polls % 50 == 0) {
            last_commit = __wt_atomic_load_uint64_relaxed(&next_ts);
            if (last_commit > TS_ADVANCE_MARGIN) {
                testutil_snprintf(conn_config, sizeof(conn_config),
                  "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64,
                  last_commit - TS_ADVANCE_MARGIN, last_commit - TS_ADVANCE_MARGIN);
                testutil_check(opts->conn->set_timestamp(opts->conn, conn_config));
            }
        }
        __wt_sleep(0, 20 * WT_THOUSAND);
    }
fill_done:

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

    /*
     * In timestamp mode, pin the stable timestamp to the last committed write and take a precise
     * checkpoint, so everything on stable is durable before the step-down.
     */
    if (ts_mode) {
        last_commit = 0;
        for (i = 0; i < nthreads; ++i)
            last_commit = WT_MAX(last_commit, worker_args[i].commit_ts);
        testutil_snprintf(conn_config, sizeof(conn_config),
          "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, last_commit, last_commit);
        testutil_check(opts->conn->set_timestamp(opts->conn, conn_config));
        printf("Stable timestamp set to the last committed write: %" PRIx64 "\n", last_commit);
        time_start = __wt_clock((WT_SESSION_IMPL *)session);
        testutil_check(session->checkpoint(session, NULL));
        time_stop = __wt_clock((WT_SESSION_IMPL *)session);
        printf("Precise checkpoint before step-down took %" PRIu64 " ms\n",
          WT_CLOCKDIFF_MS(time_stop, time_start));
    }

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
