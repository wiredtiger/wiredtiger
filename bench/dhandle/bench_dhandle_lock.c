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
#include <sys/resource.h>
#include <sys/time.h>

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <wiredtiger.h>

/*
 * Data handle and schema lock benchmark. Uses only the public API, so one source builds against
 * any library version. Modes:
 *
 *   create HOME [key=value ...]
 *       Create "tables" tables "table:c<NNNNNN>" with "rows" rows each and close.
 *   steady HOME [key=value ...]
 *       Many tables, a moving window of active tables: "writers" threads write "rate" writes per
 *       second in total to random tables of [speed * t, speed * t + window) modulo "tables". With
 *       cd_rate > 0 a thread also creates and drops a table, cd_rate operations per second.
 *   restart HOME [key=value ...]
 *       Open an existing database (all handles closed) and immediately run "threads" threads on
 *       the first "hot" tables spread across the table space.
 *   load HOME [key=value ...]
 *       "threads" threads each create a table and immediately insert "rows" rows into it, until
 *       "tables" tables exist.
 *
 * Every mode prints one line "RESULT {json}" with the configuration, throughput, latency
 * (average, p50, p99, p99.9, max), lock statistics over the measured interval and process CPU
 * time. Percentiles come from a histogram with 16 buckets per power of two (bucket midpoint, at
 * most 3% off) and are capped at the exact maximum; averages and maxima are exact.
 */

#define CHK(call)                                                                                \
    do {                                                                                         \
        int __ret = (call);                                                                      \
        if (__ret != 0) {                                                                        \
            fprintf(                                                                             \
              stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #call, wiredtiger_strerror(__ret)); \
            exit(EXIT_FAILURE);                                                                  \
        }                                                                                        \
    } while (0)

#define HIST_SUB_BITS 4
#define HIST_SUB (1 << HIST_SUB_BITS)
#define HIST_BUCKETS (64 * HIST_SUB)
#define NSEC_PER_SEC UINT64_C(1000000000)
#define VALUE_SIZE 100

typedef struct {
    uint64_t bucket[HIST_BUCKETS];
    uint64_t count;
    uint64_t max_ns;
    uint64_t sum_ns;
} LAT_HIST;

typedef struct {
    const char *home;
    const char *mode;

    char conn_config[1024];

    double cd_rate; /* Create/drop operations per second, steady mode */
    double rate;    /* Operations per second in total, 0 for no limit */
    double speed;   /* Tables the window moves per second */
    int cache_mb;   /* Cache size */
    int ckpt_wait;  /* Seconds between the end of a checkpoint and the next one, 0: none */
    int direct_io;  /* Use direct I/O for data files */
    int hot;        /* Hot tables, restart mode */
    int idle;       /* Sweep close_idle_time */
    int keys;       /* Key range per table */
    int read_pct;   /* Percentage of operations that search instead of insert */
    int rows;       /* Rows per table, create and load modes */
    int seconds;    /* Run time */
    int skip;       /* Seconds before the measured interval starts */
    int tables;     /* Table count */
    int threads;    /* Worker threads */
    int window;     /* Active tables, steady mode */
    uint64_t seed;  /* Random seed */
} CONFIG;

typedef struct {
    LAT_HIST first;  /* First operation of the thread */
    LAT_HIST hist_a; /* Main operation: write/read, or create in load mode */
    LAT_HIST hist_b; /* Cursor open alone, or first cursor open after create in load mode */
    LAT_HIST hist_c; /* Insert, load mode */
    pthread_t tid;
    uint64_t end_ns;    /* Time the thread finished */
    uint64_t lag_drops; /* Times the thread fell more than a second behind its schedule */
    uint64_t ops;       /* Operations, read by the progress report */
    uint64_t rng;
    int id;
} WORKER;

/* Statistics recorded over the measured interval. A key of -1 is not in this library. */
typedef struct {
    const char *name;
    int key;
    bool point; /* Point value, reported at the end */
} STAT_DEF;

#ifdef WT_STAT_CONN_LOCK_SCHEMA_READ_COUNT
#define BENCH_SCHEMA_LOCK "rwlock"
#define STAT_SCHEMA_READ WT_STAT_CONN_LOCK_SCHEMA_READ_COUNT
#define STAT_SCHEMA_WRITE WT_STAT_CONN_LOCK_SCHEMA_WRITE_COUNT
#elif defined(WT_STAT_CONN_LOCK_SCHEMA_COUNT)
#define BENCH_SCHEMA_LOCK "spinlock"
#define STAT_SCHEMA_READ (-1)
#define STAT_SCHEMA_WRITE WT_STAT_CONN_LOCK_SCHEMA_COUNT
#else
#define BENCH_SCHEMA_LOCK "unknown"
#define STAT_SCHEMA_READ (-1)
#define STAT_SCHEMA_WRITE (-1)
#endif
#ifdef WT_STAT_CONN_DHANDLE_LOCK_BLOCKED
#define STAT_DHANDLE_LOCK_BLOCKED WT_STAT_CONN_DHANDLE_LOCK_BLOCKED
#else
#define STAT_DHANDLE_LOCK_BLOCKED (-1)
#endif

static const STAT_DEF stat_defs[] = {
  {"schema_read", STAT_SCHEMA_READ, false},
  {"schema_write", STAT_SCHEMA_WRITE, false},
  {"schema_wait_app_us", WT_STAT_CONN_LOCK_SCHEMA_WAIT_APPLICATION, false},
  {"schema_wait_int_us", WT_STAT_CONN_LOCK_SCHEMA_WAIT_INTERNAL, false},
  {"dhandle_read", WT_STAT_CONN_LOCK_DHANDLE_READ_COUNT, false},
  {"dhandle_write", WT_STAT_CONN_LOCK_DHANDLE_WRITE_COUNT, false},
  {"dhandle_wait_app_us", WT_STAT_CONN_LOCK_DHANDLE_WAIT_APPLICATION, false},
  {"dhandle_wait_int_us", WT_STAT_CONN_LOCK_DHANDLE_WAIT_INTERNAL, false},
  {"dhandle_lock_blocked", STAT_DHANDLE_LOCK_BLOCKED, false},
  {"table_wait_app_us", WT_STAT_CONN_LOCK_TABLE_WAIT_APPLICATION, false},
  {"table_wait_int_us", WT_STAT_CONN_LOCK_TABLE_WAIT_INTERNAL, false},
  {"sweep_expired_close", WT_STAT_CONN_DH_SWEEP_EXPIRED_CLOSE, false},
  {"sweep_remove", WT_STAT_CONN_DH_SWEEP_REMOVE, false},
  {"read_io", WT_STAT_CONN_READ_IO, false},
  {"handles", WT_STAT_CONN_DH_CONN_HANDLE_COUNT, true},
  {"files_open", WT_STAT_CONN_FILE_OPEN, true},
};
#define STAT_COUNT (sizeof(stat_defs) / sizeof(stat_defs[0]))

typedef struct {
    int64_t v[STAT_COUNT];
    bool missing[STAT_COUNT];
} STATS;

typedef struct {
    char buf[16384];
    size_t len;
} JSON;

static CONFIG cfg;
static WT_CONNECTION *conn;
static WORKER *workers;

static pthread_cond_t gate_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t gate_lock = PTHREAD_MUTEX_INITIALIZER;
static bool gate_open;
static int gate_ready;

static bool measuring; /* Workers record latencies */
static bool stop;      /* Workers exit */
static uint64_t start_ns;
static uint64_t next_table; /* Next table to create, load mode */
static int workers_done;

static LAT_HIST cd_hist;
static uint64_t ckpt_count, ckpt_max_ns;

/*
 * now_ns --
 *     Return the monotonic clock in nanoseconds.
 */
static uint64_t
now_ns(void)
{
    struct timespec ts;

    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * NSEC_PER_SEC + (uint64_t)ts.tv_nsec);
}

/*
 * sleep_ns --
 *     Sleep for a number of nanoseconds.
 */
static void
sleep_ns(uint64_t ns)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ns / NSEC_PER_SEC);
    ts.tv_nsec = (long)(ns % NSEC_PER_SEC);
    (void)nanosleep(&ts, NULL);
}

/*
 * sleep_until --
 *     Sleep until a monotonic clock time or until the run stops.
 */
static void
sleep_until(uint64_t t)
{
    uint64_t now;

    while ((now = now_ns()) < t && !__atomic_load_n(&stop, __ATOMIC_RELAXED))
        sleep_ns(t - now > 100000000 ? 100000000 : t - now);
}

/*
 * rng_next --
 *     Return the next value of a pseudo-random generator.
 */
static uint64_t
rng_next(uint64_t *statep)
{
    uint64_t x;

    x = *statep;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *statep = x;
    return (x * UINT64_C(0x2545F4914F6CDD1D));
}

/*
 * hist_bucket --
 *     Return the histogram bucket of a latency.
 */
static int
hist_bucket(uint64_t ns)
{
    int e;

    if (ns < HIST_SUB)
        return ((int)ns);
    e = 63 - __builtin_clzll(ns);
    return (
      (e - HIST_SUB_BITS + 1) * HIST_SUB + (int)((ns >> (e - HIST_SUB_BITS)) & (HIST_SUB - 1)));
}

/*
 * hist_mid --
 *     Return the midpoint of a histogram bucket in nanoseconds.
 */
static double
hist_mid(int b)
{
    uint64_t lo;
    int e;

    if (b < HIST_SUB)
        return ((double)b);
    e = b / HIST_SUB + HIST_SUB_BITS - 1;
    lo = (uint64_t)(HIST_SUB + b % HIST_SUB) << (e - HIST_SUB_BITS);
    return ((double)lo + (double)((uint64_t)1 << (e - HIST_SUB_BITS)) / 2);
}

/*
 * hist_add --
 *     Record a latency.
 */
static void
hist_add(LAT_HIST *h, uint64_t ns)
{
    ++h->bucket[hist_bucket(ns)];
    ++h->count;
    h->sum_ns += ns;
    if (ns > h->max_ns)
        h->max_ns = ns;
}

/*
 * hist_merge --
 *     Add one histogram to another.
 */
static void
hist_merge(LAT_HIST *dst, const LAT_HIST *src)
{
    int b;

    for (b = 0; b < HIST_BUCKETS; ++b)
        dst->bucket[b] += src->bucket[b];
    dst->count += src->count;
    dst->sum_ns += src->sum_ns;
    if (src->max_ns > dst->max_ns)
        dst->max_ns = src->max_ns;
}

/*
 * hist_pct_us --
 *     Return a percentile of a histogram in microseconds.
 */
static double
hist_pct_us(const LAT_HIST *h, double p)
{
    uint64_t need, sum;
    double v;
    int b;

    if (h->count == 0)
        return (0);
    need = (uint64_t)(p * (double)h->count + 0.999999);
    if (need == 0)
        need = 1;
    for (sum = 0, b = 0; b < HIST_BUCKETS; ++b) {
        sum += h->bucket[b];
        if (sum >= need)
            break;
    }
    v = hist_mid(b < HIST_BUCKETS ? b : HIST_BUCKETS - 1);
    if (v > (double)h->max_ns)
        v = (double)h->max_ns;
    return (v / 1e3);
}

/*
 * json_add --
 *     Append a formatted "key":value pair to the JSON object.
 */
static void
json_add(JSON *j, const char *key, const char *fmt, ...)
{
    va_list ap;
    int n;

    n = snprintf(j->buf + j->len, sizeof(j->buf) - j->len, "%s\"%s\":", j->len > 1 ? "," : "", key);
    if (n < 0 || (size_t)n >= sizeof(j->buf) - j->len)
        goto full;
    j->len += (size_t)n;
    va_start(ap, fmt);
    n = vsnprintf(j->buf + j->len, sizeof(j->buf) - j->len, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(j->buf) - j->len)
        goto full;
    j->len += (size_t)n;
    return;

full:
    fprintf(stderr, "JSON result buffer full\n");
    exit(EXIT_FAILURE);
}

/*
 * json_hist --
 *     Append the count, average, percentiles and maximum of a histogram to the JSON object.
 */
static void
json_hist(JSON *j, const char *name, const LAT_HIST *h)
{
    char key[64];

    (void)snprintf(key, sizeof(key), "%s_ops", name);
    json_add(j, key, "%" PRIu64, h->count);
    (void)snprintf(key, sizeof(key), "%s_avg_us", name);
    json_add(j, key, "%.2f", h->count == 0 ? 0 : (double)h->sum_ns / (double)h->count / 1e3);
    (void)snprintf(key, sizeof(key), "%s_p50_us", name);
    json_add(j, key, "%.2f", hist_pct_us(h, 0.50));
    (void)snprintf(key, sizeof(key), "%s_p99_us", name);
    json_add(j, key, "%.2f", hist_pct_us(h, 0.99));
    (void)snprintf(key, sizeof(key), "%s_p999_us", name);
    json_add(j, key, "%.2f", hist_pct_us(h, 0.999));
    (void)snprintf(key, sizeof(key), "%s_max_us", name);
    json_add(j, key, "%.2f", (double)h->max_ns / 1e3);
}

/*
 * stats_read --
 *     Read the connection statistics the benchmark reports.
 */
static void
stats_read(WT_SESSION *session, STATS *st)
{
    WT_CURSOR *cursor;
    int64_t v;
    size_t i;
    int ret;
    const char *desc, *pvalue;

    CHK(session->open_cursor(session, "statistics:", NULL, NULL, &cursor));
    for (i = 0; i < STAT_COUNT; ++i) {
        st->v[i] = 0;
        st->missing[i] = true;
        if (stat_defs[i].key < 0)
            continue;
        cursor->set_key(cursor, stat_defs[i].key);
        if ((ret = cursor->search(cursor)) == WT_NOTFOUND)
            continue;
        CHK(ret);
        CHK(cursor->get_value(cursor, &desc, &pvalue, &v));
        st->v[i] = v;
        st->missing[i] = false;
    }
    CHK(cursor->close(cursor));
}

/*
 * json_stats --
 *     Append the change of the statistics over the measured interval, or their final value for
 *     point statistics. A statistic the library does not have is null.
 */
static void
json_stats(JSON *j, const STATS *before, const STATS *after)
{
    size_t i;

    for (i = 0; i < STAT_COUNT; ++i)
        if (before->missing[i] || after->missing[i])
            json_add(j, stat_defs[i].name, "null");
        else if (stat_defs[i].point)
            json_add(j, stat_defs[i].name, "%" PRId64, after->v[i]);
        else
            json_add(j, stat_defs[i].name, "%" PRId64, after->v[i] - before->v[i]);
}

/*
 * cpu_seconds --
 *     Return the user and system CPU time of the process and its context switches.
 */
static void
cpu_seconds(double *userp, double *sysp, int64_t *vcswp, int64_t *ivcswp)
{
    struct rusage ru;

    (void)getrusage(RUSAGE_SELF, &ru);
    *userp = (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec / 1e6;
    *sysp = (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec / 1e6;
    *vcswp = ru.ru_nvcsw;
    *ivcswp = ru.ru_nivcsw;
}

/*
 * gate_wait --
 *     Wait until the main thread starts the workload.
 */
static void
gate_wait(void)
{
    CHK(pthread_mutex_lock(&gate_lock));
    ++gate_ready;
    CHK(pthread_cond_broadcast(&gate_cond));
    while (!gate_open)
        CHK(pthread_cond_wait(&gate_cond, &gate_lock));
    CHK(pthread_mutex_unlock(&gate_lock));
}

/*
 * gate_release --
 *     Wait for a number of threads to reach the gate, then let them all go.
 */
static void
gate_release(int nthreads)
{
    CHK(pthread_mutex_lock(&gate_lock));
    while (gate_ready < nthreads)
        CHK(pthread_cond_wait(&gate_cond, &gate_lock));
    gate_open = true;
    CHK(pthread_cond_broadcast(&gate_cond));
    CHK(pthread_mutex_unlock(&gate_lock));
}

/*
 * pace --
 *     Wait for the next operation of a thread that runs one operation per period. A thread that
 *     falls behind by more than a second drops the backlog.
 */
static void
pace(WORKER *w, uint64_t *nextp, uint64_t period)
{
    uint64_t now;

    if (period == 0)
        return;
    *nextp += period;
    now = now_ns();
    if (*nextp + NSEC_PER_SEC < now) {
        *nextp = now;
        ++w->lag_drops;
    } else if (*nextp > now)
        sleep_ns(*nextp - now);
}

/*
 * table_uri --
 *     Format the URI of a pre-created table.
 */
static void
table_uri(char *buf, size_t len, const char *prefix, uint64_t n)
{
    (void)snprintf(buf, len, "table:%s%06" PRIu64, prefix, n);
}

/*
 * table_op --
 *     Open a cursor on a table, insert or search a random key and close the cursor; record the
 *     whole operation and the cursor open alone.
 */
static void
table_op(WORKER *w, WT_SESSION *session, const char *uri, uint64_t r, WT_ITEM *value, bool first)
{
    WT_CURSOR *cursor;
    uint64_t t0, t1, t2;
    int ret;

    t0 = now_ns();
    CHK(session->open_cursor(session, uri, NULL, NULL, &cursor));
    t1 = now_ns();
    cursor->set_key(cursor, (int64_t)((r >> 32) % (uint64_t)cfg.keys));
    if ((int)((r >> 16) % 100) < cfg.read_pct) {
        if ((ret = cursor->search(cursor)) != WT_NOTFOUND)
            CHK(ret);
    } else {
        cursor->set_value(cursor, value);
        CHK(cursor->insert(cursor));
    }
    CHK(cursor->close(cursor));
    t2 = now_ns();
    if (first)
        hist_add(&w->first, t2 - t0);
    if (__atomic_load_n(&measuring, __ATOMIC_RELAXED)) {
        hist_add(&w->hist_a, t2 - t0);
        hist_add(&w->hist_b, t1 - t0);
    }
    __atomic_store_n(&w->ops, w->ops + 1, __ATOMIC_RELAXED);
}

/*
 * window_worker --
 *     Write to random tables of the moving window, steady mode.
 */
static void *
window_worker(void *arg)
{
    WORKER *w;
    WT_ITEM value;
    WT_SESSION *session;
    uint64_t base, next, period, r;
    bool first;
    char uri[64], buf[VALUE_SIZE];

    w = arg;
    memset(buf, 'v', sizeof(buf));
    value.data = buf;
    value.size = sizeof(buf);
    period = cfg.rate > 0 ? (uint64_t)(1e9 * cfg.threads / cfg.rate) : 0;
    CHK(conn->open_session(conn, NULL, NULL, &session));
    gate_wait();
    next = now_ns();
    for (first = true; !__atomic_load_n(&stop, __ATOMIC_RELAXED); first = false) {
        pace(w, &next, period);
        base = (uint64_t)((double)(now_ns() - start_ns) / 1e9 * cfg.speed);
        r = rng_next(&w->rng);
        table_uri(uri, sizeof(uri), "c", (base + r % (uint64_t)cfg.window) % (uint64_t)cfg.tables);
        table_op(w, session, uri, r, &value, first);
    }
    CHK(session->close(session, NULL));
    return (NULL);
}

/*
 * hot_worker --
 *     Run operations on random hot tables, restart mode.
 */
static void *
hot_worker(void *arg)
{
    WORKER *w;
    WT_ITEM value;
    WT_SESSION *session;
    uint64_t next, period, r;
    bool first;
    char uri[64], buf[VALUE_SIZE];

    w = arg;
    memset(buf, 'v', sizeof(buf));
    value.data = buf;
    value.size = sizeof(buf);
    period = cfg.rate > 0 ? (uint64_t)(1e9 * cfg.threads / cfg.rate) : 0;
    CHK(conn->open_session(conn, NULL, NULL, &session));
    gate_wait();
    next = now_ns();
    for (first = true; !__atomic_load_n(&stop, __ATOMIC_RELAXED); first = false) {
        pace(w, &next, period);
        r = rng_next(&w->rng);
        /* Spread the hot tables across the table space. */
        table_uri(uri, sizeof(uri), "c",
          (r % (uint64_t)cfg.hot) * ((uint64_t)cfg.tables / (uint64_t)cfg.hot));
        table_op(w, session, uri, r, &value, first);
    }
    CHK(session->close(session, NULL));
    return (NULL);
}

/*
 * load_worker --
 *     Create a table, insert into it and repeat until the table count is reached, load mode.
 */
static void *
load_worker(void *arg)
{
    WORKER *w;
    WT_CURSOR *cursor;
    WT_ITEM value;
    WT_SESSION *session;
    uint64_t n, t0, t1;
    int row;
    char uri[64], buf[VALUE_SIZE];

    w = arg;
    memset(buf, 'v', sizeof(buf));
    value.data = buf;
    value.size = sizeof(buf);
    CHK(conn->open_session(conn, NULL, NULL, &session));
    gate_wait();
    while (!__atomic_load_n(&stop, __ATOMIC_RELAXED) &&
      (n = __atomic_fetch_add(&next_table, 1, __ATOMIC_RELAXED)) < (uint64_t)cfg.tables) {
        table_uri(uri, sizeof(uri), "l", n);
        t0 = now_ns();
        CHK(session->create(session, uri, "key_format=q,value_format=u"));
        t1 = now_ns();
        hist_add(&w->hist_a, t1 - t0);
        if (w->first.count == 0)
            hist_add(&w->first, t1 - t0);
        t0 = now_ns();
        CHK(session->open_cursor(session, uri, NULL, NULL, &cursor));
        t1 = now_ns();
        hist_add(&w->hist_b, t1 - t0);
        for (row = 0; row < cfg.rows; ++row) {
            cursor->set_key(cursor, (int64_t)row);
            cursor->set_value(cursor, &value);
            t0 = now_ns();
            CHK(cursor->insert(cursor));
            t1 = now_ns();
            hist_add(&w->hist_c, t1 - t0);
        }
        CHK(cursor->close(cursor));
        __atomic_store_n(&w->ops, w->ops + 1, __ATOMIC_RELAXED);
    }
    w->end_ns = now_ns();
    (void)__atomic_add_fetch(&workers_done, 1, __ATOMIC_RELAXED);
    CHK(session->close(session, NULL));
    return (NULL);
}

/*
 * create_drop --
 *     Create and drop a table in turn at a fixed rate of schema operations, steady mode.
 */
static void *
create_drop(void *arg)
{
    WORKER w;
    WT_SESSION *session;
    uint64_t n, next, period, t0;

    (void)arg;
    memset(&w, 0, sizeof(w));
    period = (uint64_t)(1e9 / cfg.cd_rate);
    CHK(conn->open_session(conn, NULL, NULL, &session));
    /* Remove a table left by an interrupted run of the same database copy. */
    (void)session->drop(session, "table:cd", "force=true");
    gate_wait();
    next = now_ns();
    for (n = 0; !__atomic_load_n(&stop, __ATOMIC_RELAXED); ++n) {
        pace(&w, &next, period);
        if (__atomic_load_n(&stop, __ATOMIC_RELAXED))
            break;
        t0 = now_ns();
        if (n % 2 == 0)
            CHK(session->create(session, "table:cd", "key_format=q,value_format=u"));
        else
            CHK(session->drop(session, "table:cd", NULL));
        if (__atomic_load_n(&measuring, __ATOMIC_RELAXED))
            hist_add(&cd_hist, now_ns() - t0);
    }
    CHK(session->close(session, NULL));
    return (NULL);
}

/*
 * checkpointer --
 *     Checkpoint a fixed time after the previous checkpoint ends.
 */
static void *
checkpointer(void *arg)
{
    WT_SESSION *session;
    uint64_t t0, t1;

    (void)arg;
    CHK(conn->open_session(conn, NULL, NULL, &session));
    gate_wait();
    for (;;) {
        sleep_until(now_ns() + (uint64_t)cfg.ckpt_wait * NSEC_PER_SEC);
        if (__atomic_load_n(&stop, __ATOMIC_RELAXED))
            break;
        t0 = now_ns();
        CHK(session->checkpoint(session, NULL));
        t1 = now_ns();
        printf("checkpoint t=%.1f duration_ms=%.1f\n", (double)(t0 - start_ns) / 1e9,
          (double)(t1 - t0) / 1e6);
        (void)fflush(stdout);
        if (__atomic_load_n(&measuring, __ATOMIC_RELAXED)) {
            ++ckpt_count;
            if (t1 - t0 > ckpt_max_ns)
                ckpt_max_ns = t1 - t0;
        }
    }
    CHK(session->close(session, NULL));
    return (NULL);
}

/*
 * conn_open --
 *     Open the connection with the benchmark configuration; return the time it took.
 */
static uint64_t
conn_open(bool create)
{
    uint64_t t0;

    (void)snprintf(cfg.conn_config, sizeof(cfg.conn_config),
      "%scache_size=%dMB,session_max=1000,eviction=(threads_min=4,threads_max=4),"
      "cache_cursors=false,log=(enabled=true,remove=true),transaction_sync=(enabled=false),"
      "statistics=(fast),file_manager=(close_idle_time=%d,close_scan_interval=10,"
      "close_handle_minimum=2000)%s",
      create ? "create," : "", cfg.cache_mb, cfg.idle, cfg.direct_io ? ",direct_io=(data)" : "");
    printf("config %s\n", cfg.conn_config);
    (void)fflush(stdout);
    t0 = now_ns();
    CHK(wiredtiger_open(cfg.home, NULL, cfg.conn_config, &conn));
    return (now_ns() - t0);
}

/*
 * create_tables --
 *     Create the tables of the steady and restart modes and close the connection. Logging is on so
 *     that a create does not checkpoint the metadata, which costs more as the table count grows.
 */
static void
create_tables(void)
{
    WT_CURSOR *cursor;
    WT_ITEM value;
    WT_SESSION *session;
    uint64_t t0;
    int i, row;
    char config[256], uri[64], buf[VALUE_SIZE];

    memset(buf, 'v', sizeof(buf));
    value.data = buf;
    value.size = sizeof(buf);
    (void)snprintf(config, sizeof(config),
      "create,cache_size=%dMB,checkpoint_sync=false,log=(enabled=true,remove=true),"
      "transaction_sync=(enabled=false)",
      cfg.cache_mb);
    t0 = now_ns();
    CHK(wiredtiger_open(cfg.home, NULL, config, &conn));
    CHK(conn->open_session(conn, NULL, NULL, &session));
    for (i = 0; i < cfg.tables; ++i) {
        table_uri(uri, sizeof(uri), "c", (uint64_t)i);
        CHK(session->create(session, uri, "key_format=q,value_format=u"));
        CHK(session->open_cursor(session, uri, NULL, NULL, &cursor));
        for (row = 0; row < cfg.rows; ++row) {
            cursor->set_key(cursor, (int64_t)row);
            cursor->set_value(cursor, &value);
            CHK(cursor->insert(cursor));
        }
        CHK(cursor->close(cursor));
        if ((i + 1) % 10000 == 0) {
            CHK(session->checkpoint(session, NULL));
            printf("created %d tables, %.1f s\n", i + 1, (double)(now_ns() - t0) / 1e9);
            (void)fflush(stdout);
        }
    }
    CHK(session->checkpoint(session, NULL));
    CHK(conn->close(conn, NULL));
    printf("RESULT {\"mode\":\"create\",\"tables\":%d,\"rows\":%d,\"seconds\":%.1f}\n", cfg.tables,
      cfg.rows, (double)(now_ns() - t0) / 1e9);
}

/*
 * run --
 *     Run the steady, restart or load workload and print the result line.
 */
static void
run(void)
{
    JSON j;
    LAT_HIST first, hist_a, hist_b, hist_c;
    STATS after, before;
    WT_SESSION *session;
    pthread_t cd_tid, ckpt_tid;
    double cpu_sys, cpu_sys0, cpu_user, cpu_user0, elapsed;
    int64_t ivcsw, ivcsw0, vcsw, vcsw0;
    uint64_t end_ns, lag_drops, last_ops, measure_ns, open_ns, ops, report_ns, stop_ns, t;
    int i, nthreads;
    void *(*func)(void *);
    bool load, restart, steady;

    steady = strcmp(cfg.mode, "steady") == 0;
    restart = strcmp(cfg.mode, "restart") == 0;
    load = strcmp(cfg.mode, "load") == 0;
    memset(&cd_tid, 0, sizeof(cd_tid));
    memset(&ckpt_tid, 0, sizeof(ckpt_tid));

    open_ns = conn_open(load);
    CHK(conn->open_session(conn, NULL, NULL, &session));
    stats_read(session, &before);

    func = steady ? window_worker : (restart ? hot_worker : load_worker);
    if ((workers = calloc((size_t)cfg.threads, sizeof(WORKER))) == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    for (i = 0; i < cfg.threads; ++i) {
        workers[i].id = i;
        workers[i].rng = cfg.seed + (uint64_t)(i + 1) * UINT64_C(0x9E3779B97F4A7C15);
        CHK(pthread_create(&workers[i].tid, NULL, func, &workers[i]));
    }
    nthreads = cfg.threads;
    if (cfg.ckpt_wait > 0) {
        CHK(pthread_create(&ckpt_tid, NULL, checkpointer, NULL));
        ++nthreads;
    }
    if (steady && cfg.cd_rate > 0) {
        CHK(pthread_create(&cd_tid, NULL, create_drop, NULL));
        ++nthreads;
    }

    /* Restart and load modes measure from the first operation. */
    if (cfg.skip == 0)
        __atomic_store_n(&measuring, true, __ATOMIC_RELAXED);
    cpu_seconds(&cpu_user0, &cpu_sys0, &vcsw0, &ivcsw0);
    start_ns = now_ns();
    measure_ns = start_ns;
    gate_release(nthreads);

    /*
     * Poll often so that a load run ends as soon as its workers finish; print progress every 10
     * seconds.
     */
    end_ns = start_ns + (uint64_t)cfg.seconds * NSEC_PER_SEC;
    for (last_ops = 0, report_ns = start_ns + 10 * NSEC_PER_SEC;;) {
        sleep_ns(100 * 1000000);
        t = now_ns();
        if (cfg.skip > 0 && !__atomic_load_n(&measuring, __ATOMIC_RELAXED) &&
          t >= start_ns + (uint64_t)cfg.skip * NSEC_PER_SEC) {
            stats_read(session, &before);
            cpu_seconds(&cpu_user0, &cpu_sys0, &vcsw0, &ivcsw0);
            measure_ns = now_ns();
            __atomic_store_n(&measuring, true, __ATOMIC_RELAXED);
        }
        if (t >= report_ns) {
            for (ops = 0, i = 0; i < cfg.threads; ++i)
                ops += __atomic_load_n(&workers[i].ops, __ATOMIC_RELAXED);
            printf("progress t=%" PRIu64 " ops=%" PRIu64 " ops_per_s=%.0f\n",
              (t - start_ns) / NSEC_PER_SEC, ops, (double)(ops - last_ops) / 10);
            (void)fflush(stdout);
            last_ops = ops;
            report_ns += 10 * NSEC_PER_SEC;
        }
        if (t >= end_ns || __atomic_load_n(&workers_done, __ATOMIC_RELAXED) == cfg.threads)
            break;
    }
    stop_ns = now_ns();
    __atomic_store_n(&stop, true, __ATOMIC_RELAXED);

    for (i = 0; i < cfg.threads; ++i)
        CHK(pthread_join(workers[i].tid, NULL));
    cpu_seconds(&cpu_user, &cpu_sys, &vcsw, &ivcsw);
    __atomic_store_n(&measuring, false, __ATOMIC_RELAXED);
    if (cfg.ckpt_wait > 0)
        CHK(pthread_join(ckpt_tid, NULL));
    if (steady && cfg.cd_rate > 0)
        CHK(pthread_join(cd_tid, NULL));
    stats_read(session, &after);

    memset(&first, 0, sizeof(first));
    memset(&hist_a, 0, sizeof(hist_a));
    memset(&hist_b, 0, sizeof(hist_b));
    memset(&hist_c, 0, sizeof(hist_c));
    for (lag_drops = 0, i = 0; i < cfg.threads; ++i) {
        hist_merge(&first, &workers[i].first);
        hist_merge(&hist_a, &workers[i].hist_a);
        hist_merge(&hist_b, &workers[i].hist_b);
        hist_merge(&hist_c, &workers[i].hist_c);
        lag_drops += workers[i].lag_drops;
    }
    /* A load run measures until its last worker finished. */
    if (load)
        for (stop_ns = measure_ns, i = 0; i < cfg.threads; ++i)
            if (workers[i].end_ns > stop_ns)
                stop_ns = workers[i].end_ns;
    elapsed = (double)(stop_ns - measure_ns) / 1e9;

    memset(&j, 0, sizeof(j));
    j.buf[0] = '{';
    j.len = 1;
    json_add(&j, "mode", "\"%s\"", cfg.mode);
    json_add(&j, "wt_version", "\"%s\"", wiredtiger_version(NULL, NULL, NULL));
    json_add(&j, "schema_lock", "\"%s\"", BENCH_SCHEMA_LOCK);
    json_add(&j, "conn_config", "\"%s\"", cfg.conn_config);
    json_add(&j, "tables", "%d", cfg.tables);
    json_add(&j, "threads", "%d", cfg.threads);
    json_add(&j, "rate", "%.0f", cfg.rate);
    json_add(&j, "seconds", "%d", cfg.seconds);
    json_add(&j, "skip", "%d", cfg.skip);
    json_add(&j, "seed", "%" PRIu64, cfg.seed);
    if (steady) {
        json_add(&j, "window", "%d", cfg.window);
        json_add(&j, "speed", "%.1f", cfg.speed);
        json_add(&j, "cd_rate", "%.1f", cfg.cd_rate);
    }
    if (restart)
        json_add(&j, "hot", "%d", cfg.hot);
    if (load)
        json_add(&j, "rows", "%d", cfg.rows);
    else {
        json_add(&j, "keys", "%d", cfg.keys);
        json_add(&j, "read_pct", "%d", cfg.read_pct);
    }
    json_add(&j, "conn_open_ms", "%.1f", (double)open_ns / 1e6);
    json_add(&j, "measured_s", "%.2f", elapsed);
    json_add(&j, "ops_per_s", "%.1f", elapsed > 0 ? (double)hist_a.count / elapsed : 0);
    if (load) {
        json_hist(&j, "create", &hist_a);
        json_hist(&j, "open", &hist_b);
        json_hist(&j, "insert", &hist_c);
        json_add(&j, "inserts_per_s", "%.1f", elapsed > 0 ? (double)hist_c.count / elapsed : 0);
    } else {
        json_hist(&j, "op", &hist_a);
        json_hist(&j, "open", &hist_b);
        json_add(&j, "lag_drops", "%" PRIu64, lag_drops);
    }
    json_hist(&j, "first", &first);
    if (steady && cfg.cd_rate > 0)
        json_hist(&j, "cd", &cd_hist);
    json_add(&j, "ckpt_count", "%" PRIu64, ckpt_count);
    json_add(&j, "ckpt_max_ms", "%.1f", (double)ckpt_max_ns / 1e6);
    json_add(&j, "cpu_user_s", "%.2f", cpu_user - cpu_user0);
    json_add(&j, "cpu_sys_s", "%.2f", cpu_sys - cpu_sys0);
    json_add(&j, "cpu_total_user_s", "%.2f", cpu_user);
    json_add(&j, "cpu_total_sys_s", "%.2f", cpu_sys);
    json_add(&j, "vcsw", "%" PRId64, vcsw - vcsw0);
    json_add(&j, "ivcsw", "%" PRId64, ivcsw - ivcsw0);
    json_stats(&j, &before, &after);

    CHK(session->close(session, NULL));
    t = now_ns();
    CHK(conn->close(conn, NULL));
    json_add(&j, "conn_close_ms", "%.1f", (double)(now_ns() - t) / 1e6);
    printf("RESULT %s}\n", j.buf);
    free(workers);
}

/*
 * usage --
 *     Display a usage statement and exit failure.
 */
static void
usage(void)
{
    fprintf(stderr,
      "usage: bench_dhandle_lock create|steady|restart|load HOME [key=value ...]\n"
      "  keys (defaults per mode in brackets):\n"
      "    tables=N      tables [200000; load 50000]\n"
      "    rows=N        rows per table, create and load modes [create 10; load 1]\n"
      "    threads=N     worker threads [steady 32; restart, load 64]\n"
      "    rate=R        operations per second in total, 0: no limit [steady 100000; restart "
      "100000]\n"
      "    seconds=S     run time; load mode stops early when done [steady 1300; restart 60; "
      "load 3600]\n"
      "    skip=S        seconds before the measured interval starts [steady 700; others 0]\n"
      "    window=N      active tables, steady mode [4000]\n"
      "    speed=X       tables the window moves per second, steady mode [50]\n"
      "    cd_rate=X     create/drop operations per second, steady mode [0]\n"
      "    hot=N         hot tables, restart mode [10]\n"
      "    keys=N        key range per table [100]\n"
      "    read_pct=P    percentage of searches, steady and restart modes [0]\n"
      "    ckpt_wait=S   seconds from the end of a checkpoint to the next, 0: none [60]\n"
      "    cache_mb=M    cache size [8192]\n"
      "    direct_io=0|1 direct_io=(data) [1]\n"
      "    idle=S        file_manager close_idle_time [600]\n"
      "    seed=N        random seed [1]\n");
    exit(EXIT_FAILURE);
}

/*
 * parse_option --
 *     Parse one key=value option.
 */
static void
parse_option(char *arg)
{
    char *v;

    if ((v = strchr(arg, '=')) == NULL)
        usage();
    *v++ = '\0';
    if (strcmp(arg, "tables") == 0)
        cfg.tables = atoi(v);
    else if (strcmp(arg, "rows") == 0)
        cfg.rows = atoi(v);
    else if (strcmp(arg, "threads") == 0)
        cfg.threads = atoi(v);
    else if (strcmp(arg, "rate") == 0)
        cfg.rate = atof(v);
    else if (strcmp(arg, "seconds") == 0)
        cfg.seconds = atoi(v);
    else if (strcmp(arg, "skip") == 0)
        cfg.skip = atoi(v);
    else if (strcmp(arg, "window") == 0)
        cfg.window = atoi(v);
    else if (strcmp(arg, "speed") == 0)
        cfg.speed = atof(v);
    else if (strcmp(arg, "cd_rate") == 0)
        cfg.cd_rate = atof(v);
    else if (strcmp(arg, "hot") == 0)
        cfg.hot = atoi(v);
    else if (strcmp(arg, "keys") == 0)
        cfg.keys = atoi(v);
    else if (strcmp(arg, "read_pct") == 0)
        cfg.read_pct = atoi(v);
    else if (strcmp(arg, "ckpt_wait") == 0)
        cfg.ckpt_wait = atoi(v);
    else if (strcmp(arg, "cache_mb") == 0)
        cfg.cache_mb = atoi(v);
    else if (strcmp(arg, "direct_io") == 0)
        cfg.direct_io = atoi(v);
    else if (strcmp(arg, "idle") == 0)
        cfg.idle = atoi(v);
    else if (strcmp(arg, "seed") == 0)
        cfg.seed = strtoull(v, NULL, 10);
    else
        usage();
}

/*
 * main --
 *     Parse the mode and options and run the benchmark.
 */
int
main(int argc, char *argv[])
{
    int i;
    bool load, restart, steady;

    if (argc < 3)
        usage();
    cfg.mode = argv[1];
    cfg.home = argv[2];
    steady = strcmp(cfg.mode, "steady") == 0;
    restart = strcmp(cfg.mode, "restart") == 0;
    load = strcmp(cfg.mode, "load") == 0;
    if (!steady && !restart && !load && strcmp(cfg.mode, "create") != 0)
        usage();

    cfg.tables = load ? 50000 : 200000;
    cfg.rows = load ? 1 : 10;
    cfg.threads = steady ? 32 : 64;
    cfg.rate = load ? 0 : 100000;
    cfg.seconds = steady ? 1300 : (restart ? 60 : 3600);
    cfg.skip = steady ? 700 : 0;
    cfg.window = 4000;
    cfg.speed = 50;
    cfg.hot = 10;
    cfg.keys = 100;
    cfg.ckpt_wait = 60;
    cfg.cache_mb = 8192;
    cfg.direct_io = 1;
    cfg.idle = 600;
    cfg.seed = 1;
    for (i = 3; i < argc; ++i)
        parse_option(argv[i]);
    if (cfg.tables <= 0 || cfg.threads <= 0 || cfg.keys <= 0 || cfg.window <= 0 || cfg.hot <= 0 ||
      cfg.hot > cfg.tables || cfg.skip >= cfg.seconds) {
        fprintf(stderr, "invalid configuration\n");
        return (EXIT_FAILURE);
    }

    if (strcmp(cfg.mode, "create") == 0)
        create_tables();
    else
        run();
    return (EXIT_SUCCESS);
}
