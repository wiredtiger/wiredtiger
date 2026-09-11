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
#include <math.h>
#include <queue.h>
#include "test_util.h"

#if defined(__linux__)
#include <dlfcn.h>
#include <malloc.h>
#endif
#include "bench_timer.h"

/* These are the test_util options that we want: preserve, number of threads and home directory. */
#define SHARED_PARSE_OPTIONS "GpT:h:"

/*
 * This workload is dynamic. The benchmark steps through a series of plateaus, each a fixed number
 * of tables (plateau_step) apart: 0, plateau_step, 2*plateau_step, ... up to table_count. At each
 * plateau the queuer first performs a fast transition (creating and dropping needed tables) to
 * reach that plateau's starting state, then runs a metered measurement window that grows the
 * existing-table count by measure_count while continuously searching and inserting across the
 * active range.
 *
 * The queuer is the sole source of work and the sole user of the RNG. It places work items on a
 * queue; each item is a small batch of actions (search, insert, create, drop) that the worker
 * threads execute. Determining the work predictably in one place keeps the door open for replaying
 * the same stream against a follower (see the predictable-replay model in test/format).
 *
 * Counts in the configuration are numbers of existing tables; internally the workload tracks table
 * numbers (which include dropped tables) with three markers:
 *   high   - one past the highest table slot in the current plateau range
 *   exists - lowest existing table number; tables [exists, high) exist, [0, exists) are dropped
 *   low    - lowest active table number; tables [low, high) are actively read/written
 * Tables in [exists, low) exist but are not actively used. They are drop candidates (dropped from
 * the bottom as the population grows) and they exercise the dhandle sweep server, which retires
 * inactive handles.
 */

/* Throttle the queuer when the work queue grows beyond this many items. */
#define QUEUE_THROTTLE_LEN 8192

/* Number of table creates whose initial inserts are batched into one transition transaction. */
#define TRANSITION_BATCH 1000

/* Leave this many seconds at the start of a measurement window free of checkpoints. */
#define CHECKPOINT_HOLDOFF_SECS 10

/* Give up on a plateau measurement that has not finished within this many seconds. */
#define PLATEAU_TIME_LIMIT_SECS (10 * 60)

typedef enum { ACTION_SEARCH, ACTION_INSERT, ACTION_CREATE, ACTION_DROP } ACTION_TYPE;

typedef struct {
    ACTION_TYPE type;
    int tablenum;
} ACTION;

typedef struct work_item {
    uint64_t timestamp; /* commit timestamp shared by all writes in this item */
    int nactions;
    ACTION *actions;
    TAILQ_ENTRY(work_item) q;
} WORK_ITEM;

typedef struct __thread_args THREAD_ARGS;

/* A checkpoint a follower will pick up, parsed from the leader's checkpoint log. */
typedef struct {
    uint64_t timestamp;
    char *metadata;
} CKPT_ENTRY;

typedef struct {
    TEST_OPTS opts;
    struct {
        /* Counts below are numbers of existing (not-yet-dropped) tables. */
        int table_count;   /* existing tables at the final plateau; default 50000 */
        int plateau_step;  /* existing tables between plateaus; default 10000 */
        int measure_count; /* existing tables grown during a plateau measurement; default 1000 */

        /* Fractions are in [0,1]; -1 means "unset" so a real 0 can be configured. */
        double
          table_in_use_fraction; /* of existing tables, fraction actively queued; default 0.9 */
        double table_dropped_fraction; /* of all table numbers, fraction dropped; default 0.1 */
        double insert_fraction; /* of non-create actions, fraction that insert; default 0.5 */
        double create_fraction; /* of all actions, fraction that create; default 0.0001 */

        int actions_per_work_item; /* actions bundled into one work item; default 10 */
        int checkpoint_interval;   /* seconds between checkpoints; default 30 (ASC) or 10 (DSC) */
        uint64_t seed;             /* RNG seed; must match leader/follower; default 1 */
        const char *conn_config;   /* extra wiredtiger_open config, appended to the base */
    } config;

    WT_CONNECTION *conn;

    bool is_follower; /* running as a follower: replay the stream and pick up checkpoints */
    bool done;
    bool started;
    bool checkpointing;
    bool in_transition; /* racing to the next plateau: checkpoints and pickups are deferred */
    bool measuring;     /* workers record timing only while a measurement window is active */

    CKPT_ENTRY *ckpts; /* follower: checkpoints to pick up, in timestamp order */
    int nckpts;

    int checkpoint_num;
    uint64_t queue_timestamp; /* monotone per-work-item timestamp, assigned by the queuer */
    uint64_t last_stable;     /* most recent stable timestamp set by the checkpointer */

    THREAD_ARGS *worker_args; /* the worker threads' args, for reading their commit timestamps */
    int nworkers;

    int plateau; /* current plateau start point, for reporting */
    int high;    /* high point for active tables */
    int low;     /* low point for active tables */
    int exists;  /* lowest existing table number */

    /* Locked */
    TAILQ_HEAD(work_queue, work_item) work_queue;
    int work_queue_len;
    pthread_rwlock_t work_queue_lock;
} SHARED;

struct __thread_args {
    SHARED *shared;
    int threadnum;

    uint64_t
      commit_ts; /* this thread's last-used commit timestamp, published for the checkpointer */

    BENCH_TIMER t_create;
    BENCH_TIMER t_checkpoint;
    BENCH_TIMER t_drop;
    BENCH_TIMER t_first_insert;
    BENCH_TIMER t_search;
    BENCH_TIMER t_insert;
};

/* Constants and variables declaration. */
static const char table_config[] = "leaf_page_max=64KB,key_format=i,value_format=i";

/*
 * Match MongoDB, which drops with checkpoint_wait=false so a drop does not take the checkpoint lock
 * and block for the duration of a running checkpoint (lock_wait stays default, so the schema lock
 * is still held).
 */
static const char drop_config[] = "checkpoint_wait=false";

#define BENCH_DHANDLE_CONN_BASE \
    "create,cache_size=2GB,statistics=(all),statistics_log=(json,on_close,wait=1)"

/*
 * Build a table URI into buf. Uses "layered:" prefix in DSC mode, "table:" otherwise.
 */
#define TNAME(buf, shared, n) \
    snprintf(buf, sizeof(buf), "%st%d", (shared)->opts.disagg.is_enabled ? "layered:" : "table:", n)

extern char *__wt_optarg;

/* Forward declarations. */
static void bench_dhandle(SHARED *);
static void bench_dhandle_run(SHARED *);
static void checkpoint_log_header(FILE *, SHARED *);
static void *checkpointer(void *);
static uint64_t compute_stable(SHARED *);
static void follower_apply_config(SHARED *, const char *key, const char *val);
static void follower_read_file(SHARED *);
static void remove_kv_symlink(SHARED *);
static void setup_directories(SHARED *);
static int high_for_existing(SHARED *, int existing);
static int plateau_exists(SHARED *, int high);
static int plateau_low(SHARED *, int high, int exists);
static void queue_append(SHARED *, WORK_ITEM **, ACTION_TYPE, int tablenum);
static void queue_flush(SHARED *, WORK_ITEM **);
static void queue_wait_drain(SHARED *);
static void *queuer(void *);
static double rand_double(WT_RAND_STATE *);
static void measure_plateau(SHARED *, WT_RAND_STATE *, int existing_plateau);
static void transition_to_plateau(SHARED *, WT_SESSION *, int plateau);
static void shuffle(int *arr, int n, WT_RAND_STATE *rnd);
static void *worker(void *);

#define WITH_RW_LOCK(lock, e)              \
    do {                                   \
        (void)pthread_rwlock_wrlock(lock); \
        e;                                 \
        (void)pthread_rwlock_unlock(lock); \
    } while (0)

/* Time a statement into a thread-local timer only while a measurement window is active. */
#define MAYBE_TIME(do_measure, timer, session, stmt)     \
    do {                                                 \
        if (do_measure)                                  \
            BENCH_TIME_CUMULATIVE(timer, session, stmt); \
        else                                             \
            stmt;                                        \
    } while (0)

/*
 * usage --
 *     Display usage statement and exit failure.
 */
static int
usage(void)
{
    fprintf(stderr,
      "usage: %s\n"
      "    [-G] [-p] [-T threads] [-h home] [-R role]\n"
      "    [-n table_count] [-P plateau_step] [-m measure_count]\n"
      "    [-u in_use_fraction] [-d dropped_fraction] [-i insert_fraction] [-c create_fraction]\n"
      "    [-w actions_per_work_item] [-k checkpoint_interval] [-s seed] [-C conn_config]\n",
      progname);
    fprintf(stderr, "%s",
      "\t-G  enable disaggregated storage (DSC) mode with layered tables\n"
      "\t-R  disaggregated role: leader or follower (either implies -G)\n"
      "\t-h  set a database home directory\n"
      "\t-p  preserve home directory\n"
      "\t-T  set number of worker threads\n"
      "\t-n  existing tables at the final plateau (default 50000)\n"
      "\t-P  existing tables between plateaus (default 10000)\n"
      "\t-m  existing tables grown during a plateau measurement (default 1000)\n"
      "\t-u  fraction of existing tables actively queued (default 0.9)\n"
      "\t-d  fraction of all table numbers that are dropped (default 0.1)\n"
      "\t-i  fraction of non-create actions that are inserts (default 0.5)\n"
      "\t-c  fraction of all actions that are creates (default 0.0001)\n"
      "\t-w  actions bundled into one work item (default 10)\n"
      "\t-k  seconds between checkpoints (default 30 ASC, 10 DSC)\n"
      "\t-s  RNG seed; must match between leader and follower (default 1)\n"
      "\t-C  extra wiredtiger_open config, appended to the base (e.g. for a follower,\n"
      "\t    'disaggregated=(checkpoint_pickup_defer_period=17)')\n");
    return (EXIT_FAILURE);
}

/*
 * main --
 *     Parse options, run workload and clean up.
 */
int
main(int argc, char *argv[])
{
    SHARED shared;
    size_t len;
    int ch;
    const char *role;
    char *p;

    memset(&shared, 0, sizeof(shared));
    role = NULL;
    /* Fraction fields use -1 as "unset" so that a configured 0.0 is distinguishable. */
    shared.config.table_in_use_fraction = -1.0;
    shared.config.table_dropped_fraction = -1.0;
    shared.config.insert_fraction = -1.0;
    shared.config.create_fraction = -1.0;

    __wt_stream_set_line_buffer(stdout);

    (void)testutil_set_progname(argv);
    testutil_parse_begin_opt(argc, argv, SHARED_PARSE_OPTIONS, &shared.opts);

    while ((ch = __wt_getopt(
              progname, argc, argv, "c:C:d:i:k:m:n:P:R:s:u:w:" SHARED_PARSE_OPTIONS)) != EOF)
        switch (ch) {
        case 'c':
            shared.config.create_fraction = atof(__wt_optarg);
            break;

        case 'R':
            role = __wt_optarg;
            break;

        case 's':
            shared.config.seed = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;

        case 'C':
            shared.config.conn_config = __wt_optarg;
            break;

        case 'k':
            shared.config.checkpoint_interval = atoi(__wt_optarg);
            break;

        case 'd':
            shared.config.table_dropped_fraction = atof(__wt_optarg);
            break;

        case 'i':
            shared.config.insert_fraction = atof(__wt_optarg);
            break;

        case 'm':
            shared.config.measure_count = atoi(__wt_optarg);
            break;

        case 'n':
            shared.config.table_count = atoi(__wt_optarg);
            break;

        case 'P':
            shared.config.plateau_step = atoi(__wt_optarg);
            break;

        case 'u':
            shared.config.table_in_use_fraction = atof(__wt_optarg);
            break;

        case 'w':
            shared.config.actions_per_work_item = atoi(__wt_optarg);
            break;

        default:
            /* The option is either one that we're asking testutil to support, or illegal. */
            if (testutil_parse_single_opt(&shared.opts, ch) != 0)
                return (usage());
        }

    if (role != NULL) {
        /* A role implies disaggregated storage; turn it on if -G was not also given. */
        if (!shared.opts.disagg.is_enabled)
            (void)testutil_parse_single_opt(&shared.opts, 'G');
        if (strcmp(role, "follower") == 0) {
            shared.is_follower = true;
            shared.opts.disagg.mode = "follower";
            /* Default the follower home to a "follower" subdirectory of the leader's home. */
            if (shared.opts.home == NULL) {
                len = strlen("WT_TEST.") + strlen(progname) + strlen("/follower") + 1;
                p = dmalloc(len);
                testutil_snprintf(p, len, "WT_TEST.%s/follower", progname);
                shared.opts.home = p;
            }
        } else
            testutil_assertfmt(strcmp(role, "leader") == 0, "unknown role '%s'", role);
    }

    testutil_parse_end_opt(&shared.opts);

    /*
     * palite places its store (and we place the checkpoint log) in a kv_home subdirectory that it
     * creates under the page-log home, so the page-log home is just the WT home.
     */
    if (shared.opts.disagg.is_enabled)
        shared.opts.disagg.page_log_home = shared.opts.home;

    /* Create the directory layout (and the follower's symlink) before reading the checkpoint log.
     */
    setup_directories(&shared);

    /* A follower deduces its configuration from the leader's checkpoint log before applying
     * defaults. */
    if (shared.is_follower)
        follower_read_file(&shared);

    /* Set defaults. */
    if (shared.opts.nthreads == 0)
        shared.opts.nthreads = 10;
    if (shared.config.table_count == 0)
        shared.config.table_count = 50000;
    if (shared.config.plateau_step == 0)
        shared.config.plateau_step = 10000;
    if (shared.config.measure_count == 0)
        shared.config.measure_count = 1000;
    if (shared.config.table_in_use_fraction < 0)
        shared.config.table_in_use_fraction = 0.9;
    if (shared.config.table_dropped_fraction < 0)
        shared.config.table_dropped_fraction = 0.1;
    if (shared.config.insert_fraction < 0)
        shared.config.insert_fraction = 0.5;
    if (shared.config.create_fraction < 0)
        shared.config.create_fraction = 0.0001;
    if (shared.config.actions_per_work_item == 0)
        shared.config.actions_per_work_item = 10;
    if (shared.config.checkpoint_interval == 0)
        shared.config.checkpoint_interval = shared.opts.disagg.is_enabled ? 10 : 30;
    if (shared.config.seed == 0)
        shared.config.seed = 1;

    bench_dhandle(&shared);

    testutil_cleanup(&shared.opts);

    return (EXIT_SUCCESS);
}

/*
 * bench_dhandle --
 *     Set up and initialization to do the benchmark run.
 */
static void
bench_dhandle(SHARED *shared)
{
    char *home = shared->opts.home;
    char conn_config[1024];

    testutil_snprintf(conn_config, sizeof(conn_config), "%s%s%s", BENCH_DHANDLE_CONN_BASE,
      shared->config.conn_config == NULL ? "" : ",",
      shared->config.conn_config == NULL ? "" : shared->config.conn_config);

    testutil_wiredtiger_open(&shared->opts, home, conn_config, NULL, &shared->conn, false, true);

    bench_dhandle_run(shared);

    /*
     * Remove the kv_home symlink before deleting the home: the recursive remove cannot descend a
     * symlink and would otherwise fail. This never touches the shared page log it points to.
     */
    if (!shared->opts.preserve) {
        remove_kv_symlink(shared);
        testutil_remove(home);
    }
}

/*
 * remove_kv_symlink --
 *     Remove the kv_home symlink (a leader's is under its follower subdirectory, a follower's is in
 *     its own home), so a recursive directory remove does not choke on it.
 */
static void
remove_kv_symlink(SHARED *shared)
{
    char path[1024];

    if (!shared->opts.disagg.is_enabled)
        return;
    if (shared->is_follower)
        testutil_snprintf(path, sizeof(path), "%s/kv_home", shared->opts.home);
    else
        testutil_snprintf(path, sizeof(path), "%s/follower/kv_home", shared->opts.home);
    (void)unlink(path);
}

/*
 * setup_directories --
 *     Create the directory layout. In disaggregated mode the page log lives in a kv_home
 *     subdirectory of the WT home, matching test/format and test/suite. A leader creates kv_home
 *     and a follower subdirectory holding a symlink to it; a follower recreates its own home and a
 *     symlink to the leader's shared kv_home, leaving the leader's page log untouched.
 */
static void
setup_directories(SHARED *shared)
{
    char *home = shared->opts.home;
    char path[1024];

    if (!shared->opts.disagg.is_enabled) {
        testutil_recreate_dir(home);
        return;
    }

    /* Remove any stale kv_home symlink first so the recursive remove inside recreate succeeds. */
    remove_kv_symlink(shared);

    if (shared->is_follower) {
        testutil_recreate_dir(home);
        testutil_snprintf(path, sizeof(path), "%s/kv_home", home);
        testutil_assert(symlink("../kv_home", path) == 0);
        return;
    }

    testutil_recreate_dir(home);
    testutil_snprintf(path, sizeof(path), "%s/kv_home", home);
    testutil_mkdir(path);
    testutil_snprintf(path, sizeof(path), "%s/follower", home);
    testutil_mkdir(path);
    testutil_snprintf(path, sizeof(path), "%s/follower/kv_home", home);
    testutil_assert(symlink("../kv_home", path) == 0);
}

/*
 * follower_apply_config --
 *     Apply one "key=value" pair from the leader's checkpoint-log header. Values the follower did
 *     not set on the command line are taken from the leader. Values that affect the work stream
 *     ("cannot be changed") must match the leader if the follower set them; the rest may be
 *     overridden.
 */
static void
follower_apply_config(SHARED *shared, const char *key, const char *val)
{
#define MUSTMATCH_INT(name, field)                                                              \
    if (strcmp(key, name) == 0) {                                                               \
        int _v = atoi(val);                                                                     \
        if (shared->config.field == 0)                                                          \
            shared->config.field = _v;                                                          \
        else                                                                                    \
            testutil_assertfmt(shared->config.field == _v,                                      \
              "follower " name "=%d cannot differ from leader's %d", shared->config.field, _v); \
        return;                                                                                 \
    }
#define OVERRIDE_INT(name, field)             \
    if (strcmp(key, name) == 0) {             \
        if (shared->config.field == 0)        \
            shared->config.field = atoi(val); \
        return;                               \
    }
#define MUSTMATCH_DOUBLE(name, field)                                                           \
    if (strcmp(key, name) == 0) {                                                               \
        double _v = atof(val);                                                                  \
        if (shared->config.field < 0)                                                           \
            shared->config.field = _v;                                                          \
        else                                                                                    \
            testutil_assertfmt(fabs(shared->config.field - _v) < 1e-9,                          \
              "follower " name "=%g cannot differ from leader's %g", shared->config.field, _v); \
        return;                                                                                 \
    }

    MUSTMATCH_INT("table_count", table_count);
    MUSTMATCH_INT("plateau_step", plateau_step);
    MUSTMATCH_INT("measure_count", measure_count);
    MUSTMATCH_DOUBLE("table_dropped_fraction", table_dropped_fraction);
    MUSTMATCH_DOUBLE("table_in_use_fraction", table_in_use_fraction);
    MUSTMATCH_DOUBLE("insert_fraction", insert_fraction);
    MUSTMATCH_DOUBLE("create_fraction", create_fraction);
    MUSTMATCH_INT("actions_per_work_item", actions_per_work_item);
    OVERRIDE_INT("checkpoint_interval", checkpoint_interval);

    if (strcmp(key, "seed") == 0) {
        uint64_t v = (uint64_t)strtoull(val, NULL, 10);
        if (shared->config.seed == 0)
            shared->config.seed = v;
        else
            testutil_assertfmt(shared->config.seed == v,
              "follower seed=%" PRIu64 " cannot differ from leader's %" PRIu64, shared->config.seed,
              v);
        return;
    }
    if (strcmp(key, "threads") == 0) {
        if (shared->opts.nthreads == 0)
            shared->opts.nthreads = (uint64_t)strtoull(val, NULL, 10);
        return;
    }
    /* Unknown keys are ignored for forward compatibility. */
#undef MUSTMATCH_INT
#undef OVERRIDE_INT
#undef MUSTMATCH_DOUBLE
}

/*
 * follower_read_file --
 *     Read the leader's checkpoint log: deduce and validate configuration from the header line, and
 *     load the list of checkpoints to pick up.
 */
static void
follower_read_file(SHARED *shared)
{
    FILE *fp;
    uint64_t ts;
    int cap;
    char line[2048], path[1024], *meta, *mp, *tok, *tsp;
    size_t n;

    testutil_snprintf(
      path, sizeof(path), "%s/kv_home/checkpoints.txt", shared->opts.disagg.page_log_home);
    testutil_assertfmt((fp = fopen(path, "r")) != NULL, "cannot open checkpoint log %s", path);

    cap = 0;
    while (fgets(line, sizeof(line), fp) != NULL) {
        if (WT_PREFIX_MATCH(line, "config ")) {
            for (tok = strtok(line + strlen("config "), " \t\n"); tok != NULL;
              tok = strtok(NULL, " \t\n")) {
                mp = strchr(tok, '=');
                if (mp == NULL)
                    continue;
                *mp = '\0';
                follower_apply_config(shared, tok, mp + 1);
            }
        } else if (WT_PREFIX_MATCH(line, "ckpt ")) {
            tsp = strstr(line, "timestamp=");
            mp = strstr(line, "metadata=");
            testutil_assert(tsp != NULL && mp != NULL);
            ts = (uint64_t)strtoull(tsp + strlen("timestamp="), NULL, 16);
            meta = mp + strlen("metadata=");
            n = strlen(meta);
            while (n > 0 && (meta[n - 1] == '\n' || meta[n - 1] == '\r'))
                meta[--n] = '\0';
            if (shared->nckpts == cap) {
                cap = cap == 0 ? 16 : cap * 2;
                shared->ckpts = drealloc(shared->ckpts, sizeof(CKPT_ENTRY) * (size_t)cap);
            }
            shared->ckpts[shared->nckpts].timestamp = ts;
            shared->ckpts[shared->nckpts].metadata = dstrdup(meta);
            shared->nckpts++;
        }
    }
    testutil_assert(fclose(fp) == 0);
}

/* Columns shown in the timing reports, in display order. */
typedef enum { COL_CKPT, COL_CREATE, COL_DROP, COL_FIRST_INS, COL_INS, COL_SRCH, NCOLS } COL;
static const char *const col_name[NCOLS] = {"ckpt", "create", "drop", "1st_ins", "ins", "srch"};

/* A saved per-plateau timing line, replayed in the final summary. */
typedef struct {
    int tables;
    uint64_t secs;
    uint64_t nsec[NCOLS];
    uint64_t count[NCOLS];
    uint64_t rss_bytes;    /* Process resident set size */
    uint64_t malloc_bytes; /* Heap bytes in use */
} PLATEAU_SUMMARY;

/*
 * sample_memory --
 *     Sample the process resident set size and the heap bytes in use. Either is reported as zero if
 *     the platform does not expose it. The resident set is the whole-process footprint (heap,
 *     memory-mapped regions, resident file pages); the heap figure is allocator bookkeeping only,
 *     so the two answer different questions and are shown side by side.
 */
static void
sample_memory(uint64_t *rss_bytes, uint64_t *malloc_bytes)
{
    *rss_bytes = *malloc_bytes = 0;
#if defined(__linux__)
    {
        FILE *fp;
        unsigned long resident_pages;

        /* statm field 2 is the resident set in pages. */
        if ((fp = fopen("/proc/self/statm", "r")) != NULL) {
            if (fscanf(fp, "%*u %lu", &resident_pages) == 1)
                *rss_bytes = (uint64_t)resident_pages * (uint64_t)sysconf(_SC_PAGESIZE);
            (void)fclose(fp);
        }
    }
    /*
     * Allocator bytes in use. Detect the allocator at runtime rather than at compile time: a build
     * may link tcmalloc (wt-mkme -tcmalloc), whose interposed malloc makes glibc's mallinfo2 report
     * near-zero. Prefer tcmalloc's numeric-property query if its C entry point is present, then
     * fall back to glibc mallinfo2.
     */
    {
        typedef int (*get_numeric_property_t)(const char *, size_t *);
        static get_numeric_property_t tc_get = NULL;
        static bool tc_resolved = false;
        size_t bytes;

        if (!tc_resolved) {
            /* gperftools and Google tcmalloc both expose "generic.current_allocated_bytes". */
            *(void **)&tc_get = dlsym(RTLD_DEFAULT, "MallocExtension_GetNumericProperty");
            if (tc_get == NULL)
                *(void **)&tc_get =
                  dlsym(RTLD_DEFAULT, "MallocExtension_Internal_GetNumericProperty");
            tc_resolved = true;
        }
        if (tc_get != NULL && tc_get("generic.current_allocated_bytes", &bytes) != 0)
            *malloc_bytes = (uint64_t)bytes;
#if defined(__GLIBC__) && \
  (__GLIBC__ > 2 || (__GLIBC__ == 2 && defined(__GLIBC_MINOR__) && __GLIBC_MINOR__ >= 33))
        else {
            /* mallinfo2 has size_t fields, so it does not overflow past 2GB as legacy mallinfo. */
            struct mallinfo2 mi;

/* mallinfo2 returns its struct by value, which -Waggregate-return flags. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Waggregate-return"
            mi = mallinfo2();
#pragma GCC diagnostic pop
            *malloc_bytes = (uint64_t)mi.uordblks + (uint64_t)mi.hblkhd;
        }
#endif
    }
#endif
}

/*
 * report_memory --
 *     Print a memory footprint line for a plateau.
 */
static void
report_memory(uint64_t rss_bytes, uint64_t malloc_bytes)
{
    printf("%-28smem: rss=%.4g MB  malloc=%.4g MB\n", "", rss_bytes / 1e6, malloc_bytes / 1e6);
}

/*
 * gather_totals --
 *     Sum each per-thread timer across all threads into the per-column cumulative totals.
 */
static void
gather_totals(THREAD_ARGS *args, int n, uint64_t *nsec, uint64_t *count)
{
    BENCH_TIMER t[NCOLS];
    int c, i;

    for (c = 0; c < NCOLS; ++c)
        bench_timer_init(&t[c], NULL);
    for (i = 0; i < n; ++i) {
        bench_timer_add_from_shared(&t[COL_CKPT], &args[i].t_checkpoint);
        bench_timer_add_from_shared(&t[COL_CREATE], &args[i].t_create);
        bench_timer_add_from_shared(&t[COL_DROP], &args[i].t_drop);
        bench_timer_add_from_shared(&t[COL_FIRST_INS], &args[i].t_first_insert);
        bench_timer_add_from_shared(&t[COL_INS], &args[i].t_insert);
        bench_timer_add_from_shared(&t[COL_SRCH], &args[i].t_search);
    }
    for (c = 0; c < NCOLS; ++c) {
        nsec[c] = t[c].total_nsec;
        count[c] = t[c].count;
    }
}

/*
 * format_per_op --
 *     Format an average time per op into buf, choosing a unit so up to 4 significant digits show. A
 *     column with no operations is shown as "-".
 */
static void
format_per_op(char *buf, size_t len, uint64_t nsec, uint64_t count)
{
    double v;
    const char *unit;

    if (count == 0) {
        testutil_snprintf(buf, len, "-");
        return;
    }
    v = (double)nsec / (double)count;
    if (v >= 1e9) {
        v /= 1e9;
        unit = "s";
    } else if (v >= 1e6) {
        v /= 1e6;
        unit = "ms";
    } else if (v >= 1e3) {
        v /= 1e3;
        unit = "us";
    } else
        unit = "ns";
    testutil_snprintf(buf, len, "%.4g %s", v, unit);
}

/*
 * report_header --
 *     Print a report header line, preceded by a blank line: a 28-column label followed by the
 *     column names.
 */
static void
report_header(const char *label)
{
    int c;

    printf("\n%-28s", label);
    for (c = 0; c < NCOLS; ++c)
        if (c < NCOLS - 1)
            printf("%-12s", col_name[c]);
        else
            printf("%s", col_name[c]);
    printf("\n");
}

/*
 * report_row --
 *     Print a report data row: a 28-column label followed by the per-op times. When a checkpoint is
 *     in progress but none completed in the window, the checkpoint column shows "*" rather than
 *     "-".
 */
static void
report_row(const char *label, const uint64_t *nsec, const uint64_t *count, bool ckpt_active)
{
    char cell[32];
    int c;

    printf("%-28s", label);
    for (c = 0; c < NCOLS; ++c) {
        if (c == COL_CKPT && count[c] == 0 && ckpt_active)
            testutil_snprintf(cell, sizeof(cell), "*");
        else
            format_per_op(cell, sizeof(cell), nsec[c], count[c]);
        if (c < NCOLS - 1)
            printf("%-12s", cell);
        else
            printf("%s", cell);
    }
    printf("\n");
}

/*
 * bench_dhandle_run --
 *     Run the benchmark.
 */
static void
bench_dhandle_run(SHARED *shared)
{
    PLATEAU_SUMMARY *summaries;
    THREAD_ARGS *args;
    pthread_t *tid;
    uint64_t cur_nsec[NCOLS], cur_count[NCOLS], base_nsec[NCOLS], base_count[NCOLS];
    uint64_t last_nsec[NCOLS], last_count[NCOLS], delta_nsec[NCOLS], delta_count[NCOLS];
    uint64_t p_nsec[NCOLS], p_count[NCOLS];
    uint64_t i, now, start_time, elapsed, rss_bytes, malloc_bytes;
    void *ignored;
    int c, existing, last_progress, nsummaries, plateau_level, summaries_cap, nthreads_total;
    bool measuring, prev_measuring, in_transition, checkpointing;
    void *(*thread_func)(void *);
    char label[64];

    /* The worker threads, plus the queuer and the checkpointer. */
    nthreads_total = (int)shared->opts.nthreads + 2;
    args = dcalloc((size_t)nthreads_total, sizeof(THREAD_ARGS));
    tid = dcalloc((size_t)nthreads_total, sizeof(pthread_t));

    printf("Running with %" PRIu64
           " threads, %s\n"
           "  table_count=%d plateau_step=%d measure_count=%d\n"
           "  in_use=%g dropped=%g insert=%g create=%g\n"
           "  actions_per_work_item=%d checkpoint_interval=%ds seed=%" PRIu64 "\n",
      shared->opts.nthreads,
      !shared->opts.disagg.is_enabled ? "attached storage" :
                                        (shared->is_follower ? "DSC follower" : "DSC leader"),
      shared->config.table_count, shared->config.plateau_step, shared->config.measure_count,
      shared->config.table_in_use_fraction, shared->config.table_dropped_fraction,
      shared->config.insert_fraction, shared->config.create_fraction,
      shared->config.actions_per_work_item, shared->config.checkpoint_interval,
      shared->config.seed);

    pthread_rwlock_init(&shared->work_queue_lock, NULL);
    TAILQ_INIT(&shared->work_queue);

    /* The first entries are the worker threads; the checkpointer reads their commit_ts. */
    shared->worker_args = args;
    shared->nworkers = (int)shared->opts.nthreads;

    for (i = 0; i < (uint64_t)nthreads_total; ++i) {
        args[i].threadnum = (int)i;
        args[i].shared = shared;
        bench_timer_init(&args[i].t_create, NULL);
        bench_timer_init(&args[i].t_checkpoint, NULL);
        bench_timer_init(&args[i].t_drop, NULL);
        bench_timer_init(&args[i].t_first_insert, NULL);
        bench_timer_init(&args[i].t_search, NULL);
        bench_timer_init(&args[i].t_insert, NULL);

        if (i == shared->opts.nthreads)
            thread_func = queuer;
        else if (i == shared->opts.nthreads + 1)
            thread_func = checkpointer;
        else
            thread_func = worker;
        testutil_check(pthread_create(&tid[i], NULL, thread_func, &args[i]));
    }

    summaries = NULL;
    nsummaries = summaries_cap = 0;
    prev_measuring = false;
    plateau_level = 0;
    last_progress = -1;
    memset(last_nsec, 0, sizeof(last_nsec));
    memset(last_count, 0, sizeof(last_count));
    memset(base_nsec, 0, sizeof(base_nsec));
    memset(base_count, 0, sizeof(base_count));

    __wt_seconds(NULL, &start_time);
    while (!shared->done) {
        sleep(5);
        __wt_seconds(NULL, &now);
        elapsed = now - start_time;
        gather_totals(args, nthreads_total, cur_nsec, cur_count);
        for (c = 0; c < NCOLS; ++c) {
            delta_nsec[c] = cur_nsec[c] - last_nsec[c];
            delta_count[c] = cur_count[c] - last_count[c];
        }
        measuring = shared->measuring;
        in_transition = shared->in_transition;
        checkpointing = shared->checkpointing;
        existing = shared->high - shared->exists;

        /* A finished measurement window prints its plateau summary, just before the transition. */
        if (prev_measuring && !measuring) {
            for (c = 0; c < NCOLS; ++c) {
                p_nsec[c] = cur_nsec[c] - base_nsec[c];
                p_count[c] = cur_count[c] - base_count[c];
            }
            report_header("Plateau, time per op:");
            testutil_snprintf(
              label, sizeof(label), "%d tables at %" PRIu64 " s", plateau_level, elapsed);
            report_row(label, p_nsec, p_count, false);
            sample_memory(&rss_bytes, &malloc_bytes);
            report_memory(rss_bytes, malloc_bytes);

            if (nsummaries == summaries_cap) {
                summaries_cap = summaries_cap == 0 ? 16 : summaries_cap * 2;
                summaries = drealloc(summaries, sizeof(PLATEAU_SUMMARY) * (size_t)summaries_cap);
            }
            summaries[nsummaries].tables = plateau_level;
            summaries[nsummaries].secs = elapsed;
            for (c = 0; c < NCOLS; ++c) {
                summaries[nsummaries].nsec[c] = p_nsec[c];
                summaries[nsummaries].count[c] = p_count[c];
            }
            summaries[nsummaries].rss_bytes = rss_bytes;
            summaries[nsummaries].malloc_bytes = malloc_bytes;
            ++nsummaries;
        }

        if (measuring) {
            if (!prev_measuring) {
                /* New measurement window: the header carries the time, so the first row omits it.
                 */
                plateau_level = shared->plateau;
                for (c = 0; c < NCOLS; ++c) {
                    base_nsec[c] = last_nsec[c];
                    base_count[c] = last_count[c];
                }
                testutil_snprintf(
                  label, sizeof(label), "%" PRIu64 " seconds, time per op:", elapsed);
                report_header(label);
                testutil_snprintf(label, sizeof(label), "%d tables", existing);
                report_row(label, delta_nsec, delta_count, checkpointing);
            } else {
                testutil_snprintf(
                  label, sizeof(label), "%d tables at %" PRIu64 " s", existing, elapsed);
                report_row(label, delta_nsec, delta_count, checkpointing);
            }
        } else if (in_transition && existing != last_progress) {
            /* Simple progress during a transition: just the current existing-table count. */
            printf("%d tables\n", existing);
            last_progress = existing;
        }

        for (c = 0; c < NCOLS; ++c) {
            last_nsec[c] = cur_nsec[c];
            last_count[c] = cur_count[c];
        }
        prev_measuring = measuring;
    }

    for (i = 0; i < (uint64_t)nthreads_total; ++i)
        testutil_check(pthread_join(tid[i], &ignored));

    /*
     * The last plateau's measurement end is normally not seen in the loop above: the queuer sets
     * done as soon as the measurement finishes, before the next five-second tick. Record that
     * plateau here. The same path covers a plateau cut short by the per-plateau time limit, which
     * is labeled partial because it did not reach its target table count.
     */
    if (prev_measuring) {
        __wt_seconds(NULL, &now);
        gather_totals(args, nthreads_total, cur_nsec, cur_count);
        for (c = 0; c < NCOLS; ++c) {
            p_nsec[c] = cur_nsec[c] - base_nsec[c];
            p_count[c] = cur_count[c] - base_count[c];
        }
        report_header("Plateau, time per op:");
        testutil_snprintf(
          label, sizeof(label), "%d tables at %" PRIu64 " s", plateau_level, now - start_time);
        report_row(label, p_nsec, p_count, false);
        sample_memory(&rss_bytes, &malloc_bytes);
        report_memory(rss_bytes, malloc_bytes);

        if (nsummaries == summaries_cap) {
            summaries_cap = summaries_cap == 0 ? 16 : summaries_cap * 2;
            summaries = drealloc(summaries, sizeof(PLATEAU_SUMMARY) * (size_t)summaries_cap);
        }
        summaries[nsummaries].tables = plateau_level;
        summaries[nsummaries].secs = now - start_time;
        for (c = 0; c < NCOLS; ++c) {
            summaries[nsummaries].nsec[c] = p_nsec[c];
            summaries[nsummaries].count[c] = p_count[c];
        }
        summaries[nsummaries].rss_bytes = rss_bytes;
        summaries[nsummaries].malloc_bytes = malloc_bytes;
        ++nsummaries;
    }

    /* Final summary: one row per plateau. */
    if (nsummaries > 0) {
        report_header("Summary, time per op:");
        for (c = 0; c < nsummaries; ++c) {
            testutil_snprintf(label, sizeof(label), "%d tables at %" PRIu64 " s",
              summaries[c].tables, summaries[c].secs);
            report_row(label, summaries[c].nsec, summaries[c].count, false);
            report_memory(summaries[c].rss_bytes, summaries[c].malloc_bytes);
        }
    }
    free(summaries);
}

/*
 * compute_stable --
 *     Return the minimum of the workers' last-used commit timestamps, the largest timestamp it is
 *     safe to make stable. Returns zero (do not advance) if any worker has not yet committed a
 *     timestamped write, since it may be about to commit below the current minimum.
 */
static uint64_t
compute_stable(SHARED *shared)
{
    uint64_t min_ts, ts;
    int i;

    min_ts = UINT64_MAX;
    for (i = 0; i < shared->nworkers; ++i) {
        WT_ACQUIRE_READ_WITH_BARRIER(ts, shared->worker_args[i].commit_ts);
        if (ts == 0)
            return (0);
        if (ts < min_ts)
            min_ts = ts;
    }
    return (min_ts == UINT64_MAX ? 0 : min_ts);
}

/*
 * checkpoint_log_header --
 *     Write the configuration header to the checkpoint log. A follower run verifies these values
 *     against its own; a mismatch means the replayed work stream would diverge from what the
 *     checkpoints captured. Values that do not affect the work stream (checkpoint interval, thread
 *     count) are intentionally omitted.
 */
static void
checkpoint_log_header(FILE *fp, SHARED *shared)
{
    fprintf(fp,
      "config seed=%" PRIu64
      " table_count=%d plateau_step=%d measure_count=%d "
      "table_dropped_fraction=%g table_in_use_fraction=%g insert_fraction=%g create_fraction=%g "
      "actions_per_work_item=%d checkpoint_interval=%d threads=%" PRIu64 "\n",
      shared->config.seed, shared->config.table_count, shared->config.plateau_step,
      shared->config.measure_count, shared->config.table_dropped_fraction,
      shared->config.table_in_use_fraction, shared->config.insert_fraction,
      shared->config.create_fraction, shared->config.actions_per_work_item,
      shared->config.checkpoint_interval, shared->opts.nthreads);
    fflush(fp);
}

/*
 * checkpointer --
 *     Advance the stable timestamp once a second to the minimum of the workers' commit timestamps,
 *     and take a checkpoint every checkpoint_interval seconds. The stable timestamp is set just
 *     before the checkpoint because the checkpoint derives its timestamp from it. In disaggregated
 *     mode, record each checkpoint's timestamp and metadata so a separate follower run can pick it
 *     up; the follower cannot share the page log's files or memory.
 */
static void *
checkpointer(void *void_args)
{
    WT_PAGE_LOG *page_log;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS ckpt_args;
    WT_SESSION *session;
    THREAD_ARGS *args;
    SHARED *shared;
    FILE *ckpt_log;
    uint64_t stable;
    int measure_secs, next_ckpt, ret, secs;
    char path[1024], reconfig[2048], ts_cfg[64];

    args = (THREAD_ARGS *)void_args;
    shared = args->shared;
    page_log = NULL;
    ckpt_log = NULL;
    next_ckpt = 0;
    testutil_check(shared->conn->open_session(shared->conn, NULL, NULL, &session));

    /* The leader records each checkpoint; a follower only reads what the leader recorded. */
    if (shared->opts.disagg.is_enabled && !shared->is_follower) {
        testutil_snprintf(
          path, sizeof(path), "%s/kv_home/checkpoints.txt", shared->opts.disagg.page_log_home);
        testutil_assert((ckpt_log = fopen(path, "w")) != NULL);
        checkpoint_log_header(ckpt_log, shared);
        testutil_check(
          shared->conn->get_page_log(shared->conn, shared->opts.disagg.page_log, &page_log));
    }

    secs = 0;
    measure_secs = 0;
    while (!shared->done) {
        sleep(1);

        stable = compute_stable(shared);
        if (stable != 0) {
            testutil_snprintf(ts_cfg, sizeof(ts_cfg),
              "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, stable, stable);
            testutil_check(shared->conn->set_timestamp(shared->conn, ts_cfg));
            WT_RELEASE_WRITE_WITH_BARRIER(shared->last_stable, stable);
        }

        /* While racing to the next plateau, take no checkpoints and pick none up. */
        if (shared->in_transition) {
            secs = measure_secs = 0;
            continue;
        }

        /*
         * A follower does not checkpoint. As its replayed stable timestamp passes each recorded
         * checkpoint, it picks that checkpoint up, which reopens stable cursors and pages the
         * checkpoint in from the leader's page log.
         */
        if (shared->is_follower) {
            while (next_ckpt < shared->nckpts && stable != 0 &&
              shared->ckpts[next_ckpt].timestamp <= stable) {
                testutil_snprintf(reconfig, sizeof(reconfig),
                  "disaggregated=(checkpoint_meta=\"%s\")", shared->ckpts[next_ckpt].metadata);
                testutil_check(shared->conn->reconfigure(shared->conn, reconfig));
                WT_RELEASE_WRITE_WITH_BARRIER(shared->checkpoint_num, shared->checkpoint_num + 1);
                printf("--- [follower] picked up checkpoint %d of %d (timestamp=%#" PRIx64
                       ") ---\n",
                  next_ckpt + 1, shared->nckpts, shared->ckpts[next_ckpt].timestamp);
                ++next_ckpt;
            }
            continue;
        }

        /* Checkpoints run only during a measurement window. */
        if (!shared->measuring) {
            secs = measure_secs = 0;
            continue;
        }

        /* Leave the first seconds of the plateau free of checkpoints for clean data points. */
        ++measure_secs;
        ++secs;
        if (measure_secs < CHECKPOINT_HOLDOFF_SECS)
            continue;
        if (secs < shared->config.checkpoint_interval)
            continue;
        secs = 0;

        /* A precise checkpoint (DSC) cannot run until a non-zero stable timestamp exists. */
        if (shared->opts.disagg.is_enabled && stable == 0)
            continue;

        WT_RELEASE_WRITE_WITH_BARRIER(shared->checkpoint_num, shared->checkpoint_num + 1);
        WT_RELEASE_WRITE_WITH_BARRIER(shared->checkpointing, true);
        BENCH_TIME_CUMULATIVE(
          &args->t_checkpoint, session, { testutil_check(session->checkpoint(session, NULL)); });
        WT_RELEASE_WRITE_WITH_BARRIER(shared->checkpointing, false);

        /*
         * Record the just-completed checkpoint. The page log returns the latest complete
         * checkpoint; a follower picks it up once its replayed stable timestamp reaches the
         * recorded timestamp.
         */
        if (ckpt_log != NULL) {
            memset(&ckpt_args, 0, sizeof(ckpt_args));
            ret = page_log->pl_get_complete_checkpoint(page_log, session, &ckpt_args);
            testutil_check_error_ok(ret, WT_NOTFOUND);
            if (ret == 0) {
                fprintf(ckpt_log, "ckpt timestamp=%" PRIx64 " metadata=%.*s\n",
                  ckpt_args.checkpoint_timestamp, (int)ckpt_args.checkpoint_metadata.size,
                  (const char *)ckpt_args.checkpoint_metadata.data);
                fflush(ckpt_log);
            }
            free(ckpt_args.checkpoint_metadata.mem);
        }
    }

    if (page_log != NULL)
        testutil_check(page_log->terminate(page_log, NULL));
    if (ckpt_log != NULL)
        testutil_assert(fclose(ckpt_log) == 0);
    testutil_check(session->close(session, NULL));
    return (NULL);
}

/*
 * high_for_existing --
 *     Return the high point (one past the highest table number) at which the given number of tables
 *     exist, given that table_dropped_fraction of all table numbers are dropped.
 */
static int
high_for_existing(SHARED *shared, int existing)
{
    return (int)(existing / (1.0 - shared->config.table_dropped_fraction));
}

/*
 * plateau_exists --
 *     Return the lowest existing table number for the given high point: the boundary below which
 *     tables have been dropped so that table_dropped_fraction of the table numbers are gone.
 */
static int
plateau_exists(SHARED *shared, int high)
{
    return (int)(high * shared->config.table_dropped_fraction);
}

/*
 * plateau_low --
 *     Return the lowest active table number for the given high point and existing-table boundary:
 *     table_in_use_fraction of the existing tables are active, counting down from high.
 */
static int
plateau_low(SHARED *shared, int high, int exists)
{
    return (high - (int)((high - exists) * shared->config.table_in_use_fraction));
}

/*
 * rand_double --
 *     Return a pseudo-random double in [0, 1).
 */
static double
rand_double(WT_RAND_STATE *rnd)
{
    uint32_t r;

    r = __wt_random(rnd);
    return ((double)r / ((double)0xffffffffu + 1.0));
}

/*
 * shuffle --
 *     Implements the Fisher-Yates shuffle.
 */
static void
shuffle(int *arr, int n, WT_RAND_STATE *rnd)
{
    int i, j, t;

    for (i = n - 1; i > 0; i--) {
        /* Get a random index in [0, i]. */
        j = (int)(__wt_random(rnd) % (uint32_t)(i + 1));

        /* swap i and j */
        t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
    }
}

/*
 * queue_append --
 *     Append an action to the current work item, publishing it to the work queue when full.
 */
static void
queue_append(SHARED *shared, WORK_ITEM **curp, ACTION_TYPE type, int tablenum)
{
    WORK_ITEM *cur;

    cur = *curp;
    if (cur == NULL) {
        cur = dcalloc(1, sizeof(WORK_ITEM));
        cur->actions = dcalloc((size_t)shared->config.actions_per_work_item, sizeof(ACTION));
        cur->nactions = 0;
        /*
         * Each work item carries its own timestamp. Items are created in increasing timestamp order
         * and queued FIFO, so every worker dequeues items in increasing timestamp order; that keeps
         * each thread's commit timestamps monotonic, which the checkpointer relies on.
         */
        cur->timestamp = ++shared->queue_timestamp;
        *curp = cur;
    }
    cur->actions[cur->nactions].type = type;
    cur->actions[cur->nactions].tablenum = tablenum;
    cur->nactions++;
    if (cur->nactions >= shared->config.actions_per_work_item)
        queue_flush(shared, curp);
}

/*
 * queue_flush --
 *     Publish a partially-filled work item to the work queue.
 */
static void
queue_flush(SHARED *shared, WORK_ITEM **curp)
{
    if (*curp == NULL)
        return;
    /* Insert at the tail; workers dequeue from the head, giving FIFO (increasing-timestamp) order.
     */
    WITH_RW_LOCK(&shared->work_queue_lock, {
        TAILQ_INSERT_TAIL(&shared->work_queue, *curp, q);
        shared->work_queue_len++;
    });
    *curp = NULL;
}

/*
 * queue_wait_drain --
 *     Wait until the workers have drained the work queue.
 */
static void
queue_wait_drain(SHARED *shared)
{
    int len;

    for (;;) {
        if (shared->done)
            break;
        WT_ACQUIRE_READ_WITH_BARRIER(len, shared->work_queue_len);
        if (len == 0)
            break;
        usleep(1000);
    }
}

/*
 * transition_to_plateau --
 *     Create and drop tables to reach a plateau's starting state, as fast as possible. This is done
 *     single-threaded in the session owned by the queuer (the workers stay idle) to avoid
 *     schema-lock contention and deadlock, with checkpoints deferred; the initial inserts are
 *     batched into transactions. The work is not measured.
 */
static void
transition_to_plateau(SHARED *shared, WT_SESSION *session, int plateau)
{
    WT_CURSOR *cursor;
    uint64_t ts;
    int batch_end, i, new_exists, new_low, ret, t;
    char tname[100], ts_cfg[64];

    new_exists = plateau_exists(shared, plateau);
    new_low = plateau_low(shared, plateau, new_exists);

    WT_RELEASE_WRITE_WITH_BARRIER(shared->in_transition, true);

    /*
     * Drop tables that should no longer exist at this plateau. The existing-table boundary is not
     * published until the end: while we are still creating, leaving exists at its old (lower) value
     * keeps the reported existing-table count climbing monotonically rather than going negative.
     */
    for (t = shared->exists; t < new_exists && !shared->done; ++t) {
        TNAME(tname, shared, t);
        ret = session->drop(session, tname, drop_config);
        testutil_assert(ret == 0 || ret == ENOENT || ret == EBUSY);
    }

    /*
     * Create tables up to this plateau's starting high point, inserting key 0 into each. Schema
     * creates are not transactional, so create a batch first, then insert key 0 across the whole
     * batch in a single transaction. Publish the high point after each batch so the existing-table
     * count the run reports climbs visibly during a long transition.
     */
    for (t = shared->high; t < plateau && !shared->done;) {
        batch_end = WT_MIN(t + TRANSITION_BATCH, plateau);

        for (i = t; i < batch_end; ++i) {
            TNAME(tname, shared, i);
            testutil_check(session->create(session, tname, table_config));
        }

        ts = ++shared->queue_timestamp;
        testutil_check(session->begin_transaction(session, NULL));
        for (i = t; i < batch_end; ++i) {
            TNAME(tname, shared, i);
            testutil_check(session->open_cursor(session, tname, NULL, NULL, &cursor));
            cursor->set_key(cursor, 0);
            cursor->set_value(cursor, i);
            testutil_check(cursor->insert(cursor));
            testutil_check(cursor->close(cursor));
        }
        testutil_snprintf(ts_cfg, sizeof(ts_cfg), "commit_timestamp=%" PRIx64, ts);
        testutil_check(session->commit_transaction(session, ts_cfg));

        t = batch_end;
        WT_RELEASE_WRITE_WITH_BARRIER(shared->high, t);
    }

    WT_RELEASE_WRITE_WITH_BARRIER(shared->exists, new_exists);
    WT_RELEASE_WRITE_WITH_BARRIER(shared->high, plateau);
    WT_RELEASE_WRITE_WITH_BARRIER(shared->low, new_low);

    WT_RELEASE_WRITE_WITH_BARRIER(shared->in_transition, false);
}

/*
 * measure_plateau --
 *     Run a measurement window for a plateau: grow the existing-table count by measure_count while
 *     repeatedly visiting (searching or inserting) every active table. The argument is the
 *     plateau's existing-table level.
 */
static void
measure_plateau(SHARED *shared, WT_RAND_STATE *rnd, int existing_plateau)
{
    WORK_ITEM *cur;
    double create_accum, create_fraction, insert_fraction;
    uint64_t measure_start, now;
    int *active;
    int active_count, i, lo, hi, n_creates, new_exists, new_high, new_low, target_high;
    bool timed_out;

    cur = NULL;
    active = NULL;
    create_fraction = shared->config.create_fraction;
    insert_fraction = shared->config.insert_fraction;
    create_accum = 0.0;
    target_high = high_for_existing(shared, existing_plateau + shared->config.measure_count);
    timed_out = false;
    __wt_seconds(NULL, &measure_start);

    WT_RELEASE_WRITE_WITH_BARRIER(shared->measuring, true);

    while (shared->high < target_high && !shared->done) {
        /* A high plateau may make little headway; give up after a time limit and report so far. */
        __wt_seconds(NULL, &now);
        if (now - measure_start >= PLATEAU_TIME_LIMIT_SECS) {
            timed_out = true;
            break;
        }
        lo = shared->low;
        hi = shared->high;
        active_count = hi - lo;

        /* Visit each active table once, as a search or an insert. */
        if (active_count > 0) {
            active = drealloc(active, sizeof(int) * (size_t)active_count);
            for (i = 0; i < active_count; ++i)
                active[i] = lo + i;
            shuffle(active, active_count, rnd);
            for (i = 0; i < active_count && !shared->done; ++i) {
                if (rand_double(rnd) < insert_fraction)
                    queue_append(shared, &cur, ACTION_INSERT, active[i]);
                else
                    queue_append(shared, &cur, ACTION_SEARCH, active[i]);
            }
        }

        /*
         * Accumulate the fractional number of creates for this pass so that creates are
         * create_fraction of all actions; with a tiny fraction most passes do none. Bootstrap one
         * create when the active range is empty (the first plateau starts from nothing) so the
         * window can begin to grow.
         */
        if (create_fraction >= 1.0)
            n_creates = target_high - hi;
        else {
            create_accum += active_count * create_fraction / (1.0 - create_fraction);
            n_creates = (int)create_accum;
            create_accum -= n_creates;
            if (active_count == 0 && n_creates == 0)
                n_creates = 1;
        }
        if (n_creates > target_high - hi)
            n_creates = target_high - hi;

        /* Queue the creates, growing the high point. */
        for (i = 0; i < n_creates && !shared->done; ++i)
            queue_append(shared, &cur, ACTION_CREATE, hi + i);

        /* The new high point implies a new existing-table boundary; drop from the bottom. */
        new_high = hi + n_creates;
        new_exists = plateau_exists(shared, new_high);
        new_low = plateau_low(shared, new_high, new_exists);
        for (i = shared->exists; i < new_exists && !shared->done; ++i)
            queue_append(shared, &cur, ACTION_DROP, i);

        queue_flush(shared, &cur);

        WT_RELEASE_WRITE_WITH_BARRIER(shared->exists, new_exists);
        WT_RELEASE_WRITE_WITH_BARRIER(shared->high, new_high);
        WT_RELEASE_WRITE_WITH_BARRIER(shared->low, new_low);

        /* Let the workers catch up if the queue is growing too long. */
        for (;;) {
            int len;
            if (shared->done)
                break;
            WT_ACQUIRE_READ_WITH_BARRIER(len, shared->work_queue_len);
            if (len <= QUEUE_THROTTLE_LEN)
                break;
            usleep(1000);
        }
    }

    queue_flush(shared, &cur);
    queue_wait_drain(shared);
    WT_RELEASE_WRITE_WITH_BARRIER(shared->measuring, false);

    /* A plateau that ran out of time ends the whole run; the final report covers what we have. */
    if (timed_out) {
        printf("plateau %d did not finish within %d seconds; stopping\n", existing_plateau,
          PLATEAU_TIME_LIMIT_SECS);
        WT_RELEASE_WRITE_WITH_BARRIER(shared->done, true);
    }

    free(active);
}

/*
 * queuer --
 *     Drive the benchmark: step through the plateaus, transitioning to each and then measuring it.
 *     The queuer is the sole source of work items and the sole user of the RNG.
 */
static void *
queuer(void *void_args)
{
    WT_SESSION *session;
    SHARED *shared;
    THREAD_ARGS *args;
    WT_RAND_STATE rnd;
    int existing;

    args = (THREAD_ARGS *)void_args;
    shared = args->shared;

    /* The queuer performs transitions directly in this session, single-threaded. */
    testutil_check(shared->conn->open_session(shared->conn, NULL, NULL, &session));

    /* Seed from the configured value so a follower run reproduces the identical work stream. */
    __wt_random_init_seed(&rnd, shared->config.seed);

    /* The queuer drives the run; release the workers and checkpointer. */
    WT_RELEASE_WRITE_WITH_BARRIER(shared->started, true);

    /* Plateaus are existing-table levels; convert to an internal high point for the transition. */
    for (existing = 0; existing <= shared->config.table_count && !shared->done;
      existing += shared->config.plateau_step) {
        WT_RELEASE_WRITE_WITH_BARRIER(shared->plateau, existing);
        transition_to_plateau(shared, session, high_for_existing(shared, existing));
        measure_plateau(shared, &rnd, existing);
    }

    WT_RELEASE_WRITE_WITH_BARRIER(shared->done, true);
    testutil_check(session->close(session, NULL));
    return (NULL);
}

/*
 * worker --
 *     Run the worker loop in a thread context. A worker (of which there may be many) takes an item
 *     from the work queue and performs its actions: reads, writes, creates and drops.
 */
static void *
worker(void *void_args)
{
    WT_SESSION *session;
    WT_CURSOR *cursor;
    THREAD_ARGS *args;
    WORK_ITEM *work_item;
    SHARED *shared;
    BENCH_TIMER single;
    uint64_t ignore, item_ts;
    int i, ret, table_num;
    char tname[100], ts_cfg[64];
    bool committed, measuring;

    args = (THREAD_ARGS *)void_args;
    shared = args->shared;
    testutil_check(shared->conn->open_session(shared->conn, NULL, NULL, &session));

    while (!shared->started)
        sleep(1);

    while (!shared->done) {
        WITH_RW_LOCK(&shared->work_queue_lock, {
            work_item = TAILQ_FIRST(&shared->work_queue);
            if (work_item != NULL) {
                TAILQ_REMOVE(&shared->work_queue, work_item, q);
                shared->work_queue_len--;
            }
        });
        if (work_item == NULL) {
            usleep(100);
            continue;
        }

        measuring = shared->measuring;
        item_ts = work_item->timestamp;
        committed = false;
        for (i = 0; i < work_item->nactions && !shared->done; ++i) {
            table_num = work_item->actions[i].tablenum;
            TNAME(tname, shared, table_num);

            switch (work_item->actions[i].type) {
            case ACTION_SEARCH:
                /*
                 * Time the whole search, including the cursor open: that open is the dhandle cost
                 * this benchmark exists to measure. The table may have been dropped between when
                 * the queuer added it and now; an ENOENT open means there is no operation to
                 * measure, so discard the timing rather than record a failed lookup.
                 */
                bench_timer_init(&single, NULL);
                bench_timer_start(&single, session);
                ret = session->open_cursor(session, tname, NULL, NULL, &cursor);
                if (ret == ENOENT)
                    continue;
                testutil_check(ret);
                cursor->set_key(cursor, 0);
                /*
                 * Key 0 can be briefly absent if the table's create is still in flight; tolerate
                 * not-found rather than reading an unset value.
                 */
                ret = cursor->search(cursor);
                if (ret == 0)
                    testutil_check(cursor->get_value(cursor, &ignore));
                else
                    testutil_assert(ret == WT_NOTFOUND);
                testutil_check(cursor->close(cursor));
                bench_timer_stop(&single, session);
                if (measuring)
                    bench_timer_add_to_shared(&args->t_search, &single);
                break;

            case ACTION_INSERT:
                /* As with searches, time the cursor open and discard the timing on an ENOENT open.
                 */
                bench_timer_init(&single, NULL);
                bench_timer_start(&single, session);
                ret = session->open_cursor(session, tname, NULL, NULL, &cursor);
                if (ret == ENOENT)
                    continue;
                testutil_check(ret);
                /*
                 * Write the key equal to this thread's number plus one, reserving key 0 for the
                 * create. Each (table, key) pair then has a single writer, so writes to it are
                 * serialized in increasing timestamp order and never conflict. Key 0 from the
                 * create is always present for reads to search.
                 *
                 * A consequence: the key is chosen from the executing thread, not from the work
                 * item, so a follower replaying the same stream writes to different keys than the
                 * leader did (whichever worker happened to run each write). The follower's data
                 * therefore is not identical to the leader's; that is harmless here because reads
                 * do not verify values, but it means the replay is not byte-for-byte faithful. A
                 * future change could derive the key from the work item instead, if faithful data
                 * is ever needed.
                 */
                testutil_check(session->begin_transaction(session, NULL));
                cursor->set_key(cursor, args->threadnum + 1);
                cursor->set_value(cursor, args->threadnum);
                testutil_check(cursor->insert(cursor));
                testutil_snprintf(ts_cfg, sizeof(ts_cfg), "commit_timestamp=%" PRIx64, item_ts);
                testutil_check(session->commit_transaction(session, ts_cfg));
                testutil_check(cursor->close(cursor));
                bench_timer_stop(&single, session);
                if (measuring)
                    bench_timer_add_to_shared(&args->t_insert, &single);
                committed = true;
                break;

            case ACTION_CREATE:
                MAYBE_TIME(measuring, &args->t_create, session,
                  { testutil_check(session->create(session, tname, table_config)); });
                MAYBE_TIME(measuring, &args->t_first_insert, session, {
                    testutil_check(session->open_cursor(session, tname, NULL, NULL, &cursor));
                    testutil_check(session->begin_transaction(session, NULL));
                    cursor->set_key(cursor, 0);
                    cursor->set_value(cursor, table_num);
                    testutil_check(cursor->insert(cursor));
                    testutil_snprintf(ts_cfg, sizeof(ts_cfg), "commit_timestamp=%" PRIx64, item_ts);
                    testutil_check(session->commit_transaction(session, ts_cfg));
                    testutil_check(cursor->close(cursor));
                });
                committed = true;
                break;

            case ACTION_DROP:
                /*
                 * A drop can collide with a stale read still queued for the same table; tolerate
                 * the resulting ENOENT or EBUSY. Schema operations are not timestamped.
                 */
                MAYBE_TIME(measuring, &args->t_drop, session, {
                    ret = session->drop(session, tname, drop_config);
                    testutil_assert(ret == 0 || ret == ENOENT || ret == EBUSY);
                });
                break;
            }
        }
        /*
         * Publish this thread's commit timestamp after the item's writes commit. The checkpointer
         * reads it to compute the stable timestamp.
         */
        if (committed)
            WT_RELEASE_WRITE_WITH_BARRIER(args->commit_ts, item_ts);
        free(work_item->actions);
        free(work_item);
        session->reset(session);
    }
    testutil_check(session->close(session, NULL));
    return (NULL);
}
