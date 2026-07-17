/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 */

#include <errno.h>
#include <unistd.h>

#include "test_util.h"
#include "bench_timer.h"

#define SHARED_PARSE_OPTIONS "h:pn:c:"
#define DEFAULT_RECORDS (200 * WT_THOUSAND)
#define DEFAULT_CACHE_SIZE "64MB"
#define TABLE_URI "table:cleanup"

static const char *const stat_internal_reads = "cache: internal pages read into cache";
static const char *const stat_bytes_read = "cache: bytes read into cache";
static const char *const stat_cache_bytes = "cache: bytes currently in the cache";
static const char *const stat_internal_bytes = "btree-size: internal page bytes";
static const char *const stat_cleanup_success = "checkpoint-cleanup: successful calls";

typedef struct {
    uint64_t internal_reads;
    uint64_t bytes_read;
    uint64_t cache_bytes;
    uint64_t internal_bytes;
    uint64_t cleanup_success;
} STAT_VALUES;

typedef struct {
    uint64_t records;
    int64_t internal_reads;
    int64_t bytes_read;
    int64_t cache_bytes;
    int64_t internal_bytes;
    uint64_t elapsed_us;
} RESULT;

typedef struct {
    TEST_OPTS opts;
    uint64_t records;
    const char *cache_size;
    bool cache_size_owned;
} OPTIONS;

extern char *__wt_optarg;

static int usage(void);
static void run_case(const OPTIONS *, bool, RESULT *);
static uint64_t get_stat(WT_SESSION *, const char *, const char *);
static void read_stats(WT_SESSION *, STAT_VALUES *);
static void populate(WT_SESSION *, uint64_t);
static void check_key(WT_SESSION *, uint64_t, int);
static void wait_for_cleanup(WT_SESSION *, uint64_t);

static int
usage(void)
{
    fprintf(stderr, "usage: %s [-h home] [-p] [-n records] [-c cache_size]\n", progname);
    fprintf(stderr, "\t-h set the database home directory\n"
                    "\t-p preserve the database home directory\n"
                    "\t-n number of records (default: %" PRIu64 ")\n"
                    "\t-c WiredTiger cache size (default: %s)\n",
      (uint64_t)DEFAULT_RECORDS, DEFAULT_CACHE_SIZE);
    return (EXIT_FAILURE);
}

static uint64_t
get_stat(WT_SESSION *session, const char *uri, const char *name)
{
    WT_CURSOR *cursor;
    const char *description, *printable;
    uint64_t value;
    int ret;

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_value(cursor, &description, &printable, &value));
        if (strcmp(description, name) == 0) {
            testutil_check(cursor->close(cursor));
            return (value);
        }
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
    testutil_die(WT_NOTFOUND, "statistic '%s' was not found in %s", name, uri);
}

static void
read_stats(WT_SESSION *session, STAT_VALUES *stats)
{
    /* These exact descriptions are stable public statistics. The generated WT_STAT_CONN_* names
     * are internal implementation constants and are intentionally not used by this tool. */
    stats->internal_reads = get_stat(session, "statistics:", stat_internal_reads);
    stats->bytes_read = get_stat(session, "statistics:", stat_bytes_read);
    stats->cache_bytes = get_stat(session, "statistics:", stat_cache_bytes);
    stats->internal_bytes = get_stat(session, "statistics:" TABLE_URI, stat_internal_bytes);
    stats->cleanup_success = get_stat(session, "statistics:", stat_cleanup_success);
}

static void
populate(WT_SESSION *session, uint64_t records)
{
    WT_CURSOR *cursor;
    uint64_t i;

    testutil_check(session->create(session, TABLE_URI,
      "key_format=Q,value_format=S,leaf_page_max=4KB,internal_page_max=4KB"));
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    for (i = 1; i <= records; ++i) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, "checkpoint-cleanup-benchmark-value");
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_check(session->checkpoint(session, NULL));
}

static void
check_key(WT_SESSION *session, uint64_t key, int expected)
{
    WT_CURSOR *cursor;
    int ret;

    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    cursor->set_key(cursor, key);
    ret = cursor->search(cursor);
    testutil_assert(ret == expected);
    testutil_check(cursor->close(cursor));
}

static void
wait_for_cleanup(WT_SESSION *session, uint64_t previous_success)
{
    uint64_t success;
    uint32_t attempts;

    for (attempts = 0; attempts < 3000; ++attempts) {
        success = get_stat(session, "statistics:", stat_cleanup_success);
        if (success > previous_success)
            return;
        usleep(10000);
    }
    testutil_die(ETIMEDOUT, "checkpoint cleanup did not complete");
}

static void
run_case(const OPTIONS *options, bool deleted, RESULT *result)
{
    BENCH_TIMER timer;
    STAT_VALUES before, after;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    char home[PATH_MAX], config[256];
    uint64_t deleted_key;

    result->records = options->records;
    testutil_snprintf(home, sizeof(home), "%s.%s", options->opts.home,
      deleted ? "one_delete" : "no_delete");
    testutil_recreate_dir(home);

    testutil_snprintf(config, sizeof(config), "create,cache_size=%s,statistics=(all)",
      options->cache_size);
    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    populate(session, options->records);
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));

    if (deleted) {
        testutil_check(wiredtiger_open(home, NULL, config, &conn));
        testutil_check(conn->open_session(conn, NULL, NULL, &session));
        deleted_key = options->records / 2;
        {
            WT_CURSOR *cursor;
            testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
            cursor->set_key(cursor, deleted_key);
            testutil_check(cursor->remove(cursor));
            testutil_check(cursor->close(cursor));
        }
        testutil_check(session->checkpoint(session, NULL));
        testutil_check(session->close(session, NULL));
        testutil_check(conn->close(conn, NULL));
    }

    testutil_snprintf(config, sizeof(config),
      "create,cache_size=%s,statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0)",
      options->cache_size);
    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    read_stats(session, &before);
    bench_timer_init(&timer, NULL);
    bench_timer_start(&timer, session);
    /* A checkpoint explicitly signals the cleanup thread; wait for that iteration below. */
    testutil_check(session->checkpoint(session, NULL));
    wait_for_cleanup(session, before.cleanup_success);
    bench_timer_stop(&timer, session);
    read_stats(session, &after);

    result->internal_reads = (int64_t)after.internal_reads - (int64_t)before.internal_reads;
    result->bytes_read = (int64_t)after.bytes_read - (int64_t)before.bytes_read;
    result->cache_bytes = (int64_t)after.cache_bytes - (int64_t)before.cache_bytes;
    result->internal_bytes = (int64_t)after.internal_bytes - (int64_t)before.internal_bytes;
    result->elapsed_us = timer.total_nsec / WT_THOUSAND;

    deleted_key = options->records / 2;
    check_key(session, deleted_key - 1, 0);
    check_key(session, deleted_key, deleted ? WT_NOTFOUND : 0);
    check_key(session, deleted_key + 1, 0);

    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));
    if (!options->opts.preserve)
        testutil_remove(home);
}

int
main(int argc, char *argv[])
{
    OPTIONS options;
    RESULT no_delete, one_delete;
    int ch;

    memset(&options, 0, sizeof(options));
    __wt_stream_set_line_buffer(stdout);
    testutil_set_progname(argv);
    testutil_parse_begin_opt(argc, argv, SHARED_PARSE_OPTIONS, &options.opts);
    while ((ch = __wt_getopt(progname, argc, argv, SHARED_PARSE_OPTIONS)) != EOF) {
        if (ch == 'c') {
            options.cache_size = dstrdup(__wt_optarg);
            options.cache_size_owned = true;
        } else if (testutil_parse_single_opt(&options.opts, ch) != 0)
            return (usage());
    }
    testutil_parse_end_opt(&options.opts);

    options.records = options.opts.nrecords == 0 ? DEFAULT_RECORDS : options.opts.nrecords;
    if (options.cache_size == NULL)
        options.cache_size = DEFAULT_CACHE_SIZE;
    testutil_assert(options.records >= 4 && options.records % 2 == 0);
    testutil_recreate_dir(options.opts.home);

    run_case(&options, false, &no_delete);
    run_case(&options, true, &one_delete);
    printf("RESULT case=no_delete records=%" PRIu64 " internal_reads=%" PRId64
           " bytes_read=%" PRId64 " cache_bytes=%" PRId64 " internal_bytes=%" PRId64
           " elapsed_us=%" PRIu64 "\n",
      no_delete.records, no_delete.internal_reads, no_delete.bytes_read, no_delete.cache_bytes,
      no_delete.internal_bytes, no_delete.elapsed_us);
    printf("RESULT case=one_delete records=%" PRIu64 " internal_reads=%" PRId64
           " bytes_read=%" PRId64 " cache_bytes=%" PRId64 " internal_bytes=%" PRId64
           " elapsed_us=%" PRIu64 "\n",
      one_delete.records, one_delete.internal_reads, one_delete.bytes_read, one_delete.cache_bytes,
      one_delete.internal_bytes, one_delete.elapsed_us);

    if (options.cache_size_owned)
        free((void *)options.cache_size);
    testutil_cleanup(&options.opts);
    return (EXIT_SUCCESS);
}
