/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <errno.h>
#include <unistd.h>

#include "test_util.h"
#include "bench_timer.h"

#define SHARED_PARSE_OPTIONS \
    "h:"                     \
    "p"                      \
    "n:"                     \
    "c:"                     \
    "r:"
#define DEFAULT_RECORDS (200 * WT_THOUSAND)
#define DEFAULT_CACHE_SIZE "64MB"
#define DEFAULT_REPEATS 5
#define MAX_REPEATS 20
#define INSERT_TS 10
#define DELETE_TS 20
#define OLDEST_TS 5
#define STABLE_TS 30
#define DELETE_STRIDE 16
#define BATCH_SIZE 1000
#define VALUE_SIZE 128
#define TABLE_URI "table:cleanup"

static const char *const stat_internal_reads = "cache: internal pages read into cache";
static const char *const stat_leaf_reads = "cache: leaf pages read into cache";
static const char *const stat_bytes_read = "cache: bytes read into cache";
static const char *const stat_cache_bytes = "cache: bytes currently in the cache";
static const char *const stat_pages_visited = "checkpoint-cleanup: pages visited";
static const char *const stat_cleanup_success = "checkpoint-cleanup: successful calls";
static const char *const stat_obsolete_tw_reads =
  "checkpoint-cleanup: pages read into cache due to obsolete time window";

typedef struct {
    uint64_t internal_reads;
    uint64_t leaf_reads;
    uint64_t bytes_read;
    uint64_t cache_bytes;
    uint64_t pages_visited;
    uint64_t cleanup_success;
    uint64_t obsolete_tw_reads;
    bool has_obsolete_tw_stat;
} STAT_VALUES;

typedef struct {
    uint64_t internal_reads;
    uint64_t leaf_reads;
    uint64_t bytes_read;
    int64_t cache_bytes;
    uint64_t pages_visited;
    uint64_t elapsed_us;
    uint64_t obsolete_tw_reads;
    bool has_obsolete_tw_stat;
} RESULT;

typedef struct {
    TEST_OPTS opts;
    uint64_t records;
    uint32_t repeats;
    const char *cache_size;
    bool cache_size_owned;
} OPTIONS;

typedef enum {
    METRIC_INTERNAL_READS,
    METRIC_LEAF_READS,
    METRIC_BYTES_READ,
    METRIC_CACHE_BYTES,
    METRIC_PAGES_VISITED,
    METRIC_ELAPSED_US,
    METRIC_COUNT
} METRIC;

extern char *__wt_optarg;

static int usage(void);
static bool get_stat(WT_SESSION *, const char *, const char *, uint64_t *);
static uint64_t get_required_stat(WT_SESSION *, const char *, const char *);
static void read_stats(WT_SESSION *, STAT_VALUES *);
static void populate(WT_SESSION *, uint64_t);
static void remove_sparse_keys(WT_SESSION *, uint64_t);
static void check_key(WT_SESSION *, uint64_t, int);
static void run_case(const OPTIONS *, bool, uint32_t, RESULT *);
static void wait_for_cleanup(WT_SESSION *, uint64_t);
static int64_t median_metric(const RESULT *, uint32_t, METRIC);
static uint64_t stat_delta(uint64_t, uint64_t);
static void set_timestamp(WT_CONNECTION *, const char *, uint64_t);
static void print_comparison(const RESULT *, const RESULT *, uint32_t);

/*
 * usage --
 *     Display the command-line usage.
 */
static int
usage(void)
{
    fprintf(
      stderr, "usage: %s [-h home] [-p] [-n records] [-c cache_size] [-r repeats]\n", progname);
    fprintf(stderr,
      "\t-h set the database home directory\n"
      "\t-p preserve the database home directory\n"
      "\t-n number of records (default: %" PRIu64
      ")\n"
      "\t-c WiredTiger cache size (default: %s)\n"
      "\t-r number of repetitions (default: %d, maximum: %d)\n",
      (uint64_t)DEFAULT_RECORDS, DEFAULT_CACHE_SIZE, DEFAULT_REPEATS, MAX_REPEATS);
    return (EXIT_FAILURE);
}

/*
 * get_stat --
 *     Find a statistic by its public description.
 */
static bool
get_stat(WT_SESSION *session, const char *uri, const char *name, uint64_t *valuep)
{
    WT_CURSOR *cursor;
    const char *description, *printable;
    int ret;

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_value(cursor, &description, &printable, valuep));
        if (strcmp(description, name) == 0) {
            testutil_check(cursor->close(cursor));
            return (true);
        }
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
    return (false);
}

/*
 * get_required_stat --
 *     Return a required statistic by its public description.
 */
static uint64_t
get_required_stat(WT_SESSION *session, const char *uri, const char *name)
{
    uint64_t value;

    if (!get_stat(session, uri, name, &value))
        testutil_die(WT_NOTFOUND, "statistic '%s' was not found in %s", name, uri);
    return (value);
}

/*
 * read_stats --
 *     Read the statistics used to measure a cleanup pass.
 */
static void
read_stats(WT_SESSION *session, STAT_VALUES *stats)
{
    stats->internal_reads = get_required_stat(session, "statistics:", stat_internal_reads);
    stats->leaf_reads = get_required_stat(session, "statistics:", stat_leaf_reads);
    stats->bytes_read = get_required_stat(session, "statistics:", stat_bytes_read);
    stats->cache_bytes = get_required_stat(session, "statistics:", stat_cache_bytes);
    stats->pages_visited = get_required_stat(session, "statistics:", stat_pages_visited);
    stats->cleanup_success = get_required_stat(session, "statistics:", stat_cleanup_success);
    stats->has_obsolete_tw_stat =
      get_stat(session, "statistics:", stat_obsolete_tw_reads, &stats->obsolete_tw_reads);
}

/*
 * set_timestamp --
 *     Set a timestamp using a numeric value.
 */
static void
set_timestamp(WT_CONNECTION *conn, const char *name, uint64_t timestamp)
{
    char config[128];

    testutil_snprintf(config, sizeof(config), "%s=%" PRIx64, name, timestamp);
    testutil_check(conn->set_timestamp(conn, config));
}

/*
 * populate --
 *     Insert timestamped records into the table.
 */
static void
populate(WT_SESSION *session, uint64_t records)
{
    WT_CURSOR *cursor;
    uint64_t i, batch_end;
    char value[VALUE_SIZE + 1];

    memset(value, 'v', VALUE_SIZE);
    value[VALUE_SIZE] = '\0';
    testutil_check(session->create(session, TABLE_URI,
      "key_format=Q,value_format=S,allocation_size=512,leaf_page_max=4KB,internal_page_max=512"));
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));

    for (i = 1; i <= records; i = batch_end + 1) {
        batch_end = WT_MIN(i + BATCH_SIZE - 1, records);
        testutil_check(session->begin_transaction(session, NULL));
        for (; i <= batch_end; ++i) {
            cursor->set_key(cursor, i);
            cursor->set_value(cursor, value);
            testutil_check(cursor->insert(cursor));
        }
        {
            char config[64];

            testutil_snprintf(
              config, sizeof(config), "commit_timestamp=%" PRIx64, (uint64_t)INSERT_TS);
            testutil_check(session->commit_transaction(session, config));
        }
    }
    testutil_check(cursor->close(cursor));
}

/*
 * remove_sparse_keys --
 *     Delete a sparse subset of keys while leaving live records on each affected leaf.
 */
static void
remove_sparse_keys(WT_SESSION *session, uint64_t records)
{
    WT_CURSOR *cursor;
    uint64_t key;
    char config[64];

    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    testutil_check(session->begin_transaction(session, NULL));
    for (key = DELETE_STRIDE; key <= records; key += DELETE_STRIDE) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->remove(cursor));
    }
    testutil_snprintf(config, sizeof(config), "commit_timestamp=%" PRIx64, (uint64_t)DELETE_TS);
    testutil_check(session->commit_transaction(session, config));
    testutil_check(cursor->close(cursor));
}

/*
 * check_key --
 *     Verify that a key is present or absent.
 */
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

/*
 * wait_for_cleanup --
 *     Wait for the checkpoint cleanup thread to complete a pass.
 */
static void
wait_for_cleanup(WT_SESSION *session, uint64_t previous_success)
{
    uint64_t success;
    uint32_t attempts;

    for (attempts = 0; attempts < 3000; ++attempts) {
        success = get_required_stat(session, "statistics:", stat_cleanup_success);
        if (success > previous_success)
            return;
        usleep(10000);
    }
    testutil_die(ETIMEDOUT, "checkpoint cleanup did not complete");
}

/*
 * stat_delta --
 *     Return the difference between two monotonically increasing statistics.
 */
static uint64_t
stat_delta(uint64_t before, uint64_t after)
{
    testutil_assert(after >= before);
    return (after - before);
}

/*
 * run_case --
 *     Run one cleanup measurement with or without sparse deletes.
 */
static void
run_case(const OPTIONS *options, bool partial_delete, uint32_t repetition, RESULT *result)
{
    BENCH_TIMER timer;
    STAT_VALUES before, after;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    char home[PATH_MAX], config[256];
    uint64_t deleted_key;
    const char *case_name;

    case_name = partial_delete ? "partial_delete" : "no_delete";
    testutil_snprintf(home, sizeof(home), "%s.%s.%u", options->opts.home, case_name, repetition);
    testutil_recreate_dir(home);

    testutil_snprintf(
      config, sizeof(config), "create,cache_size=%s,statistics=(all)", options->cache_size);
    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    set_timestamp(conn, "oldest_timestamp", OLDEST_TS);
    populate(session, options->records);
    set_timestamp(conn, "stable_timestamp", INSERT_TS);
    testutil_check(session->checkpoint(session, NULL));

    if (partial_delete) {
        remove_sparse_keys(session, options->records);
        set_timestamp(conn, "stable_timestamp", STABLE_TS);
        testutil_check(session->checkpoint(session, NULL));
    }
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));

    testutil_snprintf(config, sizeof(config),
      "create,cache_size=%s,statistics=(all),checkpoint_cleanup=(wait=100000,file_wait_ms=0)",
      options->cache_size);
    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
    read_stats(session, &before);

    bench_timer_init(&timer, NULL);
    bench_timer_start(&timer, session);
    testutil_check(session->checkpoint(session, "debug=(checkpoint_cleanup=true)"));
    wait_for_cleanup(session, before.cleanup_success);
    bench_timer_stop(&timer, session);
    read_stats(session, &after);

    result->internal_reads = stat_delta(before.internal_reads, after.internal_reads);
    result->leaf_reads = stat_delta(before.leaf_reads, after.leaf_reads);
    result->bytes_read = stat_delta(before.bytes_read, after.bytes_read);
    result->cache_bytes = (int64_t)after.cache_bytes - (int64_t)before.cache_bytes;
    result->pages_visited = stat_delta(before.pages_visited, after.pages_visited);
    result->elapsed_us = timer.total_nsec / WT_THOUSAND;
    result->has_obsolete_tw_stat = before.has_obsolete_tw_stat && after.has_obsolete_tw_stat;
    result->obsolete_tw_reads = result->has_obsolete_tw_stat ?
      stat_delta(before.obsolete_tw_reads, after.obsolete_tw_reads) :
      0;
    if (partial_delete && result->has_obsolete_tw_stat)
        testutil_assert(result->obsolete_tw_reads == 0);

    deleted_key = DELETE_STRIDE;
    check_key(session, deleted_key - 1, 0);
    check_key(session, deleted_key, partial_delete ? WT_NOTFOUND : 0);
    check_key(session, deleted_key + 1, 0);

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));
    if (!options->opts.preserve)
        testutil_remove(home);
}

/*
 * metric_value --
 *     Return one result field for median calculation.
 */
static int64_t
metric_value(const RESULT *result, METRIC metric)
{
    switch (metric) {
    case METRIC_INTERNAL_READS:
        return ((int64_t)result->internal_reads);
    case METRIC_LEAF_READS:
        return ((int64_t)result->leaf_reads);
    case METRIC_BYTES_READ:
        return ((int64_t)result->bytes_read);
    case METRIC_CACHE_BYTES:
        return (result->cache_bytes);
    case METRIC_PAGES_VISITED:
        return ((int64_t)result->pages_visited);
    case METRIC_ELAPSED_US:
        return ((int64_t)result->elapsed_us);
    case METRIC_COUNT:
        break;
    }
    testutil_die(EINVAL, "invalid benchmark metric");
}

/*
 * median_metric --
 *     Return the median value for a result field.
 */
static int64_t
median_metric(const RESULT *results, uint32_t count, METRIC metric)
{
    int64_t value, values[MAX_REPEATS];
    uint32_t i, j;

    testutil_assert(count <= MAX_REPEATS);
    for (i = 0; i < count; ++i)
        values[i] = metric_value(&results[i], metric);

    for (i = 1; i < count; ++i) {
        value = values[i];
        for (j = i; j > 0 && values[j - 1] > value; --j)
            values[j] = values[j - 1];
        values[j] = value;
    }
    return (values[count / 2]);
}

/*
 * print_results --
 *     Print individual benchmark results and their median.
 */
static void
print_results(const char *case_name, const RESULT *results, uint32_t count)
{
    uint32_t i;

    for (i = 0; i < count; ++i) {
        printf("RESULT case=%s repetition=%u internal_reads=%" PRIu64 " leaf_reads=%" PRIu64
               " bytes_read=%" PRIu64 " cache_bytes=%" PRId64 " pages_visited=%" PRIu64
               " elapsed_us=%" PRIu64 " obsolete_tw_reads=",
          case_name, i, results[i].internal_reads, results[i].leaf_reads, results[i].bytes_read,
          results[i].cache_bytes, results[i].pages_visited, results[i].elapsed_us);
        if (results[i].has_obsolete_tw_stat)
            printf("%" PRIu64 "\n", results[i].obsolete_tw_reads);
        else
            printf("NA\n");
    }

    printf("MEDIAN case=%s internal_reads=%" PRIu64 " leaf_reads=%" PRIu64 " bytes_read=%" PRIu64
           " cache_bytes=%" PRId64 " pages_visited=%" PRIu64 " elapsed_us=%" PRIu64 "\n",
      case_name, (uint64_t)median_metric(results, count, METRIC_INTERNAL_READS),
      (uint64_t)median_metric(results, count, METRIC_LEAF_READS),
      (uint64_t)median_metric(results, count, METRIC_BYTES_READ),
      median_metric(results, count, METRIC_CACHE_BYTES),
      (uint64_t)median_metric(results, count, METRIC_PAGES_VISITED),
      (uint64_t)median_metric(results, count, METRIC_ELAPSED_US));
}

/*
 * print_comparison --
 *     Print the additional cleanup work for the partial-delete case over the no-delete control.
 */
static void
print_comparison(const RESULT *no_delete, const RESULT *partial_delete, uint32_t count)
{
    printf("CONTROL_DELTA internal_reads=%" PRId64 " leaf_reads=%" PRId64 " bytes_read=%" PRId64
           " cache_bytes=%" PRId64 " pages_visited=%" PRId64 " elapsed_us=%" PRId64 "\n",
      median_metric(partial_delete, count, METRIC_INTERNAL_READS) -
        median_metric(no_delete, count, METRIC_INTERNAL_READS),
      median_metric(partial_delete, count, METRIC_LEAF_READS) -
        median_metric(no_delete, count, METRIC_LEAF_READS),
      median_metric(partial_delete, count, METRIC_BYTES_READ) -
        median_metric(no_delete, count, METRIC_BYTES_READ),
      median_metric(partial_delete, count, METRIC_CACHE_BYTES) -
        median_metric(no_delete, count, METRIC_CACHE_BYTES),
      median_metric(partial_delete, count, METRIC_PAGES_VISITED) -
        median_metric(no_delete, count, METRIC_PAGES_VISITED),
      median_metric(partial_delete, count, METRIC_ELAPSED_US) -
        median_metric(no_delete, count, METRIC_ELAPSED_US));
}

/*
 * main --
 *     Run the checkpoint cleanup partial-delete benchmark.
 */
int
main(int argc, char *argv[])
{
    OPTIONS options;
    RESULT no_delete[MAX_REPEATS], partial_delete[MAX_REPEATS];
    uint32_t i;
    int ch;

    memset(&options, 0, sizeof(options));
    __wt_stream_set_line_buffer(stdout);
    testutil_set_progname(argv);
    testutil_parse_begin_opt(argc, argv, SHARED_PARSE_OPTIONS, &options.opts);
    while ((ch = __wt_getopt(progname, argc, argv, SHARED_PARSE_OPTIONS)) != EOF) {
        if (ch == 'c') {
            options.cache_size = dstrdup(__wt_optarg);
            options.cache_size_owned = true;
        } else if (ch == 'r') {
            unsigned long repeats;

            repeats = strtoul(__wt_optarg, NULL, 10);
            testutil_assert(repeats > 0 && repeats <= MAX_REPEATS);
            options.repeats = (uint32_t)repeats;
        } else if (testutil_parse_single_opt(&options.opts, ch) != 0)
            return (usage());
    }
    testutil_parse_end_opt(&options.opts);

    options.records = options.opts.nrecords == 0 ? DEFAULT_RECORDS : options.opts.nrecords;
    options.repeats = options.repeats == 0 ? DEFAULT_REPEATS : options.repeats;
    if (options.cache_size == NULL)
        options.cache_size = DEFAULT_CACHE_SIZE;
    testutil_assert(options.records >= 2 * DELETE_STRIDE && options.records % DELETE_STRIDE == 0);
    testutil_assert(options.repeats <= MAX_REPEATS);

    printf("CONFIG records=%" PRIu64 " cache_size=%s repeats=%u delete_stride=%d\n",
      options.records, options.cache_size, options.repeats, DELETE_STRIDE);
    for (i = 0; i < options.repeats; ++i) {
        run_case(&options, false, i, &no_delete[i]);
        run_case(&options, true, i, &partial_delete[i]);
    }

    print_results("no_delete", no_delete, options.repeats);
    print_results("partial_delete", partial_delete, options.repeats);
    print_comparison(no_delete, partial_delete, options.repeats);
    if (options.cache_size_owned)
        free((void *)options.cache_size);
    testutil_cleanup(&options.opts);
    return (EXIT_SUCCESS);
}
