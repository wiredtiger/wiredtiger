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
 * This test measures the cost of a metadata file scan, the work a connection performs when a user
 * opens a cursor on the "metadata:" URI and walks every entry. The scan time grows with the number
 * of collections in the database, so this is a proxy for how long startup-style metadata work takes
 * as collection counts climb (for example 100K versus 1M collections).
 *
 * Creating a million collections is slow, so the create and scan steps are separated. Populate a
 * database once with -c, then re-open it with -r as many times as needed to measure the scan. The
 * database is preserved on disk in those modes so it can be reused.
 *
 * With -G (disaggregated storage, which requires the PALite page-log extension to run locally) the
 * test instead compares two ways of reading the on-disk "checkpoint size" of every collection. A
 * disaggregated table has no local data file; its size lives in the most recent checkpoint entry of
 * its stable file's metadata. The two methods compared are:
 *
 *   1. Scan: walk the "metadata:" cursor once and extract the checkpoint size from every stable
 *      file's metadata value.
 *   2. Point lookups: for each collection open a statistics=(size) cursor on its stable file, which
 *      fast-paths to a single metadata point lookup per collection.
 *
 * Both methods must report the same total size; the test cross-checks that and prints the runtime
 * of each so the scan and the N point lookups can be compared.
 */

/* Command line options handled by the shared parser. */
#define SHARED_PARSE_OPTIONS "b:Gh:p"

#define DEFAULT_COLLECTIONS 100000
#define DEFAULT_SCAN_ITERATIONS 3
#define DEFAULT_SAMPLE_OPENS 1000

#define COLLECTION_URI_PREFIX "table:coll_"
#define COLLECTION_URI_FORMAT COLLECTION_URI_PREFIX "%010" PRIu64
#define TABLE_CONFIG "key_format=q,value_format=u"

#define CONN_CONFIG \
    "create,cache_size=2GB,session_max=1024,eviction=(threads_max=8),statistics=(fast)"

/*
 * Each disaggregated collection is a stable file: a ".wt_stable" file URI uses the disaggregated
 * block manager, so it has no local data file and its on-disk size lives only in the most recent
 * checkpoint of its metadata. This is the lightest object that exercises the disaggregated size
 * path, and unlike a full layered table its handle can be swept once checkpointed, so creating a
 * million of them does not pin a handle (or an in-memory ingest btree) per collection.
 */
#define DISAGG_STABLE_PREFIX "file:coll_"
#define DISAGG_STABLE_SUFFIX ".wt_stable"
#define DISAGG_STABLE_URI_FORMAT "file:coll_%010" PRIu64 DISAGG_STABLE_SUFFIX
#define DISAGG_STABLE_STAT_URI_FORMAT "statistics:file:coll_%010" PRIu64 DISAGG_STABLE_SUFFIX
#define DISAGG_TABLE_CONFIG "block_manager=disagg,key_format=S,value_format=S,log=(enabled=false)"

/*
 * Sweep idle dhandles during the create phase so creating a million collections does not keep every
 * file handle open at once. The compare phase never opens collection dhandles (the metadata scan
 * and the statistics=(size) fast path both read metadata only), so this only matters for create.
 */
#define DISAGG_CONN_CONFIG                                      \
    "create,cache_size=2GB,session_max=1024,statistics=(fast)," \
    "file_manager=(close_handle_minimum=250,close_idle_time=4,close_scan_interval=1)"

/* Default per-collection rows; a few rows are enough for a non-zero checkpoint size. */
#define DEFAULT_DISAGG_ROWS 10
#define DISAGG_VALUE_SIZE 100

/*
 * Checkpoint partway through a large create so dirty content and the per-checkpoint dhandle work
 * stay bounded. Combined with idle-handle sweeping this keeps a million-collection create from
 * holding every handle open at once.
 */
#define DISAGG_CHECKPOINT_INTERVAL 25000

static TEST_OPTS *opts, _opts;

extern int __wt_optind;
extern char *__wt_optarg;

static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * usage --
 *     Print a usage message and exit.
 */
static void
usage(void)
{
    fprintf(stderr,
      "usage: %s [-h home] [-p] [-c] [-r] [-G] [-b build_dir] [-C collections] [-I iterations] "
      "[-R rows] [-S sample_opens]\n"
      "  -c  create-only: populate the database with collections and exit (database preserved)\n"
      "  -r  scan-only: re-open an existing database and run the benchmark\n"
      "      With neither -c nor -r, the database is created and measured in a single run.\n"
      "  -G  disaggregated storage: compare the metadata scan against statistics=(size) point\n"
      "      lookups for reading each collection's checkpoint size (requires the PALite "
      "extension).\n"
      "      Create (-c) and compare (-r) use separate connections, so a slow create can be done\n"
      "      once and re-opened repeatedly, as for the local path.\n"
      "  -b  build directory (used to locate the PALite extension for -G; auto-deduced if "
      "omitted)\n"
      "  -C  number of collections to create (default %d)\n"
      "  -D  -G decompose: break the size-gathering scan into cursor walk, value, and size-\n"
      "      extraction costs instead of running the scan-vs-point-lookup comparison\n"
      "  -I  number of timed iterations of each method (default %d)\n"
      "  -M  -G all methods: on one warm database, compare point lookups, the WiredTiger internal\n"
      "      config parse, and the hand-rolled parse at 100/1000/10000 collections, with "
      "extrapolation\n"
      "  -L  -G scan length: measure only the first L collections, 0 for all (default 0). Lets "
      "the\n"
      "      scan be timed at several lengths against one large database to check linear scaling.\n"
      "  -R  rows written per collection in -G create (default %d)\n"
      "  -S  number of random single-collection cursor opens to time, 0 to disable (default %d)\n"
      "      (ignored in -G mode)\n",
      progname, DEFAULT_COLLECTIONS, DEFAULT_SCAN_ITERATIONS, DEFAULT_DISAGG_ROWS,
      DEFAULT_SAMPLE_OPENS);
    exit(EXIT_FAILURE);
}

/*
 * create_collections --
 *     Recreate the home directory and populate it with the requested number of collections,
 *     checkpointing once at the end so the metadata is durable across a re-open.
 */
static void
create_collections(uint64_t num)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    double secs;
    uint64_t i, interval, last, now, start;
    char uri[64];

    testutil_recreate_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL, CONN_CONFIG, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    printf("Creating %" PRIu64 " collections in %s\n", num, opts->home);
    interval = num / 20;
    if (interval == 0)
        interval = 1;

    start = last = testutil_time_us(session);
    for (i = 0; i < num; i++) {
        testutil_snprintf(uri, sizeof(uri), COLLECTION_URI_FORMAT, i);
        testutil_check(session->create(session, uri, TABLE_CONFIG));

        if ((i + 1) % interval == 0) {
            now = testutil_time_us(session);
            secs = (now - last) / (double)WT_MILLION;
            printf("  created %" PRIu64 "/%" PRIu64 " (%.0f collections/s, %.1f s elapsed)\n",
              i + 1, num, secs > 0 ? interval / secs : 0.0, (now - start) / (double)WT_MILLION);
            last = now;
        }
    }

    printf("Checkpointing %" PRIu64 " collections...\n", num);
    testutil_check(session->checkpoint(session, NULL));
    printf("Created and checkpointed in %.1f s\n",
      (testutil_time_us(session) - start) / (double)WT_MILLION);

    testutil_check(conn->close(conn, NULL));
}

/*
 * metadata_scan_once --
 *     Open a cursor on the metadata and walk every entry, returning the number of collection tables
 *     seen. The first scan after a connection open is cold; later scans run against a warm cache.
 */
static uint64_t
metadata_scan_once(WT_SESSION *session, uint64_t iteration)
{
    WT_CURSOR *cursor;
    double secs;
    uint64_t coll, elapsed, entries, start, value_bytes;
    int ret;
    const char *key, *value;

    coll = entries = value_bytes = 0;
    start = testutil_time_us(session);
    testutil_check(session->open_cursor(session, WT_METADATA_URI, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        testutil_check(cursor->get_value(cursor, &value));
        ++entries;
        value_bytes += strlen(value);
        if (strncmp(key, COLLECTION_URI_PREFIX, sizeof(COLLECTION_URI_PREFIX) - 1) == 0)
            ++coll;
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
    elapsed = testutil_time_us(session) - start;
    secs = elapsed / (double)WT_MILLION;

    printf("  scan %" PRIu64 " %s: %.3f s  entries=%" PRIu64 " collections=%" PRIu64
           " config=%.1fMB  (%.2f us/entry)\n",
      iteration, iteration == 0 ? "[cold]" : "[warm]", secs, entries, coll,
      value_bytes / (1024.0 * 1024.0), entries > 0 ? (double)elapsed / (double)entries : 0.0);

    return (coll);
}

/*
 * sample_collection_opens --
 *     Time opening (and closing) cursors on randomly chosen collections. This is the per-collection
 *     analogue of the metadata scan: it measures what a user pays to open one collection's cursor.
 */
static void
sample_collection_opens(WT_SESSION *session, uint64_t samples, uint64_t coll_count)
{
    WT_CURSOR *cursor;
    uint64_t elapsed, i, id, max, min, start, total;
    char uri[64];

    max = total = 0;
    min = UINT64_MAX;
    for (i = 0; i < samples; i++) {
        id =
          (((uint64_t)testutil_random(&opts->data_rnd) << 32) | testutil_random(&opts->data_rnd)) %
          coll_count;
        testutil_snprintf(uri, sizeof(uri), COLLECTION_URI_FORMAT, id);

        start = testutil_time_us(session);
        testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
        elapsed = testutil_time_us(session) - start;
        testutil_check(cursor->close(cursor));

        total += elapsed;
        if (elapsed < min)
            min = elapsed;
        if (elapsed > max)
            max = elapsed;
    }

    printf("  opened %" PRIu64 " random collection cursors in %.3f s\n", samples,
      total / (double)WT_MILLION);
    printf("       avg %.1f us  min %" PRIu64 " us  max %" PRIu64 " us\n", (double)total / samples,
      min, max);
}

/*
 * scan_database --
 *     Open an existing database and run the metadata scan benchmark against it.
 */
static void
scan_database(uint64_t iterations, uint64_t samples)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    uint64_t coll_count, i;

    testutil_check(wiredtiger_open(opts->home, NULL, CONN_CONFIG, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    printf("Scanning metadata in %s\n", opts->home);
    coll_count = 0;
    for (i = 0; i < iterations; i++)
        coll_count = metadata_scan_once(session, i);

    if (samples > 0) {
        if (coll_count == 0)
            printf("  no collections found, skipping single-cursor open sampling\n");
        else
            sample_collection_opens(session, samples, coll_count);
    }

    testutil_check(conn->close(conn, NULL));
}

/*
 * disagg_checkpoint --
 *     Advance the stable timestamp to cover everything committed so far and checkpoint, so the
 *     stable files created up to this point gain a checkpoint with a recorded size.
 */
static void
disagg_checkpoint(WT_SESSION *session, WT_CONNECTION *conn, uint64_t committed)
{
    char tscfg[64];

    testutil_snprintf(tscfg, sizeof(tscfg), "stable_timestamp=%" PRIx64, committed);
    testutil_check(conn->set_timestamp(conn, tscfg));
    testutil_check(session->checkpoint(session, NULL));
}

/*
 * disagg_create --
 *     Create the requested number of disaggregated collections, writing a little timestamped data
 *     into each so its stable file has a non-zero checkpoint size. Checkpoints periodically so a
 *     very large create (a million collections) keeps memory and per-checkpoint work bounded, and
 *     is re-openable afterwards. Runs in its own connection.
 */
static void
disagg_create(uint64_t num, uint64_t rows)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    double done, rate;
    uint64_t i, j, interval, now, start;
    char key[64], tscfg[64], uri[64], value[DISAGG_VALUE_SIZE + 1];

    memset(value, 'x', DISAGG_VALUE_SIZE);
    value[DISAGG_VALUE_SIZE] = '\0';

    opts->disagg.page_log_home = opts->home;
    testutil_recreate_dir(opts->home);
    testutil_wiredtiger_open(opts, opts->home, DISAGG_CONN_CONFIG, NULL, &conn, false, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    printf("Creating %" PRIu64 " disaggregated collections (%" PRIu64 " rows each) in %s\n", num,
      rows, opts->home);
    interval = num / 20;
    if (interval == 0)
        interval = 1;

    start = testutil_time_us(session);
    for (i = 0; i < num; i++) {
        testutil_snprintf(uri, sizeof(uri), DISAGG_STABLE_URI_FORMAT, i);
        testutil_check(session->create(session, uri, DISAGG_TABLE_CONFIG));

        testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
        testutil_check(session->begin_transaction(session, NULL));
        for (j = 0; j < rows; j++) {
            testutil_snprintf(key, sizeof(key), "key%010" PRIu64, j);
            cursor->set_key(cursor, key);
            cursor->set_value(cursor, value);
            testutil_check(cursor->insert(cursor));
        }
        /* Commit at a unique non-zero timestamp; the stable timestamp is advanced at checkpoint. */
        testutil_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, i + 1);
        testutil_check(session->commit_transaction(session, tscfg));
        testutil_check(cursor->close(cursor));

        /* Periodic checkpoint, except on the last collection (a final checkpoint follows). */
        if ((i + 1) % DISAGG_CHECKPOINT_INTERVAL == 0 && (i + 1) < num) {
            disagg_checkpoint(session, conn, i + 1);
            printf("  checkpointed at %" PRIu64 " collections\n", i + 1);
        }

        if ((i + 1) % interval == 0) {
            now = testutil_time_us(session);
            done = (now - start) / (double)WT_MILLION;
            rate = done > 0 ? (i + 1) / done : 0.0;
            printf("  created %" PRIu64 "/%" PRIu64 " (%.0f/s, %.1f s elapsed, ETA %.0f s)\n",
              i + 1, num, rate, done, rate > 0 ? (num - (i + 1)) / rate : 0.0);
        }
    }

    printf("Final checkpoint...\n");
    disagg_checkpoint(session, conn, num);
    printf("Created, populated and checkpointed %" PRIu64 " collections in %.1f s\n", num,
      (testutil_time_us(session) - start) / (double)WT_MILLION);

    testutil_check(conn->close(conn, NULL));
}

/*
 * disagg_size_via_scan --
 *     Method 1: walk the metadata and sum the checkpoint size of every stable file. With a non-zero
 *     limit, stop after that many collections; because the collection stable files are contiguous
 *     and key-sorted, this measures the scan cost for the first "limit" collections against an
 *     existing database. Returns the elapsed microseconds and reports the collection count and
 *     total size through the out-params.
 */
static uint64_t
disagg_size_via_scan(
  WT_SESSION *session, uint64_t iteration, uint64_t limit, uint64_t *countp, uint64_t *total_sizep)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session_impl;
    uint64_t ckpt_size, count, elapsed, start, total_size;
    int ret;
    const char *key, *value;

    session_impl = (WT_SESSION_IMPL *)session;
    count = total_size = 0;

    start = testutil_time_us(session);
    testutil_check(session->open_cursor(session, WT_METADATA_URI, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        /*
         * Match this test's collection stable files only. A metadata scan also sees internal stable
         * files (the history store and shared metadata), but the point-lookup method queries
         * exactly the collections created here, so restrict the scan to the same set for a fair
         * comparison.
         */
        if (!WT_PREFIX_MATCH(key, DISAGG_STABLE_PREFIX) ||
          !WT_SUFFIX_MATCH(key, DISAGG_STABLE_SUFFIX))
            continue;
        testutil_check(cursor->get_value(cursor, &value));

        /* A stable file without a checkpoint yet returns WT_NOTFOUND; skip it. */
        ret = __wt_ckpt_last_size(session_impl, value, &ckpt_size);
        if (ret == WT_NOTFOUND)
            continue;
        testutil_check(ret);
        total_size += ckpt_size;
        ++count;

        /* Stop early when a scan length is requested, leaving the rest of the metadata unwalked. */
        if (limit != 0 && count >= limit)
            break;
    }
    testutil_assert(ret == WT_NOTFOUND || (limit != 0 && count >= limit));
    testutil_check(cursor->close(cursor));
    elapsed = testutil_time_us(session) - start;

    printf("    iter %" PRIu64 " %s: %.4f s  collections=%" PRIu64 " total_size=%" PRIu64 "\n",
      iteration, iteration == 0 ? "[cold]" : "[warm]", elapsed / (double)WT_MILLION, count,
      total_size);

    *countp = count;
    *total_sizep = total_size;
    return (elapsed);
}

/*
 * disagg_size_via_stat --
 *     Method 2: for each collection open a statistics=(size) cursor on its stable file and read the
 *     block_size statistic, which fast-paths to a single metadata point lookup. Returns the elapsed
 *     microseconds and reports the collection count and total size through the out-params.
 */
static uint64_t
disagg_size_via_stat(
  WT_SESSION *session, uint64_t iteration, uint64_t num, uint64_t *countp, uint64_t *total_sizep)
{
    WT_CURSOR *cursor;
    uint64_t count, elapsed, i, start, total_size;
    int64_t value;
    char uri[64];
    const char *desc, *pvalue;

    count = total_size = 0;

    start = testutil_time_us(session);
    for (i = 0; i < num; i++) {
        testutil_snprintf(uri, sizeof(uri), DISAGG_STABLE_STAT_URI_FORMAT, i);
        testutil_check(session->open_cursor(session, uri, NULL, "statistics=(size)", &cursor));
        cursor->set_key(cursor, WT_STAT_DSRC_BLOCK_SIZE);
        testutil_check(cursor->search(cursor));
        testutil_check(cursor->get_value(cursor, &desc, &pvalue, &value));
        testutil_check(cursor->close(cursor));

        if (value > 0) {
            total_size += (uint64_t)value;
            ++count;
        }
    }
    elapsed = testutil_time_us(session) - start;

    printf("    iter %" PRIu64 " %s: %.4f s  collections=%" PRIu64 " total_size=%" PRIu64
           "  (%.2f us/lookup)\n",
      iteration, iteration == 0 ? "[cold]" : "[warm]", elapsed / (double)WT_MILLION, count,
      total_size, num > 0 ? (double)elapsed / (double)num : 0.0);

    *countp = count;
    *total_sizep = total_size;
    return (elapsed);
}

/*
 * extract_ckpt_size_fast --
 *     Pull the most recent checkpoint's size out of a metadata config string with a targeted string
 *     scan, the way a consumer outside the WiredTiger API would: find the checkpoint sub-config,
 *     then the size token inside it. This avoids the full checkpoint parse (address hex-decode,
 *     timestamps, block-mod lists, allocations) that loading the whole checkpoint performs. The
 *     metadata keeps a single nameless checkpoint per file, so the lone size token is the answer.
 */
static int
extract_ckpt_size_fast(const char *value, uint64_t *sizep)
{
    const char *ckpt, *size;

    *sizep = 0;
    if ((ckpt = strstr(value, "checkpoint=(")) == NULL)
        return (WT_NOTFOUND);
    /* The leading comma anchors the checkpoint's own size, not allocation_size or similar. */
    if ((size = strstr(ckpt, ",size=")) == NULL)
        return (WT_NOTFOUND);
    *sizep = (uint64_t)strtoull(size + strlen(",size="), NULL, 10);
    return (0);
}

typedef enum {
    SCAN_WALK,       /* Cursor walk and key filter only. */
    SCAN_VALUE,      /* Also materialize each entry's value string. */
    SCAN_FULL_PARSE, /* Also load the checkpoint via the internal helper (current Method 1). */
    SCAN_FAST_PARSE  /* Also extract the size with a targeted string scan. */
} scan_work;

/*
 * disagg_scan_pass --
 *     Run one metadata scan, doing a chosen amount of per-entry work, and return the elapsed
 *     microseconds. The work levels isolate where a size-gathering scan spends its time: walking
 *     the cursor, materializing the value, fully loading the checkpoint, or extracting the size
 *     with a targeted string scan.
 */
static uint64_t
disagg_scan_pass(
  WT_SESSION *session, scan_work work, uint64_t limit, uint64_t *countp, uint64_t *total_sizep)
{
    WT_CURSOR *cursor;
    WT_SESSION_IMPL *session_impl;
    uint64_t ckpt_size, count, elapsed, start, total_size;
    int ret;
    const char *key, *value;

    session_impl = (WT_SESSION_IMPL *)session;
    count = total_size = 0;

    start = testutil_time_us(session);
    testutil_check(session->open_cursor(session, WT_METADATA_URI, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        if (!WT_PREFIX_MATCH(key, DISAGG_STABLE_PREFIX) ||
          !WT_SUFFIX_MATCH(key, DISAGG_STABLE_SUFFIX))
            continue;

        if (work >= SCAN_VALUE)
            testutil_check(cursor->get_value(cursor, &value));
        if (work == SCAN_FULL_PARSE) {
            ret = __wt_ckpt_last_size(session_impl, value, &ckpt_size);
            if (ret == WT_NOTFOUND)
                continue;
            testutil_check(ret);
            total_size += ckpt_size;
        } else if (work == SCAN_FAST_PARSE) {
            if (extract_ckpt_size_fast(value, &ckpt_size) == WT_NOTFOUND)
                continue;
            total_size += ckpt_size;
        }
        ++count;
        if (limit != 0 && count >= limit)
            break;
    }
    testutil_assert(ret == WT_NOTFOUND || (limit != 0 && count >= limit));
    testutil_check(cursor->close(cursor));
    elapsed = testutil_time_us(session) - start;

    *countp = count;
    *total_sizep = total_size;
    return (elapsed);
}

/*
 * disagg_decompose --
 *     Re-open an existing disaggregated database and break a size-gathering metadata scan into its
 *     cost components, so the per-collection time can be attributed to the cursor walk, value
 *     materialization, and the size extraction itself (full checkpoint load versus a targeted
 *     string scan). Runs in its own connection.
 */
static void
disagg_decompose(uint64_t iterations, uint64_t scan_limit)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    uint64_t best[4];
    uint64_t count, full_size, i, size, us;
    scan_work work;
    static const char *const names[4] = {"walk only (cursor next + key filter)",
      "  + materialize value string",
      "  + full checkpoint load (__wt_ckpt_last_size, current Method 1)",
      "  + targeted size extraction (external-style string scan)"};

    opts->disagg.page_log_home = opts->home;
    testutil_wiredtiger_open(opts, opts->home, DISAGG_CONN_CONFIG, NULL, &conn, false, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Warm the cache so the breakdown reflects steady-state work, not the cold read. */
    (void)disagg_scan_pass(session, SCAN_FULL_PARSE, scan_limit, &count, &full_size);

    for (work = SCAN_WALK; work <= SCAN_FAST_PARSE; work++) {
        best[work] = UINT64_MAX;
        for (i = 0; i < iterations; i++) {
            us = disagg_scan_pass(session, work, scan_limit, &count, &size);
            /* The two parsing passes must agree with the full load on the total size. */
            if (work == SCAN_FAST_PARSE)
                testutil_assertfmt(size == full_size,
                  "targeted extraction disagrees: fast=%" PRIu64 " full=%" PRIu64, size, full_size);
            if (us < best[work])
                best[work] = us;
        }
    }

    printf("\nScan cost breakdown over %" PRIu64 " collections (warm, best of %" PRIu64 "):\n",
      count, iterations);
    for (work = SCAN_WALK; work <= SCAN_FAST_PARSE; work++)
        printf("  %-62s %.4f s  (%.3f us/coll)\n", names[work], best[work] / (double)WT_MILLION,
          count > 0 ? (double)best[work] / (double)count : 0.0);
    printf("  ---\n");
    printf("  value materialization adds   %.3f us/coll\n",
      count > 0 ? (double)(best[SCAN_VALUE] - best[SCAN_WALK]) / (double)count : 0.0);
    printf("  full checkpoint load adds    %.3f us/coll\n",
      count > 0 ? (double)(best[SCAN_FULL_PARSE] - best[SCAN_VALUE]) / (double)count : 0.0);
    printf("  targeted extraction adds     %.3f us/coll\n",
      count > 0 ? (double)(best[SCAN_FAST_PARSE] - best[SCAN_VALUE]) / (double)count : 0.0);

    testutil_check(conn->close(conn, NULL));
}

/*
 * report_compare --
 *     Print the cold and warm-best runtime of each method and how the point lookups compare to the
 *     scan.
 */
static void
report_compare(const char *label, uint64_t scan_us, uint64_t stat_us)
{
    double scan_s, stat_s;

    scan_s = scan_us / (double)WT_MILLION;
    stat_s = stat_us / (double)WT_MILLION;
    printf("  %-7s scan=%.4f s   point_lookups=%.4f s   ", label, scan_s, stat_s);
    if (stat_us == 0 || scan_us == 0)
        printf("(durations too small to compare)\n");
    else if (stat_us <= scan_us)
        printf("point lookups %.2fx faster\n", (double)scan_us / (double)stat_us);
    else
        printf("point lookups %.2fx slower\n", (double)stat_us / (double)scan_us);
}

/*
 * disagg_report_entry_size --
 *     Report the average metadata key and value string length over the first "sample" collection
 *     entries. This is the exact size of the metadata records the scan walks, which is more useful
 *     than estimating it from the metadata file size that btree overhead and free space inflate.
 */
static void
disagg_report_entry_size(WT_SESSION *session, uint64_t sample)
{
    WT_CURSOR *cursor;
    uint64_t count, key_bytes, value_bytes;
    int ret;
    const char *key, *value;

    count = key_bytes = value_bytes = 0;

    testutil_check(session->open_cursor(session, WT_METADATA_URI, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        if (!WT_PREFIX_MATCH(key, DISAGG_STABLE_PREFIX) ||
          !WT_SUFFIX_MATCH(key, DISAGG_STABLE_SUFFIX))
            continue;
        testutil_check(cursor->get_value(cursor, &value));
        key_bytes += (uint64_t)strlen(key);
        value_bytes += (uint64_t)strlen(value);
        if (++count >= sample)
            break;
    }
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));

    if (count == 0) {
        printf("\nNo collection stable files found to sample.\n");
        return;
    }
    printf("\nMetadata entry size over %" PRIu64 " collection entries:\n", count);
    printf("  avg value=%.1f  key=%.1f  total=%.1f bytes\n", (double)value_bytes / (double)count,
      (double)key_bytes / (double)count, (double)(key_bytes + value_bytes) / (double)count);
}

/*
 * disagg_compare --
 *     Re-open an existing disaggregated database and compare reading every collection's checkpoint
 *     size via a single metadata scan against per-collection statistics=(size) point lookups. The
 *     collection count is discovered from the metadata scan so this works on a database created by
 *     an earlier run; a non-zero scan_limit caps how many collections are measured, which lets the
 *     scan be timed at several lengths against one large database. Runs in its own connection.
 */
static void
disagg_compare(uint64_t iterations, uint64_t scan_limit)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    uint64_t i, num, scan_cold, scan_count, scan_size, scan_warm, stat_cold, stat_count, stat_size,
      stat_warm, us;

    opts->disagg.page_log_home = opts->home;
    testutil_wiredtiger_open(opts, opts->home, DISAGG_CONN_CONFIG, NULL, &conn, false, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Report the average metadata entry size before the size-method comparison. */
    disagg_report_entry_size(session, 100);

    scan_cold = scan_warm = scan_count = scan_size = 0;
    stat_cold = stat_warm = stat_count = stat_size = 0;

    if (scan_limit != 0)
        printf("\nScan length limited to the first %" PRIu64 " collections\n", scan_limit);
    printf("\nMethod 1 - one metadata scan extracting every stable file's checkpoint size:\n");
    for (i = 0; i < iterations; i++) {
        us = disagg_size_via_scan(session, i, scan_limit, &scan_count, &scan_size);
        if (i == 0)
            scan_cold = scan_warm = us;
        else if (us < scan_warm)
            scan_warm = us;
    }

    /* The scan discovers how many collections it measured; the point lookups query exactly that
     * many. */
    num = scan_count;
    printf("\nMethod 2 - %" PRIu64 " statistics=(size) point lookups (one per collection):\n", num);
    for (i = 0; i < iterations; i++) {
        us = disagg_size_via_stat(session, i, num, &stat_count, &stat_size);
        if (i == 0)
            stat_cold = stat_warm = us;
        else if (us < stat_warm)
            stat_warm = us;
    }

    printf("\nResults over %" PRIu64 " collections:\n", num);
    printf("  cross-check: scan total_size=%" PRIu64 " (%" PRIu64
           " collections), point-lookup "
           "total_size=%" PRIu64 " (%" PRIu64 " collections)  [%s]\n",
      scan_size, scan_count, stat_size, stat_count,
      (scan_size == stat_size && scan_count == stat_count) ? "OK" : "MISMATCH");
    testutil_assertfmt(scan_size == stat_size && scan_count == stat_count,
      "size methods disagree: scan=%" PRIu64 "/%" PRIu64 " stat=%" PRIu64 "/%" PRIu64, scan_size,
      scan_count, stat_size, stat_count);
    report_compare("cold", scan_cold, stat_cold);
    if (iterations > 1)
        report_compare("warm", scan_warm, stat_warm);

    testutil_check(conn->close(conn, NULL));
}

/*
 * disagg_compare_all --
 *     Re-open an existing disaggregated database and, on one warm connection, time the three ways
 *     of reading every collection's checkpoint size at a fixed set of collection counts: statistics
 *     point lookups, the WiredTiger internal config parse, and the hand-rolled targeted parse. The
 *     collection stable files are contiguous and key-sorted, so measuring the first L of them is a
 *     valid subset for all three methods. Prints a table and a linear extrapolation. Runs in its
 *     own connection.
 */
static void
disagg_compare_all(uint64_t iterations)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    static const uint64_t lengths[] = {100, 1000, 10000};
    static const uint64_t targets[] = {100000, 200000, 400000, 600000};
    double m1_per[WT_ELEMENTS(lengths)], m2_per[WT_ELEMENTS(lengths)],
      point_per[WT_ELEMENTS(lengths)];
    uint64_t avail, count, i, k, limit, m1_size, m1_us, m2_size, m2_us,
      measured[WT_ELEMENTS(lengths)], point_size, point_us, total, us;

    opts->disagg.page_log_home = opts->home;
    testutil_wiredtiger_open(opts, opts->home, DISAGG_CONN_CONFIG, NULL, &conn, false, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Report the average metadata entry size, and warm the cache with a full-parse scan. */
    disagg_report_entry_size(session, 100);
    (void)disagg_scan_pass(session, SCAN_FULL_PARSE, 0, &avail, &total);
    printf("\nDatabase holds %" PRIu64 " collection stable files (warming scan)\n", avail);

    for (k = 0; k < WT_ELEMENTS(lengths); k++) {
        limit = lengths[k] > avail ? avail : lengths[k];
        measured[k] = limit;

        /* Point lookups: one statistics=(size) cursor per collection, best of iterations. */
        point_us = UINT64_MAX;
        point_size = 0;
        for (i = 0; i < iterations; i++) {
            us = disagg_size_via_stat(session, i, limit, &count, &point_size);
            if (us < point_us)
                point_us = us;
        }

        /* Method 1: single scan, WiredTiger internal checkpoint load per entry. */
        m1_us = UINT64_MAX;
        m1_size = 0;
        for (i = 0; i < iterations; i++) {
            us = disagg_scan_pass(session, SCAN_FULL_PARSE, limit, &count, &m1_size);
            if (us < m1_us)
                m1_us = us;
        }

        /* Method 2: single scan, hand-rolled targeted size extraction per entry. */
        m2_us = UINT64_MAX;
        m2_size = 0;
        for (i = 0; i < iterations; i++) {
            us = disagg_scan_pass(session, SCAN_FAST_PARSE, limit, &count, &m2_size);
            if (us < m2_us)
                m2_us = us;
        }

        /* All three methods must agree on the aggregate size for the same collection set. */
        testutil_assertfmt(point_size == m1_size && m1_size == m2_size,
          "size methods disagree at %" PRIu64 " collections: point=%" PRIu64 " m1=%" PRIu64
          " m2=%" PRIu64,
          limit, point_size, m1_size, m2_size);

        point_per[k] = limit > 0 ? (double)point_us / (double)limit : 0.0;
        m1_per[k] = limit > 0 ? (double)m1_us / (double)limit : 0.0;
        m2_per[k] = limit > 0 ? (double)m2_us / (double)limit : 0.0;
    }

    printf("\nTime to read every collection's checkpoint size (warm, best of %" PRIu64 "):\n",
      iterations);
    printf("  %-12s  %-24s  %-24s  %-24s\n", "collections", "point lookups", "Method 1 (WT parse)",
      "Method 2 (hand-rolled)");
    for (k = 0; k < WT_ELEMENTS(lengths); k++)
        printf("  %-12" PRIu64 "  %9.3f ms %6.3f us/c  %9.3f ms %6.3f us/c  %9.3f ms %6.3f us/c\n",
          measured[k], measured[k] * point_per[k] / 1000.0, point_per[k],
          measured[k] * m1_per[k] / 1000.0, m1_per[k], measured[k] * m2_per[k] / 1000.0, m2_per[k]);

    /* Extrapolate linearly from the largest measured length (each method is O(collections)). */
    k = WT_ELEMENTS(lengths) - 1;
    printf("\nExtrapolation (linear, from the %" PRIu64 "-collection per-collection cost):\n",
      measured[k]);
    printf("  %-12s  %-16s  %-16s  %-16s\n", "collections", "point lookups", "Method 1 (WT parse)",
      "Method 2 (hand-rolled)");
    for (i = 0; i < WT_ELEMENTS(targets); i++)
        printf("  %-12" PRIu64 "  %13.2f s  %13.2f s  %13.2f s\n", targets[i],
          targets[i] * point_per[k] / (double)WT_MILLION,
          targets[i] * m1_per[k] / (double)WT_MILLION, targets[i] * m2_per[k] / (double)WT_MILLION);

    testutil_check(conn->close(conn, NULL));
}

/*
 * main --
 *     Parse the arguments and drive the create and/or scan phases.
 */
int
main(int argc, char *argv[])
{
    uint64_t collections, iterations, rows, samples, scan_limit;
    int ch;
    bool all_methods, create_only, decompose, scan_only;

    (void)testutil_set_progname(argv);
    __wt_stream_set_line_buffer(stderr);
    __wt_stream_set_line_buffer(stdout);

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));

    collections = DEFAULT_COLLECTIONS;
    iterations = DEFAULT_SCAN_ITERATIONS;
    rows = DEFAULT_DISAGG_ROWS;
    samples = DEFAULT_SAMPLE_OPENS;
    scan_limit = 0;
    all_methods = create_only = decompose = scan_only = false;

    testutil_parse_begin_opt(argc, argv, SHARED_PARSE_OPTIONS, opts);
    while ((ch = __wt_getopt(progname, argc, argv, "cC:DI:L:MrR:S:" SHARED_PARSE_OPTIONS)) != EOF)
        switch (ch) {
        case 'c':
            create_only = true;
            break;
        case 'C':
            collections = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;
        case 'D':
            decompose = true;
            break;
        case 'I':
            iterations = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;
        case 'M':
            all_methods = true;
            break;
        case 'L':
            scan_limit = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;
        case 'r':
            scan_only = true;
            break;
        case 'R':
            rows = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;
        case 'S':
            samples = (uint64_t)strtoull(__wt_optarg, NULL, 10);
            break;
        default:
            if (testutil_parse_single_opt(opts, ch) != 0)
                usage();
        }
    argc -= __wt_optind;
    if (argc != 0)
        usage();

    if (create_only && scan_only) {
        fprintf(
          stderr, "%s: -c (create-only) and -r (scan-only) are mutually exclusive\n", progname);
        usage();
    }
    if (collections == 0) {
        fprintf(stderr, "%s: the collection count (-C) must be greater than zero\n", progname);
        usage();
    }

    testutil_parse_end_opt(opts);

    /*
     * Re-opening an existing database (any mode other than create-and-scan) must not delete it, so
     * a database created once can be measured repeatedly. Creating a million collections is slow,
     * so this matters for both the local and disaggregated paths.
     */
    if (create_only || scan_only)
        opts->preserve = true;

    if (scan_only && !testutil_exists(NULL, opts->home)) {
        fprintf(stderr,
          "%s: scan-only mode (-r) requires an existing database at %s; create one first with -c\n",
          progname, opts->home);
        exit(EXIT_FAILURE);
    }

    /*
     * Disaggregated mode compares two ways of reading each collection's checkpoint size: a single
     * metadata scan versus per-collection statistics=(size) point lookups. Create and compare use
     * separate connections, so -c/-r split the slow create from the re-openable measurement just
     * like the local path. The compare phase reads only local metadata, which survives a clean
     * close, so re-opening as a leader is sufficient.
     */
    if (opts->disagg.is_enabled) {
        if (!scan_only)
            disagg_create(collections, rows);
        if (!create_only) {
            if (all_methods)
                disagg_compare_all(iterations);
            else if (decompose)
                disagg_decompose(iterations, scan_limit);
            else
                disagg_compare(iterations, scan_limit);
        }
    } else {
        if (!scan_only)
            create_collections(collections);
        if (!create_only)
            scan_database(iterations, samples);
    }

    if (opts->preserve)
        printf("Database preserved at %s\n", opts->home);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}
