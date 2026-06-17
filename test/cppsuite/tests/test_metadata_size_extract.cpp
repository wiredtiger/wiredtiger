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

/*
 * This test extracts every disaggregated collection's on-disk size from the latest checkpoint by
 * walking the metadata, the way a consumer outside the WiredTiger library (such as MongoDB) would
 * have to do it. Disaggregated storage is the case that matters here: a disaggregated table has no
 * local data file, so its size lives only in the most recent checkpoint entry of its stable file's
 * metadata, and an external reader has to dig it out of that config string.
 *
 * The test creates a handful of disaggregated collections (requires the PALite page-log extension),
 * writes a little timestamped data into each, checkpoints, and re-opens the database as a fresh
 * connection (the startup case). It then opens a cursor on the "metadata:" URI and, for every
 * stable file, pulls the most recent checkpoint's size out of the metadata config string using
 * nothing but plain string parsing -- no WiredTiger function interprets the config or computes the
 * size. The metadata value is a string, and that string is all MongoDB would have to work with.
 *
 * Each parsed size is cross-checked against the statistics=(size) cursor as a test oracle. For
 * disaggregated storage this match is exact: the size statistic is itself read straight from the
 * metadata checkpoint size (__wt_block_disagg_ckpt_size -> __wt_ckpt_last_size), so the statistics
 * fast path and this hand-rolled parse return the same number. (That differs from the local block
 * manager, whose live "checkpoint size" statistic omits the root page and so would not match.)
 */

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/storage/connection_manager.h"
#include "src/storage/scoped_cursor.h"
#include "src/storage/scoped_session.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

#include <array>
#include <charconv>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

using namespace test_harness;

static constexpr int NUM_COLLECTIONS = 10;
static constexpr int ROWS_PER_COLLECTION = 100;
static constexpr int VALUE_BYTES = 100;

/*
 * Each collection is a disaggregated stable file: a ".wt_stable" file URI uses the disaggregated
 * block manager, so it has no local data file and its on-disk size lives only in the most recent
 * checkpoint of its metadata.
 */
static const std::string TABLE_CONFIG =
  "block_manager=disagg,key_format=S,value_format=S,log=(enabled=false)";
static constexpr std::string_view COLLECTION_PREFIX = "file:coll_";
static constexpr std::string_view COLLECTION_SUFFIX = ".wt_stable";

/*
 * collection_uri --
 *     Build the stable-file URI for a collection index.
 */
static std::string
collection_uri(int i)
{
    return std::string(COLLECTION_PREFIX) + std::to_string(i) + std::string(COLLECTION_SUFFIX);
}

/*
 * to_hex --
 *     Render a value as a WiredTiger timestamp string (timestamps are parsed as hex).
 */
static std::string
to_hex(uint64_t value)
{
    char buf[2 * sizeof(uint64_t) + 1];
    auto [end, ec] = std::to_chars(buf, buf + sizeof(buf), value, 16);
    testutil_assert(ec == std::errc());
    return std::string(buf, end);
}

/*
 * extract_latest_checkpoint --
 *     Find the most recent checkpoint in a file's metadata config string and return its size and
 *     order. The metadata stores a checkpoint group, checkpoint=(...), holding one block per
 *     checkpoint; each block lists its "order" before its "size", so the latest checkpoint is the
 *     block with the highest order. This is done with plain string parsing only: a consumer with no
 *     access to the WiredTiger API, walking the metadata itself, has only the config string to work
 *     with. Returning the order as well lets the caller confirm a re-checkpoint advanced it, i.e.
 *     that the latest checkpoint is genuinely being selected rather than a stale one.
 */
static bool
extract_latest_checkpoint(std::string_view value, uint64_t *sizep, long *orderp)
{
    static constexpr std::string_view group_tok = "checkpoint=(";
    static constexpr std::string_view order_tok = ",order=";
    static constexpr std::string_view size_tok = ",size=";

    *sizep = 0;
    *orderp = -1;

    /* Locate the checkpoint group. A stable file without a checkpoint yet has none. */
    size_t pos = value.find(group_tok);
    if (pos == std::string_view::npos)
        return (false);
    pos += group_tok.size();

    /* Bound the search to the checkpoint group by matching the parenthesis we are inside of. */
    int depth = 1;
    size_t scan = pos;
    for (; scan < value.size() && depth > 0; ++scan) {
        if (value[scan] == '(')
            ++depth;
        else if (value[scan] == ')')
            --depth;
    }
    std::string_view group = value.substr(pos, scan - pos);

    /*
     * Each checkpoint block lists ",order=N" ahead of its ",size=N", and the only size token inside
     * a block is the checkpoint's own size. Pair each order with the size that follows it and keep
     * the size belonging to the highest order.
     */
    long best_order = -1;
    uint64_t best_size = 0;
    bool found = false;
    for (size_t o = group.find(order_tok); o != std::string_view::npos;
         o = group.find(order_tok, o)) {
        o += order_tok.size();
        long order;
        if (std::from_chars(group.data() + o, group.data() + group.size(), order).ec != std::errc())
            break;

        size_t s = group.find(size_tok, o);
        if (s == std::string_view::npos)
            break;
        s += size_tok.size();
        uint64_t size;
        if (std::from_chars(group.data() + s, group.data() + group.size(), size).ec != std::errc())
            break;

        if (order > best_order) {
            best_order = order;
            best_size = size;
            found = true;
        }
        o = s;
    }

    if (found) {
        *sizep = best_size;
        *orderp = best_order;
    }
    return (found);
}

/*
 * collection_index --
 *     Return the collection index encoded in a "file:coll_<n>.wt_stable" metadata key, or -1 if the
 *     key is not one of this test's collection stable files.
 */
static int
collection_index(std::string_view key)
{
    if (key.size() <= COLLECTION_PREFIX.size() + COLLECTION_SUFFIX.size() ||
      key.substr(0, COLLECTION_PREFIX.size()) != COLLECTION_PREFIX ||
      key.substr(key.size() - COLLECTION_SUFFIX.size()) != COLLECTION_SUFFIX)
        return (-1);

    int idx;
    if (std::from_chars(key.data() + COLLECTION_PREFIX.size(), key.data() + key.size(), idx).ec !=
      std::errc())
        return (-1);
    return (idx);
}

/*
 * extract_latest_checkpoint_api --
 *     The same extraction as extract_latest_checkpoint, but using WiredTiger's public configuration
 *     parser (wiredtiger_config_parser_open) instead of hand-rolled string scanning. This is the
 *     other tool an embedder such as MongoDB has on hand; the microbenchmark compares the two. It
 *     needs three nested parsers: one for the metadata value, one for the checkpoint group, and one
 *     per checkpoint block to read its order and size.
 */
static bool
extract_latest_checkpoint_api(const std::string &value, uint64_t *sizep, long *orderp)
{
    WT_CONFIG_PARSER *top, *group, *block;
    WT_CONFIG_ITEM ckpt_group, ckpt_name, ckpt_block, order_item, size_item;

    *sizep = 0;
    *orderp = -1;

    testutil_check(wiredtiger_config_parser_open(nullptr, value.c_str(), value.size(), &top));
    if (top->get(top, "checkpoint", &ckpt_group) != 0) {
        testutil_check(top->close(top));
        return (false);
    }

    /* The struct item spans the brackets, which the parser strips when re-opened on it. */
    testutil_check(wiredtiger_config_parser_open(nullptr, ckpt_group.str, ckpt_group.len, &group));
    long best_order = -1;
    uint64_t best_size = 0;
    bool found = false;
    while (group->next(group, &ckpt_name, &ckpt_block) == 0) {
        testutil_check(
          wiredtiger_config_parser_open(nullptr, ckpt_block.str, ckpt_block.len, &block));
        if (block->get(block, "order", &order_item) == 0 &&
          block->get(block, "size", &size_item) == 0 && order_item.val > best_order) {
            best_order = (long)order_item.val;
            best_size = (uint64_t)size_item.val;
            found = true;
        }
        testutil_check(block->close(block));
    }
    testutil_check(group->close(group));
    testutil_check(top->close(top));

    if (found) {
        *sizep = best_size;
        *orderp = best_order;
    }
    return (found);
}

/*
 * create_collections --
 *     Create NUM_COLLECTIONS empty disaggregated stable files.
 */
static void
create_collections(scoped_session &session)
{
    for (int i = 0; i < NUM_COLLECTIONS; ++i)
        testutil_check(
          session->create(session.get(), collection_uri(i).c_str(), TABLE_CONFIG.c_str()));
}

/*
 * populate --
 *     Insert a batch of rows, with keys starting at key_start, into every collection. Each
 *     collection's batch commits at its own unique, increasing timestamp taken from ts.
 */
static void
populate(scoped_session &session, uint64_t key_start, uint64_t rows, uint64_t &ts)
{
    const std::string value(VALUE_BYTES, 'x');

    for (int i = 0; i < NUM_COLLECTIONS; ++i) {
        scoped_cursor cursor = session.open_scoped_cursor(collection_uri(i));
        testutil_check(session->begin_transaction(session.get(), nullptr));
        for (uint64_t j = 0; j < rows; ++j) {
            const std::string key = "key" + std::to_string(key_start + j);
            cursor->set_key(cursor.get(), key.c_str());
            cursor->set_value(cursor.get(), value.c_str());
            testutil_check(cursor->insert(cursor.get()));
        }
        const std::string commit_cfg = COMMIT_TS + "=" + to_hex(++ts);
        testutil_check(session->commit_transaction(session.get(), commit_cfg.c_str()));
    }
}

/*
 * checkpoint_stable --
 *     Advance the stable timestamp to cover everything committed so far, then checkpoint so each
 *     stable file's latest checkpoint records its current size.
 */
static void
checkpoint_stable(scoped_session &session, uint64_t stable)
{
    connection_manager::instance().set_timestamp(STABLE_TS + "=" + to_hex(stable));
    testutil_check(session->checkpoint(session.get(), nullptr));
}

/*
 * walk_metadata --
 *     Open a cursor on the metadata and, for every collection stable file, extract the latest
 *     checkpoint's size and order by parsing the config string. Fills sizes and orders and returns
 *     the number of collections found.
 */
static int
walk_metadata(scoped_session &session, std::array<uint64_t, NUM_COLLECTIONS> &sizes,
  std::array<long, NUM_COLLECTIONS> &orders, bool dump)
{
    scoped_cursor cursor = session.open_scoped_cursor("metadata:");
    int found = 0;
    bool dumped = false;
    int ret;
    const char *key, *value;

    while ((ret = cursor->next(cursor.get())) == 0) {
        testutil_check(cursor->get_key(cursor.get(), &key));
        int idx = collection_index(key);
        if (idx < 0 || idx >= NUM_COLLECTIONS)
            continue;
        testutil_check(cursor->get_value(cursor.get(), &value));

        /* Show one raw metadata value so the string being parsed is visible. */
        if (dump && !dumped) {
            logger::log_msg(LOG_INFO,
              "Sample metadata entry parsed by this test:\n  " + std::string(key) + " =\n    " +
                std::string(value));
            dumped = true;
        }

        uint64_t ckpt_size;
        long order;
        testutil_assertfmt(extract_latest_checkpoint(value, &ckpt_size, &order),
          "%s: no checkpoint size found in metadata value", key);
        testutil_assertfmt(ckpt_size > 0, "%s: checkpoint size is zero", key);

        sizes[idx] = ckpt_size;
        orders[idx] = order;
        ++found;
    }
    testutil_assert(ret == WT_NOTFOUND);

    return (found);
}

/*
 * verify_with_statistics --
 *     Cross-check every parsed checkpoint size against the statistics=(size) cursor for the same
 *     stable file. This is the test's oracle, not part of the extraction being demonstrated; for
 *     disaggregated storage the statistic is itself the metadata checkpoint size, so it matches the
 *     parse exactly.
 */
static void
verify_with_statistics(scoped_session &session, const std::array<uint64_t, NUM_COLLECTIONS> &sizes)
{
    for (int i = 0; i < NUM_COLLECTIONS; ++i) {
        scoped_cursor cursor =
          session.open_scoped_cursor("statistics:" + collection_uri(i), "statistics=(size)");
        cursor->set_key(cursor.get(), WT_STAT_DSRC_BLOCK_SIZE);
        testutil_check(cursor->search(cursor.get()));

        int64_t stat_value;
        const char *desc, *pvalue;
        testutil_check(cursor->get_value(cursor.get(), &desc, &pvalue, &stat_value));

        testutil_assertfmt((uint64_t)stat_value == sizes[i],
          "coll_%d: parsed checkpoint size %" PRIu64 " != statistics size %" PRId64, i, sizes[i],
          stat_value);
    }
}

/*
 * microbenchmark --
 *     Extract the latest checkpoint size from a real metadata value many times with each method and
 *     report the timings, to answer whether the WiredTiger config-parser API is faster than the
 *     hand-rolled string scan for this job.
 */
static void
microbenchmark(scoped_session &session)
{
    static constexpr int ITERATIONS = 10000;

    /* Grab one real metadata value as an owned copy to parse repeatedly. */
    std::string sample;
    {
        scoped_cursor cursor = session.open_scoped_cursor("metadata:");
        int ret;
        const char *key, *value;
        while ((ret = cursor->next(cursor.get())) == 0) {
            testutil_check(cursor->get_key(cursor.get(), &key));
            if (collection_index(key) < 0)
                continue;
            testutil_check(cursor->get_value(cursor.get(), &value));
            sample = value;
            break;
        }
        testutil_assert(!sample.empty());
    }

    /* Cross-check the two methods agree before timing them; this also warms both paths. */
    uint64_t size_scan, size_api;
    long order_scan, order_api;
    testutil_assert(extract_latest_checkpoint(sample, &size_scan, &order_scan));
    testutil_assert(extract_latest_checkpoint_api(sample, &size_api, &order_api));
    testutil_assertfmt(size_scan == size_api && order_scan == order_api,
      "method mismatch: hand-rolled (order=%ld, size=%" PRIu64 ") vs API (order=%ld, size=%" PRIu64
      ")",
      order_scan, size_scan, order_api, size_api);

    uint64_t size, sum_scan = 0, sum_api = 0;
    long order;

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < ITERATIONS; ++i) {
        extract_latest_checkpoint(sample, &size, &order);
        sum_scan += size;
    }
    auto t1 = std::chrono::steady_clock::now();
    for (int i = 0; i < ITERATIONS; ++i) {
        extract_latest_checkpoint_api(sample, &size, &order);
        sum_api += size;
    }
    auto t2 = std::chrono::steady_clock::now();

    /* The sums keep the loops from being optimized away and re-check correctness. */
    testutil_assert(sum_scan == sum_api && sum_scan > 0);

    double scan_ns =
      (double)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / ITERATIONS;
    double api_ns =
      (double)std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count() / ITERATIONS;

    std::ostringstream report;
    report << std::fixed << std::setprecision(1)
           << "Microbenchmark: extract latest checkpoint size " << ITERATIONS << " times from a "
           << sample.size() << "-byte metadata value\n"
           << "  hand-rolled string parse:    " << scan_ns << " ns/op\n"
           << "  WiredTiger config-parser API: " << api_ns << " ns/op\n"
           << "  the API is " << (api_ns / scan_ns) << "x the cost of the hand-rolled parse";
    logger::log_msg(LOG_INFO, report.str());
}

/*
 * main --
 *     Create disaggregated collections and checkpoint twice with different data, then re-open the
 *     database and confirm the metadata walk reads the latest checkpoint's size for each
 *     collection.
 */
int
main(int argc, char *argv[])
{
    const std::string progname = testutil_set_progname(argv);
    logger::trace_level = LOG_INFO;
    logger::log_msg(LOG_INFO, "Starting " + progname);

    /*
     * A leader-role disaggregated connection backed by the PALite page-log extension. The extension
     * path is relative to the test's working directory, the way the other cppsuite disagg tests
     * load it.
     */
    const std::string palite_ext = EXTPATH "page_log/palite/" EXTSUBPATH "libwiredtiger_palite.so";
    const std::string conn_config = CONNECTION_CREATE +
      ",cache_size=512MB,statistics=(fast),precise_checkpoint=true,extensions=[" + palite_ext +
      "],disaggregated=(page_log=palite,role=\"leader\")";
    const std::string home = std::string(DEFAULT_DIR) + '_' + progname;

    /* Clean up any artifacts from prior runs. */
    testutil_remove(home.c_str());

    std::array<uint64_t, NUM_COLLECTIONS> first_sizes{}, sizes{};
    std::array<long, NUM_COLLECTIONS> first_orders{}, orders{};
    uint64_t ts = 0;

    /*
     * Take two checkpoints with different data. The first records an initial per-collection size;
     * the second adds new rows so every collection grows. This is what lets the read phase prove it
     * extracts the latest checkpoint rather than a stale one.
     */
    connection_manager::instance().create(conn_config, home);
    {
        scoped_session session = connection_manager::instance().create_session();

        create_collections(session);
        populate(session, 0, ROWS_PER_COLLECTION, ts);
        checkpoint_stable(session, ts);
        /* Snapshot the first checkpoint's size and order for each collection. */
        int found = walk_metadata(session, first_sizes, first_orders, false);
        testutil_assertfmt(found == NUM_COLLECTIONS,
          "first checkpoint: found %d collections, expected %d", found, NUM_COLLECTIONS);

        populate(session, ROWS_PER_COLLECTION, ROWS_PER_COLLECTION, ts);
        checkpoint_stable(session, ts);
    }
    connection_manager::instance().close();

    /* Re-open as a process would at startup, then read sizes from the metadata. */
    connection_manager::instance().reopen(conn_config, home);
    {
        scoped_session session = connection_manager::instance().create_session();

        int found = walk_metadata(session, sizes, orders, true);
        testutil_assertfmt(found == NUM_COLLECTIONS,
          "found %d collections in the metadata, expected %d", found, NUM_COLLECTIONS);

        /* The parse must agree with WiredTiger's own (latest) size for every collection. */
        verify_with_statistics(session, sizes);

        /* Compare the hand-rolled parse against the WiredTiger config-parser API. */
        microbenchmark(session);
    }
    connection_manager::instance().close();

    /*
     * The second checkpoint must be the one we read: its order is higher than the first, and its
     * size is larger because new rows were added. If the parse returned a stale checkpoint, one of
     * these would fail.
     */
    for (int i = 0; i < NUM_COLLECTIONS; ++i) {
        testutil_assertfmt(orders[i] > first_orders[i],
          "coll_%d: read checkpoint order %ld, expected newer than %ld", i, orders[i],
          first_orders[i]);
        testutil_assertfmt(sizes[i] > first_sizes[i],
          "coll_%d: read checkpoint size %" PRIu64 ", expected larger than first %" PRIu64, i,
          sizes[i], first_sizes[i]);
    }

    uint64_t total = 0;
    logger::log_msg(
      LOG_INFO, "Latest checkpoint size of each disaggregated collection, read from the metadata:");
    for (int i = 0; i < NUM_COLLECTIONS; ++i) {
        logger::log_msg(LOG_INFO,
          "  coll_" + std::to_string(i) + " first(order=" + std::to_string(first_orders[i]) + ", " +
            std::to_string(first_sizes[i]) + " B)  ->  latest(order=" + std::to_string(orders[i]) +
            ", " + std::to_string(sizes[i]) + " B)");
        total += sizes[i];
    }
    logger::log_msg(LOG_INFO,
      std::to_string(NUM_COLLECTIONS) + " collections total " + std::to_string(total) +
        " bytes at the latest checkpoint");
    logger::log_msg(LOG_INFO,
      "Parsed latest-checkpoint sizes match the statistics=(size) cursor for all collections.");

    return (0);
}
