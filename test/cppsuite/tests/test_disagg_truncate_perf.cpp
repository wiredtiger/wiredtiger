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
 * [test_disagg_truncate_perf]: Measure what a truncate costs on a layered table, and the cache it
 * creates, for each role. The leader deletes ranges out of the stable tree, the follower records
 * them in a truncate list instead, and a final phase hands the follower a newer checkpoint so that
 * list is collected.
 *
 * The two roles need two connections sharing one page log: a single connection can never adopt a
 * checkpoint, its own already hold the highest metadata LSN.
 */

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/common/thread_manager.h"
#include "src/component/metrics_monitor.h"
#include "src/component/metrics_writer.h"
#include "src/component/timestamp_manager.h"
#include "src/main/configuration.h"
#include "src/storage/scoped_session.h"
#include "src/util/execution_timer.h"
#include "src/util/options_parser.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

using namespace test_harness;

struct options {
    int64_t cache_size_mb;
    int64_t checkpoint_interval_ms;
    int64_t follower_ingest_mb;
    int64_t insert_threads;
    int64_t leader_ingest_mb;
    int64_t marker_size_mb;
    int64_t oplog_size_mb;
    int64_t value_size;
    std::string home_path;
};

static options opt;

static const std::string TABLE_URI = "layered:oplog";
static const std::string TABLE_CONFIG = "key_format=q,value_format=S";
static const std::string INGEST_URI = "file:oplog.wt_ingest";
static const std::string PAGE_LOG = "palite";

/* A percentile is only worth reporting over a reasonable number of operations. */
static const int64_t MINIMUM_TRUNCATES = 25;

/* Truncates run in the garbage collection phase, enough to collect what the follower recorded. */
static const int64_t GC_TRUNCATES = 16;

static WT_CONNECTION *g_leader;
static WT_CONNECTION *g_follower;

/*
 * Timestamps carry the clock in their high bits and a counter in their low ones, as the rest of the
 * suite does. The stable timestamp then trails by a number of seconds, which is what keeps it from
 * overtaking a transaction that is still open.
 */
static std::atomic<uint64_t> g_timestamp{0};
static const uint64_t STABLE_LAG_SECONDS = 5;

/* The head and the volume run for the whole test, so the oplog keeps its shape across phases. */
static std::atomic<uint64_t> g_next_key{1};
static std::atomic<uint64_t> g_inserted_bytes{0};
static std::atomic<uint64_t> g_truncated_key{0};

/* Per-phase counters, and the truncates the follower has recorded in its list. */
static std::atomic<uint64_t> g_truncate_ops{0};
static std::atomic<uint64_t> g_insert_rollbacks{0};
static std::atomic<uint64_t> g_truncate_rollbacks{0};
static std::atomic<uint64_t> g_follower_truncate_entries{0};

struct named_stat {
    const char *name;
    int field;
};

/* Connection statistics, reported as the change across a phase. */
static const named_stat PHASE_STATS[] = {
  {"page_delete_fast", WT_STAT_CONN_REC_PAGE_DELETE_FAST},
  {"page_delete_fast_skipped", WT_STAT_CONN_REC_PAGE_DELETE_FAST_SKIP_DELETED},
  {"truncate_keys_deleted", WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED},
  {"eviction_blocked_truncate", WT_STAT_CONN_CACHE_EVICTION_BLOCKED_UNCOMMITTED_TRUNCATE},
  {"truncate_dirty_cache_rollback", WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK},
  {"eviction_pages_seen", WT_STAT_CONN_CACHE_EVICTION_PAGES_SEEN},
  {"truncate_list_search_calls", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS},
  {"truncate_list_entries_walked", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED},
  {"truncate_list_gc_runs", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS},
  {"truncate_list_gc_entries_removed", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED},
};

/* Cache occupancy, sampled while a phase runs rather than read once at its end. */
static const named_stat CACHE_GAUGES[] = {
  {"cache_bytes_inuse", WT_STAT_CONN_CACHE_BYTES_INUSE},
  {"cache_bytes_dirty", WT_STAT_CONN_CACHE_BYTES_DIRTY},
  {"cache_bytes_updates", WT_STAT_CONN_CACHE_BYTES_UPDATES},
  {"cache_pages_inuse", WT_STAT_CONN_CACHE_PAGES_INUSE},
};

using phase_stats = std::array<int64_t, WT_ELEMENTS(PHASE_STATS)>;

/*
 * A follower holds its pressure in the ingest table and the truncate list rather than in dirty
 * pages, so both are tracked alongside the cache gauges.
 */
struct cache_pressure {
    std::atomic<uint64_t> samples{0};
    std::atomic<uint64_t> total[WT_ELEMENTS(CACHE_GAUGES)]{};
    std::atomic<uint64_t> peak[WT_ELEMENTS(CACHE_GAUGES)]{};
    std::atomic<uint64_t> ingest_total{0}, ingest_peak{0}, list_entries_peak{0};
};

static cache_pressure g_leader_cache, g_follower_cache;

/* The cache a single truncate creates, charged per operation. */
struct truncate_pressure {
    std::atomic<uint64_t> count{0}, total{0}, peak{0};
};

static truncate_pressure g_leader_pressure, g_follower_pressure;

static uint64_t
record_bytes()
{
    return (sizeof(int64_t) + static_cast<uint64_t>(opt.value_size));
}

static uint64_t
mb_to_bytes(int64_t mb)
{
    return (static_cast<uint64_t>(mb) * WT_MEGABYTE);
}

static std::string
hex(uint64_t value)
{
    return (timestamp_manager::decimal_to_hex(value));
}

static void
set_timestamp(WT_CONNECTION *conn, const std::string &config)
{
    testutil_check(conn->set_timestamp(conn, config.c_str()));
}

static void
report(const std::string &name, uint64_t value)
{
    metrics_writer::instance().add_stat(name, value);
}

static void
record_peak(std::atomic<uint64_t> &peak, uint64_t seen)
{
    uint64_t previous = peak.load();
    while (seen > previous && !peak.compare_exchange_weak(previous, seen))
        ;
}

/*
 * Every truncate counts, including the ones that cost nothing: a truncate whose parent page was
 * already dirty pins nothing new, and leaving those out would overstate what a truncate costs.
 */
static void
record_pressure(truncate_pressure &pressure, int64_t bytes)
{
    uint64_t seen = bytes <= 0 ? 0 : static_cast<uint64_t>(bytes);

    pressure.count.fetch_add(1);
    pressure.total.fetch_add(seen);
    record_peak(pressure.peak, seen);
}

static void
require_truncates(const std::string &phase, int64_t ingest_mb)
{
    int64_t truncates = ingest_mb / opt.marker_size_mb;

    if (truncates < MINIMUM_TRUNCATES)
        testutil_die(EINVAL,
          "the %s phase would run %" PRId64 " truncates, fewer than the %" PRId64
          " needed to measure one: raise its ingest volume or lower marker_size_mb",
          phase.c_str(), truncates, MINIMUM_TRUNCATES);
}

/*
 * Read the test configuration, either from a file or from a configuration string given on the
 * command line. Anything left out falls back to the default the test declares in dist/test_data.py.
 */
static void
load_configuration(int argc, char *argv[])
{
    const std::string test_name = "test_disagg_truncate_perf";
    std::string config;

    if (option_exists("-C", argc, argv))
        config = value_for_opt("-C", argc, argv);
    else {
        /* Read the file, dropping the comments and whitespace that make it readable. */
        std::string filename = option_exists("-f", argc, argv) ?
          value_for_opt("-f", argc, argv) :
          "configs/" + test_name + "_default.txt";
        std::ifstream config_file(filename);
        std::string line;

        if (!config_file.is_open())
            testutil_die(EINVAL, "failed to open %s for reading", filename.c_str());
        while (getline(config_file, line)) {
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (!line.empty() && line[0] != '#')
                config += line;
        }
    }

    configuration cfg(test_name, config);
    opt.cache_size_mb = cfg.get_int("cache_size_mb");
    opt.checkpoint_interval_ms = cfg.get_int("checkpoint_interval_ms");
    opt.follower_ingest_mb = cfg.get_int("follower_ingest_mb");
    opt.home_path = cfg.get_string("home");
    opt.insert_threads = cfg.get_int("insert_threads");
    opt.leader_ingest_mb = cfg.get_int("leader_ingest_mb");
    opt.marker_size_mb = cfg.get_int("marker_size_mb");
    opt.oplog_size_mb = cfg.get_int("oplog_size_mb");
    opt.value_size = cfg.get_int("value_size");

    if (opt.marker_size_mb > opt.oplog_size_mb)
        testutil_die(EINVAL, "the marker size must not exceed the oplog size");

    /*
     * Once the oplog is full, every marker of inserts retires one marker, so the ingest volume and
     * the marker size decide how many truncates a phase gets to measure.
     */
    require_truncates("leader", opt.leader_ingest_mb);
    require_truncates("follower", opt.follower_ingest_mb);
}

/*
 * MongoDB reclaims the oplog in whole markers rather than a record at a time. Keys are sequential
 * and records are a fixed size, so the oldest marker is whatever sits behind the volume the oplog
 * is meant to keep: one large range per truncate, well behind the insert head.
 */
static bool
expired_marker(uint64_t *marker_keyp)
{
    uint64_t keep = mb_to_bytes(opt.oplog_size_mb) / record_bytes();
    uint64_t marker = mb_to_bytes(opt.marker_size_mb) / record_bytes();
    uint64_t head = g_next_key.load();

    if (head - g_truncated_key.load() <= keep + marker)
        return (false);

    *marker_keyp = head - keep;
    return (true);
}

static uint64_t
clock_seconds()
{
    auto now = std::chrono::steady_clock::now().time_since_epoch();

    return (
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count()) << 32);
}

static wt_timestamp_t
next_timestamp()
{
    return (clock_seconds() | (g_timestamp.fetch_add(1) & 0x00000000FFFFFFFF));
}

static phase_stats
read_phase_stats(WT_CONNECTION *conn)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    phase_stats stats;

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        stats[i] = metrics_monitor::get_stat(cursor, PHASE_STATS[i].field);
    return (stats);
}

/* Insert one record at the head of the oplog, abandoning the key if the transaction rolls back. */
static bool
insert_record(scoped_session &session, scoped_cursor &cursor, uint64_t key,
  const std::string &value, wt_timestamp_t commit_ts)
{
    int ret;

    testutil_check(session->begin_transaction(session.get(), nullptr));
    cursor->set_key(cursor.get(), static_cast<int64_t>(key));
    cursor->set_value(cursor.get(), value.c_str());
    if ((ret = cursor->insert(cursor.get())) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        testutil_check(session->rollback_transaction(session.get(), nullptr));
        return (false);
    }

    testutil_check(
      session->timestamp_transaction(session.get(), (COMMIT_TS + "=" + hex(commit_ts)).c_str()));
    if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        return (false);
    }
    return (true);
}

static void
insert_worker(WT_CONNECTION *conn, int id, uint64_t target_bytes)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    const std::string value = random_generator::instance().generate_pseudo_random_string(
      static_cast<uint64_t>(opt.value_size));

    while (g_inserted_bytes.load() < target_bytes) {
        uint64_t key = g_next_key.fetch_add(1);

        if (insert_record(session, cursor, key, value, next_timestamp()))
            g_inserted_bytes.fetch_add(record_bytes());
        else
            g_insert_rollbacks.fetch_add(1);
    }
}

/*
 * Truncate up to the marker key, charging the operation for the cache it creates. A leader pins
 * dirty internal pages until the transaction resolves, and that gauge is given back at commit, so
 * it is read while the truncate is open. A follower instead grows its ingest table and truncate
 * list, which is the change in update bytes across the whole operation.
 */
static int
truncate_to_marker(scoped_session &session, scoped_cursor &cursor, scoped_cursor &stat_cursor,
  uint64_t marker_key, wt_timestamp_t commit_ts, bool follower)
{
    truncate_pressure &pressure = follower ? g_follower_pressure : g_leader_pressure;
    int64_t before =
      follower ? metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES) : 0;
    int ret;

    testutil_check(session->begin_transaction(session.get(), nullptr));
    testutil_check(cursor->reset(cursor.get()));
    cursor->set_key(cursor.get(), static_cast<int64_t>(marker_key));
    if ((ret = session->truncate(session.get(), nullptr, nullptr, cursor.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        testutil_check(session->rollback_transaction(session.get(), nullptr));
        return (ret);
    }

    if (!follower)
        record_pressure(pressure,
          metrics_monitor::get_stat(
            stat_cursor, WT_STAT_CONN_CACHE_TRUNCATE_TXN_UNCOMMITTED_BYTES));

    testutil_check(
      session->timestamp_transaction(session.get(), (COMMIT_TS + "=" + hex(commit_ts)).c_str()));
    if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        return (ret);
    }

    if (follower) {
        record_pressure(pressure,
          metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES) - before);
        g_follower_truncate_entries.fetch_add(1);
    }
    g_truncate_ops.fetch_add(1);
    g_truncated_key.store(marker_key);
    return (0);
}

/*
 * A single truncate thread, matching the single trimming thread MongoDB runs against an oplog. It
 * trails the inserters and retires whole markers once the oplog is over its configured size.
 */
static void
truncate_worker(WT_CONNECTION *conn, const std::string &phase, uint64_t target_bytes, bool follower)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);

    /* Statistics are read from their own session so they stay outside the truncate transaction. */
    scoped_session stat_session(conn);
    scoped_cursor stat_cursor = stat_session.open_scoped_cursor(STATISTICS_URI);

    execution_timer timer(phase + "_truncate", "test_disagg_truncate_perf");
    uint64_t marker_key;

    for (;;) {
        if (!expired_marker(&marker_key)) {
            if (g_inserted_bytes.load() >= target_bytes)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        int ret = timer.track([&]() {
            return (truncate_to_marker(
              session, cursor, stat_cursor, marker_key, next_timestamp(), follower));
        });
        if (ret != 0)
            g_truncate_rollbacks.fetch_add(1);
    }
}

/*
 * Advance the stable timestamp behind the oldest transaction in flight and checkpoint on an
 * interval. Without the timestamp nothing leaves cache, and without the checkpoints the internal
 * pages stay dirty, which is the case where a truncate pins no extra cache at all.
 */
static void
leader_maintenance_worker(WT_CONNECTION *conn, uint64_t target_bytes)
{
    scoped_session session(conn);
    auto last_checkpoint = std::chrono::steady_clock::now();

    while (g_inserted_bytes.load() < target_bytes) {
        set_timestamp(conn, STABLE_TS + "=" + hex(clock_seconds() - (STABLE_LAG_SECONDS << 32)));

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_checkpoint).count() >=
          opt.checkpoint_interval_ms) {
            testutil_check(session->checkpoint(session.get(), nullptr));
            last_checkpoint = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

/*
 * Follow cache occupancy while a phase runs: what matters is how high it went while the truncates
 * were happening. A follower also carries its ingest table, which only a pickup collects.
 */
static void
cache_sampler_worker(WT_CONNECTION *conn, cache_pressure *pressure, std::atomic<bool> *stop)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    bool follower = conn == g_follower;

    while (!stop->load()) {
        pressure->samples.fetch_add(1);
        for (size_t i = 0; i < WT_ELEMENTS(CACHE_GAUGES); i++) {
            uint64_t seen =
              static_cast<uint64_t>(metrics_monitor::get_stat(cursor, CACHE_GAUGES[i].field));
            pressure->total[i].fetch_add(seen);
            record_peak(pressure->peak[i], seen);
        }

        if (follower) {
            scoped_cursor ingest_cursor = session.open_scoped_cursor(STATISTICS_URI + INGEST_URI);
            uint64_t ingest = static_cast<uint64_t>(
              metrics_monitor::get_stat(ingest_cursor, WT_STAT_DSRC_CACHE_BYTES_INUSE));
            pressure->ingest_total.fetch_add(ingest);
            record_peak(pressure->ingest_peak, ingest);

            uint64_t collected = static_cast<uint64_t>(metrics_monitor::get_stat(
              cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            uint64_t recorded = g_follower_truncate_entries.load();
            record_peak(
              pressure->list_entries_peak, recorded > collected ? recorded - collected : 0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

static void
reset_phase(truncate_pressure &pressure, cache_pressure &cache)
{
    g_truncate_ops = g_insert_rollbacks = g_truncate_rollbacks = 0;
    pressure.count = pressure.total = pressure.peak = 0;
    cache.samples = cache.ingest_total = cache.ingest_peak = cache.list_entries_peak = 0;
    for (size_t i = 0; i < WT_ELEMENTS(CACHE_GAUGES); i++)
        cache.total[i] = cache.peak[i] = 0;
}

/* Report everything a phase produced: what it did, what each truncate cost, and what it held. */
static void
report_phase(const std::string &phase, const phase_stats &before, const phase_stats &after,
  truncate_pressure &pressure, cache_pressure &cache, bool follower)
{
    uint64_t truncates = std::max(pressure.count.load(), static_cast<uint64_t>(1));
    uint64_t samples = std::max(cache.samples.load(), static_cast<uint64_t>(1));

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        report(phase + "_" + PHASE_STATS[i].name, static_cast<uint64_t>(after[i] - before[i]));

    report(phase + "_truncate_ops", g_truncate_ops.load());
    report(phase + "_truncate_rollbacks", g_truncate_rollbacks.load());
    report(phase + "_insert_rollbacks", g_insert_rollbacks.load());
    report(phase + "_truncate_pressure_bytes_mean", pressure.total.load() / truncates);
    report(phase + "_truncate_pressure_bytes_peak", pressure.peak.load());

    for (size_t i = 0; i < WT_ELEMENTS(CACHE_GAUGES); i++) {
        report(phase + "_" + CACHE_GAUGES[i].name + "_mean", cache.total[i].load() / samples);
        report(phase + "_" + CACHE_GAUGES[i].name + "_peak", cache.peak[i].load());
    }
    if (follower) {
        report(phase + "_ingest_bytes_mean", cache.ingest_total.load() / samples);
        report(phase + "_ingest_bytes_peak", cache.ingest_peak.load());
        report(phase + "_truncate_list_entries_peak", cache.list_entries_peak.load());
    }
}

/* Run the insert and truncate workload against one role, and report everything it produced. */
static void
run_phase(WT_CONNECTION *conn, const std::string &phase, int64_t phase_mb)
{
    bool leader = conn == g_leader;
    cache_pressure &cache = leader ? g_leader_cache : g_follower_cache;
    truncate_pressure &pressure = leader ? g_leader_pressure : g_follower_pressure;

    logger::log_msg(LOG_INFO, "Starting the " + phase + " phase.");
    reset_phase(pressure, cache);
    std::atomic<bool> sampling_done{false};
    uint64_t start_bytes = g_inserted_bytes.load();
    uint64_t target_bytes = start_bytes + mb_to_bytes(phase_mb);
    phase_stats before = read_phase_stats(conn);
    auto start = std::chrono::steady_clock::now();

    {
        thread_manager tm;
        for (int64_t i = 0; i < opt.insert_threads; i++)
            tm.add_thread(insert_worker, conn, static_cast<int>(i), target_bytes);
        tm.add_thread(truncate_worker, conn, phase, target_bytes, !leader);
        tm.add_thread(cache_sampler_worker, conn, &cache, &sampling_done);
        if (leader)
            tm.add_thread(leader_maintenance_worker, conn, target_bytes);

        thread_manager stopper;
        stopper.add_thread([&]() {
            while (g_inserted_bytes.load() < target_bytes)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            sampling_done.store(true);
        });
        tm.join();
        stopper.join();
    }

    int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start)
                           .count();
    uint64_t elapsed = static_cast<uint64_t>(std::max(elapsed_ms, static_cast<int64_t>(1)));

    report(phase + "_duration_ms", elapsed);
    report(phase + "_truncates_per_second", (g_truncate_ops.load() * 1000) / elapsed);
    report(phase + "_insert_mb_per_second",
      ((g_inserted_bytes.load() - start_bytes) * 1000) / WT_MEGABYTE / elapsed);
    report_phase(phase, before, read_phase_stats(conn), pressure, cache, !leader);
    logger::log_msg(LOG_INFO,
      "The " + phase + " phase completed " + std::to_string(g_truncate_ops.load()) + " truncates.");
}

/*
 * Populate the oplog up to its configured size. No truncation happens here, and the checkpoint at
 * the end is what lets the leader delete whole pages rather than walking them key by key.
 */
static void
populate()
{
    logger::log_msg(LOG_INFO, "Populating " + std::to_string(opt.oplog_size_mb) + "MB.");

    thread_manager tm;
    for (int64_t i = 0; i < opt.insert_threads; i++)
        tm.add_thread(insert_worker, g_leader, static_cast<int>(i), mb_to_bytes(opt.oplog_size_mb));
    tm.add_thread(leader_maintenance_worker, g_leader, mb_to_bytes(opt.oplog_size_mb));
    tm.join();

    scoped_session session(g_leader);
    testutil_check(session->checkpoint(session.get(), nullptr));
}

/*
 * Checkpoint the leader and hand it to the follower. A pickup is asynchronous when a snapshot older
 * than it is still open, so wait for it to land before measuring anything that depends on it.
 */
static void
deliver_checkpoint(wt_timestamp_t timestamp)
{
    std::string meta;
    {
        scoped_session session(g_leader);
        WT_PAGE_LOG *page_log;

        set_timestamp(g_leader, STABLE_TS + "=" + hex(timestamp));
        testutil_check(session->checkpoint(session.get(), nullptr));

        testutil_check(g_leader->get_page_log(g_leader, PAGE_LOG.c_str(), &page_log));
        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
        testutil_check(page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));
        page_log->terminate(page_log, nullptr);
        meta = std::string(
          static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size);
        free(args.checkpoint_metadata.mem);
    }

    std::string config = "disaggregated=(checkpoint_meta=\"" + meta + "\")";
    testutil_check(g_follower->reconfigure(g_follower, config.c_str()));

    scoped_session session(g_follower);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    for (int i = 0; i < 1000; i++) {
        if (metrics_monitor::get_stat(cursor, WT_STAT_CONN_DISAGG_CHECKPOINT_META_LSN) >=
          metrics_monitor::get_stat(cursor, WT_STAT_CONN_DISAGG_CHECKPOINT_DELIVERED_LSN))
            return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    testutil_die(ETIMEDOUT, "the follower did not pick up the delivered checkpoint");
}

/*
 * Truncate list entries are only collected by a later truncate on the same table, so this phase is
 * a short burst of truncates after a newer checkpoint has been picked up.
 */
static void
run_gc_phase()
{
    scoped_session session(g_follower);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    scoped_cursor stat_cursor = session.open_scoped_cursor(STATISTICS_URI);

    logger::log_msg(LOG_INFO, "Starting the garbage collection phase.");
    reset_phase(g_follower_pressure, g_follower_cache);
    phase_stats before = read_phase_stats(g_follower);

    /* Spread the truncates over whatever is left live, so they stay behind the insert head. */
    uint64_t marker_key = g_truncated_key.load();
    uint64_t chunk = (g_next_key.load() - marker_key) / static_cast<uint64_t>(GC_TRUNCATES + 1);
    testutil_assert(chunk > 0);

    for (int64_t i = 0; i < GC_TRUNCATES; i++) {
        marker_key += chunk;
        if (truncate_to_marker(session, cursor, stat_cursor, marker_key, next_timestamp(), true) !=
          0)
            g_truncate_rollbacks.fetch_add(1);
    }

    report_phase(
      "gc", before, read_phase_stats(g_follower), g_follower_pressure, g_follower_cache, true);
    logger::log_msg(LOG_INFO,
      "The garbage collection phase completed " + std::to_string(g_truncate_ops.load()) +
        " truncates.");
}

static std::string
connection_config(const std::string &role)
{
    std::string config = CONNECTION_CREATE + ",cache_size=" + std::to_string(opt.cache_size_mb) +
      "MB,statistics=(all),statistics_log=(json,wait=1,on_close),precise_checkpoint=true" +
      ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so]" +
      ",disaggregated=(page_log=" + PAGE_LOG + ",role=\"" + role + "\")";
    return (config);
}

/*
 * Both connections read and write the same page log, which the follower needs to see the leader's
 * checkpoints. The store lives under the leader's home, and the follower reaches it through a link.
 */
static void
open_connections()
{
    const std::filesystem::path home(opt.home_path);
    const std::filesystem::path follower_home = home / "follower";

    testutil_remove(opt.home_path.c_str());
    std::filesystem::create_directories(home);
    testutil_check(wiredtiger_open(
      opt.home_path.c_str(), nullptr, connection_config("leader").c_str(), &g_leader));

    std::filesystem::create_directories(follower_home);
    std::filesystem::create_directory_symlink("../kv_home", follower_home / "kv_home");
    testutil_check(wiredtiger_open(
      follower_home.c_str(), nullptr, connection_config("follower").c_str(), &g_follower));

    for (WT_CONNECTION *conn : {g_leader, g_follower}) {
        scoped_session session(conn);
        testutil_check(session->create(session.get(), TABLE_URI.c_str(), TABLE_CONFIG.c_str()));
        /* The oldest timestamp stays put, nothing in the test relies on moving it. */
        set_timestamp(conn, OLDEST_TS + "=" + hex(1));
    }
}

int
main(int argc, char *argv[])
{
    const std::string progname = testutil_set_progname(argv);
    logger::trace_level = LOG_INFO;

    logger::log_msg(LOG_INFO, "Starting " + progname);
    load_configuration(argc, argv);

    open_connections();
    populate();
    run_phase(g_leader, "leader", opt.leader_ingest_mb);

    /* Hand the follower everything the leader has written, then let it run its own workload. */
    deliver_checkpoint(next_timestamp());
    run_phase(g_follower, "follower", opt.follower_ingest_mb);

    /*
     * A checkpoint taken now sits above every truncate the follower recorded, so picking it up
     * moves the prune timestamp past them and the entries can be freed.
     */
    deliver_checkpoint(next_timestamp());
    run_gc_phase();

    metrics_writer::instance().output_perf_file(progname);
    testutil_check(g_follower->close(g_follower, nullptr));
    testutil_check(g_leader->close(g_leader, nullptr));
    return (EXIT_SUCCESS);
}
