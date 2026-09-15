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
 * [test_disagg_truncate_perf]: Measure truncate cost and cache pressure on a layered table, for
 * both roles. The leader deletes ranges out of the stable tree, the follower records them in the
 * truncate list instead, and a third phase hands the follower a newer checkpoint so that list is
 * reclaimed, which is the only way those entries ever go away.
 *
 * A leader and a follower connection share one page log, which is how a checkpoint taken after the
 * follower has run can be handed over: a single connection can never adopt one, its own checkpoints
 * already hold the highest metadata LSN.
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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using namespace test_harness;

struct options {
    int64_t cache_size_mb;
    int64_t insert_threads;
    int64_t value_size;
    int64_t marker_size_mb;
    int64_t oplog_size_mb;
    int64_t leader_ingest_mb;
    int64_t follower_ingest_mb;
    int64_t gc_truncate_count;
    int64_t checkpoint_interval_ms;
    int64_t replica_ingest_mb;
    int64_t apply_queue_max;
    int64_t verbose_level;
    std::string home_path;
    std::string workload;
};

static options opt;

static const std::string TABLE_URI = "layered:oplog";
static const std::string TABLE_CONFIG = "key_format=q,value_format=S";
static const std::string INGEST_URI = "file:oplog.wt_ingest";
static const std::string PAGE_LOG = "palite";

static WT_CONNECTION *g_leader;
static WT_CONNECTION *g_follower;

/* Timestamp allocator. Every commit in the test takes its timestamp from here. */
static std::atomic<uint64_t> g_timestamp{1000};

/*
 * The timestamp each worker is currently committing at, so the stable timestamp can be advanced
 * without overtaking a transaction that is still open. An idle worker parks its slot at the top of
 * the range.
 */
static std::unique_ptr<std::atomic<uint64_t>[]> g_worker_timestamps;
static int g_worker_count;

/*
 * Insert head and the volume inserted so far. Both run for the whole test, so the oplog keeps its
 * shape across a phase boundary and the follower starts truncating where the leader stopped.
 */
static std::atomic<uint64_t> g_next_key{1};
static std::atomic<uint64_t> g_inserted_bytes{0};

/* Key of the most recent truncate, so a later phase knows where the live range starts. */
static std::atomic<uint64_t> g_truncated_key{0};

/*
 * The cache pressure a single truncate creates. The leader pins dirty internal pages until the
 * transaction resolves, the follower grows its ingest table and truncate list instead, so each role
 * is measured against a different statistic.
 */
struct truncate_pressure {
    int stat_field;
    /* The leader's gauge is given back at commit, so it has to be read while the truncate is open.
     */
    bool read_before_commit;
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> peak{0};

    /*
     * Every truncate counts, including the ones that cost nothing: a truncate whose parent page was
     * already dirty pins nothing new, and leaving those out of the average would overstate the cost
     * of a truncate.
     */
    void
    record(int64_t bytes)
    {
        uint64_t seen = bytes <= 0 ? 0 : static_cast<uint64_t>(bytes);
        count.fetch_add(1);
        total.fetch_add(seen);
        uint64_t previous = peak.load();
        while (seen > previous && !peak.compare_exchange_weak(previous, seen))
            ;
    }
};

static truncate_pressure g_leader_pressure{
  WT_STAT_CONN_CACHE_TRUNCATE_TXN_UNCOMMITTED_BYTES, true, {}, {}, {}};
static truncate_pressure g_follower_pressure{WT_STAT_CONN_CACHE_BYTES_UPDATES, false, {}, {}, {}};

/*
 * Cache occupancy sampled while a phase runs. A follower holds its pressure in the ingest table and
 * the truncate list rather than in dirty pages, so both are tracked alongside the cache gauges.
 */
struct cache_pressure {
    std::atomic<uint64_t> samples{0};
    std::atomic<uint64_t> bytes_inuse_total{0}, bytes_inuse_peak{0};
    std::atomic<uint64_t> bytes_dirty_total{0}, bytes_dirty_peak{0};
    std::atomic<uint64_t> bytes_updates_total{0}, bytes_updates_peak{0};
    std::atomic<uint64_t> pages_inuse_peak{0};
    std::atomic<uint64_t> ingest_bytes_total{0}, ingest_bytes_peak{0};
    std::atomic<uint64_t> truncate_list_entries_peak{0};
};

static cache_pressure g_leader_cache;
static cache_pressure g_follower_cache;

/* Truncates the follower has recorded, so the live truncate list length can be derived. */
static std::atomic<uint64_t> g_follower_truncate_entries{0};

/* Per-phase counters. The apply counters belong to the follower in the replica workload. */
static std::atomic<uint64_t> g_truncate_ops{0};
static std::atomic<uint64_t> g_apply_inserts{0};
static std::atomic<uint64_t> g_apply_truncates{0};
static std::atomic<uint64_t> g_apply_rollbacks{0};
static std::atomic<uint64_t> g_insert_rollbacks{0};
static std::atomic<uint64_t> g_truncate_rollbacks{0};

/* Connection statistics sampled at a phase boundary. */
struct stat_sample {
    int64_t page_delete_fast;
    int64_t page_delete_fast_skipped;
    int64_t truncate_keys_deleted;
    int64_t eviction_blocked_truncate;
    int64_t truncate_dirty_rollback;
    int64_t bytes_dirty;
    int64_t eviction_pages_seen;
    int64_t list_search_calls;
    int64_t list_entries_walked;
    int64_t gc_runs;
    int64_t gc_entries_removed;
};

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

/* Read a configuration file, dropping comments and the whitespace that makes it readable. */
static std::string
read_configuration_file(const std::string &filename)
{
    std::string config, line;
    std::ifstream config_file(filename);

    if (!config_file.is_open())
        testutil_die(EINVAL, "failed to open %s for reading", filename.c_str());

    while (getline(config_file, line)) {
        line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
        if (line.empty() || line[0] == '#')
            continue;
        config += line;
    }
    return (config);
}

/* A percentile is only worth reporting over a reasonable number of operations. */
static const int64_t MINIMUM_TRUNCATES = 25;

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
    else
        config = read_configuration_file(option_exists("-f", argc, argv) ?
            value_for_opt("-f", argc, argv) :
            "configs/" + test_name + "_default.txt");

    configuration cfg(test_name, config);
    opt.cache_size_mb = cfg.get_int("cache_size_mb");
    opt.checkpoint_interval_ms = cfg.get_int("checkpoint_interval_ms");
    opt.follower_ingest_mb = cfg.get_int("follower_ingest_mb");
    opt.gc_truncate_count = cfg.get_int("gc_truncate_count");
    opt.home_path = cfg.get_string("home");
    opt.insert_threads = cfg.get_int("insert_threads");
    opt.leader_ingest_mb = cfg.get_int("leader_ingest_mb");
    opt.marker_size_mb = cfg.get_int("marker_size_mb");
    opt.oplog_size_mb = cfg.get_int("oplog_size_mb");
    opt.replica_ingest_mb = cfg.get_int("replica_ingest_mb");
    opt.apply_queue_max = cfg.get_int("apply_queue_max");
    opt.value_size = cfg.get_int("value_size");
    opt.verbose_level = cfg.get_int("verbose_level");
    opt.workload = cfg.get_string("workload");

    if (opt.marker_size_mb > opt.oplog_size_mb)
        testutil_die(EINVAL, "the marker size must not exceed the oplog size");

    /*
     * Once the oplog is full every marker of inserts retires one marker, so the ingest volume and
     * the marker size decide how many truncates a phase gets to measure.
     */
    if (opt.workload == "replica")
        require_truncates("replica", opt.replica_ingest_mb);
    else {
        require_truncates("leader", opt.leader_ingest_mb);
        require_truncates("follower", opt.follower_ingest_mb);
    }
    if (opt.workload != "phases" && opt.workload != "replica")
        testutil_die(EINVAL, "unknown workload \"%s\"", opt.workload.c_str());
}

/*
 * MongoDB reclaims the oplog in whole markers rather than a record at a time, and never in units
 * smaller than a fixed floor. Matching that shape is what makes the truncate ranges here
 * representative: one large range per marker, well behind the insert head.
 */
class oplog_markers {
public:
    oplog_markers() : _next_marker_bytes(mb_to_bytes(opt.marker_size_mb)), _truncated_bytes(0) {}

    /*
     * Close off any markers the inserters have filled, and report the oldest one once the oplog is
     * over its configured size.
     */
    bool
    expired_marker(uint64_t head_key, uint64_t inserted_bytes, uint64_t *marker_keyp)
    {
        while (inserted_bytes >= _next_marker_bytes) {
            _marker_keys.push_back(head_key);
            _next_marker_bytes += mb_to_bytes(opt.marker_size_mb);
        }

        if (_marker_keys.empty() ||
          inserted_bytes - _truncated_bytes <= mb_to_bytes(opt.oplog_size_mb))
            return (false);

        *marker_keyp = _marker_keys.front();
        _marker_keys.pop_front();
        _truncated_bytes += mb_to_bytes(opt.marker_size_mb);
        return (true);
    }

private:
    std::deque<uint64_t> _marker_keys;
    uint64_t _next_marker_bytes;
    uint64_t _truncated_bytes;
};

static std::unique_ptr<oplog_markers> g_markers;

/* One leader write, waiting for the follower to apply it. */
struct replication_op {
    bool truncate;
    uint64_t key;
    wt_timestamp_t timestamp;
};

/*
 * The leader's write stream, handed to the follower in order. It is bounded, so a follower that
 * falls behind holds the leader back rather than growing without limit, which is what replication
 * flow control does.
 */
class replication_queue {
public:
    void
    push(const replication_op &op)
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _not_full.wait(
          lock, [this]() { return (_ops.size() < static_cast<size_t>(opt.apply_queue_max)); });
        _ops.push_back(op);
        _peak_depth = std::max(_peak_depth, static_cast<uint64_t>(_ops.size()));
        _not_empty.notify_one();
    }

    /* Returns false once the leader is done and everything it wrote has been handed over. */
    bool
    pop(replication_op *opp)
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _not_empty.wait(lock, [this]() { return (!_ops.empty() || _closed); });
        if (_ops.empty())
            return (false);
        *opp = _ops.front();
        _ops.pop_front();
        _not_full.notify_one();
        return (true);
    }

    void
    close()
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _closed = true;
        _not_empty.notify_all();
    }

    uint64_t
    peak_depth()
    {
        std::unique_lock<std::mutex> lock(_mutex);
        return (_peak_depth);
    }

private:
    std::mutex _mutex;
    std::condition_variable _not_full, _not_empty;
    std::deque<replication_op> _ops;
    uint64_t _peak_depth = 0;
    bool _closed = false;
};

/* Only set for the replica workload, where the follower applies what the leader writes. */
static std::unique_ptr<replication_queue> g_replication_queue;

static void
replicate(bool truncate, uint64_t key, wt_timestamp_t timestamp)
{
    if (g_replication_queue != nullptr)
        g_replication_queue->push({truncate, key, timestamp});
}

/* Hand the leader's newest checkpoint to the follower. */
static void deliver_latest_checkpoint(bool wait);

/*
 * Take the next commit timestamp, publishing it first so the stable timestamp can never be moved
 * past a transaction that has not committed yet.
 */
static wt_timestamp_t
reserve_timestamp(int worker)
{
    g_worker_timestamps[worker].store(g_timestamp.load());
    return (g_timestamp.fetch_add(1));
}

static void
release_timestamp(int worker)
{
    g_worker_timestamps[worker].store(WT_TS_MAX);
}

static int64_t
get_stat(scoped_cursor &cursor, int field)
{
    return (metrics_monitor::get_stat(cursor, field));
}

static stat_sample
read_stats(scoped_cursor &cursor)
{
    stat_sample s;

    s.page_delete_fast = get_stat(cursor, WT_STAT_CONN_REC_PAGE_DELETE_FAST);
    s.page_delete_fast_skipped = get_stat(cursor, WT_STAT_CONN_REC_PAGE_DELETE_FAST_SKIP_DELETED);
    s.truncate_keys_deleted = get_stat(cursor, WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED);
    s.eviction_blocked_truncate =
      get_stat(cursor, WT_STAT_CONN_CACHE_EVICTION_BLOCKED_UNCOMMITTED_TRUNCATE);
    s.truncate_dirty_rollback = get_stat(cursor, WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK);
    s.bytes_dirty = get_stat(cursor, WT_STAT_CONN_CACHE_BYTES_DIRTY_TOTAL);
    s.eviction_pages_seen = get_stat(cursor, WT_STAT_CONN_CACHE_EVICTION_PAGES_SEEN);
    s.list_search_calls = get_stat(cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS);
    s.list_entries_walked =
      get_stat(cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED);
    s.gc_runs = get_stat(cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS);
    s.gc_entries_removed = get_stat(cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED);
    return (s);
}

static stat_sample
read_stats(WT_CONNECTION *conn)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    return (read_stats(cursor));
}

static void
reset_phase_counters()
{
    g_truncate_ops = 0;
    g_insert_rollbacks = 0;
    g_truncate_rollbacks = 0;
}

/*
 * Insert one record at the head of the oplog. Returns false if the transaction rolled back, in
 * which case the key is simply abandoned.
 */
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
        wt_timestamp_t commit_ts = reserve_timestamp(id);

        if (insert_record(session, cursor, key, value, commit_ts)) {
            g_inserted_bytes.fetch_add(record_bytes());
            replicate(false, key, commit_ts);
        } else
            g_insert_rollbacks.fetch_add(1);
        release_timestamp(id);
    }
}

/*
 * Truncate everything up to and including the marker key. The stop cursor only needs its key set,
 * truncate positions it.
 */
static int
truncate_to_marker(scoped_session &session, scoped_cursor &cursor, scoped_cursor &stat_cursor,
  uint64_t marker_key, wt_timestamp_t commit_ts, truncate_pressure *pressure)
{
    int64_t before = 0;
    int ret;

    if (pressure != nullptr && !pressure->read_before_commit)
        before = get_stat(stat_cursor, pressure->stat_field);

    testutil_check(session->begin_transaction(session.get(), nullptr));
    testutil_check(cursor->reset(cursor.get()));
    cursor->set_key(cursor.get(), static_cast<int64_t>(marker_key));
    if ((ret = session->truncate(session.get(), nullptr, nullptr, cursor.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        testutil_check(session->rollback_transaction(session.get(), nullptr));
        return (ret);
    }

    if (pressure != nullptr && pressure->read_before_commit)
        pressure->record(get_stat(stat_cursor, pressure->stat_field));

    testutil_check(
      session->timestamp_transaction(session.get(), (COMMIT_TS + "=" + hex(commit_ts)).c_str()));
    if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        return (ret);
    }

    if (pressure != nullptr && !pressure->read_before_commit)
        pressure->record(get_stat(stat_cursor, pressure->stat_field) - before);

    return (0);
}

/*
 * A single truncate thread, matching the single trimming thread MongoDB runs against an oplog. It
 * trails the inserters and retires whole markers once the oplog is over its configured size.
 */
static void
truncate_worker(
  WT_CONNECTION *conn, const std::string &phase, uint64_t target_bytes, truncate_pressure *pressure)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);

    /* Statistics are read from their own session so they stay outside the truncate transaction. */
    scoped_session stat_session(conn);
    scoped_cursor stat_cursor = stat_session.open_scoped_cursor(STATISTICS_URI);

    execution_timer timer(phase + "_truncate", "test_disagg_truncate_perf");
    uint64_t marker_key;

    for (;;) {
        uint64_t inserted = g_inserted_bytes.load();
        bool expired = g_markers->expired_marker(g_next_key.load() - 1, inserted, &marker_key);

        if (!expired) {
            if (inserted >= target_bytes)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        wt_timestamp_t commit_ts = reserve_timestamp(static_cast<int>(opt.insert_threads));
        int ret = timer.track([&]() {
            return (
              truncate_to_marker(session, cursor, stat_cursor, marker_key, commit_ts, pressure));
        });
        release_timestamp(static_cast<int>(opt.insert_threads));
        if (ret != 0)
            g_truncate_rollbacks.fetch_add(1);
        else {
            g_truncate_ops.fetch_add(1);
            g_truncated_key.store(marker_key);
            if (conn == g_follower)
                g_follower_truncate_entries.fetch_add(1);
            replicate(true, marker_key, commit_ts);
        }
    }
}

/*
 * Keep the leader's stable timestamp just below the oldest transaction still in flight, and
 * checkpoint on an interval the way a running server would. Without the timestamp nothing can be
 * written out of cache and the connection stalls on unstable updates, and without the checkpoints
 * the internal pages stay dirty, which is the case where a truncate pins no extra dirty cache.
 */
static void
leader_maintenance_worker(WT_CONNECTION *conn, uint64_t target_bytes)
{
    scoped_session session(conn);
    auto last_checkpoint = std::chrono::steady_clock::now();

    while (g_inserted_bytes.load() < target_bytes) {
        uint64_t stable = g_timestamp.load();
        for (int i = 0; i < g_worker_count; i++)
            stable = std::min(stable, g_worker_timestamps[i].load());

        if (stable > 1)
            set_timestamp(conn, STABLE_TS + "=" + hex(stable - 1));

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_checkpoint).count() >=
          opt.checkpoint_interval_ms) {
            testutil_check(session->checkpoint(session.get(), nullptr));
            last_checkpoint = now;

            /*
             * Shipping a checkpoint is what lets the follower drain its ingest table and collect
             * truncate list entries. Do not wait for the pickup: a real follower adopts it while it
             * keeps applying, and waiting here would stall the leader.
             */
            if (g_replication_queue != nullptr)
                deliver_latest_checkpoint(false);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

/*
 * Apply the leader's write stream in order. A single thread keeps inserts and truncates in the
 * order the leader committed them, which is what makes the follower's view of the oplog match.
 */
static void
apply_worker(WT_CONNECTION *conn)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    scoped_cursor stat_cursor = session.open_scoped_cursor(STATISTICS_URI);
    const std::string value = random_generator::instance().generate_pseudo_random_string(
      static_cast<uint64_t>(opt.value_size));

    execution_timer insert_timer("replica_apply_insert", "test_disagg_truncate_perf");
    execution_timer truncate_timer("replica_apply_truncate", "test_disagg_truncate_perf");
    replication_op op;

    while (g_replication_queue->pop(&op)) {
        if (op.truncate) {
            int ret = truncate_timer.track([&]() {
                return (truncate_to_marker(
                  session, cursor, stat_cursor, op.key, op.timestamp, &g_follower_pressure));
            });
            if (ret == 0) {
                g_apply_truncates.fetch_add(1);
                g_follower_truncate_entries.fetch_add(1);
            } else
                g_apply_rollbacks.fetch_add(1);
        } else {
            int ret = insert_timer.track([&]() {
                return (insert_record(session, cursor, op.key, value, op.timestamp) ? 0 : 1);
            });
            if (ret == 0)
                g_apply_inserts.fetch_add(1);
            else
                g_apply_rollbacks.fetch_add(1);
        }
    }
}

static void
record_peak(std::atomic<uint64_t> &peak, uint64_t seen)
{
    uint64_t previous = peak.load();
    while (seen > previous && !peak.compare_exchange_weak(previous, seen))
        ;
}

/*
 * Follow cache occupancy while a phase runs. Reading these at a phase boundary says nothing: what
 * matters is how high they went while the truncates were happening. For the follower that includes
 * the ingest table, which only drains when a checkpoint is picked up, and the truncate list, whose
 * live length is what its searches have to walk.
 */
static void
cache_sampler_worker(WT_CONNECTION *conn, cache_pressure *pressure, std::atomic<bool> *stop)
{
    scoped_session session(conn);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    bool follower = conn == g_follower;

    while (!stop->load()) {
        uint64_t inuse = static_cast<uint64_t>(get_stat(cursor, WT_STAT_CONN_CACHE_BYTES_INUSE));
        uint64_t dirty = static_cast<uint64_t>(get_stat(cursor, WT_STAT_CONN_CACHE_BYTES_DIRTY));
        uint64_t updates =
          static_cast<uint64_t>(get_stat(cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES));
        uint64_t pages = static_cast<uint64_t>(get_stat(cursor, WT_STAT_CONN_CACHE_PAGES_INUSE));

        pressure->samples.fetch_add(1);
        pressure->bytes_inuse_total.fetch_add(inuse);
        pressure->bytes_dirty_total.fetch_add(dirty);
        pressure->bytes_updates_total.fetch_add(updates);
        record_peak(pressure->bytes_inuse_peak, inuse);
        record_peak(pressure->bytes_dirty_peak, dirty);
        record_peak(pressure->bytes_updates_peak, updates);
        record_peak(pressure->pages_inuse_peak, pages);

        if (follower) {
            scoped_cursor ingest_cursor = session.open_scoped_cursor(STATISTICS_URI + INGEST_URI);
            uint64_t ingest =
              static_cast<uint64_t>(get_stat(ingest_cursor, WT_STAT_DSRC_CACHE_BYTES_INUSE));
            pressure->ingest_bytes_total.fetch_add(ingest);
            record_peak(pressure->ingest_bytes_peak, ingest);

            uint64_t collected = static_cast<uint64_t>(
              get_stat(cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            uint64_t recorded = g_follower_truncate_entries.load();
            record_peak(pressure->truncate_list_entries_peak,
              recorded > collected ? recorded - collected : 0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

class cache_sampler {
public:
    cache_sampler(WT_CONNECTION *conn, cache_pressure *pressure)
    {
        _threads.add_thread(cache_sampler_worker, conn, pressure, &_stop);
    }

    ~cache_sampler()
    {
        _stop.store(true);
        _threads.join();
    }

private:
    std::atomic<bool> _stop{false};
    thread_manager _threads;
};

static void
report_phase_rate(const std::string &phase, int64_t elapsed_ms, uint64_t inserted_bytes)
{
    metrics_writer &writer = metrics_writer::instance();
    uint64_t elapsed = elapsed_ms <= 0 ? 1 : static_cast<uint64_t>(elapsed_ms);

    writer.add_stat(phase + "_duration_ms", elapsed_ms);
    writer.add_stat(phase + "_truncates_per_second", (g_truncate_ops.load() * 1000) / elapsed);
    writer.add_stat(
      phase + "_insert_mb_per_second", (inserted_bytes * 1000) / WT_MEGABYTE / elapsed);
}

/* Run the insert and truncate workload for one phase. */
static void
run_workload(WT_CONNECTION *conn, const std::string &phase, uint64_t phase_bytes,
  truncate_pressure *pressure, bool advance_stable)
{
    logger::log_msg(LOG_INFO, "Starting the " + phase + " phase.");
    reset_phase_counters();

    uint64_t start_bytes = g_inserted_bytes.load();
    uint64_t target_bytes = start_bytes + phase_bytes;
    auto start = std::chrono::steady_clock::now();
    cache_sampler sampler(conn, conn == g_leader ? &g_leader_cache : &g_follower_cache);
    thread_manager tm;
    for (int64_t i = 0; i < opt.insert_threads; i++)
        tm.add_thread(insert_worker, conn, static_cast<int>(i), target_bytes);
    tm.add_thread(truncate_worker, conn, phase, target_bytes, pressure);
    if (advance_stable)
        tm.add_thread(leader_maintenance_worker, conn, target_bytes);
    tm.join();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start)
                     .count();

    report_phase_rate(phase, elapsed, g_inserted_bytes.load() - start_bytes);
    logger::log_msg(LOG_INFO,
      "The " + phase + " phase completed " + std::to_string(g_truncate_ops.load()) + " truncates.");
}

/*
 * Populate the oplog up to its configured size. No truncation happens here, and the checkpoint at
 * the end is what lets the leader delete whole pages rather than walking them key by key.
 */
static void
populate(WT_CONNECTION *conn)
{
    logger::log_msg(LOG_INFO, "Populating " + std::to_string(opt.oplog_size_mb) + "MB.");
    reset_phase_counters();

    thread_manager tm;
    for (int64_t i = 0; i < opt.insert_threads; i++)
        tm.add_thread(insert_worker, conn, static_cast<int>(i), mb_to_bytes(opt.oplog_size_mb));
    tm.add_thread(leader_maintenance_worker, conn, mb_to_bytes(opt.oplog_size_mb));
    tm.join();

    scoped_session session(conn);
    testutil_check(session->checkpoint(session.get(), nullptr));
}

/* Read the metadata of the leader's newest complete checkpoint. */
static std::string
latest_checkpoint_meta()
{
    scoped_session session(g_leader);
    WT_PAGE_LOG *page_log;

    testutil_check(g_leader->get_page_log(g_leader, PAGE_LOG.c_str(), &page_log));
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
    testutil_check(page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));
    page_log->terminate(page_log, nullptr);

    std::string meta(
      static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size);
    free(args.checkpoint_metadata.mem);

    return (meta);
}

/* Take a leader checkpoint at the given timestamp and return its metadata. */
static std::string
leader_checkpoint(wt_timestamp_t timestamp)
{
    scoped_session session(g_leader);

    set_timestamp(g_leader, STABLE_TS + "=" + hex(timestamp));
    testutil_check(session->checkpoint(session.get(), nullptr));

    return (latest_checkpoint_meta());
}

/*
 * Delivering a checkpoint is asynchronous when a snapshot older than it is still open, so wait for
 * the pickup to land before measuring anything that depends on it.
 */
static void
wait_for_pickup()
{
    scoped_session session(g_follower);
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);

    for (int i = 0; i < 1000; i++) {
        int64_t delivered = get_stat(cursor, WT_STAT_CONN_DISAGG_CHECKPOINT_DELIVERED_LSN);
        int64_t adopted = get_stat(cursor, WT_STAT_CONN_DISAGG_CHECKPOINT_META_LSN);
        if (adopted >= delivered)
            return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    testutil_die(ETIMEDOUT, "the follower did not pick up the delivered checkpoint");
}

static void
deliver_checkpoint(const std::string &meta, bool wait = true)
{
    std::string config = "disaggregated=(checkpoint_meta=\"" + meta + "\")";

    testutil_check(g_follower->reconfigure(g_follower, config.c_str()));
    if (wait)
        wait_for_pickup();
}

static void
deliver_latest_checkpoint(bool wait)
{
    deliver_checkpoint(latest_checkpoint_meta(), wait);
}

/*
 * Truncate list entries are only reclaimed by a later truncate on the same table, so the garbage
 * collection phase is a short burst of truncates after the newer checkpoint has been picked up.
 */
static void
run_gc_truncates()
{
    scoped_session session(g_follower);
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    scoped_cursor stat_cursor = session.open_scoped_cursor(STATISTICS_URI);

    logger::log_msg(LOG_INFO, "Starting the garbage collection phase.");
    reset_phase_counters();
    cache_sampler sampler(g_follower, &g_follower_cache);

    /* Spread the truncates over whatever is left live, so they stay behind the insert head. */
    uint64_t marker_key = g_truncated_key.load();
    uint64_t live_keys = g_next_key.load() - marker_key;
    uint64_t chunk = live_keys / static_cast<uint64_t>(opt.gc_truncate_count + 1);
    testutil_assert(chunk > 0);

    for (int64_t i = 0; i < opt.gc_truncate_count; i++) {
        marker_key += chunk;
        if (truncate_to_marker(session, cursor, stat_cursor, marker_key, g_timestamp.fetch_add(1),
              &g_follower_pressure) != 0)
            g_truncate_rollbacks.fetch_add(1);
        else {
            g_truncate_ops.fetch_add(1);
            g_truncated_key.store(marker_key);
            g_follower_truncate_entries.fetch_add(1);
        }
    }
    logger::log_msg(LOG_INFO,
      "The garbage collection phase completed " + std::to_string(g_truncate_ops.load()) +
        " truncates.");
}

static void
report_counters(const std::string &phase)
{
    metrics_writer::instance().add_stat(phase + "_truncate_ops", g_truncate_ops.load());
    metrics_writer::instance().add_stat(phase + "_truncate_rollbacks", g_truncate_rollbacks.load());
    metrics_writer::instance().add_stat(phase + "_insert_rollbacks", g_insert_rollbacks.load());
}

/* How much cache a single truncate costs, which is the number the benchmark exists to produce. */
static void
report_truncate_pressure(const std::string &prefix, truncate_pressure &pressure)
{
    metrics_writer &writer = metrics_writer::instance();
    uint64_t count = pressure.count.load();

    writer.add_stat(
      prefix + "_truncate_pressure_bytes_mean", count == 0 ? 0 : pressure.total.load() / count);
    writer.add_stat(prefix + "_truncate_pressure_bytes_peak", pressure.peak.load());
    writer.add_stat(prefix + "_truncate_pressure_samples", count);
}

static void
report_cache_pressure(const std::string &prefix, cache_pressure &pressure, bool follower)
{
    metrics_writer &writer = metrics_writer::instance();
    uint64_t samples = pressure.samples.load();
    uint64_t divisor = samples == 0 ? 1 : samples;

    writer.add_stat(
      prefix + "_cache_bytes_inuse_mean", pressure.bytes_inuse_total.load() / divisor);
    writer.add_stat(prefix + "_cache_bytes_inuse_peak", pressure.bytes_inuse_peak.load());
    writer.add_stat(
      prefix + "_cache_bytes_dirty_mean", pressure.bytes_dirty_total.load() / divisor);
    writer.add_stat(prefix + "_cache_bytes_dirty_peak", pressure.bytes_dirty_peak.load());
    writer.add_stat(
      prefix + "_cache_bytes_updates_mean", pressure.bytes_updates_total.load() / divisor);
    writer.add_stat(prefix + "_cache_bytes_updates_peak", pressure.bytes_updates_peak.load());
    writer.add_stat(prefix + "_cache_pages_inuse_peak", pressure.pages_inuse_peak.load());

    if (follower) {
        writer.add_stat(
          prefix + "_ingest_bytes_mean", pressure.ingest_bytes_total.load() / divisor);
        writer.add_stat(prefix + "_ingest_bytes_peak", pressure.ingest_bytes_peak.load());
        writer.add_stat(
          prefix + "_truncate_list_entries_peak", pressure.truncate_list_entries_peak.load());
    }
}

static void
report_leader_stats(const stat_sample &before, const stat_sample &after)
{
    metrics_writer &writer = metrics_writer::instance();

    writer.add_stat("leader_page_delete_fast", after.page_delete_fast - before.page_delete_fast);
    writer.add_stat("leader_page_delete_fast_skipped",
      after.page_delete_fast_skipped - before.page_delete_fast_skipped);
    writer.add_stat(
      "leader_truncate_keys_deleted", after.truncate_keys_deleted - before.truncate_keys_deleted);
    report_truncate_pressure("leader", g_leader_pressure);
    report_cache_pressure("leader", g_leader_cache, false);
    writer.add_stat("leader_eviction_blocked_truncate",
      after.eviction_blocked_truncate - before.eviction_blocked_truncate);
    writer.add_stat("leader_truncate_dirty_cache_rollback",
      after.truncate_dirty_rollback - before.truncate_dirty_rollback);
    writer.add_stat("leader_bytes_dirty", after.bytes_dirty - before.bytes_dirty);
    writer.add_stat(
      "leader_eviction_pages_seen", after.eviction_pages_seen - before.eviction_pages_seen);
    report_counters("leader");
}

static void
report_list_search_cost(
  const std::string &phase, const stat_sample &before, const stat_sample &after)
{
    metrics_writer &writer = metrics_writer::instance();
    int64_t calls = after.list_search_calls - before.list_search_calls;
    int64_t walked = after.list_entries_walked - before.list_entries_walked;

    writer.add_stat(phase + "_truncate_list_search_calls", calls);
    writer.add_stat(phase + "_truncate_list_entries_walked", walked);
    writer.add_stat(
      phase + "_truncate_list_entries_walked_per_call", calls == 0 ? 0 : walked / calls);
}

static void
report_follower_stats(const stat_sample &before, const stat_sample &after)
{
    metrics_writer &writer = metrics_writer::instance();

    report_list_search_cost("follower", before, after);
    report_truncate_pressure("follower", g_follower_pressure);
    report_cache_pressure("follower", g_follower_cache, true);
    writer.add_stat("follower_bytes_dirty", after.bytes_dirty - before.bytes_dirty);
    writer.add_stat(
      "follower_eviction_pages_seen", after.eviction_pages_seen - before.eviction_pages_seen);
    report_counters("follower");
}

static void
report_gc_stats(const stat_sample &before, const stat_sample &after)
{
    metrics_writer &writer = metrics_writer::instance();

    writer.add_stat("gc_runs", after.gc_runs - before.gc_runs);
    writer.add_stat("gc_entries_removed", after.gc_entries_removed - before.gc_entries_removed);
    report_list_search_cost("gc", before, after);
    report_counters("gc");
}

static std::string
connection_config(const std::string &role)
{
    std::string config = CONNECTION_CREATE + ",cache_size=" + std::to_string(opt.cache_size_mb) +
      "MB,statistics=(all),statistics_log=(json,wait=1,on_close),precise_checkpoint=true" +
      ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so]" +
      ",disaggregated=(page_log=" + PAGE_LOG + ",role=\"" + role + "\")";

    if (opt.verbose_level > 0)
        config += ",verbose=(disaggregated_storage:" + std::to_string(opt.verbose_level) + ")";
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
}

static void
create_table(WT_CONNECTION *conn)
{
    scoped_session session(conn);

    testutil_check(session->create(session.get(), TABLE_URI.c_str(), TABLE_CONFIG.c_str()));
}

static void
report_replica_stats(const stat_sample &leader_before, const stat_sample &leader_after,
  const stat_sample &follower_before, const stat_sample &follower_after)
{
    metrics_writer &writer = metrics_writer::instance();

    writer.add_stat("replica_leader_page_delete_fast",
      leader_after.page_delete_fast - leader_before.page_delete_fast);
    writer.add_stat("replica_leader_truncate_keys_deleted",
      leader_after.truncate_keys_deleted - leader_before.truncate_keys_deleted);
    report_truncate_pressure("replica_leader", g_leader_pressure);
    report_cache_pressure("replica_leader", g_leader_cache, false);
    report_truncate_pressure("replica_follower", g_follower_pressure);
    report_cache_pressure("replica_follower", g_follower_cache, true);
    writer.add_stat("replica_leader_truncate_ops", g_truncate_ops.load());
    writer.add_stat(
      "replica_leader_bytes_dirty", leader_after.bytes_dirty - leader_before.bytes_dirty);

    report_list_search_cost("replica_follower", follower_before, follower_after);
    writer.add_stat("replica_follower_gc_runs", follower_after.gc_runs - follower_before.gc_runs);
    writer.add_stat("replica_follower_gc_entries_removed",
      follower_after.gc_entries_removed - follower_before.gc_entries_removed);
    writer.add_stat(
      "replica_follower_bytes_dirty", follower_after.bytes_dirty - follower_before.bytes_dirty);
    writer.add_stat("replica_apply_inserts", g_apply_inserts.load());
    writer.add_stat("replica_apply_truncates", g_apply_truncates.load());
    writer.add_stat("replica_apply_rollbacks", g_apply_rollbacks.load());
    writer.add_stat("replica_apply_queue_peak_depth", g_replication_queue->peak_depth());
}

/*
 * Run both roles at once, with the follower applying the leader's write stream and picking up its
 * checkpoints as they are taken. This is the shape a running replica set has, and the only one
 * where the ingest table drains and the truncate list settles rather than only growing.
 */
static void
run_replica_workload()
{
    logger::log_msg(LOG_INFO, "Starting the replica phase.");
    g_replication_queue.reset(new replication_queue());
    reset_phase_counters();

    stat_sample leader_before = read_stats(g_leader);
    stat_sample follower_before = read_stats(g_follower);

    uint64_t target_bytes = g_inserted_bytes.load() + mb_to_bytes(opt.replica_ingest_mb);
    auto start = std::chrono::steady_clock::now();
    {
        cache_sampler leader_sampler(g_leader, &g_leader_cache);
        cache_sampler follower_sampler(g_follower, &g_follower_cache);
        thread_manager tm;
        tm.add_thread(apply_worker, g_follower);
        for (int64_t i = 0; i < opt.insert_threads; i++)
            tm.add_thread(insert_worker, g_leader, static_cast<int>(i), target_bytes);
        tm.add_thread(truncate_worker, g_leader, "replica", target_bytes, &g_leader_pressure);
        tm.add_thread(leader_maintenance_worker, g_leader, target_bytes);

        /*
         * The leader's threads finish first. Closing the stream is what lets the follower finish
         * applying and stop.
         */
        std::thread closer([&]() {
            while (g_inserted_bytes.load() < target_bytes)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            g_replication_queue->close();
        });
        tm.join();
        closer.join();
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start)
                     .count();

    /* One last checkpoint, so the follower prunes what it recorded at the end of the run. */
    deliver_checkpoint(leader_checkpoint(g_timestamp.fetch_add(1)));

    report_phase_rate("replica", elapsed, g_inserted_bytes.load());
    report_replica_stats(
      leader_before, read_stats(g_leader), follower_before, read_stats(g_follower));
    logger::log_msg(LOG_INFO,
      "The replica phase applied " + std::to_string(g_apply_inserts.load()) + " inserts and " +
        std::to_string(g_apply_truncates.load()) + " truncates.");
}

/* Measure each role on its own, with the follower picking up a single checkpoint at the end. */
static void
run_phase_workload()
{
    stat_sample leader_before = read_stats(g_leader);
    run_workload(g_leader, "leader", mb_to_bytes(opt.leader_ingest_mb), &g_leader_pressure, true);
    stat_sample leader_after = read_stats(g_leader);
    report_leader_stats(leader_before, leader_after);

    /* Hand the follower everything the leader has written, then let it run its own workload. */
    deliver_checkpoint(leader_checkpoint(g_timestamp.fetch_add(1)));

    stat_sample follower_before = read_stats(g_follower);
    run_workload(
      g_follower, "follower", mb_to_bytes(opt.follower_ingest_mb), &g_follower_pressure, false);
    stat_sample follower_after = read_stats(g_follower);
    report_follower_stats(follower_before, follower_after);

    /*
     * A checkpoint taken now sits above every truncate the follower recorded, so picking it up
     * moves the prune timestamp past them and the entries can be freed.
     */
    deliver_checkpoint(leader_checkpoint(g_timestamp.fetch_add(1)));

    stat_sample gc_before = read_stats(g_follower);
    run_gc_truncates();
    stat_sample gc_after = read_stats(g_follower);
    report_gc_stats(gc_before, gc_after);
}

int
main(int argc, char *argv[])
{
    const std::string progname = testutil_set_progname(argv);
    logger::trace_level = LOG_INFO;

    logger::log_msg(LOG_INFO, "Starting " + progname);
    load_configuration(argc, argv);

    g_markers.reset(new oplog_markers());
    g_worker_count = static_cast<int>(opt.insert_threads) + 1;
    g_worker_timestamps.reset(new std::atomic<uint64_t>[g_worker_count]);
    for (int i = 0; i < g_worker_count; i++)
        g_worker_timestamps[i].store(WT_TS_MAX);

    open_connections();
    create_table(g_leader);
    create_table(g_follower);

    /* The oldest timestamp stays where it is, nothing in the test relies on moving it. */
    set_timestamp(g_leader, OLDEST_TS + "=" + hex(1));
    set_timestamp(g_follower, OLDEST_TS + "=" + hex(1));

    populate(g_leader);

    if (opt.workload == "replica")
        run_replica_workload();
    else
        run_phase_workload();

    metrics_writer::instance().output_perf_file(progname);
    testutil_check(g_follower->close(g_follower, nullptr));
    testutil_check(g_leader->close(g_leader, nullptr));
    return (EXIT_SUCCESS);
}
