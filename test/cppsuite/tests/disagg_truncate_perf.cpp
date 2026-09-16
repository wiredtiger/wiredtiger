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

#include "src/common/random_generator.h"
#include "src/component/metrics_writer.h"
#include "src/main/test.h"
#include "src/util/execution_timer.h"

#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace test_harness;

/*
 * This test measures what a truncate costs on a layered table, and the cache it creates. It runs an
 * oplog shaped workload in one role, chosen by configuration.
 *
 * A leader deletes ranges out of the stable tree while it appends to the oplog. A follower is
 * loaded as a leader first, then restarted in the follower role, where its truncates go to a
 * truncate list rather than the tree, and it picks up the checkpoints taken during the load, which
 * is what lets that list be collected.
 */
class disagg_truncate_perf : public test {
public:
    explicit disagg_truncate_perf(const test_args &args) : test(args) {}

    void run() override final;

private:
    struct named_stat {
        const char *name;
        int field;
    };

    /* Connection statistics, reported as the change across the measured phase. */
    static constexpr named_stat PHASE_STATS[] = {
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
      {"checkpoints_picked_up",
        WT_STAT_CONN_LAYERED_TABLE_MANAGER_CHECKPOINTS_DISAGG_PICK_UP_FOLLOWER},
    };

    /* Cache occupancy, sampled while the phase runs rather than read once at its end. */
    static constexpr named_stat CACHE_GAUGES[] = {
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

    /* The cache a single truncate creates, charged per operation. */
    struct truncate_cost {
        std::atomic<uint64_t> count{0}, total{0}, peak{0};
    };

    /* Configuration. */
    int64_t _cache_size_mb, _checkpoint_interval_ms, _insert_mb, _insert_threads;
    int64_t _marker_size_mb, _oplog_size_mb, _truncate_count, _value_size;
    bool _follower;

    std::atomic<uint64_t> _next_key{1}, _inserted_bytes{0}, _truncated_key{0};
    std::atomic<uint64_t> _truncate_ops{0}, _insert_rollbacks{0}, _truncate_rollbacks{0};
    std::atomic<uint64_t> _truncate_list_entries{0};

    /* Checkpoints taken while loading, handed to the follower one at a time once it restarts. */
    std::vector<std::string> _checkpoints;

    cache_pressure _cache;
    truncate_cost _cost;

    void load_config();
    void load(bool capture_checkpoints);
    void run_leader();
    void run_follower();
    void insert_worker(uint64_t target_bytes);
    void truncate_worker(uint64_t target_bytes);
    void checkpoint_worker(uint64_t target_bytes, bool capture_checkpoints);
    void pickup_worker(std::atomic<bool> *stop);
    void sampler_worker(std::atomic<bool> *stop);
    bool expired_marker(uint64_t *marker_keyp);
    int truncate_to_marker(
      scoped_session &session, scoped_cursor &cursor, scoped_cursor &stat_cursor, uint64_t key);
    phase_stats read_phase_stats();
    void report_phase(const phase_stats &before, const phase_stats &after, uint64_t elapsed_ms);
    std::string connection_config() const;

    uint64_t
    record_bytes() const
    {
        return (sizeof(int64_t) + static_cast<uint64_t>(_value_size));
    }

    uint64_t
    marker_records() const
    {
        return (mb_to_bytes(_marker_size_mb) / record_bytes());
    }

    static uint64_t
    mb_to_bytes(int64_t mb)
    {
        return (static_cast<uint64_t>(mb) * WT_MEGABYTE);
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
};

constexpr disagg_truncate_perf::named_stat disagg_truncate_perf::PHASE_STATS[];
constexpr disagg_truncate_perf::named_stat disagg_truncate_perf::CACHE_GAUGES[];

static const std::string TABLE_URI = "layered:oplog";
static const std::string INGEST_URI = "file:oplog.wt_ingest";
static const std::string PAGE_LOG = "palite";

/* A percentile is only worth reporting over a reasonable number of operations. */
static const int64_t MINIMUM_TRUNCATES = 25;

void
disagg_truncate_perf::load_config()
{
    _cache_size_mb = _config->get_int(CACHE_SIZE_MB);
    _checkpoint_interval_ms = _config->get_int("checkpoint_interval_ms");
    _insert_mb = _config->get_int("insert_mb");
    _insert_threads = _config->get_int("insert_threads");
    _marker_size_mb = _config->get_int("marker_size_mb");
    _oplog_size_mb = _config->get_int("oplog_size_mb");
    _truncate_count = _config->get_int("truncate_count");
    _value_size = _config->get_int("value_size");
    _follower = _config->get_string("role") == "follower";

    if (_marker_size_mb > _oplog_size_mb)
        testutil_die(EINVAL, "the marker size must not exceed the oplog size");

    /*
     * A leader truncates as the oplog overflows, so its insert volume and the marker size decide
     * how many truncates it gets to measure. A follower is told how many to run.
     */
    int64_t truncates = _follower ? _truncate_count : _insert_mb / _marker_size_mb;
    if (truncates < MINIMUM_TRUNCATES)
        testutil_die(EINVAL,
          "the run would measure %" PRId64 " truncates, fewer than the %" PRId64
          " needed to measure one: raise insert_mb or truncate_count, or lower marker_size_mb",
          truncates, MINIMUM_TRUNCATES);
}

std::string
disagg_truncate_perf::connection_config() const
{
    return (CONNECTION_CREATE + ",cache_size=" + std::to_string(_cache_size_mb) +
      "MB,statistics=(all),statistics_log=(json,wait=1,on_close),precise_checkpoint=true" +
      ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so]" +
      ",disaggregated=(page_log=" + PAGE_LOG + ",role=\"%ROLE%\")" + _args.wt_open_config);
}

/*
 * MongoDB reclaims the oplog in whole markers rather than a record at a time. Keys are sequential
 * and records are a fixed size, so the oldest marker is whatever sits behind the volume the oplog
 * is meant to keep: one large range per truncate, well behind the insert head.
 */
bool
disagg_truncate_perf::expired_marker(uint64_t *marker_keyp)
{
    uint64_t keep = mb_to_bytes(_oplog_size_mb) / record_bytes();
    uint64_t head = _next_key.load();

    if (head - _truncated_key.load() <= keep + marker_records())
        return (false);

    *marker_keyp = head - keep;
    return (true);
}

disagg_truncate_perf::phase_stats
disagg_truncate_perf::read_phase_stats()
{
    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    phase_stats stats;

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        stats[i] = metrics_monitor::get_stat(cursor, PHASE_STATS[i].field);
    return (stats);
}

void
disagg_truncate_perf::insert_worker(uint64_t target_bytes)
{
    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    const std::string value = random_generator::instance().generate_pseudo_random_string(
      static_cast<uint64_t>(_value_size));

    while (_inserted_bytes.load() < target_bytes) {
        int ret;

        testutil_check(session->begin_transaction(session.get(), nullptr));
        cursor->set_key(cursor.get(), static_cast<int64_t>(_next_key.fetch_add(1)));
        cursor->set_value(cursor.get(), value.c_str());
        if ((ret = cursor->insert(cursor.get())) != 0) {
            testutil_assert(ret == WT_ROLLBACK);
            testutil_check(session->rollback_transaction(session.get(), nullptr));
            _insert_rollbacks.fetch_add(1);
            continue;
        }

        testutil_check(session->timestamp_transaction(session.get(),
          (COMMIT_TS + "=" + timestamp_manager::decimal_to_hex(_timestamp_manager->get_next_ts()))
            .c_str()));
        if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
            testutil_assert(ret == WT_ROLLBACK);
            _insert_rollbacks.fetch_add(1);
        } else
            _inserted_bytes.fetch_add(record_bytes());
    }
}

/*
 * Truncate up to the marker key, charging the operation for the cache it creates. A leader pins
 * dirty internal pages until the transaction resolves, and that gauge is given back at commit, so
 * it is read while the truncate is open. A follower instead grows its ingest table and truncate
 * list, which is the change in update bytes across the whole operation.
 */
int
disagg_truncate_perf::truncate_to_marker(
  scoped_session &session, scoped_cursor &cursor, scoped_cursor &stat_cursor, uint64_t marker_key)
{
    int64_t before =
      _follower ? metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES) : 0;
    int ret;

    testutil_check(session->begin_transaction(session.get(), nullptr));
    testutil_check(cursor->reset(cursor.get()));
    cursor->set_key(cursor.get(), static_cast<int64_t>(marker_key));
    if ((ret = session->truncate(session.get(), nullptr, nullptr, cursor.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        testutil_check(session->rollback_transaction(session.get(), nullptr));
        return (ret);
    }

    int64_t cost = _follower ?
      0 :
      metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_TRUNCATE_TXN_UNCOMMITTED_BYTES);

    testutil_check(session->timestamp_transaction(session.get(),
      (COMMIT_TS + "=" + timestamp_manager::decimal_to_hex(_timestamp_manager->get_next_ts()))
        .c_str()));
    if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        return (ret);
    }

    if (_follower) {
        cost = metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES) - before;
        _truncate_list_entries.fetch_add(1);
    }

    /*
     * Every truncate counts, including the ones that cost nothing: a truncate whose parent page was
     * already dirty pins nothing new, and leaving those out would overstate what a truncate costs.
     */
    uint64_t charged = cost <= 0 ? 0 : static_cast<uint64_t>(cost);
    _cost.count.fetch_add(1);
    _cost.total.fetch_add(charged);
    record_peak(_cost.peak, charged);

    _truncate_ops.fetch_add(1);
    _truncated_key.store(marker_key);
    return (0);
}

/*
 * A single truncate thread, matching the single trimming thread MongoDB runs against an oplog. It
 * trails the inserters and retires whole markers once the oplog is over its configured size.
 */
void
disagg_truncate_perf::truncate_worker(uint64_t target_bytes)
{
    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);

    /* Statistics are read from their own session so they stay outside the truncate transaction. */
    scoped_session stat_session = connection_manager::instance().create_session();
    scoped_cursor stat_cursor = stat_session.open_scoped_cursor(STATISTICS_URI);

    execution_timer timer("truncate", _args.test_name);
    uint64_t marker_key;

    for (;;) {
        if (!expired_marker(&marker_key)) {
            if (_inserted_bytes.load() >= target_bytes)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        if (timer.track([&]() {
                return (truncate_to_marker(session, cursor, stat_cursor, marker_key));
            }) != 0)
            _truncate_rollbacks.fetch_add(1);
    }
}

/*
 * Checkpoint on an interval the way a running server would. Without the checkpoints the internal
 * pages stay dirty, which is the case where a truncate pins no extra cache at all, and their
 * metadata is what a follower later picks up.
 */
void
disagg_truncate_perf::checkpoint_worker(uint64_t target_bytes, bool capture_checkpoints)
{
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    scoped_session session = connection_manager::instance().create_session();

    while (_inserted_bytes.load() < target_bytes) {
        std::this_thread::sleep_for(std::chrono::milliseconds(_checkpoint_interval_ms));
        testutil_check(session->checkpoint(session.get(), nullptr));

        if (!capture_checkpoints)
            continue;

        WT_PAGE_LOG *page_log;
        testutil_check(conn->get_page_log(conn, PAGE_LOG.c_str(), &page_log));
        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
        testutil_check(page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));
        page_log->terminate(page_log, nullptr);

        _checkpoints.push_back(std::string(
          static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size));
        free(args.checkpoint_metadata.mem);
    }
}

/*
 * Hand the follower the checkpoints taken while it was loaded, one at a time. Picking one up is
 * what advances the prune timestamp, and so what lets the truncate list be collected. A checkpoint
 * the connection already holds is refused, which is expected for the ones it wrote most recently.
 */
void
disagg_truncate_perf::pickup_worker(std::atomic<bool> *stop)
{
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    size_t next = 0;

    while (!stop->load() && next < _checkpoints.size()) {
        std::string config = "disaggregated=(checkpoint_meta=\"" + _checkpoints[next++] + "\")";
        int ret = conn->reconfigure(conn, config.c_str());

        if (ret != 0 && ret != EINVAL)
            testutil_check(ret);
        std::this_thread::sleep_for(std::chrono::milliseconds(_checkpoint_interval_ms));
    }
}

/*
 * Follow cache occupancy while the phase runs: what matters is how high it went while the truncates
 * were happening. A follower also carries its ingest table and its truncate list.
 */
void
disagg_truncate_perf::sampler_worker(std::atomic<bool> *stop)
{
    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);

    while (!stop->load()) {
        _cache.samples.fetch_add(1);
        for (size_t i = 0; i < WT_ELEMENTS(CACHE_GAUGES); i++) {
            uint64_t seen =
              static_cast<uint64_t>(metrics_monitor::get_stat(cursor, CACHE_GAUGES[i].field));
            _cache.total[i].fetch_add(seen);
            record_peak(_cache.peak[i], seen);
        }

        if (_follower) {
            scoped_cursor ingest_cursor = session.open_scoped_cursor(STATISTICS_URI + INGEST_URI);
            uint64_t ingest = static_cast<uint64_t>(
              metrics_monitor::get_stat(ingest_cursor, WT_STAT_DSRC_CACHE_BYTES_INUSE));
            _cache.ingest_total.fetch_add(ingest);
            record_peak(_cache.ingest_peak, ingest);

            uint64_t collected = static_cast<uint64_t>(metrics_monitor::get_stat(
              cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            uint64_t recorded = _truncate_list_entries.load();
            record_peak(_cache.list_entries_peak, recorded > collected ? recorded - collected : 0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

/* Report everything the phase produced: what it did, what each truncate cost, and what it held. */
void
disagg_truncate_perf::report_phase(
  const phase_stats &before, const phase_stats &after, uint64_t elapsed_ms)
{
    const std::string role = _follower ? "follower" : "leader";
    uint64_t truncates = std::max(_cost.count.load(), static_cast<uint64_t>(1));
    uint64_t samples = std::max(_cache.samples.load(), static_cast<uint64_t>(1));
    uint64_t elapsed = std::max(elapsed_ms, static_cast<uint64_t>(1));

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        report(role + "_" + PHASE_STATS[i].name, static_cast<uint64_t>(after[i] - before[i]));

    report(role + "_duration_ms", elapsed);
    report(role + "_truncates_per_second", (_truncate_ops.load() * 1000) / elapsed);
    report(role + "_truncate_ops", _truncate_ops.load());
    report(role + "_truncate_rollbacks", _truncate_rollbacks.load());
    report(role + "_insert_rollbacks", _insert_rollbacks.load());
    report(role + "_truncate_pressure_bytes_mean", _cost.total.load() / truncates);
    report(role + "_truncate_pressure_bytes_peak", _cost.peak.load());

    for (size_t i = 0; i < WT_ELEMENTS(CACHE_GAUGES); i++) {
        report(role + "_" + CACHE_GAUGES[i].name + "_mean", _cache.total[i].load() / samples);
        report(role + "_" + CACHE_GAUGES[i].name + "_peak", _cache.peak[i].load());
    }
    if (_follower) {
        report(role + "_ingest_bytes_mean", _cache.ingest_total.load() / samples);
        report(role + "_ingest_bytes_peak", _cache.ingest_peak.load());
        report(role + "_truncate_list_entries_peak", _cache.list_entries_peak.load());
    }
    logger::log_msg(LOG_INFO,
      "The " + role + " phase completed " + std::to_string(_truncate_ops.load()) + " truncates.");
}

/*
 * Fill the oplog as a leader. The follower run loads its data this way too, and keeps the
 * checkpoints taken along the way to pick up once it restarts.
 */
void
disagg_truncate_perf::load(bool capture_checkpoints)
{
    logger::log_msg(LOG_INFO, "Loading " + std::to_string(_oplog_size_mb) + "MB as a leader.");

    thread_manager tm;
    uint64_t target_bytes = mb_to_bytes(_oplog_size_mb);
    for (int64_t i = 0; i < _insert_threads; i++)
        tm.add_thread(&disagg_truncate_perf::insert_worker, this, target_bytes);
    tm.add_thread(
      &disagg_truncate_perf::checkpoint_worker, this, target_bytes, capture_checkpoints);
    tm.join();

    scoped_session session = connection_manager::instance().create_session();
    testutil_check(session->checkpoint(session.get(), nullptr));
}

/* Append to the oplog and trim it as it overflows, which is what a primary does. */
void
disagg_truncate_perf::run_leader()
{
    logger::log_msg(LOG_INFO, "Starting the leader phase.");

    std::atomic<bool> sampling_done{false};
    uint64_t target_bytes = _inserted_bytes.load() + mb_to_bytes(_insert_mb);
    phase_stats before = read_phase_stats();
    auto start = std::chrono::steady_clock::now();

    {
        thread_manager tm;
        for (int64_t i = 0; i < _insert_threads; i++)
            tm.add_thread(&disagg_truncate_perf::insert_worker, this, target_bytes);
        tm.add_thread(&disagg_truncate_perf::truncate_worker, this, target_bytes);
        tm.add_thread(&disagg_truncate_perf::checkpoint_worker, this, target_bytes, false);
        tm.add_thread(&disagg_truncate_perf::sampler_worker, this, &sampling_done);

        thread_manager stopper;
        stopper.add_thread([&]() {
            while (_inserted_bytes.load() < target_bytes)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            sampling_done.store(true);
        });
        tm.join();
        stopper.join();
    }

    report_phase(before, read_phase_stats(),
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start)
          .count()));
}

/*
 * Trim an oplog a leader wrote, while picking up that leader's checkpoints. A follower does not
 * append: its rows arrive in the checkpoints it adopts.
 */
void
disagg_truncate_perf::run_follower()
{
    logger::log_msg(LOG_INFO, "Starting the follower phase.");

    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(TABLE_URI);
    scoped_cursor stat_cursor = session.open_scoped_cursor(STATISTICS_URI);

    std::atomic<bool> done{false};
    phase_stats before = read_phase_stats();
    auto start = std::chrono::steady_clock::now();
    execution_timer timer("truncate", _args.test_name);

    thread_manager tm;
    tm.add_thread(&disagg_truncate_perf::sampler_worker, this, &done);
    tm.add_thread(&disagg_truncate_perf::pickup_worker, this, &done);

    uint64_t marker_key = _truncated_key.load();
    for (int64_t i = 0; i < _truncate_count; i++) {
        marker_key += marker_records();
        testutil_assert(marker_key < _next_key.load());
        if (timer.track([&]() {
                return (truncate_to_marker(session, cursor, stat_cursor, marker_key));
            }) != 0)
            _truncate_rollbacks.fetch_add(1);
    }

    done.store(true);
    tm.join();

    report_phase(before, read_phase_stats(),
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start)
          .count()));
}

void
disagg_truncate_perf::run()
{
    load_config();

    std::string home = _args.home.empty() ? DEFAULT_DIR : _args.home;
    std::string config = connection_config();
    std::string leader_config = config;
    leader_config.replace(leader_config.find("%ROLE%"), strlen("%ROLE%"), "leader");

    testutil_remove(home.c_str());
    connection_manager::instance().create(leader_config, home);

    {
        scoped_session session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session.get(), TABLE_URI.c_str(), "key_format=q,value_format=S"));
    }

    /*
     * The framework's timestamp manager moves the stable and oldest timestamps behind the clock,
     * which is what lets anything leave cache. Run it for the whole test.
     */
    _timestamp_manager->load();
    thread_manager timestamps;
    timestamps.add_thread(&component::run, _timestamp_manager);

    load(_follower);

    if (!_follower) {
        run_leader();
        _timestamp_manager->end_run();
        timestamps.join();
    } else {
        /*
         * Restart in the follower role. A connection cannot adopt a checkpoint it wrote itself, so
         * the restart is what lets the checkpoints kept during the load be picked up. The timestamp
         * manager stops first: it moves timestamps on the connection that is about to close, and a
         * follower has no need of it, only of the timestamps it hands out.
         */
        _timestamp_manager->end_run();
        timestamps.join();

        logger::log_msg(LOG_INFO, "Restarting as a follower.");
        std::string follower_config = config;
        follower_config.replace(follower_config.find("%ROLE%"), strlen("%ROLE%"), "follower");
        connection_manager::instance().close();
        connection_manager::instance().reopen(follower_config, home);
        run_follower();
    }

    metrics_writer::instance().output_perf_file(_args.test_name);
}
