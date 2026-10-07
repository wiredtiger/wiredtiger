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
#include <memory>
#include <shared_mutex>
#include <thread>
#include <vector>

using namespace test_harness;

/* Append fixed-size records to a layered table and measure truncation as leader, restarted
 * follower, or in-place switch.
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
      {"truncate_keys_deleted", WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED},
      {"truncate_dirty_cache_rollback", WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK},
    };

    static constexpr named_stat FOLLOWER_STATS[] = {
      {"truncate_list_search_calls", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS},
      {"truncate_list_entries_walked", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED},
      {"truncate_list_gc_runs", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS},
      {"truncate_list_gc_entries_removed", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED},
    };

    using connection_stat_snapshot =
      std::array<int64_t, WT_ELEMENTS(PHASE_STATS) + WT_ELEMENTS(FOLLOWER_STATS)>;

    /* Configuration. */
    int64_t _cache_size_mb;
    int64_t _insert_mb;
    int64_t _marker_size_mb;
    int64_t _oplog_size_mb;
    int64_t _value_size;
    std::string _role;

    /* Set once the connection steps down to follower. */
    bool _follower = false;

    /* Shared by workload workers. */
    std::atomic<uint64_t> _next_key{1};
    std::atomic<uint64_t> _inserted_bytes{0};

    uint64_t _truncated_key{0};
    uint64_t _truncate_ops{0};
    uint64_t _truncate_rollbacks{0};
    uint64_t _truncate_list_entries{0};
    uint64_t _truncate_list_entries_peak{0};
    uint64_t _truncate_pressure_bytes_peak{0};

    /* Checkpoints captured during load and adopted after restarting as a follower. */
    std::vector<std::string> _checkpoints;

    /* Exclude inserts while measuring a truncate's connection-wide cache delta. */
    std::shared_mutex _insert_gate;

    /* The timestamp manager in use, replaced after stepping down. */
    timestamp_manager *_tsm = nullptr;

    void load_config();
    void load(bool capture_checkpoints);
    void run_workload();
    void insert_worker(uint64_t target_bytes);
    void truncate_worker(uint64_t target_bytes);
    void checkpoint_worker(uint64_t target_bytes, bool capture_checkpoints);
    void pickup_worker(std::atomic<bool> *stop);
    bool expired_marker(uint64_t *marker_keyp);
    int truncate_marker(
      scoped_session &session, scoped_cursor &cursor, scoped_cursor &stat_cursor, uint64_t key);
    connection_stat_snapshot read_phase_stats(bool include_follower_stats);
    void report_phase(const connection_stat_snapshot &before, const connection_stat_snapshot &after,
      uint64_t elapsed_ms);

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
};

constexpr disagg_truncate_perf::named_stat disagg_truncate_perf::PHASE_STATS[];
constexpr disagg_truncate_perf::named_stat disagg_truncate_perf::FOLLOWER_STATS[];

static const std::string TABLE_URI = "layered:oplog";
static const std::string PAGE_LOG = "palite";

/* A percentile is only worth reporting over a reasonable number of operations. */
static const int64_t MINIMUM_TRUNCATES = 25;
static const int64_t CHECKPOINT_INTERVAL_MS = 2000;
/* Multiple appenders keep the single truncate worker supplied with oplog records. */
static const int64_t INSERT_THREADS = 4;

void
disagg_truncate_perf::load_config()
{
    _cache_size_mb = _config->get_int(CACHE_SIZE_MB);
    _insert_mb = _config->get_int("insert_mb");
    _marker_size_mb = _config->get_int("marker_size_mb");
    _oplog_size_mb = _config->get_int("oplog_size_mb");
    _value_size = _config->get_int("value_size");
    _role = _config->get_string("role");
    if (_role != "leader" && _role != "switch" && _role != "follower")
        testutil_die(EINVAL, "unknown role \"%s\"", _role.c_str());

    if (_marker_size_mb > _oplog_size_mb)
        testutil_die(EINVAL, "the marker size must not exceed the oplog size");

    /*
     * Truncation follows the oplog overflowing, so the insert volume and the marker size set its
     * rate.
     */
    int64_t truncates = _insert_mb / _marker_size_mb;
    if (truncates < MINIMUM_TRUNCATES)
        testutil_die(EINVAL,
          "the run would measure %" PRId64 " truncates, fewer than the %" PRId64
          " needed to measure one: raise insert_mb or lower marker_size_mb",
          truncates, MINIMUM_TRUNCATES);
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

    if (head - _truncated_key <= keep + marker_records())
        return (false);

    *marker_keyp = head - keep;
    return (true);
}

disagg_truncate_perf::connection_stat_snapshot
disagg_truncate_perf::read_phase_stats(bool include_follower_stats)
{
    scoped_session session = connection_manager::instance().create_session();
    scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
    connection_stat_snapshot stats{};

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        stats[i] = metrics_monitor::get_stat(cursor, PHASE_STATS[i].field);
    if (_follower && include_follower_stats) {
        for (size_t i = 0; i < WT_ELEMENTS(FOLLOWER_STATS); i++)
            stats[WT_ELEMENTS(PHASE_STATS) + i] =
              metrics_monitor::get_stat(cursor, FOLLOWER_STATS[i].field);
    }
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
        std::shared_lock<std::shared_mutex> gate(_insert_gate);
        int ret;

        testutil_check(session->begin_transaction(session.get(), nullptr));
        cursor->set_key(cursor.get(), static_cast<int64_t>(_next_key.fetch_add(1)));
        cursor->set_value(cursor.get(), value.c_str());
        if ((ret = cursor->insert(cursor.get())) != 0) {
            testutil_assert(ret == WT_ROLLBACK);
            testutil_check(session->rollback_transaction(session.get(), nullptr));
            continue;
        }

        testutil_check(session->timestamp_transaction(session.get(),
          (COMMIT_TS + "=" + timestamp_manager::decimal_to_hex(_tsm->get_next_ts())).c_str()));
        /*
         * The oldest timestamp can overtake the commit timestamp chosen above while the commit is
         * in flight, which the API reports as EINVAL rather than a rollback. Either way the
         * transaction is resolved and the record is simply retried under a fresh timestamp.
         */
        if ((ret = session->commit_transaction(session.get(), nullptr)) != 0)
            testutil_assert(ret == WT_ROLLBACK || ret == EINVAL);
        else
            _inserted_bytes.fetch_add(record_bytes());
    }
}

/*
 * Apply one expired marker and measure its cache cost: before commit for leaders, or as
 * truncate-list growth for followers.
 */
int
disagg_truncate_perf::truncate_marker(
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
      (COMMIT_TS + "=" + timestamp_manager::decimal_to_hex(_tsm->get_next_ts())).c_str()));
    if ((ret = session->commit_transaction(session.get(), nullptr)) != 0) {
        testutil_assert(ret == WT_ROLLBACK);
        return (ret);
    }

    if (_follower) {
        cost = metrics_monitor::get_stat(stat_cursor, WT_STAT_CONN_CACHE_BYTES_UPDATES) - before;
        ++_truncate_list_entries;
        uint64_t collected = static_cast<uint64_t>(metrics_monitor::get_stat(
          stat_cursor, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
        uint64_t uncollected =
          _truncate_list_entries > collected ? _truncate_list_entries - collected : 0;
        if (uncollected > _truncate_list_entries_peak)
            _truncate_list_entries_peak = uncollected;
    }

    /*
     * Every truncate counts, including the ones that cost nothing: a truncate whose parent page was
     * already dirty pins nothing new, and leaving those out would overstate what a truncate costs.
     */
    uint64_t charged = cost <= 0 ? 0 : static_cast<uint64_t>(cost);
    if (charged > _truncate_pressure_bytes_peak)
        _truncate_pressure_bytes_peak = charged;

    ++_truncate_ops;
    _truncated_key = marker_key;
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

        std::unique_lock<std::shared_mutex> gate(_insert_gate);
        if (timer.track(
              [&]() { return (truncate_marker(session, cursor, stat_cursor, marker_key)); }) != 0)
            ++_truncate_rollbacks;
    }
}

/*
 * Checkpoint on an interval the way a running server would. For a follower run, retain checkpoint
 * metadata so the restarted connection can adopt it during the measured phase.
 */
void
disagg_truncate_perf::checkpoint_worker(uint64_t target_bytes, bool capture_checkpoints)
{
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    scoped_session session = connection_manager::instance().create_session();

    while (_inserted_bytes.load() < target_bytes) {
        std::this_thread::sleep_for(std::chrono::milliseconds(CHECKPOINT_INTERVAL_MS));
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
 * The restarted follower applies saved checkpoints to advance prune time and collect truncate-list
 * entries.
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
        std::this_thread::sleep_for(std::chrono::milliseconds(CHECKPOINT_INTERVAL_MS));
    }
}

/* Report everything the phase produced: what it did, what each truncate cost, and what it held. */
void
disagg_truncate_perf::report_phase(const connection_stat_snapshot &before,
  const connection_stat_snapshot &after, uint64_t elapsed_ms)
{
    const std::string &role = _role;
    uint64_t elapsed = std::max(elapsed_ms, static_cast<uint64_t>(1));

    for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); i++)
        report(role + "_" + PHASE_STATS[i].name, static_cast<uint64_t>(after[i] - before[i]));
    if (_follower) {
        for (size_t i = 0; i < WT_ELEMENTS(FOLLOWER_STATS); i++)
            report(role + "_" + FOLLOWER_STATS[i].name,
              static_cast<uint64_t>(after[WT_ELEMENTS(PHASE_STATS) + i]));
        report(role + "_truncate_list_entries_peak", _truncate_list_entries_peak);
    }

    report(role + "_duration_ms", elapsed);
    report(role + "_truncate_ops", _truncate_ops);
    report(role + "_truncate_rollbacks", _truncate_rollbacks);
    report(role + "_truncate_pressure_bytes_peak", _truncate_pressure_bytes_peak);
    logger::log_msg(LOG_INFO,
      "The " + role + " phase completed " + std::to_string(_truncate_ops) + " truncates.");
}

/*
 * Fill the oplog as a leader before measuring each role.
 */
void
disagg_truncate_perf::load(bool capture_checkpoints)
{
    logger::log_msg(LOG_INFO, "Loading " + std::to_string(_oplog_size_mb) + "MB as a leader.");

    thread_manager tm;
    uint64_t target_bytes = mb_to_bytes(_oplog_size_mb);
    for (int64_t i = 0; i < INSERT_THREADS; i++)
        tm.add_thread(&disagg_truncate_perf::insert_worker, this, target_bytes);
    tm.add_thread(
      &disagg_truncate_perf::checkpoint_worker, this, target_bytes, capture_checkpoints);
    tm.join();

    scoped_session session = connection_manager::instance().create_session();
    testutil_check(session->checkpoint(session.get(), nullptr));
}

/* Append to the oplog and trim it as it overflows. */
void
disagg_truncate_perf::run_workload()
{
    logger::log_msg(LOG_INFO, "Starting the " + _role + " phase.");

    std::atomic<bool> done{false};
    uint64_t target_bytes = _inserted_bytes.load() + mb_to_bytes(_insert_mb);
    connection_stat_snapshot before = read_phase_stats(false);
    auto start = std::chrono::steady_clock::now();

    {
        thread_manager tm;
        for (int64_t i = 0; i < INSERT_THREADS; i++)
            tm.add_thread(&disagg_truncate_perf::insert_worker, this, target_bytes);
        tm.add_thread([&]() {
            truncate_worker(target_bytes);
            done.store(true);
        });

        /* Only the leader checkpoints during the measured phase. */
        if (!_follower)
            tm.add_thread(&disagg_truncate_perf::checkpoint_worker, this, target_bytes, false);
        if (!_checkpoints.empty())
            tm.add_thread(&disagg_truncate_perf::pickup_worker, this, &done);
        tm.join();
    }

    report_phase(before, read_phase_stats(true),
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start)
          .count()));
}

void
disagg_truncate_perf::run()
{
    load_config();

    std::string home = _args.home.empty() ? DEFAULT_DIR : _args.home;
    std::string config = CONNECTION_CREATE + ",cache_size=" + std::to_string(_cache_size_mb) +
      "MB,statistics=(all),statistics_log=(json,wait=1,on_close),precise_checkpoint=true" +
      ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so]" +
      ",disaggregated=(page_log=" + PAGE_LOG + ",role=\"%ROLE%\")" + _args.wt_open_config;
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
    _tsm = _timestamp_manager;
    _tsm->load();
    thread_manager timestamps;
    timestamps.add_thread(&component::run, _tsm);

    /* Prefill as a leader; only the restarted-follower run needs checkpoint metadata. */
    load(_role == "follower");

    if (_role == "leader") {
        run_workload();
        _tsm->end_run();
        timestamps.join();
        metrics_writer::instance().output_perf_file(_args.test_name);
        return;
    }

    /* Stop the timestamp manager: it moves timestamps on a connection about to change underneath.
     */
    _tsm->end_run();
    timestamps.join();

    std::string follower_config = config;
    follower_config.replace(follower_config.find("%ROLE%"), strlen("%ROLE%"), "follower");

    if (_role == "switch") {
        /* Step down in place after all workers have joined, as step-down requires. */
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        logger::log_msg(LOG_INFO, "Stepping down to follower.");
        testutil_check(conn->reconfigure(conn, "disaggregated=(role=\"follower\")"));

        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor cursor = session.open_scoped_cursor(STATISTICS_URI);
        report("switch_step_down_time",
          static_cast<uint64_t>(
            metrics_monitor::get_stat(cursor, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME)));
    } else {
        /* Restart to adopt checkpoints written by the leader. */
        logger::log_msg(LOG_INFO, "Restarting as a follower.");
        connection_manager::instance().close();
        connection_manager::instance().reopen(follower_config, home);
    }

    /* Keep the stable timestamp advancing after the role change. */
    std::unique_ptr<timestamp_manager> follower_tsm(
      new timestamp_manager(_config->get_subconfig(TIMESTAMP_MANAGER)));
    _tsm = follower_tsm.get();
    _tsm->load();
    thread_manager follower_timestamps;
    follower_timestamps.add_thread(&component::run, _tsm);

    _follower = true;
    run_workload();

    _tsm->end_run();
    follower_timestamps.join();
    metrics_writer::instance().output_perf_file(_args.test_name);
}
