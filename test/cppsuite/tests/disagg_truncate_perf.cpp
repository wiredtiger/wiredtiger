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

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/component/metrics_monitor.h"
#include "src/main/test.h"
#include "src/util/execution_timer.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <vector>

using namespace test_harness;

/*
 * This test measures truncation latency on layered:oplog while insert workers maintain a rolling
 * retention window. Each operation trims one expired marker. The role selects a leader run, a
 * follower restart from the final leader checkpoint, the slow follower path, or an in-place
 * step-down. See the role config files for workload settings.
 */
class disagg_truncate_perf : public test {
public:
    explicit disagg_truncate_perf(test_args &args) : test(args)
    {
        std::string connection_config =
          "precise_checkpoint=true,extensions=[../../ext/page_log/palite/libwiredtiger_palite.so],"
          "disaggregated=(page_log=palite,role=leader)";
        if (!args.wt_open_config.empty())
            connection_config += "," + args.wt_open_config;
        args.wt_open_config = std::move(connection_config);

        init_operation_tracker();
        configure_workload();
    }

    /* Prepare the selected role before the framework launches measured operation workers. */
    void
    populate(database &db, timestamp_manager *, configuration *populate_config,
      operation_tracker *op_tracker) override final
    {
        /* All roles load the same table as leader. Loading and role changes are outside the
         * measurement. */
        load_table(db, populate_config, op_tracker);
        change_role();
        /* Population may overshoot its target. Keep the full measured append allowance. */
        _insert_target_bytes = _inserted_bytes.load() + _append_limit_bytes;

        /* This baseline excludes loading and role changes from the measured counters. */
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        for (size_t i = 0; i < PHASE_STATS.size(); ++i)
            _baseline_stats[i] = metrics_monitor::get_stat(stats, PHASE_STATS[i].field);
        _phase_start = std::chrono::steady_clock::now();
        logger::log_msg(LOG_INFO, "Starting the " + _role + " phase.");
    }

    void
    insert_operation(thread_worker *tc) override final
    {
        append(tc, _insert_target_bytes);
    }

    /* Run the single trimming worker and time expired-range transactions during appends. */
    void
    custom_operation(thread_worker *tc) override final
    {
        scoped_cursor start_cursor = tc->session.open_scoped_cursor("layered:oplog");
        scoped_cursor stop_cursor = tc->session.open_scoped_cursor("layered:oplog");
        scoped_session stat_session = connection_manager::instance().create_session();
        scoped_cursor stats = stat_session.open_scoped_cursor(STATISTICS_URI);
        execution_timer timer("truncate_attempt", _args.test_name);
        while (true) {
            /* Drain in-flight inserts before selecting the next expired marker. */
            std::unique_lock<std::shared_mutex> gate(_insert_gate);
            const uint64_t append_head = _next_key.load();
            const uint64_t untrimmed_records = append_head - _truncated_key;
            /* Trim only after a full marker expires beyond the retained window. */
            if (untrimmed_records <= _retained_records + _records_per_marker) {
                if (!tc->running() || _inserted_bytes.load() >= _insert_target_bytes)
                    break;
                gate.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            const uint64_t marker_start_key = _truncated_key + 1;
            const uint64_t marker_stop_key = _truncated_key + _records_per_marker;
            const int truncate_result = timer.track([&]() {
                return (truncate_marker(
                          tc, start_cursor, stop_cursor, stats, marker_start_key, marker_stop_key) ?
                    0 :
                    WT_ROLLBACK);
            });
            if (truncate_result != 0)
                ++_truncate_rollbacks;
        }
        _truncation_done.store(true);
    }

    /* Advance timestamps until truncation completes and checkpoint only as leader. */
    void
    checkpoint_operation(thread_worker *tc) override final
    {
        /* Checkpoints can wait for stable data, so timestamps must advance on a separate thread. */
        std::atomic<bool> stop_timestamp_updates{false};
        thread_manager timestamp_threads;
        timestamp_threads.add_thread(
          &disagg_truncate_perf::advance_timestamps, this, &stop_timestamp_updates);
        while (!_truncation_done.load()) {
            if (!tc->running()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            tc->sleep();
            if (_truncation_done.load())
                break;
            if (tc->running() && _role == "leader")
                testutil_check(tc->session->checkpoint(tc->session.get(), nullptr));
        }
        stop_timestamp_updates.store(true);
        timestamp_threads.join();
        scoped_session stats_session = connection_manager::instance().create_session();
        scoped_cursor stats = stats_session.open_scoped_cursor(STATISTICS_URI);
        for (size_t i = 0; i < PHASE_STATS.size(); ++i)
            _phase_end_stats[i] = metrics_monitor::get_stat(stats, PHASE_STATS[i].field);
        const auto elapsed = std::chrono::steady_clock::now() - _phase_start;
        _phase_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    }

    /* Log per-role measurements once all workload threads have joined. Only the truncate latency
     * reaches the perf file. */
    void
    validate(bool, const std::string &, const std::string &, database &) override final
    {
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        const std::string metric_prefix = _role + "_";
        int64_t eviction_attempts = 0;
        int64_t eviction_failures = 0;
        for (size_t i = 0; i < PHASE_STATS.size(); ++i) {
            const int64_t phase_delta = _phase_end_stats[i] - _baseline_stats[i];
            log_stat(metric_prefix + PHASE_STATS[i].name, phase_delta);
            if (PHASE_STATS[i].field == WT_STAT_CONN_EVICTION_SERVER_EVICT_ATTEMPT ||
              PHASE_STATS[i].field == WT_STAT_CONN_EVICTION_WORKER_EVICT_ATTEMPT)
                eviction_attempts += phase_delta;
            else if (PHASE_STATS[i].field == WT_STAT_CONN_EVICTION_SERVER_EVICT_FAIL ||
              PHASE_STATS[i].field == WT_STAT_CONN_EVICTION_WORKER_EVICT_FAIL)
                eviction_failures += phase_delta;
        }
        const int64_t evictions = std::max<int64_t>(0, eviction_attempts - eviction_failures);
        const double eviction_pages_per_second = _phase_duration_ms > 0 ?
          static_cast<double>(evictions) * 1000.0 / _phase_duration_ms :
          0.0;
        /* Setup performs no follower workload, so list counters need no baseline subtraction. */
        if (_role != "leader") {
            for (const auto &stat : FOLLOWER_STATS)
                log_stat(metric_prefix + stat.name, metrics_monitor::get_stat(stats, stat.field));
            log_stat(metric_prefix + "truncate_list_entries_peak", _truncate_list_entries_peak);
        }
        log_stat(metric_prefix + "duration_ms", _phase_duration_ms);
        log_stat(metric_prefix + "truncate_attempts", _truncate_ops + _truncate_rollbacks);
        log_stat(metric_prefix + "truncate_ops", _truncate_ops);
        log_stat(metric_prefix + "truncate_rollbacks", _truncate_rollbacks);
        log_stat(metric_prefix + "truncate_dirty_bytes_peak", _truncate_dirty_bytes_peak);
        log_stat(metric_prefix + "cache_truncate_txn_uncommitted_bytes_peak",
          _cache_truncate_txn_uncommitted_bytes_peak);
        log_stat(metric_prefix + "eviction_pages_per_second", eviction_pages_per_second);
        logger::log_msg(LOG_INFO,
          "The " + _role + " phase completed " + std::to_string(_truncate_ops) + " truncates.");
    }

private:
    /* Validate sizing and the single-trimmer/single-checkpointer layout before threads start. */
    void
    configure_workload()
    {
        _role = _config->get_string("role");
        testutil_assert(_role == "leader" || _role == "follower" || _role == "slow_follower" ||
          _role == "switch");

        _value_size = _config->get_int("value_size");
        testutil_assert(_value_size > 0);
        _record_bytes = sizeof(int64_t) + static_cast<uint64_t>(_value_size);

        const int64_t oplog_size_mb = _config->get_int("oplog_size_mb");
        const int64_t insert_mb = _config->get_int("insert_mb");
        const int64_t marker_size_mb = _config->get_int("marker_size_mb");
        testutil_assert(oplog_size_mb > 0 && insert_mb > 0 && marker_size_mb > 0);

        _load_target_bytes = static_cast<uint64_t>(oplog_size_mb) * WT_MEGABYTE;
        _retained_records = _load_target_bytes / _record_bytes;
        _append_limit_bytes = static_cast<uint64_t>(insert_mb) * WT_MEGABYTE;

        const auto marker_bytes = static_cast<uint64_t>(marker_size_mb) * WT_MEGABYTE;
        _records_per_marker = marker_bytes / _record_bytes;
        testutil_assert(_records_per_marker != 0 && marker_bytes <= _load_target_bytes);
        /* Require enough markers for a useful p90 estimate. */
        static constexpr uint64_t MIN_TRUNCATE_SAMPLES = 25;
        testutil_assert(_append_limit_bytes / marker_bytes >= MIN_TRUNCATE_SAMPLES);
        testutil_assert(_config->get_int(DURATION_SECONDS) > 0);

        /* The framework cannot suspend its background components around the follower restart. */
        testutil_assert(!_timestamp_manager->enabled() && !_operation_tracker->enabled());
        testutil_assert(!_config->get_bool("metrics_monitor.enabled"));

        std::unique_ptr<configuration> workload_config(_config->get_subconfig(WORKLOAD_MANAGER));
        testutil_assert(workload_config->get_int("custom_config.thread_count") == 1);
        testutil_assert(workload_config->get_int("insert_config.thread_count") > 0);
        testutil_assert(workload_config->get_int("populate_config.thread_count") > 0);
        testutil_assert(workload_config->get_int("checkpoint_config.thread_count") == 1);
        std::unique_ptr<configuration> checkpoint_config(
          workload_config->get_subconfig(CHECKPOINT_OP_CONFIG));
        _checkpoint_interval = std::chrono::milliseconds(checkpoint_config->get_throttle_ms());

        /* Enable the locally managed timestamps while the framework's background manager is off. */
        static constexpr std::string_view timestamp_config =
          "enabled=true,oldest_lag=1,stable_lag=1,op_rate=1s";
        WT_CONFIG_ITEM timestamp_options = {timestamp_config.data(), timestamp_config.size(), 0,
          WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT};
        _timestamps = std::make_unique<timestamp_manager>(new configuration(timestamp_options));
    }

    /* Fill the retained window as leader and finish all setup workers before changing roles. */
    void
    load_table(database &db, configuration *populate_config, operation_tracker *op_tracker)
    {
        logger::log_msg(LOG_INFO, "Loading the layered table as a leader.");
        _timestamps->load();

        std::atomic<bool> stop_timestamp_updates{false};
        thread_manager timestamp_threads;
        timestamp_threads.add_thread(
          &disagg_truncate_perf::advance_timestamps, this, &stop_timestamp_updates);

        scoped_session session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session.get(), "layered:oplog", "key_format=q,value_format=S"));

        std::vector<std::unique_ptr<thread_worker>> populate_workers;
        thread_manager populate_threads;
        const int64_t populate_thread_count = populate_config->get_int(THREAD_COUNT);
        for (int64_t thread_id = 0; thread_id < populate_thread_count; ++thread_id) {
            auto session = connection_manager::instance().create_session();
            auto worker = std::make_unique<thread_worker>(thread_id, thread_type::INSERT,
              populate_config, std::move(session), _timestamps.get(), op_tracker, db);
            populate_threads.add_thread(
              &disagg_truncate_perf::append, this, worker.get(), _load_target_bytes);
            populate_workers.push_back(std::move(worker));
        }
        populate_threads.add_thread(&disagg_truncate_perf::checkpoint_during_load, this);
        populate_threads.join();
        wait_for_stable_timestamp(_last_commit_ts.load());
        testutil_check(session->checkpoint(session.get(), nullptr));
        if (_role == "follower" || _role == "slow_follower")
            capture_checkpoint(session.get());

        /* change_role() may close the connection as soon as this phase returns. */
        stop_timestamp_updates.store(true);
        timestamp_threads.join();
    }

    /* Wait until the final load transaction is visible to a stable checkpoint. */
    void
    wait_for_stable_timestamp(wt_timestamp_t target)
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        char stable_timestamp[256];
        while (true) {
            testutil_check(conn->query_timestamp(conn, stable_timestamp, "get=stable"));
            if (timestamp_manager::hex_to_decimal(stable_timestamp) >= target)
                return;
            testutil_assert(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    /* Take periodic checkpoints while the leader is loading the oplog. */
    void
    checkpoint_during_load()
    {
        scoped_session session = connection_manager::instance().create_session();
        while (_inserted_bytes.load() < _load_target_bytes) {
            std::this_thread::sleep_for(_checkpoint_interval);
            testutil_check(session->checkpoint(session.get(), nullptr));
        }
    }

    void
    advance_timestamps(std::atomic<bool> *stop_updates)
    {
        while (!stop_updates->load()) {
            {
                std::lock_guard timestamp_lock(_timestamp_api_mutex);
                _timestamps->do_work();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    /* Save metadata for the completed leader checkpoint used when opening a follower. */
    void
    capture_checkpoint(WT_SESSION *session)
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        WT_PAGE_LOG *page_log;
        testutil_check(conn->get_page_log(conn, "palite", &page_log));
        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
        testutil_check(page_log->pl_get_complete_checkpoint(page_log, session, &args));
        page_log->terminate(page_log, nullptr);
        _checkpoint_metadata.assign(
          static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size);
        free(args.checkpoint_metadata.mem);
    }

    /* Transition between setup and measurement, with loading sessions and timestamp work stopped.
     */
    void
    change_role()
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        if (_role == "follower" || _role == "slow_follower") {
            testutil_assert(!_checkpoint_metadata.empty());
            const std::string home = conn->get_home(conn);
            std::string follower_config =
              "cache_size=" + std::to_string(_config->get_int(CACHE_SIZE_MB)) + "MB," +
              _args.wt_open_config;
            if (_role == "slow_follower")
                follower_config += ",debug_mode=(disagg_slow_truncate_follower=true)";
            follower_config +=
              ",disaggregated=(role=follower,checkpoint_meta=\"" + _checkpoint_metadata + "\")";
            connection_manager::instance().close();
            connection_manager::instance().reopen(follower_config, home);
        } else if (_role == "switch") {
            /* Population workers have joined, so step-down cannot race an application write. */
            testutil_check(conn->reconfigure(conn, "disaggregated=(role=follower)"));
            scoped_session session = connection_manager::instance().create_session();
            scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
            log_stat("switch_step_down_time",
              metrics_monitor::get_stat(stats, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME));
        }
    }

    bool
    begin_timestamped_transaction(thread_worker *tc, wt_timestamp_t *commit_timestamp = nullptr)
    {
        tc->tsm = _timestamps.get();
        tc->begin();
        const wt_timestamp_t timestamp = tc->tsm->get_next_ts();
        int ret = tc->set_commit_timestamp(timestamp);
        /* A concurrent stable/oldest update can overtake the chosen timestamp. */
        testutil_assert(ret == 0 || ret == EINVAL);
        if (ret != 0)
            tc->rollback();
        else if (commit_timestamp != nullptr)
            *commit_timestamp = timestamp;
        return (ret == 0);
    }

    void
    record_commit_timestamp(wt_timestamp_t timestamp)
    {
        wt_timestamp_t last_timestamp = _last_commit_ts.load();
        while (last_timestamp < timestamp &&
          !_last_commit_ts.compare_exchange_weak(last_timestamp, timestamp)) {
        }
    }

    template <typename T>
    void
    log_stat(const std::string &name, T value)
    {
        logger::log_msg(LOG_INFO, name + "=" + std::to_string(value));
    }

    /* Reuse the fixed-record append loop for loading and measurement, with separate byte budgets.
     */
    void
    append(thread_worker *tc, uint64_t target_bytes)
    {
        scoped_cursor cursor = tc->session.open_scoped_cursor("layered:oplog");
        const std::string value = random_generator::instance().generate_pseudo_random_string(
          static_cast<uint64_t>(_value_size));
        while (tc->running()) {
            std::shared_lock<std::shared_mutex> gate(_insert_gate);
            if (_inserted_bytes.load() >= target_bytes)
                break;
            wt_timestamp_t commit_timestamp;
            if (!begin_timestamped_transaction(tc, &commit_timestamp))
                continue;
            cursor->set_key(cursor.get(), static_cast<int64_t>(_next_key.fetch_add(1)));
            cursor->set_value(cursor.get(), value.c_str());
            int ret = cursor->insert(cursor.get());
            if (ret != 0) {
                testutil_assert(ret == WT_ROLLBACK);
                tc->rollback();
                continue;
            }
            /* Rolled-back inserts must not consume the byte budget. */
            if (tc->commit()) {
                _inserted_bytes.fetch_add(_record_bytes);
                record_commit_timestamp(commit_timestamp);
            }
        }
    }

    /* Sample the dirty cache bytes recorded for the truncate transaction. */
    bool
    truncate_marker(thread_worker *tc, scoped_cursor &start_cursor, scoped_cursor &stop_cursor,
      scoped_cursor &stats, uint64_t marker_start_key, uint64_t marker_stop_key)
    {
        const bool is_leader = _role == "leader";
        if (!begin_timestamped_transaction(tc))
            return (false);
        testutil_check(start_cursor->reset(start_cursor.get()));
        start_cursor->set_key(start_cursor.get(), static_cast<int64_t>(marker_start_key));
        testutil_check(stop_cursor->reset(stop_cursor.get()));
        stop_cursor->set_key(stop_cursor.get(), static_cast<int64_t>(marker_stop_key));
        int ret = tc->session->truncate(
          tc->session.get(), nullptr, start_cursor.get(), stop_cursor.get(), nullptr);
        if (ret != 0) {
            testutil_assert(ret == WT_ROLLBACK);
            tc->rollback();
            return (false);
        }
        scoped_cursor txn_stats = tc->session.open_scoped_cursor("statistics:session");
        const int64_t cache_charge_bytes =
          metrics_monitor::get_stat(txn_stats, WT_STAT_SESSION_TXN_BYTES_DIRTY);
        const int64_t fast_truncate_uncommitted_bytes = is_leader ?
          metrics_monitor::get_stat(stats, WT_STAT_CONN_CACHE_TRUNCATE_TXN_UNCOMMITTED_BYTES) :
          0;
        if (!tc->commit())
            return (false);

        ++_truncate_ops;
        if (_role == "follower" || _role == "switch") {
            const uint64_t entries_removed = static_cast<uint64_t>(metrics_monitor::get_stat(
              stats, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            const uint64_t outstanding_entries =
              _truncate_ops > entries_removed ? _truncate_ops - entries_removed : 0;
            _truncate_list_entries_peak =
              std::max(_truncate_list_entries_peak, outstanding_entries);
        }
        if (cache_charge_bytes > 0)
            _truncate_dirty_bytes_peak =
              std::max(_truncate_dirty_bytes_peak, static_cast<uint64_t>(cache_charge_bytes));
        if (fast_truncate_uncommitted_bytes > 0)
            _cache_truncate_txn_uncommitted_bytes_peak =
              std::max(_cache_truncate_txn_uncommitted_bytes_peak,
                static_cast<uint64_t>(fast_truncate_uncommitted_bytes));
        _truncated_key = marker_stop_key;
        return (true);
    }

    struct named_stat {
        const char *name;
        int field;
    };
    static constexpr std::array<named_stat, 8> PHASE_STATS = {
      {{"page_delete_fast", WT_STAT_CONN_REC_PAGE_DELETE_FAST},
        {"truncate_keys_deleted", WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED},
        {"truncate_dirty_cache_rollback", WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK},
        {"cache_eviction_blocked_uncommitted_truncate",
          WT_STAT_CONN_CACHE_EVICTION_BLOCKED_UNCOMMITTED_TRUNCATE},
        {"eviction_server_evict_attempt", WT_STAT_CONN_EVICTION_SERVER_EVICT_ATTEMPT},
        {"eviction_server_evict_fail", WT_STAT_CONN_EVICTION_SERVER_EVICT_FAIL},
        {"eviction_worker_evict_attempt", WT_STAT_CONN_EVICTION_WORKER_EVICT_ATTEMPT},
        {"eviction_worker_evict_fail", WT_STAT_CONN_EVICTION_WORKER_EVICT_FAIL}}};
    static constexpr std::array<named_stat, 4> FOLLOWER_STATS = {
      {{"truncate_list_search_calls", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS},
        {"truncate_list_entries_walked", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED},
        {"truncate_list_gc_runs", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS},
        {"truncate_list_gc_entries_removed",
          WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED}}};

    std::string _role;
    int64_t _value_size;
    uint64_t _record_bytes;
    uint64_t _load_target_bytes;
    uint64_t _retained_records;
    uint64_t _records_per_marker;
    uint64_t _append_limit_bytes;
    uint64_t _insert_target_bytes{0};

    std::atomic<uint64_t> _next_key{1};
    std::atomic<uint64_t> _inserted_bytes{0};
    std::atomic<wt_timestamp_t> _last_commit_ts{0};
    std::atomic<bool> _truncation_done{false};

    uint64_t _truncated_key{0};
    uint64_t _truncate_ops{0};
    uint64_t _truncate_rollbacks{0};
    uint64_t _truncate_list_entries_peak{0};
    uint64_t _truncate_dirty_bytes_peak{0};
    uint64_t _cache_truncate_txn_uncommitted_bytes_peak{0};
    int64_t _phase_duration_ms{0};
    std::array<int64_t, PHASE_STATS.size()> _baseline_stats{};
    std::array<int64_t, PHASE_STATS.size()> _phase_end_stats{};
    std::chrono::steady_clock::time_point _phase_start;

    std::string _checkpoint_metadata;
    std::chrono::milliseconds _checkpoint_interval;
    std::unique_ptr<timestamp_manager> _timestamps;
    std::mutex _timestamp_api_mutex;
    /* Exclude inserts while measuring a marker truncate. */
    std::shared_mutex _insert_gate;
};
