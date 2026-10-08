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
#include "src/component/metrics_writer.h"
#include "src/main/test.h"
#include "src/util/execution_timer.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

using namespace test_harness;

namespace {
/* Keep timestamp updates scoped to a connection's lifetime. */
class scoped_timestamp_updates {
public:
    scoped_timestamp_updates(timestamp_manager &timestamps, std::mutex &gate)
        : _thread([this, &timestamps, &gate]() {
              while (!_stop.load()) {
                  {
                      std::lock_guard<std::mutex> lock(gate);
                      timestamps.do_work();
                  }
                  std::this_thread::sleep_for(std::chrono::milliseconds(100));
              }
          })
    {
    }

    ~scoped_timestamp_updates()
    {
        _stop.store(true);
        _thread.join();
    }

private:
    std::atomic<bool> _stop{false};
    std::thread _thread;
};
} // namespace

/* Measure truncation during an append workload in each disaggregated role. */
class disagg_truncate_perf : public test {
public:
    explicit disagg_truncate_perf(test_args &args) : test(args)
    {
        args.wt_open_config =
          "precise_checkpoint=true,statistics=(all),extensions=[../../ext/page_log/palite/"
          "libwiredtiger_palite.so],disaggregated=(page_log=palite,role=leader)" +
          (args.wt_open_config.empty() ? "" : "," + args.wt_open_config);
        init_operation_tracker();
        configure_workload();
    }

    void
    populate(database &db, timestamp_manager *, configuration *config,
      operation_tracker *op_tracker) override final
    {
        load_table(db, config, op_tracker);
        change_role();
        _target_bytes += _inserted_bytes.load();

        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); ++i)
            _before[i] = metrics_monitor::get_stat(stats, PHASE_STATS[i].field);
        _start = std::chrono::steady_clock::now();
        logger::log_msg(LOG_INFO, "Starting the " + _role + " phase.");
    }

    void
    insert_operation(thread_worker *tc) override final
    {
        append(tc, _target_bytes);
    }

    void
    custom_operation(thread_worker *tc) override final
    {
        scoped_cursor cursor = tc->session.open_scoped_cursor("layered:oplog");
        scoped_session stat_session = connection_manager::instance().create_session();
        scoped_cursor stats = stat_session.open_scoped_cursor(STATISTICS_URI);
        execution_timer timer("truncate", _args.test_name);
        while (tc->running()) {
            std::unique_lock<std::shared_mutex> gate(_insert_gate);
            uint64_t head = _next_key.load();
            if (head - _truncated_key <= _keep_records + _marker_records) {
                if (_inserted_bytes.load() >= _target_bytes)
                    break;
                gate.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            uint64_t marker_key = head - _keep_records;
            if (timer.track([&]() {
                    return (truncate_marker(tc, cursor, stats, marker_key) ? 0 : WT_ROLLBACK);
                }) != 0)
                ++_truncate_rollbacks;
        }
        _duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - _start)
                         .count();
        _done.store(true);
    }

    void
    checkpoint_operation(thread_worker *tc) override final
    {
        scoped_timestamp_updates timestamps(*_timestamps, _timestamp_gate);
        if (_role == "follower") {
            pick_up_checkpoints(tc);
            return;
        }
        while (tc->running() && !_done.load()) {
            tc->sleep();
            if (!tc->running() || _done.load())
                break;
            if (_role == "leader")
                testutil_check(tc->session->checkpoint(tc->session.get(), nullptr));
        }
    }

    void
    validate(bool, const std::string &, const std::string &, database &) override final
    {
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        auto &writer = metrics_writer::instance();
        for (size_t i = 0; i < WT_ELEMENTS(PHASE_STATS); ++i)
            writer.add_stat(_role + "_" + PHASE_STATS[i].name,
              metrics_monitor::get_stat(stats, PHASE_STATS[i].field) - _before[i]);
        if (_role != "leader") {
            for (const auto &stat : FOLLOWER_STATS)
                writer.add_stat(
                  _role + "_" + stat.name, metrics_monitor::get_stat(stats, stat.field));
            writer.add_stat(_role + "_truncate_list_entries_peak", _truncate_list_entries_peak);
        }
        writer.add_stat(_role + "_duration_ms", _duration_ms);
        writer.add_stat(_role + "_truncate_ops", _truncate_ops);
        writer.add_stat(_role + "_truncate_rollbacks", _truncate_rollbacks);
        writer.add_stat(_role + "_truncate_pressure_bytes_peak", _truncate_pressure_bytes_peak);
        logger::log_msg(LOG_INFO,
          "The " + _role + " phase completed " + std::to_string(_truncate_ops) + " truncates.");
    }

private:
    void
    configure_workload()
    {
        _role = _config->get_string("role");
        _value_size = _config->get_int("value_size");
        testutil_assert(_value_size > 0);
        _record_bytes = sizeof(int64_t) + static_cast<uint64_t>(_value_size);
        _oplog_bytes = static_cast<uint64_t>(_config->get_int("oplog_size_mb")) * WT_MEGABYTE;
        _keep_records = _oplog_bytes / _record_bytes;
        uint64_t marker_bytes =
          static_cast<uint64_t>(_config->get_int("marker_size_mb")) * WT_MEGABYTE;
        _marker_records = marker_bytes / _record_bytes;
        _target_bytes = static_cast<uint64_t>(_config->get_int("insert_mb")) * WT_MEGABYTE;
        testutil_assert(_role == "leader" || _role == "follower" || _role == "switch");
        testutil_assert(_marker_records != 0 && marker_bytes <= _oplog_bytes);
        testutil_assert(_target_bytes / marker_bytes >= 25);
        testutil_assert(_config->get_int(DURATION_SECONDS) > 0);
        testutil_assert(!_timestamp_manager->enabled() && !_operation_tracker->enabled());
        std::unique_ptr<configuration> monitor(_config->get_subconfig(METRICS_MONITOR));
        testutil_assert(!monitor->get_bool(ENABLED));
        std::unique_ptr<configuration> workload(_config->get_subconfig(WORKLOAD_MANAGER));
        std::unique_ptr<configuration> operation(workload->get_subconfig(CUSTOM_OP_CONFIG));
        testutil_assert(operation->get_int(THREAD_COUNT) == 1);
        operation.reset(workload->get_subconfig(INSERT_OP_CONFIG));
        testutil_assert(operation->get_int(THREAD_COUNT) > 0);
        operation.reset(workload->get_subconfig(POPULATE_CONFIG));
        testutil_assert(operation->get_int(THREAD_COUNT) > 0);
        operation.reset(workload->get_subconfig(CHECKPOINT_OP_CONFIG));
        testutil_assert(operation->get_int(THREAD_COUNT) == 1);
        _checkpoint_interval = std::chrono::milliseconds(operation->get_throttle_ms());
        WT_CONFIG_ITEM timestamps = {"enabled=true,oldest_lag=1,stable_lag=1,op_rate=1s", 0, 0,
          WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT};
        timestamps.len = strlen(timestamps.str);
        _timestamps.reset(new timestamp_manager(new configuration(timestamps)));
    }

    void
    load_table(database &db, configuration *config, operation_tracker *op_tracker)
    {
        logger::log_msg(LOG_INFO, "Loading the layered table as a leader.");
        _timestamps->load();
        scoped_timestamp_updates timestamps(*_timestamps, _timestamp_gate);
        scoped_session session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session.get(), "layered:oplog", "key_format=q,value_format=S"));
        std::vector<std::unique_ptr<thread_worker>> workers;
        thread_manager threads;
        for (int64_t i = 0; i < config->get_int(THREAD_COUNT); ++i) {
            workers.emplace_back(new thread_worker(i, thread_type::INSERT, config,
              connection_manager::instance().create_session(), _timestamps.get(), op_tracker, db));
            threads.add_thread(
              &disagg_truncate_perf::append, this, workers.back().get(), _oplog_bytes);
        }
        threads.add_thread([&]() {
            scoped_session checkpoint_session = connection_manager::instance().create_session();
            while (_inserted_bytes.load() < _oplog_bytes) {
                std::this_thread::sleep_for(_checkpoint_interval);
                testutil_check(checkpoint_session->checkpoint(checkpoint_session.get(), nullptr));
                if (_role == "follower")
                    capture_checkpoint(checkpoint_session.get());
            }
        });
        threads.join();
        testutil_check(session->checkpoint(session.get(), nullptr));
    }

    void
    capture_checkpoint(WT_SESSION *session)
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        WT_PAGE_LOG *page_log;
        testutil_check(conn->get_page_log(conn, "palite", &page_log));
        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
        testutil_check(page_log->pl_get_complete_checkpoint(page_log, session, &args));
        page_log->terminate(page_log, nullptr);
        _checkpoints.emplace_back(
          static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size);
        free(args.checkpoint_metadata.mem);
    }

    void
    change_role()
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        if (_role == "follower") {
            std::string home = conn->get_home(conn);
            std::string config = "cache_size=" + std::to_string(_config->get_int(CACHE_SIZE_MB)) +
              "MB," + _args.wt_open_config + ",disaggregated=(role=follower)";
            connection_manager::instance().close();
            connection_manager::instance().reopen(config, home);
        } else if (_role == "switch") {
            testutil_check(conn->reconfigure(conn, "disaggregated=(role=follower)"));
            scoped_session session = connection_manager::instance().create_session();
            scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
            metrics_writer::instance().add_stat("switch_step_down_time",
              metrics_monitor::get_stat(stats, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME));
        }
    }

    void
    pick_up_checkpoints(thread_worker *tc)
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        size_t next = 0;
        while (tc->running() && !_done.load()) {
            if (next < _checkpoints.size()) {
                std::string config =
                  "disaggregated=(checkpoint_meta=\"" + _checkpoints[next++] + "\")";
                std::lock_guard<std::mutex> gate(_timestamp_gate);
                int ret = conn->reconfigure(conn, config.c_str());
                testutil_assert(ret == 0 || ret == EINVAL);
            }
            tc->sleep();
        }
    }

    bool
    begin_timestamped_transaction(thread_worker *tc)
    {
        tc->tsm = _timestamps.get();
        tc->begin();
        int ret = tc->set_commit_timestamp(tc->tsm->get_next_ts());
        testutil_assert(ret == 0 || ret == EINVAL);
        if (ret != 0)
            tc->rollback();
        return (ret == 0);
    }

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
            if (!begin_timestamped_transaction(tc))
                continue;
            cursor->set_key(cursor.get(), static_cast<int64_t>(_next_key.fetch_add(1)));
            cursor->set_value(cursor.get(), value.c_str());
            int ret = cursor->insert(cursor.get());
            if (ret != 0) {
                testutil_assert(ret == WT_ROLLBACK);
                tc->rollback();
                continue;
            }
            if (tc->commit())
                _inserted_bytes.fetch_add(_record_bytes);
        }
    }

    bool
    truncate_marker(
      thread_worker *tc, scoped_cursor &cursor, scoped_cursor &stats, uint64_t marker_key)
    {
        int64_t before = _role == "leader" ?
          0 :
          metrics_monitor::get_stat(stats, WT_STAT_CONN_CACHE_BYTES_UPDATES);
        if (!begin_timestamped_transaction(tc))
            return (false);
        testutil_check(cursor->reset(cursor.get()));
        cursor->set_key(cursor.get(), static_cast<int64_t>(marker_key));
        int ret = tc->session->truncate(tc->session.get(), nullptr, nullptr, cursor.get(), nullptr);
        if (ret != 0) {
            testutil_assert(ret == WT_ROLLBACK);
            tc->rollback();
            return (false);
        }
        int64_t cost = _role == "leader" ?
          metrics_monitor::get_stat(stats, WT_STAT_CONN_CACHE_TRUNCATE_TXN_UNCOMMITTED_BYTES) :
          0;
        if (!tc->commit())
            return (false);

        ++_truncate_ops;
        if (_role != "leader") {
            cost = metrics_monitor::get_stat(stats, WT_STAT_CONN_CACHE_BYTES_UPDATES) - before;
            uint64_t collected = static_cast<uint64_t>(metrics_monitor::get_stat(
              stats, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            uint64_t uncollected = _truncate_ops > collected ? _truncate_ops - collected : 0;
            _truncate_list_entries_peak = std::max(_truncate_list_entries_peak, uncollected);
        }
        if (cost > 0)
            _truncate_pressure_bytes_peak =
              std::max(_truncate_pressure_bytes_peak, static_cast<uint64_t>(cost));
        _truncated_key = marker_key;
        return (true);
    }

    struct named_stat {
        const char *name;
        int field;
    };
    static constexpr named_stat PHASE_STATS[] = {
      {"page_delete_fast", WT_STAT_CONN_REC_PAGE_DELETE_FAST},
      {"truncate_keys_deleted", WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED},
      {"truncate_dirty_cache_rollback", WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK}};
    static constexpr named_stat FOLLOWER_STATS[] = {
      {"truncate_list_search_calls", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS},
      {"truncate_list_entries_walked", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED},
      {"truncate_list_gc_runs", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS},
      {"truncate_list_gc_entries_removed", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED}};

    std::string _role;
    int64_t _value_size;
    uint64_t _record_bytes;
    uint64_t _oplog_bytes;
    uint64_t _keep_records;
    uint64_t _marker_records;
    uint64_t _target_bytes;
    std::atomic<uint64_t> _next_key{1};
    std::atomic<uint64_t> _inserted_bytes{0};
    std::atomic<bool> _done{false};
    int64_t _duration_ms{0};
    uint64_t _truncated_key{0};
    uint64_t _truncate_ops{0};
    uint64_t _truncate_rollbacks{0};
    uint64_t _truncate_list_entries_peak{0};
    uint64_t _truncate_pressure_bytes_peak{0};
    std::vector<std::string> _checkpoints;
    std::array<int64_t, WT_ELEMENTS(PHASE_STATS)> _before{};
    std::chrono::steady_clock::time_point _start;
    std::chrono::milliseconds _checkpoint_interval;
    std::unique_ptr<timestamp_manager> _timestamps;
    std::mutex _timestamp_gate;
    /* Exclude inserts while measuring a truncate's connection-wide cache delta. */
    std::shared_mutex _insert_gate;
};
