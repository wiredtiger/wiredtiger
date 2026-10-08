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
#include <shared_mutex>
#include <thread>
#include <vector>

using namespace test_harness;

/* Measure truncation during an append workload as leader, restarted follower, or in-place switch.
 */
class disagg_truncate_perf : public test {
public:
    explicit disagg_truncate_perf(const test_args &args) : test(args)
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
        _target_bytes =
          _oplog_bytes + static_cast<uint64_t>(_config->get_int("insert_mb")) * WT_MEGABYTE;

        testutil_assert(_role == "leader" || _role == "follower" || _role == "switch");
        testutil_assert(_marker_records != 0 && marker_bytes <= _oplog_bytes);
        testutil_assert((_target_bytes - _oplog_bytes) / marker_bytes >= 25);
        testutil_assert(_timestamp_manager->enabled());
        init_operation_tracker();
        testutil_assert(!_operation_tracker->enabled());
        std::unique_ptr<configuration> monitor(_config->get_subconfig(METRICS_MONITOR));
        testutil_assert(!monitor->get_bool(ENABLED));
        std::unique_ptr<configuration> workload(_config->get_subconfig(WORKLOAD_MANAGER));
        std::unique_ptr<configuration> custom(workload->get_subconfig(CUSTOM_OP_CONFIG));
        testutil_assert(custom->get_int(THREAD_COUNT) == 1);
        std::unique_ptr<configuration> inserts(workload->get_subconfig(INSERT_OP_CONFIG));
        testutil_assert(inserts->get_int(THREAD_COUNT) > 0);
        std::unique_ptr<configuration> population(workload->get_subconfig(POPULATE_CONFIG));
        testutil_assert(population->get_int(THREAD_COUNT) > 0);
    }

    void
    populate(database &db, timestamp_manager *tsm, configuration *config,
      operation_tracker *op_tracker) override final
    {
        logger::log_msg(LOG_INFO, "Loading the layered table as a leader.");
        {
            scoped_session session = connection_manager::instance().create_session();
            testutil_check(
              session->create(session.get(), "layered:oplog", "key_format=q,value_format=S"));
        }

        {
            std::vector<std::unique_ptr<thread_worker>> workers;
            thread_manager threads;
            std::unique_ptr<configuration> workload(_config->get_subconfig(WORKLOAD_MANAGER));
            std::unique_ptr<configuration> checkpoints(
              workload->get_subconfig(CHECKPOINT_OP_CONFIG));
            std::chrono::milliseconds checkpoint_interval(checkpoints->get_throttle_ms());
            for (int64_t i = 0; i < config->get_int(THREAD_COUNT); ++i) {
                workers.emplace_back(new thread_worker(i, thread_type::INSERT, config,
                  connection_manager::instance().create_session(), tsm, op_tracker, db));
                threads.add_thread(
                  &disagg_truncate_perf::append, this, workers.back().get(), _oplog_bytes);
            }
            threads.add_thread([&]() {
                scoped_session session = connection_manager::instance().create_session();
                while (_inserted_bytes.load() < _oplog_bytes) {
                    std::this_thread::sleep_for(checkpoint_interval);
                    testutil_check(session->checkpoint(session.get(), nullptr));
                    if (_role == "follower") {
                        WT_CONNECTION *conn = connection_manager::instance().get_connection();
                        WT_PAGE_LOG *page_log;
                        testutil_check(conn->get_page_log(conn, "palite", &page_log));
                        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
                        testutil_check(
                          page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));
                        page_log->terminate(page_log, nullptr);
                        _checkpoints.emplace_back(
                          static_cast<const char *>(args.checkpoint_metadata.data),
                          args.checkpoint_metadata.size);
                        free(args.checkpoint_metadata.mem);
                    }
                }
            });
            threads.join();
        }
        _target_bytes += _inserted_bytes.load() - _oplog_bytes;

        {
            scoped_session session = connection_manager::instance().create_session();
            testutil_check(session->checkpoint(session.get(), nullptr));
        }
        if (_role == "follower")
            connection_manager::instance().restart("disaggregated=(role=follower)");
        else if (_role == "switch") {
            testutil_check(
              connection_manager::instance().reconfigure("disaggregated=(role=follower)"));
            scoped_session session = connection_manager::instance().create_session();
            scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
            metrics_writer::instance().add_stat("switch_step_down_time",
              metrics_monitor::get_stat(stats, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME));
        }

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
        {
            scoped_cursor cursor = tc->session.open_scoped_cursor("layered:oplog");
            scoped_session stat_session = connection_manager::instance().create_session();
            scoped_cursor stats = stat_session.open_scoped_cursor(STATISTICS_URI);
            execution_timer timer("truncate", _args.test_name);
            for (;;) {
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
        }
        _duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - _start)
                         .count();
        _done.store(true);
    }

    void
    checkpoint_operation(thread_worker *tc) override final
    {
        if (_role == "switch")
            return;
        size_t next = 0;
        while (!_done.load()) {
            if (_role == "leader") {
                tc->sleep();
                testutil_check(tc->session->checkpoint(tc->session.get(), nullptr));
            } else {
                if (next == _checkpoints.size())
                    break;
                int ret = connection_manager::instance().reconfigure(
                  "disaggregated=(checkpoint_meta=\"" + _checkpoints[next++] + "\")");
                testutil_assert(ret == 0 || ret == EINVAL);
                tc->sleep();
            }
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

protected:
    std::string
    extra_connection_config() const override final
    {
        return (
          "precise_checkpoint=true,extensions=[../../ext/page_log/palite/"
          "libwiredtiger_palite.so],disaggregated=(page_log=palite,role=leader)");
    }

    void
    wait_for_workload() override final
    {
        while (!_done.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

private:
    void
    append(thread_worker *tc, uint64_t target_bytes)
    {
        scoped_cursor cursor = tc->session.open_scoped_cursor("layered:oplog");
        const std::string value = random_generator::instance().generate_pseudo_random_string(
          static_cast<uint64_t>(_value_size));
        for (;;) {
            std::shared_lock<std::shared_mutex> gate(_insert_gate);
            if (_inserted_bytes.load() >= target_bytes)
                break;
            tc->begin();
            int ret = tc->set_commit_timestamp(tc->tsm->get_next_ts());
            testutil_assert(ret == 0 || ret == EINVAL);
            if (ret != 0) {
                tc->rollback();
                continue;
            }
            cursor->set_key(cursor.get(), static_cast<int64_t>(_next_key.fetch_add(1)));
            cursor->set_value(cursor.get(), value.c_str());
            ret = cursor->insert(cursor.get());
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
        tc->begin();
        int ret = tc->set_commit_timestamp(tc->tsm->get_next_ts());
        testutil_assert(ret == 0 || ret == EINVAL);
        if (ret != 0) {
            tc->rollback();
            return (false);
        }
        testutil_check(cursor->reset(cursor.get()));
        cursor->set_key(cursor.get(), static_cast<int64_t>(marker_key));
        ret = tc->session->truncate(tc->session.get(), nullptr, nullptr, cursor.get(), nullptr);
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

        if (_role != "leader") {
            cost = metrics_monitor::get_stat(stats, WT_STAT_CONN_CACHE_BYTES_UPDATES) - before;
            ++_truncate_list_entries;
            uint64_t collected = static_cast<uint64_t>(metrics_monitor::get_stat(
              stats, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED));
            uint64_t uncollected =
              _truncate_list_entries > collected ? _truncate_list_entries - collected : 0;
            _truncate_list_entries_peak = std::max(_truncate_list_entries_peak, uncollected);
        }
        if (cost > 0)
            _truncate_pressure_bytes_peak =
              std::max(_truncate_pressure_bytes_peak, static_cast<uint64_t>(cost));
        ++_truncate_ops;
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
    uint64_t _truncate_list_entries{0};
    uint64_t _truncate_list_entries_peak{0};
    uint64_t _truncate_pressure_bytes_peak{0};
    std::vector<std::string> _checkpoints;
    std::array<int64_t, WT_ELEMENTS(PHASE_STATS)> _before{};
    std::chrono::steady_clock::time_point _start;
    /* Exclude inserts while measuring a truncate's connection-wide cache delta. */
    std::shared_mutex _insert_gate;
};
