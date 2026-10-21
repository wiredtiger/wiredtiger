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

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

using namespace test_harness;

/*
 * This test measures truncation latency on layered:oplog. The table is loaded and checkpointed as
 * leader, then the run takes its role and trims each marker beyond the retained window, timing one
 * truncate per marker. The leader keeps its role, the follower and slow follower restart from the
 * final checkpoint, and the switch role steps down in place. Before each leader truncate the
 * marker's pages are evicted, so the truncate can fast delete them. See the role config files for
 * workload settings.
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
        /*
         * All roles load the same table as leader. Loading and role changes are outside the
         * measurement.
         */
        load_table(db, populate_config, op_tracker);
        change_role();
    }

    /* Trim every marker beyond the retained window, timing each truncate on its own. */
    void
    custom_operation(thread_worker *tc) override final
    {
        scoped_cursor start_cursor = tc->session.open_scoped_cursor("layered:oplog");
        scoped_cursor stop_cursor = tc->session.open_scoped_cursor("layered:oplog");
        record_phase_start();
        execution_timer timer("truncate_attempt", _args.test_name);
        while (_next_key.load() - _truncated_key > _retained_records + _records_per_marker) {
            const uint64_t marker_start_key = _truncated_key + 1;
            const uint64_t marker_stop_key = _truncated_key + _records_per_marker;
            if (_role == "leader")
                evict_marker(marker_start_key, marker_stop_key);
            const int truncate_result = timer.track([&]() {
                return (truncate_marker(
                          tc, start_cursor, stop_cursor, marker_start_key, marker_stop_key) ?
                    0 :
                    WT_ROLLBACK);
            });
            if (truncate_result != 0)
                ++_truncate_rollbacks;
        }
        record_phase_end();
    }

    /*
     * Log per-role measurements once all workload threads have joined. Only the truncate latency
     * reaches the perf file.
     */
    void
    validate(bool, const std::string &, const std::string &, database &) override final
    {
        const std::string metric_prefix = _role + "_";
        log_phase_stats(metric_prefix);
        if (_role != "leader")
            log_follower_stats(metric_prefix);
        log_truncate_stats(metric_prefix);
        logger::log_msg(LOG_INFO,
          "The " + _role + " phase completed " + std::to_string(_truncate_ops) + " truncates.");
    }

private:
    void
    configure_workload()
    {
        _role = _config->get_string("role");
        testutil_assert(_role == "leader" || _role == "follower" || _role == "slow_follower" ||
          _role == "switch");
        configure_sizing();
        configure_threads();
        configure_timestamps();
    }

    /* Size the load, the retained window and the markers, and pace the load checkpoints. */
    void
    configure_sizing()
    {
        _value_size = _config->get_int("value_size");
        testutil_assert(_value_size > 0);
        _record_bytes = sizeof(int64_t) + static_cast<uint64_t>(_value_size);

        const int64_t oplog_size_mb = _config->get_int("oplog_size_mb");
        const int64_t trim_mb = _config->get_int("trim_mb");
        const int64_t marker_size_mb = _config->get_int("marker_size_mb");
        testutil_assert(oplog_size_mb > 0 && trim_mb > 0 && marker_size_mb > 0);

        const auto retained_bytes = static_cast<uint64_t>(oplog_size_mb) * WT_MEGABYTE;
        const auto trimmed_bytes = static_cast<uint64_t>(trim_mb) * WT_MEGABYTE;
        _retained_records = retained_bytes / _record_bytes;
        _load_target_bytes = retained_bytes + trimmed_bytes;

        const auto marker_bytes = static_cast<uint64_t>(marker_size_mb) * WT_MEGABYTE;
        _records_per_marker = marker_bytes / _record_bytes;
        testutil_assert(_records_per_marker != 0);
        /* Require enough markers for a useful p90 estimate. */
        static constexpr uint64_t MIN_TRUNCATE_SAMPLES = 25;
        testutil_assert(trimmed_bytes / marker_bytes >= MIN_TRUNCATE_SAMPLES);
        testutil_assert(_config->get_int(DURATION_SECONDS) > 0);
        _load_checkpoint_interval =
          std::chrono::milliseconds(_config->get_int("load_checkpoint_interval_ms"));
    }

    /* Only the trimmer may run in the measured phase, with no insert or checkpoint threads. */
    void
    configure_threads()
    {
        /* Framework components would keep using the connection while a follower reopens it. */
        testutil_assert(!_timestamp_manager->enabled() && !_operation_tracker->enabled());
        testutil_assert(!_config->get_bool("metrics_monitor.enabled"));

        std::unique_ptr<configuration> workload_config(_config->get_subconfig(WORKLOAD_MANAGER));
        testutil_assert(workload_config->get_int("custom_config.thread_count") == 1);
        testutil_assert(workload_config->get_int("insert_config.thread_count") == 0);
        testutil_assert(workload_config->get_int("populate_config.thread_count") > 0);
        testutil_assert(workload_config->get_int("checkpoint_config.thread_count") == 0);
    }

    /*
     * The framework's timestamp manager would set timestamps on a connection the follower restart
     * closes, so the load advances its own timestamps instead.
     */
    void
    configure_timestamps()
    {
        static constexpr std::string_view timestamp_config =
          "enabled=true,oldest_lag=1,stable_lag=1,op_rate=1s";
        WT_CONFIG_ITEM timestamp_options = {timestamp_config.data(), timestamp_config.size(), 0,
          WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT};
        _timestamps = std::make_unique<timestamp_manager>(new configuration(timestamp_options));
    }

    /* Load the retained window and the data to trim as leader, then checkpoint all of it. */
    void
    load_table(database &db, configuration *populate_config, operation_tracker *op_tracker)
    {
        logger::log_msg(LOG_INFO, "Loading the layered table as a leader.");
        _timestamps->load();
        scoped_session session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session.get(), "layered:oplog", "key_format=q,value_format=S"));
        run_load_workers(db, populate_config, op_tracker);
        /* Every load transaction has finished, so a new timestamp is above all of their commits. */
        connection_manager::instance().set_timestamp(
          STABLE_TS + "=" + timestamp_manager::decimal_to_hex(_timestamps->get_next_ts()));
        testutil_check(session->checkpoint(session.get(), nullptr));
    }

    /* Append the whole load across the populate workers, checkpointing while it grows. */
    void
    run_load_workers(database &db, configuration *populate_config, operation_tracker *op_tracker)
    {
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
        populate_threads.add_thread(&disagg_truncate_perf::checkpoint_while_loading, this);
        populate_threads.join();
    }

    /*
     * Precise checkpoints write only data at or below stable, so stable must advance for each load
     * checkpoint to write the rows appended before it.
     */
    void
    checkpoint_while_loading()
    {
        scoped_session session = connection_manager::instance().create_session();
        auto next_checkpoint = std::chrono::steady_clock::now() + _load_checkpoint_interval;
        /* Poll so the final load checkpoint does not wait out a full interval. */
        while (_inserted_bytes.load() < _load_target_bytes) {
            if (std::chrono::steady_clock::now() < next_checkpoint) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            _timestamps->do_work();
            testutil_check(session->checkpoint(session.get(), nullptr));
            next_checkpoint = std::chrono::steady_clock::now() + _load_checkpoint_interval;
        }
    }

    /* Read the metadata of the final leader checkpoint, which the follower picks up. */
    std::string
    pickup_checkpoint()
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        scoped_session session = connection_manager::instance().create_session();
        WT_PAGE_LOG *page_log;
        testutil_check(conn->get_page_log(conn, "palite", &page_log));
        WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
        testutil_check(page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));
        page_log->terminate(page_log, nullptr);
        std::string metadata(
          static_cast<const char *>(args.checkpoint_metadata.data), args.checkpoint_metadata.size);
        free(args.checkpoint_metadata.mem);
        return (metadata);
    }

    /* Move from the leader load to the measured role once every load thread has stopped. */
    void
    change_role()
    {
        if (_role == "follower" || _role == "slow_follower")
            restart_as_follower();
        else if (_role == "switch")
            step_down();
    }

    /* Reopen the connection as a follower at the final leader checkpoint. */
    void
    restart_as_follower()
    {
        const std::string checkpoint_metadata = pickup_checkpoint();
        testutil_assert(!checkpoint_metadata.empty());
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        const std::string home = conn->get_home(conn);
        std::string follower_config =
          "cache_size=" + std::to_string(_config->get_int(CACHE_SIZE_MB)) + "MB," +
          _args.wt_open_config;
        if (_role == "slow_follower")
            follower_config += ",debug_mode=(disagg_slow_truncate_follower=true)";
        follower_config +=
          ",disaggregated=(role=follower,checkpoint_meta=\"" + checkpoint_metadata + "\")";
        connection_manager::instance().close();
        connection_manager::instance().reopen(follower_config, home);
    }

    /* Step the open connection down to a follower and log how long that took. */
    void
    step_down()
    {
        /* Population workers have joined, so step-down cannot race an application write. */
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        testutil_check(conn->reconfigure(conn, "disaggregated=(role=follower)"));
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        log_stat("switch_step_down_time",
          metrics_monitor::get_stat(stats, WT_STAT_CONN_DISAGG_STEP_DOWN_TIME));
    }

    bool
    begin_timestamped_transaction(thread_worker *tc)
    {
        tc->tsm = _timestamps.get();
        tc->begin();
        int ret = tc->set_commit_timestamp(tc->tsm->get_next_ts());
        /* A concurrent stable/oldest update can overtake the chosen timestamp. */
        testutil_assert(ret == 0 || ret == EINVAL);
        if (ret != 0)
            tc->rollback();
        return (ret == 0);
    }

    template <typename T>
    void
    log_stat(const std::string &name, T value)
    {
        logger::log_msg(LOG_INFO, name + "=" + std::to_string(value));
    }

    /* Append fixed-size records until the shared byte budget is reached. */
    void
    append(thread_worker *tc, uint64_t target_bytes)
    {
        scoped_cursor cursor = tc->session.open_scoped_cursor("layered:oplog");
        const std::string value = random_generator::instance().generate_pseudo_random_string(
          static_cast<uint64_t>(_value_size));
        while (tc->running() && _inserted_bytes.load() < target_bytes) {
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
            /* Rolled-back inserts must not consume the byte budget. */
            if (tc->commit())
                _inserted_bytes.fetch_add(_record_bytes);
        }
    }

    /* Evict the marker's cached pages so the measured truncate finds them on disk. */
    void
    evict_marker(uint64_t marker_start_key, uint64_t marker_stop_key)
    {
        /* This session evicts each page as its cursor moves off it. */
        scoped_session session = connection_manager::instance().create_session();
        testutil_check(session->reconfigure(session.get(), "debug=(release_evict_page=true)"));
        scoped_cursor cursor = session.open_scoped_cursor("layered:oplog");
        cursor->set_key(cursor.get(), static_cast<int64_t>(marker_start_key));
        int exact;
        int ret = cursor->search_near(cursor.get(), &exact);
        while (ret == 0) {
            int64_t key;
            testutil_check(cursor->get_key(cursor.get(), &key));
            if (static_cast<uint64_t>(key) > marker_stop_key)
                break;
            ret = cursor->next(cursor.get());
        }
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        testutil_check(cursor->reset(cursor.get()));
    }

    /* Truncate one marker in a timestamped transaction. */
    bool
    truncate_marker(thread_worker *tc, scoped_cursor &start_cursor, scoped_cursor &stop_cursor,
      uint64_t marker_start_key, uint64_t marker_stop_key)
    {
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
        if (!tc->commit())
            return (false);
        ++_truncate_ops;
        _truncated_key = marker_stop_key;
        return (true);
    }

    /* This baseline excludes loading and role changes from the measured counters. */
    void
    record_phase_start()
    {
        _baseline_stats = read_phase_stats();
        _phase_start = std::chrono::steady_clock::now();
        logger::log_msg(LOG_INFO, "Starting the " + _role + " phase.");
    }

    void
    record_phase_end()
    {
        _phase_end_stats = read_phase_stats();
        const auto elapsed = std::chrono::steady_clock::now() - _phase_start;
        _phase_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    }

    /* Log how far each phase counter moved and how long the phase took. */
    void
    log_phase_stats(const std::string &metric_prefix)
    {
        for (size_t i = 0; i < PHASE_STATS.size(); ++i)
            log_stat(metric_prefix + PHASE_STATS[i].name, _phase_end_stats[i] - _baseline_stats[i]);
        log_stat(metric_prefix + "duration_ms", _phase_duration_ms);
    }

    /* Setup performs no follower workload, so list counters need no baseline subtraction. */
    void
    log_follower_stats(const std::string &metric_prefix)
    {
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        for (const auto &stat : FOLLOWER_STATS)
            log_stat(metric_prefix + stat.name, metrics_monitor::get_stat(stats, stat.field));
    }

    void
    log_truncate_stats(const std::string &metric_prefix)
    {
        log_stat(metric_prefix + "truncate_attempts", _truncate_ops + _truncate_rollbacks);
        log_stat(metric_prefix + "truncate_ops", _truncate_ops);
        log_stat(metric_prefix + "truncate_rollbacks", _truncate_rollbacks);
    }

    struct named_stat {
        const char *name;
        int field;
    };
    static constexpr std::array<named_stat, 3> PHASE_STATS = {
      {{"page_delete_fast", WT_STAT_CONN_REC_PAGE_DELETE_FAST},
        {"truncate_keys_deleted", WT_STAT_CONN_CURSOR_TRUNCATE_KEYS_DELETED},
        {"truncate_dirty_cache_rollback", WT_STAT_CONN_TXN_TRUNCATE_DIRTY_CACHE_ROLLBACK}}};
    static constexpr std::array<named_stat, 4> FOLLOWER_STATS = {
      {{"truncate_list_search_calls", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_CALLS},
        {"truncate_list_entries_walked", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_SEARCH_ENTRIES_WALKED},
        {"truncate_list_gc_runs", WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS},
        {"truncate_list_gc_entries_removed",
          WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED}}};
    using phase_stat_values = std::array<int64_t, PHASE_STATS.size()>;

    phase_stat_values
    read_phase_stats()
    {
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor stats = session.open_scoped_cursor(STATISTICS_URI);
        phase_stat_values values{};
        for (size_t i = 0; i < PHASE_STATS.size(); ++i)
            values[i] = metrics_monitor::get_stat(stats, PHASE_STATS[i].field);
        return (values);
    }

    std::string _role;
    int64_t _value_size;
    uint64_t _record_bytes;
    uint64_t _load_target_bytes;
    uint64_t _retained_records;
    uint64_t _records_per_marker;

    std::atomic<uint64_t> _next_key{1};
    std::atomic<uint64_t> _inserted_bytes{0};

    uint64_t _truncated_key{0};
    uint64_t _truncate_ops{0};
    uint64_t _truncate_rollbacks{0};
    int64_t _phase_duration_ms{0};
    phase_stat_values _baseline_stats{};
    phase_stat_values _phase_end_stats{};
    std::chrono::steady_clock::time_point _phase_start;

    std::chrono::milliseconds _load_checkpoint_interval;
    std::unique_ptr<timestamp_manager> _timestamps;
};
