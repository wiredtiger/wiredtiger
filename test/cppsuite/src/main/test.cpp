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

#include "test.h"

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/component/metrics_writer.h"

namespace test_harness {
test::test(const test_args &args) : _args(args)
{
    _config = new Configuration(args.test_name, args.test_config);
    _metrics_monitor =
      new MetricsMonitor(args.test_name, _config->GetSubconfig(metricsMonitor), _database);
    _timestamp_manager = new TimestampManager(_config->GetSubconfig(timestampManager));
    _workload_manager = new WorkloadManager(
      _config->GetSubconfig(workloadManager), this, _timestamp_manager, _database);
    _thread_manager = new ThreadManager();

    _database.set_timestamp_manager(_timestamp_manager);
    _database.set_create_config(
      _config->GetBool(compressionEnabled), _config->GetBool(reverseCollator));

    /*
     * Ordering is not important here, any dependencies between components should be resolved
     * internally by the components.
     */
    _components = {_workload_manager, _timestamp_manager, _metrics_monitor};
}

void
test::init_operation_tracker(OperationTracker *op_tracker)
{
    delete _operation_tracker;
    if (op_tracker == nullptr) {
        /* Fallback to default behavior. */
        op_tracker = new OperationTracker(_config->GetSubconfig(operationTracker),
          _config->GetBool(compressionEnabled), *_timestamp_manager);
    }
    _operation_tracker = op_tracker;
    _workload_manager->SetOperationTracker(_operation_tracker);
    _database.SetOperationTracker(_operation_tracker);
    _components.push_back(_operation_tracker);
}

test::~test()
{
    delete _config;
    delete _metrics_monitor;
    delete _timestamp_manager;
    delete _thread_manager;
    delete _workload_manager;
    delete _operation_tracker;
    _config = nullptr;
    _metrics_monitor = nullptr;
    _timestamp_manager = nullptr;
    _thread_manager = nullptr;
    _workload_manager = nullptr;
    _operation_tracker = nullptr;

    _components.clear();
}

void
test::run()
{
    int64_t cache_max_wait_ms, cache_size_mb, duration_seconds;
    bool enable_logging, statistics_logging;
    Configuration *statistics_config;
    std::string statistics_type;
    /* Build the database creation config string. */
    std::string db_create_config = connectionCreate;

    /* Enable snappy compression or reverse collator if required. */
    if (_config->GetBool(compressionEnabled) || _config->GetBool(reverseCollator)) {
        db_create_config += ",extensions=[";
        db_create_config +=
          _config->GetBool(compressionEnabled) ? std::string(SNAPPY_PATH) + "," : "";
        db_create_config +=
          _config->GetBool(reverseCollator) ? std::string(REVERSE_COLLATOR_PATH) : "";
        db_create_config += "]";
    }

    /* Get the cache size. */
    cache_size_mb = _config->GetInt(cacheSizeMB);
    db_create_config += ",cache_size=" + std::to_string(cache_size_mb) + "MB";

    /* Get the statistics configuration for this run. */
    statistics_config = _config->GetSubconfig(statisticsConfig);
    statistics_type = statistics_config->GetString(type);
    statistics_logging = statistics_config->GetBool(enableLogging);
    db_create_config += statistics_logging ? "," + statisticsLog : "";
    db_create_config += ",statistics=(" + statistics_type + ")";
    /* Don't forget to delete. */
    delete statistics_config;

    /* Enable or disable write ahead logging. */
    enable_logging = _config->GetBool(enableLogging);
    db_create_config += ",log=(enabled=" + std::string(enable_logging ? "true" : "false") + ")";

    /* Maximum waiting time for the cache to get unstuck. */
    cache_max_wait_ms = _config->GetInt(cacheMaxWaitMs);
    db_create_config += ",cache_max_wait_ms=" + std::to_string(cache_max_wait_ms);

    /* Add the user supplied wiredtiger open config. */
    db_create_config += _args.wt_open_config;

    /* Create connection. */
    connection_manager::instance().create(db_create_config, DEFAULT_DIR);

    /* Initiate the load stage of each component. */
    for (const auto &it : _components)
        it->Load();

    /* Spawn threads for all Component::Run() functions. */
    for (const auto &it : _components)
        _thread_manager->addThread(&Component::Run, it);

    /* The initial population phase needs to be finished before starting the actual test. */
    while (_workload_manager->IsEnabled() && !_workload_manager->IsDatabasePopulated())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    /* The test will run for the duration as defined in the config. */
    duration_seconds = _config->GetInt(durationSecs);
    testutil_assert(duration_seconds >= 0);
    Logger::LogMessage(LOG_INFO,
      "Waiting {" + std::to_string(duration_seconds) + "} seconds for testing to complete.");
    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));

    /* Notify components that they should complete their last iteration. */
    for (const auto &it : _components)
        it->EndRun();

    /* Call join on the components threads so we know they have finished their loop. */
    Logger::LogMessage(LOG_INFO,
      "Joining all component threads.\n This could take a while as we need to wait"
      " for all components to finish their current loop.");
    _thread_manager->Join();

    /* End the test by calling finish on all known components. */
    for (const auto &it : _components)
        it->Finish();

    /* Validation stage. */
    if (_operation_tracker->IsEnabled()) {
        std::unique_ptr<Configuration> tracking_config(_config->GetSubconfig(operationTracker));
        this->validate(_operation_tracker->getOperationTableName(),
          _operation_tracker->getSchemaTableName(),
          _workload_manager->GetDatabase().get_collection_ids());
    }

    /* Log perf stats. */
    MetricsWriter::GetInstance().WriteToFile(_args.test_name);

    Logger::LogMessage(LOG_INFO, "SUCCESS");
}
} // namespace test_harness
