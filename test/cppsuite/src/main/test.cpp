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
Test::Test(const test_args &args) : _args(args)
{
    _config = new Configuration(args.testName, args.testConfig);
    _metricsMonitor =
      new MetricsMonitor(args.testName, _config->GetSubconfig(metricsMonitor), _database);
    _timestampManager = new TimestampManager(_config->GetSubconfig(timestampManager));
    _workloadManager = new WorkloadManager(
      _config->GetSubconfig(workloadManager), this, _timestampManager, _database);
    _threadManager = new ThreadManager();

    _database.SetTimestampManager(_timestampManager);
    _database.SetCreateConfig(
      _config->GetBool(compressionEnabled), _config->GetBool(reverseCollator));

    /*
     * Ordering is not important here, any dependencies between components should be resolved
     * internally by the components.
     */
    _components = {_workloadManager, _timestampManager, _metricsMonitor};
}

void
Test::InitOperationTracker(OperationTracker *operation_tracker)
{
    delete _operationTracker;
    if (operation_tracker == nullptr) {
        /* Fallback to default behavior. */
        operation_tracker = new OperationTracker(_config->GetSubconfig(operationTracker),
          _config->GetBool(compressionEnabled), *_timestampManager);
    }
    _operationTracker = operation_tracker;
    _workloadManager->SetOperationTracker(_operationTracker);
    _database.SetOperationTracker(_operationTracker);
    _components.push_back(_operationTracker);
}

Test::~Test()
{
    delete _config;
    delete _metricsMonitor;
    delete _timestampManager;
    delete _threadManager;
    delete _workloadManager;
    delete _operationTracker;
    _config = nullptr;
    _metricsMonitor = nullptr;
    _timestampManager = nullptr;
    _threadManager = nullptr;
    _workloadManager = nullptr;
    _operationTracker = nullptr;

    _components.clear();
}

void
Test::Run()
{
    /* Build the database creation config string. */
    std::string databaseCreateConfig = connectionCreate;

    /* Enable snappy compression or reverse collator if required. */
    if (_config->GetBool(compressionEnabled) || _config->GetBool(reverseCollator)) {
        databaseCreateConfig += ",extensions=[";
        databaseCreateConfig +=
          _config->GetBool(compressionEnabled) ? std::string(SNAPPY_PATH) + "," : "";
        databaseCreateConfig +=
          _config->GetBool(reverseCollator) ? std::string(REVERSE_COLLATOR_PATH) : "";
        databaseCreateConfig += "]";
    }

    /* Get the cache size. */
    int64_t cache_size_mb = _config->GetInt(cacheSizeMB);
    databaseCreateConfig += ",cache_size=" + std::to_string(cache_size_mb) + "MB";

    /* Get the statistics configuration for this run. */
    Configuration *statistics_config = _config->GetSubconfig(statisticsConfig);
    databaseCreateConfig += statistics_config->GetBool(enableLogging) ? "," + statisticsLog : "";
    databaseCreateConfig += ",statistics=(" + statistics_config->GetString(type) + ")";
    /* Don't forget to delete. */
    delete statistics_config;

    /* Enable or disable write ahead logging. */
    databaseCreateConfig +=
      ",log=(enabled=" + std::string(_config->GetBool(enableLogging) ? "true" : "false") + ")";

    /* Maximum waiting time for the cache to get unstuck. */
    databaseCreateConfig += ",cache_max_wait_ms=" + std::to_string(_config->GetInt(cacheMaxWaitMs));

    /* Add the user supplied wiredtiger open config. */
    databaseCreateConfig += _args.wtOpenConfig;

    /* Create connection. */
    ConnectionManager::GetInstance().Create(databaseCreateConfig, DEFAULT_DIR);

    /* Initiate the load stage of each component. */
    for (const auto &it : _components)
        it->Load();

    /* Spawn threads for all Component::Run() functions. */
    for (const auto &it : _components)
        _threadManager->addThread(&Component::Run, it);

    /* The initial population phase needs to be finished before starting the actual test. */
    while (_workloadManager->IsEnabled() && !_workloadManager->IsDatabasePopulated())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    /* The test will run for the duration as defined in the config. */
    int64_t duration_secs = _config->GetInt(durationSecs);
    Logger::LogMessage(
      LOG_INFO, "Waiting {" + std::to_string(duration_secs) + "} seconds for testing to complete.");
    std::this_thread::sleep_for(std::chrono::seconds(duration_secs));

    /* Notify components that they should complete their last iteration. */
    for (const auto &it : _components)
        it->EndRun();

    /* Call join on the components threads so we know they have finished their loop. */
    Logger::LogMessage(LOG_INFO,
      "Joining all component threads.\n This could take a while as we need to wait"
      " for all components to finish their current loop.");
    _threadManager->Join();

    /* End the test by calling finish on all known components. */
    for (const auto &it : _components)
        it->Finish();

    /* Validation stage. */
    if (_operationTracker->IsEnabled()) {
        std::unique_ptr<Configuration> tracking_config(_config->GetSubconfig(operationTracker));
        this->Validate(_operationTracker->getOperationTableName(),
          _operationTracker->getSchemaTableName(),
          _workloadManager->GetDatabase().GetCollectionIds());
    }

    /* Log perf stats. */
    MetricsWriter::GetInstance().WriteToFile(_args.testName);

    Logger::LogMessage(LOG_INFO, "SUCCESS");
}
} // namespace test_harness
