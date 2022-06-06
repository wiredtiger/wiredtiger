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

#include "workload_manager.h"

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/main/operation_configuration.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
WorkloadManager::WorkloadManager(Configuration *configuration, DatabaseOperation *databaseOperation,
  TimestampManager *timestampManager, Database &database)
    : Component(kWorkloadManager, configuration), _database(database),
      _databaseOperation(databaseOperation), _timestampManager(timestampManager)
{
}

WorkloadManager::~WorkloadManager()
{
    for (auto &it : _workers)
        delete it;
}

void
WorkloadManager::SetOperationTracker(OperationTracker *operationTracker)
{
    testutil_assert(_operationTracker == nullptr);
    _operationTracker = operationTracker;
}

void
WorkloadManager::Run()
{
    std::vector<OperationConfiguration> operationConfigs;

    /* Retrieve useful parameters from the test configuration. */
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kCheckpointOpConfig), ThreadType::kCheckpoint));
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kCustomOpConfig), ThreadType::kCustom));
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kInsertOpConfig), ThreadType::kInsert));
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kReadOpConfig), ThreadType::kRead));
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kRemoveOpConfig), ThreadType::kRemove));
    operationConfigs.push_back(
      OperationConfiguration(_config->GetSubconfig(kUpdateOpConfig), ThreadType::kUpdate));
    Configuration *populatedConfig = _config->GetSubconfig(kPopulateConfig);

    /* Populate the database. */
    _databaseOperation->Populate(_database, _timestampManager, populatedConfig, _operationTracker);
    _isDatabasePopulated = true;
    delete populatedConfig;

    uint64_t thread_id = 0;
    /* Generate threads to execute the different operations on the collections. */
    for (auto &it : operationConfigs) {
        if (it.thread_count != 0)
            Logger::LogMessage(LOG_INFO,
              "WorkloadManager: Creating " + std::to_string(it.thread_count) + " " +
                ThreadTypeToString(it.type) + " threads.");
        for (size_t i = 0; i < it.thread_count && _running; ++i) {
            ThreadWorker *threadWorker = new ThreadWorker(thread_id++, it.type, it.config,
              ConnectionManager::GetInstance().CreateSession(), _timestampManager,
              _operationTracker, _database);
            _workers.push_back(threadWorker);
            _threadManager.addThread(it.GetFunction(_databaseOperation), threadWorker);
        }
        /*
         * Don't forget to delete the config we created earlier. While we do pass the config into
         * the thread context it is not saved, so we are safe to do this.
         */
        delete it.config;

        /*
         * Reset the thread_id counter to 0 as we're only interested in knowing per operation type
         * which thread we are.
         */
        thread_id = 0;
    }
}

void
WorkloadManager::Finish()
{
    Component::Finish();
    for (const auto &it : _workers)
        it->Finish();
    _threadManager.Join();
    Logger::LogMessage(LOG_TRACE, "Workload generator: run stage done");
}

Database &
WorkloadManager::GetDatabase()
{
    return (_database);
}

bool
WorkloadManager::IsDatabasePopulated() const
{
    return (_isDatabasePopulated);
}
} // namespace test_harness
