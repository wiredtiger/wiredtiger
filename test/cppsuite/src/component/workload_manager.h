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

#ifndef WORKLOAD_MANAGER_H
#define WORKLOAD_MANAGER_H

#include <functional>

#include "src/common/thread_manager.h"
#include "src/main/configuration.h"
#include "src/main/database_operation.h"
#include "src/main/thread_worker.h"

namespace test_harness {
/*
 * Class that can execute operations based on a given configuration.
 */
class WorkloadManager : public Component {
    public:
    WorkloadManager(configuration *configuration, database_operation *databaseOperation,
      TimestampManager *timestampManager, database &database);

    ~WorkloadManager();

    /* Delete the copy constructor and the assignment operator. */
    WorkloadManager(const WorkloadManager &) = delete;
    WorkloadManager &operator=(const WorkloadManager &) = delete;

    /* Do the work of the main part of the workload. */
    void Run() override final;
    void Finish() override final;

    database &GetDatabase();
    bool IsDatabasePopulated() const;

    /* Set the tracking component. */
    void SetOperationTracker(OperationTracker *op_tracker);

    private:
    database &_database;
    database_operation *_databaseOperation = nullptr;
    ThreadManager _threadManager;
    TimestampManager *_timestampManager = nullptr;
    OperationTracker *_operationTracker = nullptr;
    std::vector<thread_worker *> _workers;
    bool _isDatabasePopulated = false;
};
} // namespace test_harness

#endif
