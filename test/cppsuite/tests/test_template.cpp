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
#include "src/main/test.h"

namespace test_harness {
/* Defines what data is written to the tracking table for use in custom validation. */
class OperationTrackerTemplate : public OperationTracker {

    public:
    OperationTrackerTemplate(
      Configuration *config, const bool useCompression, TimestampManager &timestampManager)
        : OperationTracker(config, useCompression, timestampManager)
    {
    }

    void
    setTrackingCursor(const uint64_t transactionId, const TrackingOperation &operation,
      const uint64_t &collectionId, const std::string &key, const std::string &value,
      wt_timestamp_t timestamp, ScopedCursor &opTrackingCursor) override final
    {
        /* You can replace this call to define your own tracking table contents. */
        OperationTracker::setTrackingCursor(
          transactionId, operation, collectionId, key, value, timestamp, opTrackingCursor);
    }
};

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class TestTemplate : public Test {
    public:
    TestTemplate(const test_args &args) : Test(args)
    {
        InitOperationTracker(new OperationTrackerTemplate(_config->GetSubconfig(kOperationTracker),
          _config->GetBool(kCompressionEnabled), *_timestampManager));
    }

    void
    Run() override final
    {
        /* You can remove the call to the base class to fully customize your test. */
        Test::Run();
    }

    void
    Populate(Database &, TimestampManager *, Configuration *, OperationTracker *) override final
    {
        Logger::LogMessage(LOG_WARN, "populate: nothing done");
    }

    void
    CheckpointOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "CheckpointOperation: nothing done");
    }

    void
    CustomOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "CustomOperation: nothing done");
    }

    void
    InsertOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "InsertOperation: nothing done");
    }

    void
    ReadOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "ReadOperation: nothing done");
    }

    void
    RemoveOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "RemoveOperation: nothing done");
    }

    void
    UpdateOperation(ThreadWorker *) override final
    {
        Logger::LogMessage(LOG_WARN, "UpdateOperation: nothing done");
    }

    void
    Validate(const std::string &, const std::string &, const std::vector<uint64_t> &) override final
    {
        Logger::LogMessage(LOG_WARN, "validate: nothing done");
    }
};

} // namespace test_harness
