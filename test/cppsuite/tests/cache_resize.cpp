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
#include "src/component/operation_tracker.h"
#include "src/main/test.h"

using namespace test_harness;

/* Defines what data is written to the tracking table for use in custom validation. */
class OperationTrackerCacheResize : public OperationTracker {

    public:
    OperationTrackerCacheResize(
      Configuration *config, const bool useCompression, TimestampManager &timestampManager)
        : OperationTracker(config, useCompression, timestampManager)
    {
    }

    void
    setTrackingCursor(const uint64_t transactionId, const trackingOperation &operation,
      const uint64_t &, const std::string &, const std::string &value, wt_timestamp_t timestamp,
      ScopedCursor &cursor) override final
    {
        cursor->set_key(cursor.Get(), timestamp, transactionId);
        cursor->set_value(cursor.Get(), operation, value.c_str());
    }
};

/*
 * This test continuously writes transactions larger than 1MB but less than 500MB into the database,
 * while switching the connection cache size between 1MB and 500MB. When transactions are larger
 * than the cache size they are rejected, so only transactions made when cache size is 500MB should
 * be allowed.
 */
class CacheResize : public Test {
    public:
    CacheResize(const test_args &args) : Test(args)
    {
        InitOperationTracker(
          new OperationTrackerCacheResize(_config->GetSubconfig(kOperationTracker),
            _config->GetBool(kCompressionEnabled), *_timestampManager));
    }

    void
    CustomOperation(thread_worker *threadWorker) override final
    {
        WT_CONNECTION *connection = ConnectionManager::GetInstance().GetConnection();
        WT_CONNECTION_IMPL *connectionImpl = (WT_CONNECTION_IMPL *)connection;
        bool increaseCache = false;
        const std::string smallCacheSize = "cache_size=1MB";
        const std::string bigCacheSize = "cache_size=500MB";

        while (threadWorker->running()) {
            threadWorker->sleep();

            /* Get the current cache size. */
            uint64_t previousCacheSize = connectionImpl->cache_size;

            /* Reconfigure with the new cache size. */
            testutil_check(connection->reconfigure(
              connection, increaseCache ? bigCacheSize.c_str() : smallCacheSize.c_str()));

            /* Get the new cache size. */
            uint64_t newCacheSize = connectionImpl->cache_size;

            Logger::LogMessage(LOG_TRACE,
              "The cache size was updated from " + std::to_string(previousCacheSize) + " to " +
                std::to_string(newCacheSize));

            /*
             * The collection id and the key are dummy fields which are required by the
             * save_operation API but not needed for this test.
             */
            const uint64_t collectionId = 0;
            const std::string key;
            const std::string value = std::to_string(newCacheSize);

            /* Retrieve the current transaction id. */
            uint64_t transactionId = ((WT_SESSION_IMPL *)threadWorker->session.Get())->txn->id;

            /* Save the change of cache size in the tracking table. */
            threadWorker->txn.Start();
            int ret = threadWorker->op_tracker->save_operation(transactionId,
              trackingOperation::CUSTOM, collectionId, key, value,
              threadWorker->tsm->GetNextTimestamp(), threadWorker->op_track_cursor);

            if (ret == 0)
                testutil_assert(threadWorker->txn.Commit());
            else {
                /* Due to the cache pressure, it is possible to fail when saving the operation. */
                testutil_assert(ret == WT_ROLLBACK);
                Logger::LogMessage(LOG_WARN,
                  "The cache size reconfiguration could not be saved in the tracking table, ret: " +
                    std::to_string(ret));
                threadWorker->txn.Rollback();
            }
            increaseCache = !increaseCache;
        }
    }

    void
    InsertOperation(thread_worker *threadWorker) override final
    {
        const uint64_t collectionCount = threadWorker->db.GetCollectionCount();
        testutil_assert(collectionCount > 0);
        Collection &collection = threadWorker->db.GetCollection(collectionCount - 1);
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);

        while (threadWorker->running()) {
            threadWorker->sleep();

            /* Insert the current cache size value using a random key. */
            const std::string key =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->key_size);
            const uint64_t cacheSize =
              ((WT_CONNECTION_IMPL *)ConnectionManager::GetInstance().GetConnection())->cache_size;
            /* Take into account the value size given in the test configuration file. */
            const std::string value = std::to_string(cacheSize);

            threadWorker->txn.TryStart();
            if (!threadWorker->insert(cursor, collection.id, key, value)) {
                threadWorker->txn.Rollback();
            } else if (threadWorker->txn.CanCommit()) {
                /*
                 * The transaction can fit in the current cache size and is ready to be committed.
                 * This means the tracking table will contain a new record to represent this
                 * transaction which will be used during the validation stage.
                 */
                testutil_assert(threadWorker->txn.Commit());
            }
        }

        /* Make sure the last transaction is rolled back now the work is finished. */
        if (threadWorker->txn.Active())
            threadWorker->txn.Rollback();
    }

    void
    Validate(const std::string &operationTableName, const std::string &,
      const std::vector<uint64_t> &) override final
    {
        bool firstRecord = false;
        int ret;
        uint64_t cacheSize, numRecords = 0, previousTransactionId;
        const uint64_t cacheSize500MB = 500000000;

        /* FIXME-WT-9339. */
        (void)cacheSize;
        (void)cacheSize500MB;

        /* Open a cursor on the tracking table to read it. */
        ScopedSession session = ConnectionManager::GetInstance().CreateSession();
        ScopedCursor cursor = session.OpenScopedCursor(operationTableName);

        /*
         * Parse the tracking table. Each operation is tracked and each transaction is made of
         * multiple operations, hence we expect multiple records for each transaction. We only need
         * to verify that the cache size was big enough when the transaction was committed, which
         * means at the last operation.
         */
        while ((ret = cursor->next(cursor.Get())) == 0) {

            uint64_t trackedTimestamp, trackedTransactionId;
            int trackedOperationType;
            const char *trackedCacheSize;

            testutil_check(cursor->get_key(cursor.Get(), &trackedTimestamp, &trackedTransactionId));
            testutil_check(
              cursor->get_value(cursor.Get(), &trackedOperationType, &trackedCacheSize));

            Logger::LogMessage(LOG_TRACE,
              "Timestamp: " + std::to_string(trackedTimestamp) +
                ", transaction id: " + std::to_string(trackedTransactionId) +
                ", cache size: " + std::to_string(std::stoull(trackedCacheSize)));

            trackingOperation opType = static_cast<trackingOperation>(trackedOperationType);
            /* There are only two types of operation tracked. */
            testutil_assert(
              opType == trackingOperation::CUSTOM || opType == trackingOperation::INSERT);

            /*
             * There is nothing to do if we are reading a record that indicates a cache size change.
             */
            if (opType == trackingOperation::CUSTOM)
                continue;

            if (firstRecord) {
                firstRecord = false;
            } else if (previousTransactionId != trackedTransactionId) {
                /*
                 * We have moved to a new transaction, make sure the cache was big enough when the
                 * previous transaction was committed.
                 *
                 * FIXME-WT-9339 - Somehow we have some transactions that go through while the cache
                 * is very low. Enable the check when this is no longer the case.
                 *
                 * testutil_assert(cacheSize > cacheSize500MB);
                 */
            }
            previousTransactionId = trackedTransactionId;
            /* Save the last cache size seen by the transaction. */
            cacheSize = std::stoull(trackedCacheSize);
            ++numRecords;
        }
        /* All records have been parsed, the last one still needs the be checked. */
        testutil_assert(ret == WT_NOTFOUND);
        testutil_assert(numRecords > 0);
        /*
         * FIXME-WT-9339 - Somehow we have some transactions that go through while the cache is very
         * low. Enable the check when this is no longer the case.
         *
         * testutil_assert(cacheSize > cacheSize500MB);
         */
    }
};
