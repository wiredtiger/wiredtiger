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
#include "src/main/test.h"

using namespace test_harness;

/*
 * This test inserts and reads a large quantity of data in bursts, this is intended to simulate a
 * mongod instance loading a large amount of data over a long period of time.
 */
class BurstInserts : public Test {
    public:
    BurstInserts(const test_args &args) : Test(args)
    {
        _burstDurationSecs = _config->GetInt("burst_duration");
        Logger::LogMessage(
          LOG_INFO, "Burst duration set to: " + std::to_string(_burstDurationSecs));
        InitOperationTracker();
    }

    /*
     * Insert operation that inserts continuously for insert_duration with no throttling. It then
     * sleeps for op_rate.
     */
    void
    InsertOperation(ThreadWorker *threadWorker) override final
    {
        Logger::LogMessage(LOG_INFO,
          ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        /* Helper struct which stores a pointer to a collection and a cursor associated with it. */
        struct collection_cursor {
            collection_cursor(
              Collection &collection, ScopedCursor &&writeCursor, ScopedCursor &&readCursor)
                : collection(collection), readCursor(std::move(readCursor)),
                  writeCursor(std::move(writeCursor))
            {
            }
            Collection &collection;
            ScopedCursor readCursor;
            ScopedCursor writeCursor;
        };

        /* Collection cursor vector. */
        std::vector<collection_cursor> ccv;
        uint64_t collectionCount = threadWorker->database.GetCollectionCount();
        uint64_t collectionsPerThread = collectionCount / threadWorker->threadCount;
        /* Must have unique collections for each thread. */
        testutil_assert(collectionCount % threadWorker->threadCount == 0);
        int threadOffset = threadWorker->id * collectionsPerThread;
        for (int i = threadOffset;
             i < threadOffset + collectionsPerThread && threadWorker->Running(); ++i) {
            Collection &collection = threadWorker->database.GetCollection(i);
            /*
             * Create a reading cursor that will read random documents for every next call. This
             * will help generate cache pressure.
             */
            ccv.push_back(
              {collection, std::move(threadWorker->session.OpenScopedCursor(collection.name)),
                std::move(
                  threadWorker->session.OpenScopedCursor(collection.name, "next_random=true"))});
        }

        uint64_t counter = 0;
        while (threadWorker->Running()) {
            uint64_t startKey = ccv[counter].collection.GetKeyCount();
            uint64_t addedCount = 0;
            auto &cc = ccv[counter];
            auto burst_start = std::chrono::system_clock::now();
            while (threadWorker->Running() &&
              std::chrono::system_clock::now() - burst_start <
                std::chrono::seconds(_burstDurationSecs)) {
                threadWorker->transaction.TryBegin();
                auto key = threadWorker->PadString(
                  std::to_string(startKey + addedCount), threadWorker->keySize);
                cc.writeCursor->set_key(cc.writeCursor.Get(), key.c_str());
                testutil_assert(cc.writeCursor->search(cc.writeCursor.Get()) == WT_NOTFOUND);

                /* A return value of true implies the insert was successful. */
                auto value = RandomGenerator::GetInstance().GeneratePseudoRandomString(
                  threadWorker->valueSize);
                if (!threadWorker->Insert(cc.writeCursor, cc.collection.id, key, value)) {
                    threadWorker->transaction.Rollback();
                    addedCount = 0;
                    continue;
                }
                addedCount++;

                /* Walk our random reader intended to generate cache pressure. */
                int ret = 0;
                if ((ret = cc.readCursor->next(cc.readCursor.Get())) != 0) {
                    if (ret == WT_NOTFOUND) {
                        testutil_check(cc.readCursor->reset(cc.readCursor.Get()));
                    } else if (ret == WT_ROLLBACK) {
                        threadWorker->transaction.Rollback();
                        addedCount = 0;
                        continue;
                    } else {
                        testutil_die(ret, "Unhandled error in cursor->next()");
                    }
                }

                if (threadWorker->transaction.CanCommit()) {
                    if (threadWorker->transaction.Commit()) {
                        cc.collection.IncreaseKeyCount(addedCount);
                        startKey = cc.collection.GetKeyCount();
                    }
                    addedCount = 0;
                }

                /* Sleep as currently this loop is too fast. */
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            /* Close out our current txn. */
            if (threadWorker->transaction.Running()) {
                if (threadWorker->transaction.Commit()) {
                    Logger::LogMessage(LOG_TRACE,
                      "Committed an insertion of " + std::to_string(addedCount) + " keys.");
                    cc.collection.IncreaseKeyCount(addedCount);
                }
            }

            testutil_check(cc.writeCursor->reset(cc.writeCursor.Get()));
            testutil_check(cc.readCursor->reset(cc.readCursor.Get()));
            counter++;
            if (counter == collectionsPerThread)
                counter = 0;
            testutil_assert(counter < collectionsPerThread);
            threadWorker->Sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        if (threadWorker->transaction.Running())
            threadWorker->transaction.Rollback();
    }

    private:
    int _burstDurationSecs = 0;
};
