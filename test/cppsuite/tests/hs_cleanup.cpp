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

#include "src/common/random_generator.h"
#include "src/main/test.h"

using namespace test_harness;

/*
 * Here we want to age out entire pages, i.e. the stop time pair on a page should be globally
 * visible. To do so we'll update ranges of keys with increasing timestamps which will age out the
 * pre-existing data. It may not trigger a cleanup on the data file but should result in data
 * getting cleaned up from the history store.
 *
 * This is then tracked using the associated statistic which can be found in the MetricsMonitor.
 */
class HsCleanup : public Test {
    public:
    HsCleanup(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    void
    UpdateOperation(ThreadWorker *threadWorker) override final
    {
        Logger::LogMessage(LOG_INFO,
          ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        const uint64_t kMaxRollbacks = 100;
        uint32_t rolbackRetries = 0;

        Collection &collection = threadWorker->database.GetCollection(threadWorker->id);

        /* In this test each thread gets a single collection. */
        testutil_assert(threadWorker->database.GetCollectionCount() == threadWorker->threadCount);
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);

        /* We don't know the keyrange we're operating over here so we can't be much smarter here. */
        while (threadWorker->Running()) {
            threadWorker->Sleep();

            auto ret = cursor->next(cursor.Get());
            if (ret != 0) {
                if (ret == WT_NOTFOUND) {
                    testutil_check(cursor->reset(cursor.Get()));
                    continue;
                }
                if (ret == WT_ROLLBACK) {
                    /*
                     * As a result of the logic in this test its possible that the previous next
                     * call can happen outside the context of a transaction. Assert that we are in
                     * one if we got a rollback.
                     */
                    testutil_check(threadWorker->transaction.CanRollback());
                    threadWorker->transaction.Rollback();
                    continue;
                }
                testutil_die(ret, "Unexpected error returned from cursor->next()");
            }

            const char *keyTmp;
            testutil_check(cursor->get_key(cursor.Get(), &keyTmp));

            /* Start a transaction if possible. */
            threadWorker->transaction.TryStart();

            /*
             * The retrieved key needs to be passed inside the update function. However, the update
             * API doesn't guarantee our buffer will still be valid once it is called, as such we
             * copy the buffer and then pass it into the API.
             */
            std::string value =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
            if (threadWorker->Update(cursor, collection.id, keyTmp, value)) {
                if (threadWorker->transaction.CanCommit()) {
                    if (threadWorker->transaction.Commit())
                        rolbackRetries = 0;
                    else
                        ++rolbackRetries;
                }
            } else {
                threadWorker->transaction.Rollback();
                ++rolbackRetries;
            }
            testutil_assert(rolbackRetries < kMaxRollbacks);
        }
        /* Ensure our last transaction is resolved. */
        if (threadWorker->transaction.Active())
            threadWorker->transaction.Rollback();
    }
};
