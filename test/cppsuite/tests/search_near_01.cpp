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
 * In this test, we want to verify that search_near with prefix enabled only traverses the portion
 * of the tree that follows the prefix portion of the search key. The test is composed of a populate
 * phase followed by a read phase. The populate phase will insert a set of random generated keys
 * with a prefix of aaa -> zzz. During the read phase, we have one read thread that performs:
 *  - Spawning multiple threads to perform one prefix search near.
 *  - Waiting on all threads to finish.
 *  - Using WiredTiger statistics to validate that the number of entries traversed is within
 * bounds of the search key.
 */
class SearchNear01 : public Test {
    uint64_t keysPerPrefix = 0;
    uint64_t searchKeyLength = 0;
    const std::string kAlphabet{"abcdefghijklmnopqrstuvwxyz"};
    const uint64_t PREFIX_KEY_LEN = 3;
    const int64_t MINIMUM_EXPECTED_ENTRIES = 40;

    static void
    populate_worker(
      ThreadWorker *threadWorker, const std::string &kAlphabet, uint64_t prefixKeyLength)
    {
        Logger::LogMessage(
          LOG_INFO, "Populate with thread id: " + std::to_string(threadWorker->id));

        uint64_t collectionsPerThread = threadWorker->collectionCount;
        const uint64_t kMaxRollbacks = 100;
        uint32_t rollbackRetries = 0;

        /*
         * Generate a table of data with prefix keys aaa -> zzz. We have 26 threads from ids
         * starting from 0 to 26. Each populate thread will insert separate prefix keys based on the
         * id.
         */
        for (int64_t i = 0; i < collectionsPerThread; ++i) {
            Collection &collection = threadWorker->database.GetCollection(i);
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);
            for (uint64_t j = 0; j < kAlphabet.size(); ++j) {
                for (uint64_t k = 0; k < kAlphabet.size(); ++k) {
                    for (uint64_t count = 0; count < threadWorker->keyCount; ++count) {
                        threadWorker->transaction.Begin();
                        /*
                         * Generate the prefix key, and append a random generated key string based
                         * on the key size configuration.
                         */
                        std::string prefixKey = {
                          kAlphabet.at(threadWorker->id), kAlphabet.at(j), kAlphabet.at(k)};
                        prefixKey += RandomGenerator::GetInstance().GenerateRandomString(
                          threadWorker->keySize - prefixKeyLength);
                        std::string value =
                          RandomGenerator::GetInstance().GeneratePseudoRandomString(
                            threadWorker->valueSize);
                        if (!threadWorker->Insert(cursor, collection.id, prefixKey, value)) {
                            testutil_assert(rollbackRetries < kMaxRollbacks);
                            /* We failed to insert, rollback our transaction and retry. */
                            threadWorker->transaction.Rollback();
                            ++rollbackRetries;
                            if (count > 0)
                                --count;
                        } else {
                            /* Commit txn at commit timestamp 100. */
                            testutil_assert(threadWorker->transaction.Commit("commit_timestamp=" +
                              threadWorker->timestampManager->DecimalToHex(100)));
                            rollbackRetries = 0;
                        }
                    }
                }
            }
        }
    }

    public:
    SearchNear01(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    void
    Populate(Database &database, TimestampManager *timestampManager, Configuration *config,
      OperationTracker *operationTracker) override final
    {
        /* Validate our config. */
        int64_t collection_count = config->GetInt(kCollectionCount);
        keysPerPrefix = config->GetInt(kKeyCountPerCollection);
        int64_t keySize = config->GetInt(kKeySize);
        testutil_assert(collection_count > 0);
        testutil_assert(keysPerPrefix > 0);
        /* Check the prefix length is not greater than the key size. */
        testutil_assert(keySize >= PREFIX_KEY_LEN);

        Logger::LogMessage(LOG_INFO,
          "Populate configuration with key size: " + std::to_string(keySize) +
            " key count: " + std::to_string(keysPerPrefix) +
            " number of collections: " + std::to_string(collection_count));

        /* Create n collections as per the configuration. */
        for (uint64_t i = 0; i < collection_count; ++i)
            /*
             * The database model will call into the API and create the collection, with its own
             * session.
             */
            database.AddCollection();

        /* Spawn 26 threads to populate the database. */
        std::vector<ThreadWorker *> workers;
        ThreadManager threadManager;
        for (uint64_t i = 0; i < kAlphabet.size(); ++i) {
            ThreadWorker *threadWorker = new ThreadWorker(i, ThreadType::kInsert, config,
              ConnectionManager::GetInstance().CreateSession(), timestampManager, operationTracker,
              database);
            workers.push_back(threadWorker);
            threadManager.addThread(populate_worker, threadWorker, kAlphabet, PREFIX_KEY_LEN);
        }

        /* Wait for our populate threads to finish and then join them. */
        Logger::LogMessage(LOG_INFO, "Populate: waiting for threads to complete.");
        threadManager.Join();

        /* Cleanup our workers. */
        for (auto &it : workers) {
            delete it;
            it = nullptr;
        }

        /* Force evict all the populated keys in all of the collections. */
        int cmpp;
        ScopedSession session = ConnectionManager::GetInstance().CreateSession();
        for (uint64_t count = 0; count < collection_count; ++count) {
            Collection &collection = database.GetCollection(count);
            ScopedCursor evictionCursor =
              session.OpenScopedCursor(collection.name.c_str(), "debug=(release_evict=true)");

            for (uint64_t i = 0; i < kAlphabet.size(); ++i) {
                for (uint64_t j = 0; j < kAlphabet.size(); ++j) {
                    for (uint64_t k = 0; k < kAlphabet.size(); ++k) {
                        std::string key = {kAlphabet.at(i), kAlphabet.at(j), kAlphabet.at(k)};
                        evictionCursor->set_key(evictionCursor.Get(), key.c_str());
                        testutil_check(evictionCursor->search_near(evictionCursor.Get(), &cmpp));
                        testutil_check(evictionCursor->reset(evictionCursor.Get()));
                    }
                }
            }
        }
        searchKeyLength =
          RandomGenerator::GetInstance().GenerateInteger(static_cast<uint64_t>(1), PREFIX_KEY_LEN);
        Logger::LogMessage(LOG_INFO, "Populate: finished.");
    }

    static void
    perform_search_near(ThreadWorker *threadWorker, std::string collection_name,
      uint64_t searchKeyLength, std::atomic<int64_t> &zKeySearches)
    {
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection_name);
        testutil_check(cursor->reconfigure(cursor.Get(), "prefix_search=true"));
        /* Generate search prefix key of random length between a -> zzz. */
        const std::string srch_key = RandomGenerator::GetInstance().GenerateRandomString(
          searchKeyLength, CharactersType::kAlphabet);
        Logger::LogMessage(LOG_TRACE,
          "Search near thread {" + std::to_string(threadWorker->id) +
            "} performing prefix search near with key: " + srch_key);

        /*
         * Read at timestamp 10, so that no keys are visible to this transaction. When performing
         * prefix search near, we expect the search to early exit out of its prefix range and return
         * WT_NOTFOUND.
         */
        threadWorker->transaction.Begin(
          "read_timestamp=" + threadWorker->timestampManager->DecimalToHex(10));
        if (threadWorker->transaction.Running()) {
            cursor->set_key(cursor.Get(), srch_key.c_str());
            int cmpp;
            testutil_assert(cursor->search_near(cursor.Get(), &cmpp) == WT_NOTFOUND);
            threadWorker->transaction.IncrementOpCounter();

            /*
             * There is an edge case where we may not early exit the prefix search near call because
             * the specified prefix matches the rest of the entries in the tree.
             *
             * In this test, the keys in our database start with prefixes aaa -> zzz. If we search
             * with a prefix such as "z", we will not early exit the search near call because the
             * rest of the keys will also start with "z" and match the prefix. The statistic will
             * stay the same if we do not early exit search near, track this through incrementing
             * the number of z key searches we have done this iteration.
             */
            if (srch_key == "z" || srch_key == "zz" || srch_key == "zzz")
                ++zKeySearches;
            threadWorker->transaction.Rollback();
        }
    }

    void
    ReadOperation(ThreadWorker *threadWorker) override final
    {
        /* Make sure that thread statistics cursor is null before we open it. */
        testutil_assert(threadWorker->statisticsCursor.Get() == nullptr);
        /* This test will only work with one read thread. */
        testutil_assert(threadWorker->threadCount == 1);
        std::atomic<int64_t> zKeySearches;
        int64_t entriesStatistics, expectedEntries, prefixStatistics, prevEntriesStatistics,
          prevPrefixStatistics;

        prevEntriesStatistics = 0;
        prevPrefixStatistics = 0;
        int64_t threadsCount = _config->GetInt("search_near_threads");
        threadWorker->statisticsCursor = threadWorker->session.OpenScopedCursor(kStatisticsURI);
        Configuration *workloadConfig = _config->GetSubconfig(kWorkloadManager);
        Configuration *readConfig = workloadConfig->GetSubconfig(kReadOpConfig);
        zKeySearches = 0;

        Logger::LogMessage(LOG_INFO,
          ThreadTypeToString(threadWorker->type) + " thread commencing. Spawning " +
            std::to_string(threadsCount) + " search near threads.");

        /*
         * The number of expected entries is calculated to account for the maximum allowed entries
         * per search near function call. The key we search near can be different in length, which
         * will increase the number of entries search by a factor of 26.
         */
        expectedEntries = keysPerPrefix * pow(kAlphabet.size(), PREFIX_KEY_LEN - searchKeyLength);
        while (threadWorker->Running()) {
            MetricsMonitor::GetStatistics(threadWorker->statisticsCursor,
              WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100, &prevEntriesStatistics);
            MetricsMonitor::GetStatistics(threadWorker->statisticsCursor,
              WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS, &prevPrefixStatistics);

            ThreadManager threadManager;
            std::vector<ThreadWorker *> workers;
            for (uint64_t i = 0; i < threadsCount; ++i) {
                /* Get a collection and find a cached cursor. */
                Collection &collection = threadWorker->database.GetRandomCollection();
                ThreadWorker *searchNearThreadWorker = new ThreadWorker(i, ThreadType::kRead,
                  readConfig, ConnectionManager::GetInstance().CreateSession(),
                  threadWorker->timestampManager, threadWorker->operationTracker,
                  threadWorker->database);
                workers.push_back(searchNearThreadWorker);
                threadManager.addThread(perform_search_near, searchNearThreadWorker,
                  collection.name, searchKeyLength, std::ref(zKeySearches));
            }

            threadManager.Join();

            /* Cleanup our workers. */
            for (auto &it : workers) {
                delete it;
                it = nullptr;
            }
            workers.clear();

            MetricsMonitor::GetStatistics(threadWorker->statisticsCursor,
              WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100, &entriesStatistics);
            MetricsMonitor::GetStatistics(threadWorker->statisticsCursor,
              WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS, &prefixStatistics);
            Logger::LogMessage(LOG_TRACE,
              "Read thread skipped entries: " +
                std::to_string(entriesStatistics - prevEntriesStatistics) + " prefix early exit: " +
                std::to_string(prefixStatistics - prevPrefixStatistics - zKeySearches));
            /*
             * It is possible that WiredTiger increments the entries skipped stat irrelevant to
             * prefix search near. This is dependent on how many read threads are present in the
             * test. Account for this by creating a small buffer using thread count. Assert that the
             * number of expected entries is the upper limit which the prefix search near can
             * traverse.
             *
             * Assert that the number of expected entries is the maximum allowed limit that the
             * prefix search nears can traverse and that the prefix fast path has increased by the
             * number of threads minus the number of search nears with z key.
             */
            testutil_assert(threadsCount * expectedEntries + (2 * threadsCount) >=
              entriesStatistics - prevEntriesStatistics);
            testutil_assert(prefixStatistics - prevPrefixStatistics == threadsCount - zKeySearches);
            zKeySearches = 0;
            threadWorker->Sleep();
        }
        delete readConfig;
        delete workloadConfig;
    }
};
