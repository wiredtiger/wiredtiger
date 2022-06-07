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
 * In this test, we want to verify search_near with prefix enabled when performing unique index
 * insertions. For the test duration:
 *  - N thread will perform unique index insertions on existing keys in the table. These insertions
 * are expected to fail.
 *  - M threads will traverse the collections and ensure that the number of records in the
 * collections don't change.
 */
class SearchNear03 : public Test {
    /* A 2D array consisted of a mapping between each collection and their inserted prefixes. */
    std::vector<std::vector<std::string>> existingPrefixes;
    const std::string kAlphabet{"abcdefghijklmnopqrstuvwxyz"};

    public:
    SearchNear03(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    /*
     * Here's how we insert an entry into a unique index:
     * 1. Insert the prefix.
     * 2. Remove the prefix.
     * 3. Search near for the prefix. In the case we find a record, we stop here as a value with the
     * prefix already exists in the table. Otherwise if the record is not found, we can proceed to
     * insert the full value.
     * 4. Insert the full value (prefix, id).
     * All of these operations are wrapped in the same transaction.
     */
    static bool
    PerformUniqueIndexInsertions(ThreadWorker *threadWorker, ScopedCursor &cursor,
      Collection &collection, std::string &prefixKey)
    {
        /* Insert the prefix. */
        std::string value =
          RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
        if (!threadWorker->Insert(cursor, collection.id, prefixKey, value))
            return false;

        /* Remove the prefix. */
        if (!threadWorker->Remove(cursor, collection.id, prefixKey))
            return false;

        /*
         * Prefix search near for the prefix. We expect that the prefix is not visible to us and a
         * WT_NOTFOUND error code is returned. If the prefix is present it means the (prefix, id)
         * has been inserted already. Double check that the prefix potion matches.
         */
        cursor->set_key(cursor.Get(), prefixKey.c_str());
        int exactPrefix;
        int ret = cursor->search_near(cursor.Get(), &exactPrefix);
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == 0) {
            const char *keyTmp;
            testutil_check(cursor->get_key(cursor.Get(), &keyTmp));
            testutil_assert(exactPrefix == 1);
            testutil_assert(prefixKey == GetPrefixFromKey(std::string(keyTmp)));
            return false;
        }

        /* Now insert the key with prefix and id. Use thread id to guarantee uniqueness. */
        value = RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
        return threadWorker->Insert(
          cursor, collection.id, prefixKey + "," + std::to_string(threadWorker->id), value);
    }

    static void
    populate_worker(ThreadWorker *threadWorker)
    {
        Logger::LogMessage(
          LOG_INFO, "Populate with thread id: " + std::to_string(threadWorker->id));

        const uint64_t kMaxRollbacks = 100;
        uint32_t rollbackRetries = 0;

        /*
         * Each populate thread perform unique index insertions on each collection, with a randomly
         * generated prefix and thread id.
         */
        Collection &collection = threadWorker->database.GetCollection(threadWorker->id);
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);
        testutil_check(cursor->reconfigure(cursor.Get(), "prefix_search=true"));
        for (uint64_t count = 0; count < threadWorker->keyCount; ++count) {
            threadWorker->transaction.Begin();
            /*
             * Generate the prefix key, and append a random generated key string based on the key
             * size configuration.
             */
            std::string prefixKey =
              RandomGenerator::GetInstance().GenerateRandomString(threadWorker->keySize);
            if (PerformUniqueIndexInsertions(threadWorker, cursor, collection, prefixKey)) {
                threadWorker->transaction.Commit();
            } else {
                threadWorker->transaction.Rollback();
                ++rollbackRetries;
                if (count > 0)
                    --count;
            }
            testutil_assert(rollbackRetries < kMaxRollbacks);
        }
    }

    static const std::string
    GetPrefixFromKey(const std::string &key)
    {
        const std::string::size_type pos = key.find(',');
        return pos != std::string::npos ? key.substr(0, pos) : "";
    }

    void
    Populate(Database &database, TimestampManager *timestampManager, Configuration *config,
      OperationTracker *operationTracker) override final
    {
        /* Validate our config. */
        int64_t collectionCount = config->GetInt(kCollectionCount);
        int64_t keyCount = config->GetInt(kKeyCountPerCollection);
        int64_t keySize = config->GetInt(kKeySize);
        testutil_assert(collectionCount > 0);
        testutil_assert(keyCount > 0);
        testutil_assert(keySize > 0);

        Logger::LogMessage(LOG_INFO,
          "Populate configuration with " + std::to_string(collectionCount) +
            "collections, number of keys: " + std::to_string(keyCount) +
            ", key size: " + std::to_string(keySize));

        /* Create n collections as per the configuration. */
        for (uint64_t i = 0; i < collectionCount; ++i)
            /*
             * The database model will call into the API and create the collection, with its own
             * session.
             */
            database.AddCollection();

        /* Spawn a populate thread for each collection in the database. */
        std::vector<ThreadWorker *> workers;
        ThreadManager threadManager;
        for (uint64_t i = 0; i < collectionCount; ++i) {
            ThreadWorker *threadWorker = new ThreadWorker(i, ThreadType::kInsert, config,
              ConnectionManager::GetInstance().CreateSession(), timestampManager, operationTracker,
              database);
            workers.push_back(threadWorker);
            threadManager.addThread(populate_worker, threadWorker);
        }

        /* Wait for our populate threads to finish and then join them. */
        Logger::LogMessage(LOG_INFO, "Populate: waiting for threads to complete.");
        threadManager.Join();

        /* Cleanup our workers. */
        for (auto &it : workers) {
            delete it;
            it = nullptr;
        }

        /*
         * Construct a mapping of all the inserted prefixes to their respective collections. We
         * traverse through each collection using a cursor to collect the prefix and push it into a
         * 2D vector.
         */
        ScopedSession session = ConnectionManager::GetInstance().CreateSession();
        const char *keyTmp;
        int ret = 0;
        for (uint64_t i = 0; i < database.GetCollectionCount(); i++) {
            Collection &collection = database.GetCollection(i);
            ScopedCursor cursor = session.OpenScopedCursor(collection.name);
            std::vector<std::string> prefixes;
            ret = 0;
            while (ret == 0) {
                ret = cursor->next(cursor.Get());
                testutil_assertfmt(ret == 0 || ret == WT_NOTFOUND,
                  "Unexpected error %d returned from cursor->next()", ret);
                if (ret == WT_NOTFOUND)
                    continue;
                testutil_check(cursor->get_key(cursor.Get(), &keyTmp));
                prefixes.push_back(keyTmp);
            }
            existingPrefixes.push_back(prefixes);
        }
        Logger::LogMessage(LOG_INFO, "Populate: finished.");
    }

    void
    InsertOperation(ThreadWorker *threadWorker) override final
    {
        std::map<uint64_t, ScopedCursor> cursors;

        /*
         * Each insert operation will attempt to perform unique index insertions with an existing
         * prefix on a collection.
         */
        Logger::LogMessage(LOG_INFO,
          ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        while (threadWorker->Running()) {
            /* Get a collection and find a cached cursor. */
            Collection &collection = threadWorker->database.GetRandomCollection();
            if (cursors.find(collection.id) == cursors.end()) {
                ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);
                testutil_check(cursor->reconfigure(cursor.Get(), "prefix_search=true"));
                cursors.emplace(collection.id, std::move(cursor));
            }

            /* Do a second lookup now that we know it exists. */
            auto &cursor = cursors[collection.id];
            threadWorker->transaction.Begin();
            /*
             * Grab a random existing prefix and perform unique index insertion. We expect it to
             * fail to insert, because it should already exist.
             */
            testutil_assert(existingPrefixes.at(collection.id).size() != 0);
            size_t random_index = RandomGenerator::GetInstance().GenerateInteger<size_t>(
              static_cast<size_t>(0), existingPrefixes.at(collection.id).size() - 1);
            std::string prefixKey =
              GetPrefixFromKey(existingPrefixes.at(collection.id).at(random_index));
            Logger::LogMessage(LOG_TRACE,
              ThreadTypeToString(threadWorker->type) +
                " thread: Perform unique index insertions with existing prefix key " + prefixKey +
                ".");
            testutil_assert(
              !PerformUniqueIndexInsertions(threadWorker, cursor, collection, prefixKey));
            testutil_check(cursor->reset(cursor.Get()));
            threadWorker->transaction.Rollback();
        }
    }

    void
    ReadOperation(ThreadWorker *threadWorker) override final
    {
        uint64_t keyCount = 0;
        int ret = 0;
        Logger::LogMessage(LOG_INFO,
          ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");
        /*
         * Each read thread will count the number of keys in each collection, and will double check
         * if the size of the table hasn't changed.
         */
        threadWorker->transaction.Begin();
        while (threadWorker->Running()) {
            for (int i = 0; i < threadWorker->database.GetCollectionCount(); i++) {
                Collection &collection = threadWorker->database.GetCollection(i);
                ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);
                ret = 0;
                while (ret == 0) {
                    ret = cursor->next(cursor.Get());
                    testutil_assertfmt(ret == 0 || ret == WT_NOTFOUND,
                      "Unexpected error %d returned from cursor->next()", ret);
                    if (ret == WT_NOTFOUND)
                        continue;
                    keyCount++;
                }
                threadWorker->Sleep();
            }
            if (threadWorker->Running()) {
                Logger::LogMessage(LOG_TRACE,
                  ThreadTypeToString(threadWorker->type) +
                    " thread: calculated count: " + std::to_string(keyCount) + " expected size: " +
                    std::to_string(existingPrefixes.size() * existingPrefixes.at(0).size()));
                testutil_assert(
                  keyCount == existingPrefixes.size() * existingPrefixes.at(0).size());
            }
            keyCount = 0;
        }
        threadWorker->transaction.Rollback();
    }
};
