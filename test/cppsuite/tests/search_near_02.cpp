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
 * In this test, we want to verify search_near with prefix enabled returns the correct key.
 * During the test duration:
 *  - N threads will keep inserting new random keys
 *  - M threads will execute search_near calls with prefix enabled using random prefixes as well.
 * Each search_near call with prefix enabled is verified using the default search_near.
 */
class SearchNear02 : public Test {
    public:
    SearchNear02(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    void
    Populate(Database &database, TimestampManager *, Configuration *config,
      OperationTracker *) override final
    {
        /*
         * The populate phase only creates empty collections. The number of collections is defined
         * in the configuration.
         */
        int64_t collection_count = config->GetInt(kCollectionCount);

        Logger::LogMessage(
          LOG_INFO, "Populate: " + std::to_string(collection_count) + " creating collections.");

        for (uint64_t i = 0; i < collection_count; ++i)
            database.AddCollection();

        Logger::LogMessage(LOG_INFO, "Populate: finished.");
    }

    void
    InsertOperation(thread_worker *threadWorker) override final
    {
        /* Each insert operation will insert new keys in the collections. */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        /* Helper struct which stores a pointer to a collection and a cursor associated with it. */
        struct collection_cursor {
            collection_cursor(Collection &collection, ScopedCursor &&cursor)
                : collection(collection), cursor(std::move(cursor))
            {
            }
            Collection &collection;
            ScopedCursor cursor;
        };

        /* Collection cursor vector. */
        std::vector<collection_cursor> ccv;
        int64_t collectionCount = threadWorker->db.GetCollectionCount();
        int64_t collectionsPerThread = collectionCount / threadWorker->thread_count;

        /* Must have unique collections for each thread. */
        testutil_assert(collectionCount % threadWorker->thread_count == 0);
        const uint64_t threadOffset = threadWorker->id * collectionsPerThread;
        for (uint64_t i = threadOffset;
             i < threadOffset + collectionsPerThread && threadWorker->running(); ++i) {
            Collection &collection = threadWorker->db.GetCollection(i);
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(collection.name);
            ccv.push_back({collection, std::move(cursor)});
        }

        const uint64_t kMaxRollbacks = 100;
        uint64_t counter = 0;
        uint32_t rollbackRetries = 0;

        while (threadWorker->running()) {

            auto &cc = ccv[counter];
            threadWorker->txn.Start();

            while (threadWorker->txn.Active() && threadWorker->running()) {

                /* Generate a random key/value pair. */
                std::string key =
                  RandomGenerator::GetInstance().GenerateRandomString(threadWorker->key_size);
                std::string value =
                  RandomGenerator::GetInstance().GenerateRandomString(threadWorker->value_size);

                /* Insert a key value pair. */
                if (threadWorker->insert(cc.cursor, cc.collection.id, key, value)) {
                    if (threadWorker->txn.CanCommit()) {
                        /* We are not checking the result of commit as it is not necessary. */
                        if (threadWorker->txn.Commit())
                            rollbackRetries = 0;
                        else
                            ++rollbackRetries;
                    }
                } else {
                    threadWorker->txn.Rollback();
                    ++rollbackRetries;
                }
                testutil_assert(rollbackRetries < kMaxRollbacks);

                /* Sleep the duration defined by the configuration. */
                threadWorker->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (threadWorker->txn.Active())
                threadWorker->txn.Rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cc.cursor->reset(cc.cursor.Get()));
            if (++counter == ccv.size())
                counter = 0;
            testutil_assert(counter < collectionsPerThread);
        }
    }

    void
    ReadOperation(thread_worker *threadWorker) override final
    {
        /*
         * Each read operation performs search_near calls with and without prefix enabled on random
         * collections. Each prefix is randomly generated. The result of the search_near call with
         * prefix enabled is then validated using the search_near call without prefix enabled.
         */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        std::map<uint64_t, ScopedCursor> cursors;

        while (threadWorker->running()) {
            /* Get a random collection to work on. */
            Collection &collection = threadWorker->db.GetRandomCollection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(collection.id) == cursors.end()) {
                cursors.emplace(collection.id,
                  std::move(threadWorker->session.OpenScopedCursor(collection.name)));
                auto &cursorPrefix = cursors[collection.id];
                /* The cached cursors have the prefix configuration enabled. */
                testutil_check(
                  cursorPrefix.Get()->reconfigure(cursorPrefix.Get(), "prefix_search=true"));
            }

            auto &cursorPrefix = cursors[collection.id];

            wt_timestamp_t timestamp = threadWorker->tsm->GetValidReadTimestamp();
            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            threadWorker->txn.Start("roundup_timestamps=(read=true),read_timestamp=" +
              threadWorker->tsm->DecimalToHex(timestamp));

            while (threadWorker->txn.Active() && threadWorker->running()) {
                /*
                 * Generate a random prefix. For this, we start by generating a random size and then
                 * its value.
                 */
                int64_t prefixSize = RandomGenerator::GetInstance().GenerateInteger(
                  static_cast<int64_t>(1), threadWorker->key_size);
                const std::string generatedPrefix =
                  RandomGenerator::GetInstance().GenerateRandomString(
                    prefixSize, charactersType::ALPHABET);

                /* Call search near with the prefix cursor. */
                int exactPrefix;
                cursorPrefix->set_key(cursorPrefix.Get(), generatedPrefix.c_str());
                int ret = cursorPrefix->search_near(cursorPrefix.Get(), &exactPrefix);
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);
                std::string keyPrefixString;
                if (ret == 0) {
                    const char *keyPrefix;
                    testutil_check(cursorPrefix->get_key(cursorPrefix.Get(), &keyPrefix));
                    keyPrefixString = keyPrefix;
                }

                /* Open a cursor with the default configuration on the selected collection. */
                ScopedCursor cursorDefault(threadWorker->session.OpenScopedCursor(collection.name));

                /* Verify the prefix search_near output using the default cursor. */
                validate_prefix_search_near(
                  ret, exactPrefix, keyPrefixString, cursorDefault, generatedPrefix);

                threadWorker->txn.IncrementOp();
                threadWorker->txn.TryRollback();
                threadWorker->sleep();
            }
            testutil_check(cursorPrefix->reset(cursorPrefix.Get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (threadWorker->txn.Active())
            threadWorker->txn.Rollback();
    }

    private:
    /* Validate prefix search_near call outputs using a cursor without prefix key enabled. */
    void
    validate_prefix_search_near(int retPrefix, int exactPrefix, const std::string &keyPrefix,
      ScopedCursor &cursorDefault, const std::string &prefix)
    {
        /* Call search near with the default cursor using the given prefix. */
        int exactDefault;
        cursorDefault->set_key(cursorDefault.Get(), prefix.c_str());
        int retDefault = cursorDefault->search_near(cursorDefault.Get(), &exactDefault);

        /*
         * It is not possible to have a prefix search near call successful and the default search
         * near call unsuccessful.
         */
        testutil_assert(retDefault == retPrefix || (retDefault == 0 && retPrefix == WT_NOTFOUND));

        /* We only have to perform validation when the default search near call is successful. */
        if (retDefault == 0) {
            /* Both calls are successful. */
            if (retPrefix == 0)
                validateSuccessfulPrefixCall(
                  exactPrefix, keyPrefix, cursorDefault, exactDefault, prefix);
            /* The prefix search near call failed. */
            else
                validateUnsuccessfulPrefixCalls(cursorDefault, prefix, exactDefault);
        }
    }

    /*
     * Validate a successful prefix enabled search near call using a successful default search near
     * call.
     * The exact value set by the prefix search near call has to be either 0 or 1. Indeed, it cannot
     * be -1 as the key needs to contain the prefix.
     * - If it is 0, both search near calls should return the same outputs and both cursors should
     * be positioned on the prefix we are looking for.
     * - If it is 1, it will depend on the exact value set by the default search near call which can
     * be -1 or 1. If it is -1, calling next on the default cursor should get us ti the key found by
     * the prefix search near call. If it is 1, it means both search near calls have found the same
     * key that is lexicographically greater than the prefix but still contains the prefix.
     */
    void
    validateSuccessfulPrefixCall(int exactPrefix, const std::string &keyPrefix,
      ScopedCursor &cursorDefault, int exactDefault, const std::string &prefix)
    {
        /*
         * The prefix search near call cannot retrieve a key with a smaller value than the prefix we
         * searched.
         */
        testutil_assert(exactPrefix >= 0);

        /* The key at the prefix cursor should contain the prefix. */
        testutil_assert(keyPrefix.substr(0, prefix.size()) == prefix);

        /* Retrieve the key the default cursor is pointing at. */
        const char *keyDefault;
        testutil_check(cursorDefault->get_key(cursorDefault.Get(), &keyDefault));
        std::string keyDefaultString = keyDefault;

        Logger::LogMessage(LOG_TRACE,
          "search_near (normal) exact " + std::to_string(exactDefault) + " key " + keyDefault);
        Logger::LogMessage(LOG_TRACE,
          "search_near (prefix) exact " + std::to_string(exactPrefix) + " key " + keyPrefix);

        /* Example: */
        /* keys: a, bb, bba. */
        /* Only bb is not visible. */
        /* Default search_near(bb) returns a, exact < 0. */
        /* Prefix search_near(bb) returns bba, exact > 0. */
        if (exactDefault < 0) {
            /* The key at the default cursor should not contain the prefix. */
            testutil_assert((keyDefaultString.substr(0, prefix.size()) != prefix));

            /*
             * The prefix cursor should be positioned at a key lexicographically greater than the
             * prefix.
             */
            testutil_assert(exactPrefix > 0);

            /*
             * The next key of the default cursor should be equal to the key pointed by the prefix
             * cursor.
             */
            testutil_assert(cursorDefault->next(cursorDefault.Get()) == 0);
            const char *k;
            testutil_check(cursorDefault->get_key(cursorDefault.Get(), &k));
            testutil_assert(k == keyPrefix);
        }
        /* Example: */
        /* keys: a, bb, bba */
        /* Case 1: all keys are visible. */
        /* Default search_near(bb) returns bb, exact = 0 */
        /* Prefix search_near(bb) returns bb, exact = 0 */
        /* Case 2: only bb is not visible. */
        /* Default search_near(bb) returns bba, exact > 0. */
        /* Prefix search_near(bb) returns bba, exact > 0. */
        else {
            /* Both cursors should be pointing at the same key. */
            testutil_assert(exactPrefix == exactDefault);
            testutil_assert(keyDefaultString == keyPrefix);
            /* Both cursors should have found the exact key. */
            if (exactDefault == 0)
                testutil_assert(keyDefaultString == prefix);
            /* Both cursors have found a key that is lexicographically greater than the prefix. */
            else
                testutil_assert(keyDefaultString != prefix);
        }
    }

    /*
     * Validate that no keys with the prefix used for the search have been found.
     * To validate this, we can use the exact value set by the default search near. Since the prefix
     * search near failed, the exact value set by the default search near call has to be either -1
     * or 1:
     * - If it is -1, we need to check the next key, if it exists, is lexicographically greater than
     * the prefix we looked for.
     * - If it is 1, we need to check the previous keys, if it exists, if lexicographically smaller
     * than the prefix we looked for.
     */
    void
    validateUnsuccessfulPrefixCalls(
      ScopedCursor &cursorDefault, const std::string &prefix, int exactDefault)
    {
        int ret;
        const char *k;
        std::string kString;

        /*
         * The exact value from the default search near call cannot be 0, otherwise the prefix
         * search near should be successful too.
         */
        testutil_assert(exactDefault != 0);

        /* Retrieve the key at the default cursor. */
        const char *keyDefault;
        testutil_check(cursorDefault->get_key(cursorDefault.Get(), &keyDefault));
        std::string keyDefaultString = keyDefault;

        /* The key at the default cursor should not contain the prefix. */
        testutil_assert(keyDefaultString.substr(0, prefix.size()) != prefix);

        /* Example: */
        /* keys: a, bb, bbb. */
        /* All keys are visible. */
        /* Default search_near(bba) returns bb, exact < 0. */
        /* Prefix search_near(bba) returns WT_NOTFOUND. */
        if (exactDefault < 0) {
            /*
             * The current key of the default cursor should be lexicographically smaller than the
             * prefix.
             */
            testutil_assert(std::lexicographical_compare(
              keyDefaultString.begin(), keyDefaultString.end(), prefix.begin(), prefix.end()));

            /*
             * The next key of the default cursor should be lexicographically greater than the
             * prefix if it exists.
             */
            ret = cursorDefault->next(cursorDefault.Get());
            if (ret == 0) {
                testutil_check(cursorDefault->get_key(cursorDefault.Get(), &k));
                kString = k;
                testutil_assert(!std::lexicographical_compare(
                  kString.begin(), kString.end(), prefix.begin(), prefix.end()));
            } else {
                /* End of the table. */
                testutil_assert(ret == WT_NOTFOUND);
            }
        }
        /* Example: */
        /* keys: a, bb, bbb. */
        /* All keys are visible. */
        /* Default search_near(bba) returns bbb, exact > 0. */
        /* Prefix search_near(bba) returns WT_NOTFOUND. */
        else {
            /*
             * The current key of the default cursor should be lexicographically greater than the
             * prefix.
             */
            testutil_assert(!std::lexicographical_compare(
              keyDefaultString.begin(), keyDefaultString.end(), prefix.begin(), prefix.end()));

            /*
             * The next key of the default cursor should be lexicographically smaller than the
             * prefix if it exists.
             */
            ret = cursorDefault->prev(cursorDefault.Get());
            if (ret == 0) {
                testutil_check(cursorDefault->get_key(cursorDefault.Get(), &k));
                kString = k;
                testutil_assert(std::lexicographical_compare(
                  kString.begin(), kString.end(), prefix.begin(), prefix.end()));
            } else {
                /* End of the table. */
                testutil_assert(ret == WT_NOTFOUND);
            }
        }
    }
};
