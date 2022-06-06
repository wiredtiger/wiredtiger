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
 * In this test, we want to verify the usage of the cursor bound API and check that the cursor
 * returns the correct key when bounds are set.
 * During the test duration:
 *  - M threads will keep inserting new random keys.
 *  - N threads will execute search_near calls with random bounds set. Each search_near
 * call with bounds set is verified using the standard cursor's search and next/prev calls.
 *  - O threads will continuously remove random keys.
 *  - P threads will continuously update random keys.
 *  - Q threads will utilize the custom operation and will execute next() and prev() calls with
 * random bounds set. Both next() and prev() calls with bounds set is verified against the
 * default cursor next() and prev() calls.
 */
class CursorBound01 : public Test {
    /* Class helper to represent the lower and uppers bounds for the range cursor. */
    class Bound {
        public:
        Bound() = default;
        Bound(uint64_t keySizeMax, bool lowerBound, const std::string& key)
            : _lowerBound(lowerBound), _key(key)
        {
            // FIXME: Use random strings, once bounds are implemented properly.
            // auto key_size =
            //   RandomGenerator::GetInstance().GenerateInteger(static_cast<uint64_t>(1),
            //   keySizeMax);
            // auto random_key = RandomGenerator::GetInstance().GenerateRandomString(
            //   key_size, charactersType::ALPHABET);
            // _key = random_key;
            _inclusive = RandomGenerator::GetInstance().GenerateInteger(0, 1);
        }

        std::string
        GetConfig() const
        {
            return "bound=" + std::string(_lowerBound ? "lower" : "upper") +
              ",inclusive=" + std::string(_inclusive ? "true" : "false");
        }

        std::string
        GetKey() const
        {
            return _key;
        }

        bool
        GetInclusive() const
        {
            return _inclusive;
        }

        private:
        std::string _key;
        bool _inclusive;
        bool _lowerBound;
    };

    private:
    bool _reverseCollatorEnabled = false;
    const uint64_t kMaxRollbacks = 100;
    enum bounds { NO_BOUNDS, LOWER_BOUND_SET, UPPER_BOUND_SET, ALL_BOUNDS_SET };

    public:
    CursorBound01(const test_args &args) : Test(args)
    {
        /* Track reverse_collator value as it is required for the custom comparator. */
        _reverseCollatorEnabled = _config->GetBool(kReverseCollator);
        InitOperationTracker();
    }

    bool
    CustomLexicoGraphicalCompare(
      const std::string &firstKey, const std::string &secondKey, bool inclusive)
    {
        if (_reverseCollatorEnabled)
            if (!inclusive)
                return firstKey.compare(secondKey) > 0;
            else
                return firstKey.compare(secondKey) >= 0;
        else if (!inclusive)
            return firstKey.compare(secondKey) < 0;
        else
            return firstKey.compare(secondKey) <= 0;
    }

    /*
     * Helper function which traverses the tree, given the range cursor and normal cursor. The next
     * variable decides where we traverse forwards or backwards in the tree. Also perform lower
     * bound and upper bound checks while walking the tree.
     */
    void
    CursorTraversal(ScopedCursor &rangeCursor, ScopedCursor &normalCursor, const Bound &lowerBound,
      const Bound &upperBound, bool next)
    {
        int exact, normalRet, rangeRet;
        exact = normalRet = rangeRet = 0;

        auto lowerKey = lowerBound.GetKey();
        auto upperKey = upperBound.GetKey();
        if (next) {
            rangeRet = rangeCursor->next(rangeCursor.Get());
            /*
             * If the key exists, position the cursor to the lower key using search near otherwise
             * use prev().
             */
            if (!lowerKey.empty()) {
                normalCursor->set_key(normalCursor.Get(), lowerKey.c_str());
                normalRet = normalCursor->search_near(normalCursor.Get(), &exact);
                if (normalRet == WT_NOTFOUND)
                    return;
                if (exact < 0)
                    normalRet = normalCursor->next(normalCursor.Get());
            } else
                normalRet = normalCursor->next(normalCursor.Get());
        } else {
            rangeRet = rangeCursor->prev(rangeCursor.Get());
            /*
             * If the key exists, position the cursor to the upper key using search near otherwise
             * use next().
             */
            if (!upperKey.empty()) {
                normalCursor->set_key(normalCursor.Get(), upperKey.c_str());
                normalRet = normalCursor->search_near(normalCursor.Get(), &exact);
                if (normalRet == WT_NOTFOUND)
                    return;
                if (exact > 0)
                    normalRet = normalCursor->prev(normalCursor.Get());
            } else
                normalRet = normalCursor->prev(normalCursor.Get());
        }

        if (normalRet == WT_NOTFOUND)
            return;
        testutil_assert(rangeRet == 0 && normalRet == 0);

        /* Retrieve the key the cursor is pointing at. */
        const char *normalKey, *rangeKey;
        testutil_check(normalCursor->get_key(normalCursor.Get(), &normalKey));
        testutil_check(rangeCursor->get_key(rangeCursor.Get(), &rangeKey));
        testutil_assert(std::string(normalKey).compare(rangeKey) == 0);
        while (true) {
            if (next) {
                normalRet = normalCursor->next(normalCursor.Get());
                rangeRet = rangeCursor->next(rangeCursor.Get());
            } else {
                normalRet = normalCursor->prev(normalCursor.Get());
                rangeRet = rangeCursor->prev(rangeCursor.Get());
            }
            testutil_assert(normalRet == 0 || normalRet == WT_NOTFOUND);
            testutil_assert(rangeRet == 0 || rangeRet == WT_NOTFOUND);

            /* Early exit if we have reached the end of the table. */
            if (rangeRet == WT_NOTFOUND && normalRet == WT_NOTFOUND)
                break;
            /* It is possible that we have reached the end of the bounded range. */
            else if (rangeRet == WT_NOTFOUND && normalRet == 0) {
                testutil_check(normalCursor->get_key(normalCursor.Get(), &normalKey));
                /*  Make sure that normal cursor returns a key that is outside of the range. */
                if (next) {
                    testutil_assert(!upperKey.empty());
                    testutil_assert(!CustomLexicoGraphicalCompare(normalKey, upperKey, true));
                } else {
                    testutil_assert(!lowerKey.empty());
                    testutil_assert(CustomLexicoGraphicalCompare(normalKey, lowerKey, false));
                }
                break;
            }

            if (next && !upperKey.empty())
                testutil_assert(
                  CustomLexicoGraphicalCompare(rangeKey, upperKey, upperBound.GetInclusive()));
            else if (!next && !lowerKey.empty())
                testutil_assert(
                  CustomLexicoGraphicalCompare(lowerKey, rangeKey, lowerBound.GetInclusive()));
            /* Make sure that records match between both cursors. */
            testutil_check(normalCursor->get_key(normalCursor.Get(), &normalKey));
            testutil_check(rangeCursor->get_key(rangeCursor.Get(), &rangeKey));
            testutil_assert(std::string(normalKey).compare(rangeKey) == 0);
        }
    }

    /*
     * Use the random generator either set no bounds, only lower bounds, only upper bounds or both
     * bounds on the range cursor. The lower and upper bounds are randomly generated strings and the
     * inclusive configuration is also randomly set as well.
     */
    std::pair<Bound, Bound>
    SetRandomBounds(thread_worker *threadWorker, ScopedCursor &rangeCursor)
    {
        int ret;
        Bound lowerBound, upperBound;

        auto setRandomBounds = RandomGenerator::GetInstance().GenerateInteger(0, 3);
        if (setRandomBounds == NO_BOUNDS)
            rangeCursor->bound(rangeCursor.Get(), "action=clear");

        if (setRandomBounds == LOWER_BOUND_SET || setRandomBounds == ALL_BOUNDS_SET) {
            /* Reverse case. */
            if (_reverseCollatorEnabled)
                lowerBound =
                  Bound(threadWorker->key_size, true, std::string(threadWorker->key_size, 'z'));
            /* Normal case. */
            else
                lowerBound = Bound(threadWorker->key_size, true, "0");
            rangeCursor->set_key(rangeCursor.Get(), lowerBound.GetKey().c_str());
            ret = rangeCursor->bound(rangeCursor.Get(), lowerBound.GetConfig().c_str());
            testutil_assert(ret == 0 || ret == EINVAL);
        }

        if (setRandomBounds == UPPER_BOUND_SET || setRandomBounds == ALL_BOUNDS_SET) {
            /* Reverse case. */
            if (_reverseCollatorEnabled)
                upperBound = Bound(threadWorker->key_size, false, "0");
            /* Normal case. */
            else
                upperBound =
                  Bound(threadWorker->key_size, false, std::string(threadWorker->key_size, 'z'));
            rangeCursor->set_key(rangeCursor.Get(), upperBound.GetKey().c_str());
            ret = rangeCursor->bound(rangeCursor.Get(), upperBound.GetConfig().c_str());
            testutil_assert(ret == 0 || ret == EINVAL);
        }

        return std::make_pair(lowerBound, upperBound);
    }

    /*
     * Validate the bound search_near call. There are three scenarios that needs to be validated
     * differently.
     *  Scenario 1: Range cursor has returned WT_NOTFOUND, this indicates that no records exist in
     * the bounded range. Validate this through traversing all records within the range on a normal
     * cursor.
     *  Scenario 2: Range cursor has returned a key and the search key is outside the range bounds.
     * Validate that the returned key is either the first or last record in the bounds.
     *  Scenario 3: Range cursor has returned a key and the search key is inside the range bounds.
     * Validate that the returned key is visible and that it is indeed the closest key that range
     * cursor could find.
     */
    void
    ValidateBoundSearchNear(int rangeRet, int rangeExact, ScopedCursor &rangeCursor,
      ScopedCursor &normalCursor, const std::string &search_key, const Bound &lowerBound,
      const Bound &upperBound)
    {
        /* Range cursor has successfully returned with a key. */
        if (rangeRet == 0) {
            auto lowerKey = lowerBound.GetKey();
            auto upperKey = upperBound.GetKey();
            auto lowerInclusive = lowerBound.GetInclusive();
            auto upperInclusive = upperBound.GetInclusive();

            const char *key;
            testutil_check(rangeCursor->get_key(rangeCursor.Get(), &key));
            Logger::LogMessage(LOG_TRACE,
              "bounded search_near found key: " + std::string(key) +
                " with lower bound: " + lowerKey + " upper bound: " + upperKey);

            /* Assert that the range cursor has returned a key inside the bounded range. */
            auto aboveLowerKey =
              lowerKey.empty() || CustomLexicoGraphicalCompare(lowerKey, key, lowerInclusive);
            auto belowUpperKey =
              upperKey.empty() || CustomLexicoGraphicalCompare(key, upperKey, upperInclusive);
            testutil_assert(aboveLowerKey && belowUpperKey);

            /* Decide whether the search key is inside or outside the bounded range. */
            aboveLowerKey = lowerKey.empty() ||
              CustomLexicoGraphicalCompare(lowerKey, search_key, lowerInclusive);
            belowUpperKey = upperKey.empty() ||
              CustomLexicoGraphicalCompare(search_key, upperKey, upperInclusive);
            auto search_key_inside_range = aboveLowerKey && belowUpperKey;

            normalCursor->set_key(normalCursor.Get(), key);
            /* Position the normal cursor on the found key from range cursor. */
            testutil_check(normalCursor->search(normalCursor.Get()));

            /*
             * Call different validation methods depending on whether the search key is inside or
             * outside the range.
             */
            if (search_key_inside_range)
                ValidateSuccessfulSearchNearInsideRange(normalCursor, rangeExact, search_key);
            else {
                testutil_assert(rangeExact != 0);
                validateSuccessfulSearchNearOutsideRange(
                  normalCursor, lowerBound, upperBound, aboveLowerKey);
            }
            /* Range cursor has not found anything within the set bounds. */
        } else
            ValidateSearchNearNotFound(normalCursor, lowerBound, upperBound);
    }

    /*
     * Validate that if the search key is inside the bounded range, that the range cursor has
     * returned a record that is visible and is a viable record that is closest to the search key.
     * We can use exact to perform this validation.
     */
    void
    ValidateSuccessfulSearchNearInsideRange(
      ScopedCursor &normalCursor, int rangeExact, const std::string &search_key)
    {
        int ret = 0;
        /* Retrieve the key the normal cursor is pointing at. */
        const char *key;
        testutil_check(normalCursor->get_key(normalCursor.Get(), &key));
        Logger::LogMessage(LOG_TRACE,
          "bounded search_near validating correct returned key with search key inside range as: " +
            search_key + " and exact: " + std::to_string(rangeExact));
        /* When exact = 0, the returned key should be equal to the search key. */
        if (rangeExact == 0) {
            testutil_assert(std::string(key).compare(search_key) == 0);
        } else if (rangeExact > 0) {
            /*
             * When exact > 0, the returned key should be greater than the search key and performing
             * a prev() should be less than the search key.
             */
            testutil_assert(!CustomLexicoGraphicalCompare(key, search_key, true));

            /* Check that the previous key is less than the search key. */
            ret = normalCursor->prev(normalCursor.Get());
            testutil_assert(ret == WT_NOTFOUND || ret == 0);
            if (ret == WT_NOTFOUND)
                return;
            testutil_check(normalCursor->get_key(normalCursor.Get(), &key));
            testutil_assert(CustomLexicoGraphicalCompare(key, search_key, false));
            /*
             * When exact < 0, the returned key should be less than the search key and performing a
             * next() should be greater than the search key.
             */
        } else if (rangeExact < 0) {
            testutil_assert(CustomLexicoGraphicalCompare(key, search_key, false));

            /* Check that the next key is greater than the search key. */
            ret = normalCursor->next(normalCursor.Get());
            testutil_assert(ret == WT_NOTFOUND || ret == 0);
            if (ret == WT_NOTFOUND)
                return;
            testutil_check(normalCursor->get_key(normalCursor.Get(), &key));
            testutil_assert(!CustomLexicoGraphicalCompare(key, search_key, true));
        }
    }

    /*
     * Validate that if the search key is outside the bounded range, that the range cursor has
     * returned a record that is either the first or last entry within the range. Do this through
     * checking if the position of the search key is greater than the range or smaller than the
     * range. Further perform a next or prev call on the normal cursor and we expect that the key is
     * outside of the range.
     */
    void
    validateSuccessfulSearchNearOutsideRange(ScopedCursor &normalCursor, const Bound &lowerBound,
      const Bound &upperBound, bool largerSearchKey)
    {
        int ret = largerSearchKey ? normalCursor->next(normalCursor.Get()) :
                                    normalCursor->prev(normalCursor.Get());
        auto lowerKey = lowerBound.GetKey();
        auto upperKey = upperBound.GetKey();
        if (ret == WT_NOTFOUND)
            return;
        testutil_assert(ret == 0);

        const char *key;
        testutil_check(normalCursor->get_key(normalCursor.Get(), &key));
        /*
         * Assert that the next() or prev() call has placed the normal cursor outside of the bounded
         * range.
         */
        auto aboveLowerKey = lowerKey.empty() ||
          CustomLexicoGraphicalCompare(lowerKey, key, lowerBound.GetInclusive());
        auto belowUpperKey = upperKey.empty() ||
          CustomLexicoGraphicalCompare(key, upperKey, upperBound.GetInclusive());
        testutil_assert(!(aboveLowerKey && belowUpperKey));
    }

    /*
     * Validate that the normal cursor is positioned at a key that is outside of the bounded range,
     * and that no visible keys exist in the bounded range.
     */
    void
    ValidateSearchNearNotFound(
      ScopedCursor &normalCursor, const Bound &lowerBound, const Bound &upperBound)
    {
        int ret, exact;
        auto lowerKey = lowerBound.GetKey();
        auto upperKey = upperBound.GetKey();
        Logger::LogMessage(LOG_TRACE,
          "bounded search_near found WT_NOTFOUND on lower bound: " + lowerKey + " upper bound: " +
            upperKey + " traversing range to validate that there are no keys within range.");
        if (!lowerKey.empty()) {
            normalCursor->set_key(normalCursor.Get(), lowerKey.c_str());
            ret = normalCursor->search_near(normalCursor.Get(), &exact);
        } else
            ret = normalCursor->next(normalCursor.Get());

        testutil_assert(ret == 0 || ret == WT_NOTFOUND);

        /*
         * If search near has positioned the cursor before the lower key, perform a next() to to
         * place the cursor in the first record in the range.
         */
        if (exact < 0)
            ret = normalCursor->next(normalCursor.Get());

        /*
         * Validate that there are no keys in the bounded range that the range cursor could have
         * returned.
         */
        const char *key;
        while (ret != WT_NOTFOUND) {
            testutil_assert(ret == 0);

            testutil_check(normalCursor->get_key(normalCursor.Get(), &key));
            /* Asserted that the traversed key is not within the range bound. */
            auto aboveLowerKey = lowerKey.empty() ||
              CustomLexicoGraphicalCompare(lowerKey, key, lowerBound.GetInclusive());
            auto belowUpperKey = upperKey.empty() ||
              CustomLexicoGraphicalCompare(key, upperKey, upperBound.GetInclusive());
            testutil_assert(!(aboveLowerKey && belowUpperKey));

            /*
             * Optimization to early exit, if we have traversed past all possible records in the
             * range bound.
             */
            if (!belowUpperKey)
                break;

            ret = normalCursor->next(normalCursor.Get());
        }
    }

    void
    InsertOperation(thread_worker *threadWorker) override final
    {
        /* Each insert operation will insert new keys in the collections. */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        uint32_t rollback_retries = 0;
        while (threadWorker->running()) {

            Collection &coll = threadWorker->db.GetRandomCollection();
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
            threadWorker->txn.Start();

            while (threadWorker->txn.Active() && threadWorker->running()) {

                /* Generate a random key. */
                auto key =
                  RandomGenerator::GetInstance().GenerateRandomString(threadWorker->key_size);
                auto value =
                  RandomGenerator::GetInstance().GenerateRandomString(threadWorker->value_size);
                /* Insert a key/value pair. */
                if (threadWorker->insert(cursor, coll.id, key, value)) {
                    if (threadWorker->txn.CanCommit()) {
                        /* We are not checking the result of commit as it is not necessary. */
                        if (threadWorker->txn.Commit())
                            rollback_retries = 0;
                        else
                            ++rollback_retries;
                    }
                } else {
                    threadWorker->txn.Rollback();
                    ++rollback_retries;
                }
                testutil_assert(rollback_retries < kMaxRollbacks);

                /* Sleep the duration defined by the configuration. */
                threadWorker->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (threadWorker->txn.Active())
                threadWorker->txn.Rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cursor->reset(cursor.Get()));
        }
    }

    void
    UpdateOperation(thread_worker *threadWorker) override final
    {
        /* Each update operation will update existing keys in the collections. */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        uint32_t rollback_retries = 0;
        while (threadWorker->running()) {

            Collection &coll = threadWorker->db.GetRandomCollection();
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
            ScopedCursor rnd_cursor =
              threadWorker->session.OpenScopedCursor(coll.name, "next_random=true");
            threadWorker->txn.Start();

            while (threadWorker->txn.Active() && threadWorker->running()) {
                int ret = rnd_cursor->next(rnd_cursor.Get());

                /* It is possible not to find anything if the collection is empty. */
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);
                if (ret == WT_NOTFOUND) {
                    /*
                     * If we cannot find any record, finish the current transaction as we might be
                     * able to see new records after starting a new one.
                     */
                    WT_IGNORE_RET_BOOL(threadWorker->txn.Commit());
                    continue;
                }

                const char *key;
                testutil_check(rnd_cursor->get_key(rnd_cursor.Get(), &key));

                /* Update the found key with a randomized value. */
                auto value =
                  RandomGenerator::GetInstance().GenerateRandomString(threadWorker->value_size);
                if (threadWorker->update(cursor, coll.id, key, value)) {
                    if (threadWorker->txn.CanCommit()) {
                        /* We are not checking the result of commit as it is not necessary. */
                        if (threadWorker->txn.Commit())
                            rollback_retries = 0;
                        else
                            ++rollback_retries;
                    }
                } else {
                    threadWorker->txn.Rollback();
                    ++rollback_retries;
                }
                testutil_assert(rollback_retries < kMaxRollbacks);

                /* Sleep the duration defined by the configuration. */
                threadWorker->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (threadWorker->txn.Active())
                threadWorker->txn.Rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cursor->reset(cursor.Get()));
        }
    }

    void
    ReadOperation(thread_worker *threadWorker) override final
    {
        /*
         * Each read operation will perform search nears with a range bounded cursor and a normal
         * cursor without any bounds set. The normal cursor will be used to validate the results
         * from the range cursor.
         */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        Bound lowerBound, upperBound;
        std::map<uint64_t, ScopedCursor> cursors;

        while (threadWorker->running()) {
            /* Get a random collection to work on. */
            Collection &coll = threadWorker->db.GetRandomCollection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(
                  coll.id, std::move(threadWorker->session.OpenScopedCursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &rangeCursor = cursors[coll.id];
            auto boundedPair = SetRandomBounds(threadWorker, rangeCursor);
            /* Only update the bounds when the bounds have a key. */
            if (!boundedPair.first.GetKey().empty())
                lowerBound = boundedPair.first;
            if (!boundedPair.second.GetKey().empty())
                upperBound = boundedPair.second;

            /* Clear all bounds if both bounds don't have a key. */
            if (boundedPair.first.GetKey().empty() && boundedPair.second.GetKey().empty()) {
                lowerBound = boundedPair.first;
                upperBound = boundedPair.second;
            }

            ScopedCursor normalCursor = threadWorker->session.OpenScopedCursor(coll.name);
            wt_timestamp_t timestamp = threadWorker->tsm->GetValidReadTimestamp();
            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            threadWorker->txn.Start("roundup_timestamps=(read=true),read_timestamp=" +
              threadWorker->tsm->DecimalToHex(timestamp));

            while (threadWorker->txn.Active() && threadWorker->running()) {
                /* Generate a random string. */
                auto key_size = RandomGenerator::GetInstance().GenerateInteger(
                  static_cast<int64_t>(1), threadWorker->key_size);
                auto srch_key = RandomGenerator::GetInstance().GenerateRandomString(
                  key_size, charactersType::ALPHABET);

                int exact;
                rangeCursor->set_key(rangeCursor.Get(), srch_key.c_str());
                auto ret = rangeCursor->search_near(rangeCursor.Get(), &exact);
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);

                /* Verify the bound search_near result using the normal cursor. */
                ValidateBoundSearchNear(
                  ret, exact, rangeCursor, normalCursor, srch_key, lowerBound, upperBound);

                threadWorker->txn.IncrementOp();
                threadWorker->txn.TryRollback();
                threadWorker->sleep();
            }
            testutil_check(rangeCursor->reset(rangeCursor.Get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (threadWorker->txn.Active())
            threadWorker->txn.Rollback();
    }

    void
    CustomOperation(thread_worker *threadWorker) override final
    {
        /*
         * Each custom operation will use the range bounded cursor to traverse through existing keys
         * in the collection. The records will be validated against with the normal cursor to check
         * for any potential missing records.
         */
        Logger::LogMessage(LOG_INFO,
          type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
            "} commencing.");

        std::map<uint64_t, ScopedCursor> cursors;
        Bound lowerBound, upperBound;
        while (threadWorker->running()) {
            /* Get a random collection to work on. */
            Collection &coll = threadWorker->db.GetRandomCollection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(
                  coll.id, std::move(threadWorker->session.OpenScopedCursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &rangeCursor = cursors[coll.id];
            auto boundedPair = SetRandomBounds(threadWorker, rangeCursor);
            /* Only update the bounds when the bounds have a key. */
            if (!boundedPair.first.GetKey().empty())
                lowerBound = boundedPair.first;
            if (!boundedPair.second.GetKey().empty())
                upperBound = boundedPair.second;

            /* Clear all bounds if both bounds doesn't have a key. */
            if (boundedPair.first.GetKey().empty() && boundedPair.second.GetKey().empty()) {
                lowerBound = boundedPair.first;
                upperBound = boundedPair.second;
            }

            ScopedCursor normalCursor = threadWorker->session.OpenScopedCursor(coll.name);
            wt_timestamp_t timestamp = threadWorker->tsm->GetValidReadTimestamp();
            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            threadWorker->txn.Start("roundup_timestamps=(read=true),read_timestamp=" +
              threadWorker->tsm->DecimalToHex(timestamp));
            while (threadWorker->txn.Active() && threadWorker->running()) {

                CursorTraversal(rangeCursor, normalCursor, lowerBound, upperBound, true);
                CursorTraversal(rangeCursor, normalCursor, lowerBound, upperBound, false);
                threadWorker->txn.IncrementOp();
                threadWorker->txn.TryRollback();
                threadWorker->sleep();
            }
            testutil_check(rangeCursor->reset(rangeCursor.Get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (threadWorker->txn.Active())
            threadWorker->txn.Rollback();
    }
};
