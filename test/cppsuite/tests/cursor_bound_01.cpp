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

#include "test_harness/test.h"
#include "test_harness/util/api_const.h"
#include "test_harness/workload/random_generator.h"

using namespace test_harness;

/*
 * In this test, we want to verify the usage of the cursor bound API and check that the cursor
 * returns the correct key when bounds are set.
 * During the test duration:
 *  - M threads will keep inserting new random keys.
 *  - N threads will execute search_near calls with random bounds set. Each search_near
 * call with bounds set is verified against the default search_near.
 *  - O threads will continously remove random keys.
 *  - P threads will continously update random keys.
 *  - Q threads will utilise the custom operation and will execute next() or prev() calls with
 * bounds set using random bounds. Each next() or prev() with bounds set is verified against the
 * default cursor next() and prev() calls.
 * 
 * TODO: add support for reverse collator. 
 */
namespace test_harness {

class cursor_bound_01 : public test_harness::test {
    const uint64_t MAX_ROLLBACKS = 100;
    enum bounds{NO_BOUNDS, LOWER_BOUND_SET, UPPER_BOUND_SET, ALL_BOUNDS_SET};

    public:
    cursor_bound_01(const test_harness::test_args &args) : test(args) {}

    /*
     * This function acts as a helper function for both the remove and update operation. The run_operation function
     * runs in a loop, performing:
     *  1. Generate random key of set key size
     *  2. Perform search near, to get a valid key from collection
     *  3. Grab the nearest key's record
     *  4. Perform either remove or update on the key.
     */
    void
    run_operation(test_harness::thread_context *tc, bool (test_harness::thread_context::*op_func)(scoped_cursor &, uint64_t, const std::string &))
    {
        std::string random_key;
        uint32_t rollback_retries = 0;

        while (tc->running()) {

            collection &coll = tc->db.get_random_collection();
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            tc->transaction.begin();

            while (tc->transaction.active() && tc->running()) {

                /* Generate a random key. */
                random_key = random_generator::instance().generate_random_string(tc->key_size);

                /* Call search near to position cursor. */
                int exact;
                cursor->set_key(cursor.get(), random_key);
                int ret = cursor->search_near(cursor.get(), &exact);
                if (ret == WT_NOTFOUND)
                    continue;

                /* Retrieve the key the cursor is pointing at. */
                const char *key;
                testutil_check(cursor->get_key(cursor.get(), &key));

                /* Perform the operation on the key. */
                if ((tc->*op_func)(cursor, coll.id, key)) {
                    if (tc->transaction.can_commit()) {
                        /* We are not checking the result of commit as it is not necessary. */
                        if (tc->transaction.commit())
                            rollback_retries = 0;
                        else
                            ++rollback_retries;
                    }
                } else {
                    tc->transaction.rollback();
                    ++rollback_retries;
                }
                testutil_assert(rollback_retries < MAX_ROLLBACKS);

                /* Sleep the duration defined by the configuration. */
                tc->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (tc->transaction.active())
                tc->transaction.rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cursor->reset(cursor.get()));
        }
    }

    /* 
     * Use the random generator either set no bounds, only lower bounds, only upper bounds or both
     * bounds on the range cursor. The lower and upper bounds are randomly generated strings as well.
     * The inclusive configuration is also randomly set as well.
     */
    std::pair<std::string, std::string> set_random_bounds(test_harness::thread_context *tc, scoped_cursor &range_cursor) {
        int set_random_bounds;
        int64_t key_size;
        std::string lower_key, upper_key;


        set_random_bounds = random_generator::instance().generate_integer(0, 3);
        if (set_random_bounds == LOWER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            key_size = random_generator::instance().generate_integer(
                static_cast<int64_t>(1), tc->key_size);
            (void) key_size;
            // lower_key = random_generator::instance().generate_random_string(
            //     key_size, characters_type::ALPHABET);
            lower_key = std::string("0");
        }

        if (set_random_bounds == UPPER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            key_size = random_generator::instance().generate_integer(
                static_cast<int64_t>(1), tc->key_size);
            (void) key_size;
            // upper_key = random_generator::instance().generate_random_string(
            //     key_size, characters_type::ALPHABET);
            upper_key = std::string(tc->key_size, 'z');
        }

        /* TODO: Check that upper bound should be greater than lower key bound. */
        //range_cursor->bound(range_cursor.get(), key.c_str());
        return std::make_pair(lower_key, upper_key);
    }

    /* Validate bound search_near call outputs using a cursor without bounds set. */
    void
    validate_bound_search_near(int range_ret, int range_exact, scoped_cursor &range_cursor,
        scoped_cursor &normal_cursor, const std::string &search_key, const std::string &lower_key, const std::string &upper_key)
    {
        /* Call search near with the default cursor using the given prefix. */
        int normal_exact;
        normal_cursor->set_key(normal_cursor.get(), search_key.c_str());
        int normal_ret = normal_cursor->search_near(normal_cursor.get(), &normal_exact);

        /*
         * It is not possible to have a prefix search near call successful and the default search
         * near call unsuccessful.
         */
        testutil_assert(
          normal_ret == range_ret || (normal_ret == 0 && range_ret == WT_NOTFOUND));

        /* We only have to perform validation when the default search near call is successful. */
        if (normal_ret == WT_NOTFOUND)
            return;

        /* If there are no bounds set, the return value of the range cursor needs to match the normal cursor. */
        if ((lower_key.length() == 0 && upper_key.length() == 0))
            testutil_assert(range_ret == normal_ret);

        /* Both calls are successful. */
        if (range_ret == 0)
            validate_successful_search_near_calls(
                normal_cursor, range_cursor, normal_exact, range_exact, search_key, lower_key, upper_key);
        /* The prefix search near call failed. */
        else
            validate_unsuccessful_search_near_call(normal_cursor, lower_key, upper_key);
    }

    /*
     * If both cursor has returned a valid key, there are two scenarios that need to be validated
     * differently:
     *  Scenario 1: normal cursor is positioned outside of the bounded range, then the range cursor
     * must be at the either at the first or last key of the bounded key range. Therefore we validate
     * this behaviour through using the normal cursor to traverse until the first or last key, and
     * then check that the keys are the same.
     *  Scenario 2: normal cursor is positioned inside the bounded range. In this case we check the
     * exact values of both the cursors. If the exact values are equal or zero, then check if the
     * keys match. Align the normal cursor to the match the same as the range cursor, and further
     * check if the keys match.
     */
    void
    validate_successful_search_near_calls(scoped_cursor &normal_cursor, scoped_cursor &range_cursor, int normal_exact, int range_exact ,const std::string &search_key, const std::string &lower_key, const std::string &upper_key)
    {
        int ret = 0;
        bool above_lower_key, below_upper_key, start_upper;

        /* Retrieve the key the default cursor is pointing at. */
        const char *key_default, *key_range;
        testutil_check(normal_cursor->get_key(normal_cursor.get(), &key_default));
        std::string key_default_str = key_default;

        testutil_check(range_cursor->get_key(range_cursor.get(), &key_range));
        std::string key_range_str = key_range;

        logger::log_msg(LOG_TRACE,
          "search_near (normal) exact " + std::to_string(normal_exact) + " key " + key_default);
        logger::log_msg(LOG_TRACE,
          "search_near (bound) exact " + std::to_string(range_exact) + " key " + key_range);

        /* Assert that the range cursor has returned a key inside the bounded range. */
        above_lower_key = lower_key.length() == 0 || std::lexicographical_compare(lower_key.begin(), lower_key.end(), key_range_str.begin(), key_range_str.end());
        below_upper_key = upper_key.length() == 0 || std::lexicographical_compare(key_range_str.begin(), key_range_str.end(), upper_key.begin(), upper_key.end());
        testutil_assert(above_lower_key && below_upper_key);

        /* Check whether the normal cursor has returned a key inside or outside the range. */
        above_lower_key = lower_key.length() == 0 || std::lexicographical_compare(lower_key.begin(), lower_key.end(), key_default_str.begin(), key_default_str.end());
        below_upper_key = upper_key.length() == 0 || std::lexicographical_compare(key_default_str.begin(), key_default_str.end(), upper_key.begin(), upper_key.end());
        /* 
         * Scenario: Normal cursor positioned outside bounded key range. Traverse until we find the
         * first or last key of the bounded range.
         */
        if (!(above_lower_key && below_upper_key)) {
            start_upper = above_lower_key;
            /* Traverse backwards or forwards depending on where the normal cursor was positioned. */
            while (true) {
                ret = start_upper ? ret = normal_cursor->prev(normal_cursor.get()) : normal_cursor->next(normal_cursor.get());
                if (ret == WT_NOTFOUND)
                    break;
                testutil_assert(ret == 0);

                testutil_check(normal_cursor->get_key(normal_cursor.get(), &key_default));
                key_default_str = key_default;
                
                above_lower_key = lower_key.length() == 0 || std::lexicographical_compare(lower_key.begin(), lower_key.end(), key_default_str.begin(), key_default_str.end());
                below_upper_key = upper_key.length() == 0 || std::lexicographical_compare(key_default_str.begin(), key_default_str.end(), upper_key.begin(), upper_key.end());
                /* Assert that the keys should match the first time we find a key within the bounded range. */
                if ((!start_upper && above_lower_key) || (start_upper && below_upper_key)) {
                    testutil_assert(strcmp(key_default, key_range) == 0);
                    break;
                }
            }
        /* 
         * Scenario: Normal cursor positioned inside the bounded key range. Check exact values and
         * align the default cursor with the range cursor if needed.
         */
        } else if (above_lower_key && below_upper_key) {
            if (normal_exact == 0 && range_exact == 0)
                /* Check that the keys match. */
                testutil_assert(strcmp(key_default, key_range) == 0);           
            else { 
                testutil_assert(range_exact != 0 && normal_exact != 0);
                /* Perform cursor position alignment. */
                if (normal_exact > 0 && range_exact < 0)
                    ret = normal_cursor->prev(normal_cursor.get());
                if (normal_exact < 0 && range_exact > 0)
                    ret = normal_cursor->next(normal_cursor.get());
                testutil_assert(ret == 0);

                /* Check that the keys match. */
                testutil_check(normal_cursor->get_key(normal_cursor.get(), &key_default));
                key_default_str = key_default;
                testutil_assert(strcmp(key_default, key_range) == 0);  
            }
        }
    }

    /*
     * Validate that the normal cursor is positioned at a key that is outside of the bounded range,
     * and that no visible keys exist in the bounded range. 
     */
    void
    validate_unsuccessful_search_near_call(scoped_cursor &normal_cursor, const std::string &lower_key, const std::string &upper_key)
    {
        int ret;
        bool above_lower_key, below_upper_key;

        /* Retrieve the key at the default cursor. */
        const char *key_default;
        testutil_check(normal_cursor->get_key(normal_cursor.get(), &key_default));
        std::string key_default_str = key_default;

        /* Check if the normal cursor's key is below the range or above the range bound. */
        bool start_upper = lower_key.length() == 0 || std::lexicographical_compare(lower_key.begin(), lower_key.end(), key_default_str.begin(), key_default_str.end());

        /* Here we validate that there are no keys in the bounded range that the range cursor could have returned. */
        while (true) {
            /* Traverse backwards or forwards depending on where the normal cursor is positioned. */
            ret = start_upper ? normal_cursor->prev(normal_cursor.get()) : normal_cursor->next(normal_cursor.get());
            if (ret == WT_NOTFOUND)
                break;
            testutil_assert(ret == 0);

            testutil_check(normal_cursor->get_key(normal_cursor.get(), &key_default));
            key_default_str = key_default;
            /* Asserted that the traveresed key is not within the range bound. */
            above_lower_key = lower_key.length() == 0 || std::lexicographical_compare(lower_key.begin(), lower_key.end(), key_default_str.begin(), key_default_str.end());
            below_upper_key = upper_key.length() == 0 || std::lexicographical_compare(key_default_str.begin(), key_default_str.end(), upper_key.begin(), upper_key.end());
            testutil_assert(!(above_lower_key && below_upper_key));
            
            /* Optimisation to early exit, if we have traversed past all possible records in the range bound. */
            if ((!start_upper && !below_upper_key) || (start_upper && !above_lower_key))
                break;
        }
    }

    void
    populate(test_harness::database &database, test_harness::timestamp_manager *,
      test_harness::configuration *config, test_harness::workload_tracking *) override final
    {
        /*
         * The populate phase only creates empty collections. The number of collections is defined
         * in the configuration.
         */
        int64_t collection_count = config->get_int(COLLECTION_COUNT);

        logger::log_msg(
          LOG_INFO, "Populate: " + std::to_string(collection_count) + " creating collections.");

        for (uint64_t i = 0; i < collection_count; ++i)
            database.add_collection();

        logger::log_msg(LOG_INFO, "Populate: finished.");
    }

    void
    insert_operation(test_harness::thread_context *tc) override final
    {
        /* Each insert operation will insert new keys in the collections. */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        std::string key;
        uint32_t rollback_retries = 0;

        while (tc->running()) {

            collection &coll = tc->db.get_random_collection();
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            tc->transaction.begin();

            while (tc->transaction.active() && tc->running()) {

                /* Generate a random key. */
                key = random_generator::instance().generate_random_string(tc->key_size);

                /* Insert a key/value pair. */
                if (tc->insert(cursor, coll.id, key)) {
                    if (tc->transaction.can_commit()) {
                        /* We are not checking the result of commit as it is not necessary. */
                        if (tc->transaction.commit())
                            rollback_retries = 0;
                        else
                            ++rollback_retries;
                    }
                } else {
                    tc->transaction.rollback();
                    ++rollback_retries;
                }
                testutil_assert(rollback_retries < MAX_ROLLBACKS);

                /* Sleep the duration defined by the configuration. */
                tc->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (tc->transaction.active())
                tc->transaction.rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cursor->reset(cursor.get()));
        }
    }

    void
    remove_operation(test_harness::thread_context *tc) override final
    {
        /* Each remove operation will remove existing keys in the collections. */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        run_operation(tc, &test_harness::thread_context::remove);
    }

    void
    update_operation(test_harness::thread_context *tc) override final
    {
        /* Each update operation will update existing keys in the collections. */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");
       
        run_operation(tc, &test_harness::thread_context::update);
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {
        /*
         * Each read operation will perform search nears with a range bounded cursor and a normal
         * cursor without any bounds set. The normal cursor will be used to validate the results
         * from the range cursor. validate existing keys in the collections.
         */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        int ret, key_size;
        std::map<uint64_t, scoped_cursor> cursors;
        std::string lower_key, upper_key, srch_key;

        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(coll.id, std::move(tc->session.open_scoped_cursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &range_cursor = cursors[coll.id];
            //range_cursor->bounds("action=clear");
            auto bound_pair = set_random_bounds(tc, range_cursor);
            lower_key = bound_pair.first;
            upper_key = bound_pair.second;

            scoped_cursor normal_cursor = tc->session.open_scoped_cursor(coll.name);
            /*
             * Pick a random timestamp between the oldest and now. Get rid of the last 32 bits as
             * they represent an increment for uniqueness.
             */
            wt_timestamp_t ts = random_generator::instance().generate_integer(
              (tc->tsm->get_oldest_ts() >> 32), (tc->tsm->get_next_ts() >> 32));
            /* Put back the timestamp in the correct format. */
            ts <<= 32;

            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            tc->transaction.begin(
              "roundup_timestamps=(read=true),read_timestamp=" + tc->tsm->decimal_to_hex(ts));

            while (tc->transaction.active() && tc->running()) {
                /* Generate a random string. */
                key_size = random_generator::instance().generate_integer(
                  static_cast<int64_t>(1), tc->key_size);
                srch_key = random_generator::instance().generate_random_string(
                  key_size, characters_type::ALPHABET);

                int exact;
                range_cursor->set_key(range_cursor.get(), srch_key.c_str());
                ret = range_cursor->search_near(range_cursor.get(), &exact);
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);

                /* Verify the bound search_near result using the normal cursor. */
                validate_bound_search_near(ret, exact, range_cursor, normal_cursor, srch_key, lower_key, upper_key);

                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            testutil_check(range_cursor->reset(range_cursor.get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }

    void
    custom_operation(test_harness::thread_context *tc) override final
    {
        /* 
         * Each custom operation will use the range bounded cursor to traverse through existing keys
         * in the collection. The records will be validated against with the normal cursor to check
         * for any potential missing records.
         */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        int normal_ret, range_ret;
        std::map<uint64_t, scoped_cursor> cursors;
        std::string lower_key, upper_key;
        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(coll.id, std::move(tc->session.open_scoped_cursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &range_cursor = cursors[coll.id];
            //range_cursor->bounds("action=clear");
            auto bound_pair = set_random_bounds(tc, range_cursor);
            lower_key = bound_pair.first;
            upper_key = bound_pair.second;

            scoped_cursor normal_cursor = tc->session.open_scoped_cursor(coll.name);

            /*
             * Pick a random timestamp between the oldest and now. Get rid of the last 32 bits as
             * they represent an increment for uniqueness.
             */
            wt_timestamp_t ts = random_generator::instance().generate_integer(
              (tc->tsm->get_oldest_ts() >> 32), (tc->tsm->get_next_ts() >> 32));
            /* Put back the timestamp in the correct format. */
            ts <<= 32;

            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            tc->transaction.begin(
              "roundup_timestamps=(read=true),read_timestamp=" + tc->tsm->decimal_to_hex(ts));
            while (tc->transaction.active() && tc->running()) {
                /* Call search near to position the normal cursor. */  
                int exact;
                normal_cursor->set_key(normal_cursor.get(), "0");
                normal_ret = normal_cursor->search_near(normal_cursor.get(), &exact);
                if (normal_ret == WT_NOTFOUND) {
                    tc->transaction.rollback();
                    break;
                }

                /* Search near can position before the lower key bound, perform a next call here */
                if (exact < 0) {
                    normal_ret = normal_cursor->next(normal_cursor.get());
                }
                range_ret = range_cursor->next(range_cursor.get());
                testutil_assert(normal_ret == range_ret && (normal_ret == 0 || normal_ret == WT_NOTFOUND));

                /* Retrieve the key the cursor is pointing at. */
                const char *normal_key, *range_key;
                testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                testutil_check(range_cursor->get_key(range_cursor.get(), &range_key));
                testutil_assert(strcmp(range_key, normal_key) == 0);
                while (true) {
                    normal_ret = normal_cursor->next(normal_cursor.get());
                    range_ret = range_cursor->next(range_cursor.get());
                    testutil_assert(normal_ret == 0 || normal_ret == WT_NOTFOUND);
                    testutil_assert(range_ret == 0 || range_ret == WT_NOTFOUND);

                    /* Early exit if we have reached the end of the table. */
                    if (range_ret == WT_NOTFOUND && normal_ret == WT_NOTFOUND)
                        break;
                    /* 
                     * It is possible that we have reached the end of the bounded range, make sure
                     * that normal cursor returns a key that is greater than the upper bound.
                     */
                    else if (range_ret == WT_NOTFOUND && normal_ret == 0) {
                        testutil_assert(upper_key.length() != 0);
                        testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                        testutil_assert(strcmp(normal_key, upper_key.c_str()) > 0);
                        break;
                    }

                    /* Make sure that records match between both cursors. */
                    testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                    testutil_check(range_cursor->get_key(range_cursor.get(), &range_key));
                    testutil_assert(strcmp(range_key, normal_key) == 0);

                }
                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            testutil_check(range_cursor->reset(range_cursor.get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }
};

} // namespace test_harness
