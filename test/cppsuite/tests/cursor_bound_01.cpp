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
 *  - M threads will keep inserting new random keys
 *  - N threads will execute search_near calls with random bounds set. Each search_near
 * call with bounds set is verified against the default search_near.
 *  - O threads will continously remove random keys
 *  - P threads will continously update random keys
 *  - Q threads will utilise the custom operation and will execute next() or prev() calls with
 * bounds set using random bounds. Each next() or prev() with bounds set is verified against the
 * default cursor next() and prev() calls.
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

    std::pair<std::string, std::string> set_random_bounds(test_harness::thread_context *tc, scoped_cursor &range_cursor) {
        int set_random_bounds;
        int64_t key_size;
        std::string lower_key, upper_key;


        set_random_bounds = random_generator::instance().generate_integer(0, 3);
        /*
         * Generate a random prefix. For this, we start by generating a random size and then
         * its value.
         */
        if (set_random_bounds == LOWER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            key_size = random_generator::instance().generate_integer(
                static_cast<int64_t>(1), tc->key_size);
            lower_key = random_generator::instance().generate_random_string(
                key_size, characters_type::ALPHABET);
        }

        if (set_random_bounds == UPPER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            key_size = random_generator::instance().generate_integer(
                static_cast<int64_t>(1), tc->key_size);
            upper_key = random_generator::instance().generate_random_string(
                key_size, characters_type::ALPHABET);
        }
        /* Perform validation here. */
        //range_cursor->bound(range_cursor.get(), key.c_str());
        return std::make_pair(lower_key, upper_key);
    }

    /* Validate prefix search_near call outputs using a cursor without prefix key enabled. */
    void
    validate_bound_search_near(int ret_prefix, scoped_cursor &range_cursor, scoped_cursor &normal_cursor, const std::string &search_key, const std::string &lower_key, const std::string &upper_key)
    {
        /* Call search near with the default cursor using the given prefix. */
        int exact_default;

        
        normal_cursor->set_key(normal_cursor.get(), search_key.c_str());
        int ret_default = normal_cursor->search_near(normal_cursor.get(), &exact_default);

        /*
         * It is not possible to have a prefix search near call successful and the default search
         * near call unsuccessful.
         */
        testutil_assert(
          ret_default == ret_prefix || (ret_default == 0 && ret_prefix == WT_NOTFOUND));

        /* We only have to perform validation when the default search near call is successful. */
        if (ret_default == 0) {
            /* Both calls are successful. */
            // if (ret_prefix == 0)
            //     validate_successful_calls(
            //       ret_prefix, exact_prefix, key_prefix, normal_cursor, exact_default, prefix);
            // /* The prefix search near call failed. */
            // else
            //     validate_unsuccessful_prefix_call(normal_cursor, prefix, exact_default);
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

                /* Insert a key value pair. */
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
        /* Each read operation will read and validate existing keys in the collections. */
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
                key_size = random_generator::instance().generate_integer(
                  static_cast<int64_t>(1), tc->key_size);
                srch_key = random_generator::instance().generate_random_string(
                  key_size, characters_type::ALPHABET);

                int exact;
                range_cursor->set_key(range_cursor.get(), srch_key.c_str());
                ret = range_cursor->search_near(range_cursor.get(), &exact);
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);

                /* Verify the prefix search_near output using the default cursor. */
                validate_bound_search_near(ret, range_cursor, normal_cursor, srch_key, lower_key, upper_key);

                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            /* Each read operation will update existing keys in the collections. */
            logger::log_msg(
                LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} succeeded one op.");
            testutil_check(range_cursor->reset(range_cursor.get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }

    void
    custom_operation(test_harness::thread_context *tc) override final
    {
        /* Each read operation will update existing keys in the collections. */
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
                /* Call search near to position cursor. */  
                int exact;
                normal_cursor->set_key(normal_cursor.get(), "0");
                normal_ret = normal_cursor->search_near(normal_cursor.get(), &exact);
                if (normal_ret == WT_NOTFOUND) {
                    tc->transaction.rollback();
                    break;
                }

                if (exact < 0) {
                    testutil_assert(normal_cursor->next(normal_cursor.get()) == 0);
                }
                range_cursor->next(range_cursor.get());

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
                    if (range_ret == WT_NOTFOUND && normal_ret == WT_NOTFOUND)
                        break;
                    else if (range_ret == WT_NOTFOUND && normal_ret == 0) {
                        testutil_assert(upper_key.length() != 0);
                        testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                        testutil_assert(strcmp(normal_key, upper_key.c_str()) > 0);
                    }

                    testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                    testutil_check(range_cursor->get_key(range_cursor.get(), &range_key));
                    testutil_assert(strcmp(range_key, normal_key) == 0);

                }
                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            /* Each read operation will update existing keys in the collections. */
            logger::log_msg(
                LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} succeeded one op.");
            testutil_check(range_cursor->reset(range_cursor.get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }
};

} // namespace test_harness
