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

/*
 * In this test, we want to verify the usage of the cursor bound API and check that the cursor
 * returns the correct key when bounds are set.
 * During the test duration:
 *  - M threads will keep inserting new random keys.
 *  - N threads will execute search_near calls with random bounds set. Each search_near
 * call with bounds set is verified against the default search_near.
 *  - O threads will continuously remove random keys.
 *  - P threads will continuously update random keys.
 *  - Q threads will utilize the custom operation and will execute next() or prev() calls with
 * bounds set using random bounds. Each next() or prev() with bounds set is verified against the
 * default cursor next() and prev() calls.
 *
 */
namespace test_harness {

class cursor_bound_01 : public test_harness::test {
    const uint64_t MAX_ROLLBACKS = 100;
    enum bounds { NO_BOUNDS, LOWER_BOUND_SET, UPPER_BOUND_SET, ALL_BOUNDS_SET };
    bool reverse_collator_enabled = false;

    public:
    cursor_bound_01(const test_harness::test_args &args) : test(args) {}

    bool
    custom_lexicographical_compare(
      const std::string &first_key, const std::string &second_key, bool inclusive)
    {
        if (this->reverse_collator_enabled)
            if (!inclusive)
                return first_key.compare(second_key) > 0;
            else
                return first_key.compare(second_key) >= 0;
        else if (!inclusive)
            return first_key.compare(second_key) < 0;
        else
            return first_key.compare(second_key) <= 0;
    }

    void
    cursor_traversal(scoped_cursor &range_cursor, scoped_cursor &normal_cursor,
      const std::pair<std::string, bool> &lower_bound_pair,
      const std::pair<std::string, bool> &upper_bound_pair, bool next)
    {
        int exact, normal_ret, range_ret;
        exact = normal_ret = range_ret = 0;
        if (next) {
            range_ret = range_cursor->next(range_cursor.get());
            /* If the key exists, position the cursor to the lower key using search near otherwise
             * use prev(). */
            if (lower_bound_pair.first.length() != 0) {
                normal_cursor->set_key(normal_cursor.get(), lower_bound_pair.first.c_str());
                normal_ret = normal_cursor->search_near(normal_cursor.get(), &exact);
                if (normal_ret == WT_NOTFOUND)
                    return;
                if (exact < 0)
                    normal_ret = normal_cursor->next(normal_cursor.get());
            } else
                normal_ret = normal_cursor->next(normal_cursor.get());
        } else {
            range_ret = range_cursor->prev(range_cursor.get());
            /* If the key exists, position the cursor to the upper key using search near otherwise
             * use next(). */
            if (upper_bound_pair.first.length() != 0) {
                normal_cursor->set_key(normal_cursor.get(), upper_bound_pair.first.c_str());
                normal_ret = normal_cursor->search_near(normal_cursor.get(), &exact);
                if (normal_ret == WT_NOTFOUND)
                    return;
                if (exact > 0)
                    normal_ret = normal_cursor->prev(normal_cursor.get());
            } else
                normal_ret = normal_cursor->prev(normal_cursor.get());
        }

        if (normal_ret == WT_NOTFOUND)
            return;

        testutil_assert(normal_ret == range_ret && (normal_ret == 0 || normal_ret == WT_NOTFOUND));

        /* Retrieve the key the cursor is pointing at. */
        const char *normal_key, *range_key;
        testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
        testutil_check(range_cursor->get_key(range_cursor.get(), &range_key));
        testutil_assert(strcmp(range_key, normal_key) == 0);
        while (true) {
            if (next) {
                normal_ret = normal_cursor->next(normal_cursor.get());
                range_ret = range_cursor->next(range_cursor.get());
            } else {
                normal_ret = normal_cursor->prev(normal_cursor.get());
                range_ret = range_cursor->prev(range_cursor.get());
            }
            testutil_assert(normal_ret == 0 || normal_ret == WT_NOTFOUND);
            testutil_assert(range_ret == 0 || range_ret == WT_NOTFOUND);

            /* Early exit if we have reached the end of the table. */
            if (range_ret == WT_NOTFOUND && normal_ret == WT_NOTFOUND)
                break;

            /* It is possible that we have reached the end of the bounded range. */
            else if (range_ret == WT_NOTFOUND && normal_ret == 0) {
                testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
                std::string normal_key_str = normal_key;
                /*  Make sure that normal cursor returns a key that is outside of the range. */
                if (next) {
                    testutil_assert(upper_bound_pair.first.length() != 0);
                    testutil_assert(custom_lexicographical_compare(
                                      normal_key_str, upper_bound_pair.first, true) == false);
                } else {
                    testutil_assert(lower_bound_pair.first.length() != 0);
                    testutil_assert(custom_lexicographical_compare(
                                      normal_key_str, lower_bound_pair.first, false) == true);
                }
                break;
            }

            std::string range_key_str = range_key;
            if (next && upper_bound_pair.first.length() != 0)
                testutil_assert(custom_lexicographical_compare(range_key_str,
                                  upper_bound_pair.first, upper_bound_pair.second) == true);
            else if (!next && lower_bound_pair.first.length() != 0)
                testutil_assert(custom_lexicographical_compare(lower_bound_pair.first,
                                  range_key_str, lower_bound_pair.second) == true);
            /* Make sure that records match between both cursors. */
            testutil_check(normal_cursor->get_key(normal_cursor.get(), &normal_key));
            testutil_check(range_cursor->get_key(range_cursor.get(), &range_key));
            testutil_assert(strcmp(range_key, normal_key) == 0);
        }
    }

    /*
     * Use the random generator either set no bounds, only lower bounds, only upper bounds or both
     * bounds on the range cursor. The lower and upper bounds are randomly generated strings as
     * well. The inclusive configuration is also randomly set as well.
     */
    std::pair<std::pair<std::string, bool>, std::pair<std::string, bool>>
    set_random_bounds(test_harness::thread_context *tc, scoped_cursor &range_cursor)
    {
        bool set_lower_inclusive, set_upper_inclusive;
        int set_random_bounds;
        int64_t key_size;
        std::string lower_key, upper_key;

        set_random_bounds = random_generator::instance().generate_integer(0, 3);
        if (set_random_bounds == LOWER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            set_lower_inclusive = random_generator::instance().generate_integer(0, 1);
            key_size =
              random_generator::instance().generate_integer(static_cast<int64_t>(1), tc->key_size);
            (void)key_size;
            lower_key = random_generator::instance().generate_random_string(
                key_size, characters_type::ALPHABET);
            // Reverse case
            lower_key = std::string(tc->key_size, 'z');
            // Normal case
            // lower_key = std::string("0");
            range_cursor->bound(range_cursor.get(), "bound=lower");
            range_cursor->set_key(range_cursor.get(), lower_key);
        }

        if (set_random_bounds == UPPER_BOUND_SET || set_random_bounds == ALL_BOUNDS_SET) {
            set_upper_inclusive = random_generator::instance().generate_integer(0, 1);
            key_size =
              random_generator::instance().generate_integer(static_cast<int64_t>(1), tc->key_size);
            (void)key_size;
            upper_key = random_generator::instance().generate_random_string(
                key_size, characters_type::ALPHABET);
            // Reverse case
            upper_key = std::string("0");
            // Normal case
            // upper_key = std::string(tc->key_size, 'z');
            range_cursor->bound(range_cursor.get(), "bound=upper");
            range_cursor->set_key(range_cursor.get(), upper_key);
        }

        return std::make_pair(std::make_pair(lower_key, set_lower_inclusive),
          std::make_pair(upper_key, set_upper_inclusive));
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
    validate_bound_search_near(int range_ret, int range_exact, scoped_cursor &range_cursor,
      scoped_cursor &normal_cursor, const std::string &search_key,
      const std::pair<std::string, bool> &lower_bound_pair,
      const std::pair<std::string, bool> &upper_bound_pair)
    {
        bool above_lower_key, below_upper_key, search_key_inside_range;
        const char *key;

        /* Range cursor has successfully returned with a key. */
        if (range_ret == 0) {
            testutil_check(range_cursor->get_key(range_cursor.get(), &key));
            std::string key_str = key;

            logger::log_msg(LOG_TRACE,
              "bounded search_near found key: " + key_str + " with lower bound: " +
                lower_bound_pair.first + " upper bound: " + upper_bound_pair.first);
            /* Assert that the range cursor has returned a key inside the bounded range. */
            above_lower_key = lower_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                lower_bound_pair.first, key_str, lower_bound_pair.second);
            below_upper_key = upper_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                key_str, upper_bound_pair.first, upper_bound_pair.second);
            testutil_assert(above_lower_key && below_upper_key);

            /* Decide whether the search key is inside or outside the bounded range. */
            above_lower_key = lower_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                lower_bound_pair.first, search_key, lower_bound_pair.second);
            below_upper_key = upper_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                search_key, upper_bound_pair.first, upper_bound_pair.second);
            search_key_inside_range = above_lower_key && below_upper_key;

            normal_cursor->set_key(normal_cursor.get(), key);
            /* Position the normal cursor on the found key from range cursor. */
            testutil_check(normal_cursor->search(normal_cursor.get()));

            /* Call different validation methods depending on whether the search key is inside or
             * outside the range. */
            if (search_key_inside_range)
                validate_successful_search_near_inside_range(
                  normal_cursor, range_exact, search_key);
            else {
                testutil_assert(range_exact != 0);
                validate_successful_search_near_outside_range(
                  normal_cursor, lower_bound_pair, upper_bound_pair, above_lower_key);
            }
            /* Range cursor has not found anything within the set bounds. */
        } else
            validate_search_near_not_found(normal_cursor, lower_bound_pair, upper_bound_pair);
    }

    /*
     * Validate that if the search key is inside the bounded range, that the range cursor has
     * returned a record that is visible and is a viable record that is closest to the search key.
     * We can use exact to perform this validation.
     */
    void
    validate_successful_search_near_inside_range(
      scoped_cursor &normal_cursor, int range_exact, const std::string &search_key)
    {
        int ret = 0;
        /* Retrieve the key the normal cursor is pointing at. */
        const char *key;
        testutil_check(normal_cursor->get_key(normal_cursor.get(), &key));
        std::string key_str = key;

        logger::log_msg(LOG_TRACE,
          "bounded search_near validating correct returned key with search key inside range as: " +
            search_key + " and exact: " + std::to_string(range_exact));
        /* When exact = 0, the returned key should be equal to the search key. */
        if (range_exact == 0) {
            testutil_assert(key_str.compare(search_key) == 0);
            /* When exact > 0, the returned key should be greater than the search key and performing
             * a prev() should be less than the search key. */
        }
        if (range_exact > 0) {
            testutil_assert(custom_lexicographical_compare(key_str, search_key, true) == false);

            /* Check that the previous key is less than the search key. */
            ret = normal_cursor->prev(normal_cursor.get());
            testutil_assert(ret == WT_NOTFOUND || ret == 0);
            if (ret == WT_NOTFOUND)
                return;
            testutil_check(normal_cursor->get_key(normal_cursor.get(), &key));
            key_str = key;
            testutil_assert(custom_lexicographical_compare(key_str, search_key, false) == true);
            /* When exact < 0, the returned key should be less than the search key and performing a
             * next() should be greater than the search key. */
        } else if (range_exact < 0) {
            testutil_assert(custom_lexicographical_compare(key_str, search_key, false) == true);

            /* Check that the next key is greater than the search key. */
            ret = normal_cursor->next(normal_cursor.get());
            testutil_assert(ret == WT_NOTFOUND || ret == 0);
            if (ret == WT_NOTFOUND)
                return;
            testutil_check(normal_cursor->get_key(normal_cursor.get(), &key));
            key_str = key;
            testutil_assert(custom_lexicographical_compare(key_str, search_key, true) == false);
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
    validate_successful_search_near_outside_range(scoped_cursor &normal_cursor,
      const std::pair<std::string, bool> &lower_bound_pair,
      const std::pair<std::string, bool> &upper_bound_pair, bool larger_search_key)
    {

        int ret;
        bool above_lower_key, below_upper_key;
        char *key;

        ret = larger_search_key ? normal_cursor->next(normal_cursor.get()) :
                                  normal_cursor->prev(normal_cursor.get());
        if (ret == WT_NOTFOUND)
            return;
        testutil_assert(ret == 0);

        testutil_check(normal_cursor->get_key(normal_cursor.get(), &key));
        std::string key_str = key;
        /* Assert that the next() or prev() call has placed the normal cursor outside of the bounded
         * range. */
        above_lower_key = lower_bound_pair.first.length() == 0 ||
          custom_lexicographical_compare(lower_bound_pair.first, key_str, lower_bound_pair.second);
        below_upper_key = upper_bound_pair.first.length() == 0 ||
          custom_lexicographical_compare(key_str, upper_bound_pair.first, upper_bound_pair.second);
        testutil_assert(!(above_lower_key && below_upper_key));
    }

    /*
     * Validate that the normal cursor is positioned at a key that is outside of the bounded range,
     * and that no visible keys exist in the bounded range.
     */
    void
    validate_search_near_not_found(scoped_cursor &normal_cursor,
      const std::pair<std::string, bool> &lower_bound_pair,
      const std::pair<std::string, bool> &upper_bound_pair)
    {
        int ret, exact;
        bool above_lower_key, below_upper_key;
        const char *key;
        std::string key_str;

        logger::log_msg(LOG_TRACE,
          "bounded search_near found WT_NOTFOUND on lower bound: " + lower_bound_pair.first +
            " upper bound: " + upper_bound_pair.first +
            " traversing range to validate that there are no keys within range.");
        if (lower_bound_pair.first.length() != 0) {
            normal_cursor->set_key(normal_cursor.get(), lower_bound_pair.first.c_str());
            ret = normal_cursor->search_near(normal_cursor.get(), &exact);
        } else
            ret = normal_cursor->next(normal_cursor.get());

        testutil_assert(ret == 0 || ret == WT_NOTFOUND);

        if (exact < 0)
            ret = normal_cursor->next(normal_cursor.get());

        /* Validate that there are no keys in the bounded range that the range cursor could have
         * returned. */
        while (true) {
            if (ret == WT_NOTFOUND)
                break;
            testutil_assert(ret == 0);

            testutil_check(normal_cursor->get_key(normal_cursor.get(), &key));
            key_str = key;
            /* Asserted that the traversed key is not within the range bound. */
            above_lower_key = lower_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                lower_bound_pair.first, key_str, lower_bound_pair.second);
            below_upper_key = upper_bound_pair.first.length() == 0 ||
              custom_lexicographical_compare(
                key_str, upper_bound_pair.first, upper_bound_pair.second);
            testutil_assert(!(above_lower_key && below_upper_key));

            /*
             * Optimization to early exit, if we have traversed past all possible records in the
             * range bound.
             */
            if (!below_upper_key)
                break;

            ret = normal_cursor->next(normal_cursor.get());
        }
    }

    void
    populate(database &database, timestamp_manager *tsm, configuration *config,
      workload_tracking *tracking)
    {
        int64_t collection_count, key_count, key_size, thread_count, value_size;
        std::vector<thread_context *> workers;
        std::string collection_name;
        thread_manager tm;

        /* Validate our config. */
        collection_count = config->get_int(COLLECTION_COUNT);
        key_count = config->get_int(KEY_COUNT_PER_COLLECTION);
        value_size = config->get_int(VALUE_SIZE);
        thread_count = config->get_int(THREAD_COUNT);
        testutil_assert(thread_count == 0 || collection_count % thread_count == 0);
        testutil_assert(value_size > 0);
        key_size = config->get_int(KEY_SIZE);
        testutil_assert(key_size > 0);
        /* Keys must be unique. */
        testutil_assert(key_count <= pow(10, key_size));

        /* Track reverse_collator value as it is required for the custom comparator. */
        this->reverse_collator_enabled = _config->get_bool(REVERSE_COLLATOR);

        logger::log_msg(
          LOG_INFO, "Populate: creating " + std::to_string(collection_count) + " collections.");

        /* Create n collections as per the configuration. */
        for (int64_t i = 0; i < collection_count; ++i)
            /*
             * The database model will call into the API and create the collection, with its own
             * session.
             */
            database.add_collection(key_count);
        logger::log_msg(
          LOG_INFO, "Populate: " + std::to_string(collection_count) + " collections created.");

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
                std::string value =
                  random_generator::instance().generate_random_string(tc->value_size);
                /* Insert a key/value pair. */
                if (tc->insert(cursor, coll.id, key, value)) {
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
    update_operation(test_harness::thread_context *tc) override final
    {
        /* Each update operation will update existing keys in the collections. */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

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
                cursor->set_key(cursor.get(), random_key.c_str());
                int ret = cursor->search_near(cursor.get(), &exact);
                if (ret == WT_NOTFOUND)
                    continue;

                /* Retrieve the key the cursor is pointing at. */
                const char *key;
                testutil_check(cursor->get_key(cursor.get(), &key));

                /* Update the found key with a randomized value. */
                std::string value =
                  random_generator::instance().generate_random_string(tc->value_size);
                if (tc->update(cursor, coll.id, key, value)) {
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
        std::pair<std::string, bool> lower_bound_pair, upper_bound_pair;
        std::string srch_key;

        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(coll.id, std::move(tc->session.open_scoped_cursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &range_cursor = cursors[coll.id];
            range_cursor->bound(range_cursor.get(), "action=clear");
            auto bound_pair = set_random_bounds(tc, range_cursor);
            lower_bound_pair = bound_pair.first;
            upper_bound_pair = bound_pair.second;

            scoped_cursor normal_cursor = tc->session.open_scoped_cursor(coll.name);
            wt_timestamp_t ts = tc->tsm->get_random_ts();
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
                validate_bound_search_near(ret, exact, range_cursor, normal_cursor, srch_key,
                  lower_bound_pair, upper_bound_pair);

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

        std::map<uint64_t, scoped_cursor> cursors;
        std::pair<std::string, bool> lower_key, upper_key;
        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end())
                cursors.emplace(coll.id, std::move(tc->session.open_scoped_cursor(coll.name)));

            /* Set random bounds on cached range cursor. */
            auto &range_cursor = cursors[coll.id];
            range_cursor->bound(range_cursor.get(), "action=clear");
            auto bound_pair = set_random_bounds(tc, range_cursor);
            lower_key = bound_pair.first;
            upper_key = bound_pair.second;

            scoped_cursor normal_cursor = tc->session.open_scoped_cursor(coll.name);
            wt_timestamp_t ts = tc->tsm->get_random_ts();
            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * To tackle this issue, we round the timestamp to the oldest timestamp value.
             */
            tc->transaction.begin(
              "roundup_timestamps=(read=true),read_timestamp=" + tc->tsm->decimal_to_hex(ts));
            while (tc->transaction.active() && tc->running()) {

                cursor_traversal(range_cursor, normal_cursor, lower_key, upper_key, true);
                cursor_traversal(range_cursor, normal_cursor, lower_key, upper_key, false);
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
