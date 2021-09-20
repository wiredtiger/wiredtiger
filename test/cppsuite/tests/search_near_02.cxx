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
 * In this test, we want to verify search_near with prefix enabled returns the correct key.
 * During the test duration:
 *  - N threads will keep inserting new random keys
 *  - M threads will execute search_near calls with prefix enabled using random prefixes as well.
 * Each search_near call with prefix enabled is verified using the default search_near.
 */
class search_near_02 : public test_harness::test {
    public:
    search_near_02(const test_harness::test_args &args) : test(args) {}

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

        for (int64_t i = 0; i < collection_count; ++i)
            database.add_collection();

        logger::log_msg(LOG_INFO, "Populate: finished.");
    }

    void
    insert_operation(test_harness::thread_context *tc) override final
    {
        /* Each insert operation will insert new keys in the collections. */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        /* Helper struct which stores a pointer to a collection and a cursor associated with it. */
        struct collection_cursor {
            collection_cursor(collection &coll, scoped_cursor &&cursor)
                : coll(coll), cursor(std::move(cursor))
            {
            }
            collection &coll;
            scoped_cursor cursor;
        };

        /* Collection cursor vector. */
        std::vector<collection_cursor> ccv;
        uint64_t collection_count = tc->db.get_collection_count();
        uint64_t collections_per_thread = collection_count / tc->thread_count;

        /* Must have unique collections for each thread. */
        testutil_assert(collection_count % tc->thread_count == 0);
        for (int i = tc->id * collections_per_thread;
             i < (tc->id * collections_per_thread) + collections_per_thread && tc->running(); ++i) {
            collection &coll = tc->db.get_collection(i);
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name.c_str());
            ccv.push_back({coll, std::move(cursor)});
        }

        std::string key;
        uint64_t counter = 0;

        while (tc->running()) {

            auto &cc = ccv[counter];
            tc->transaction.begin();

            while (tc->transaction.active() && tc->running()) {

                /* Generate a random key. */
                key = random_generator::instance().generate_random_string(tc->key_size);

                /* Insert a key value pair. */
                if (tc->insert(cc.cursor, cc.coll.id, key)) {
                    if (tc->transaction.can_commit())
                        /* We are not checking the result of commit as it is not necessary. */
                        tc->transaction.commit();
                } else {
                    tc->transaction.rollback();
                }

                /* Sleep the duration defined by the configuration. */
                tc->sleep();
            }

            /* Rollback any transaction that could not commit before the end of the test. */
            if (tc->transaction.active())
                tc->transaction.rollback();

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cc.cursor->reset(cc.cursor.get()));
            if (++counter == collections_per_thread)
                counter = 0;
            testutil_assert(counter < collections_per_thread);
        }
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {
        /*
         * Each read operation performs search_near calls with and without prefix enabled on random
         * collections. Each prefix is randomly generated. The result of the seach_near call with
         * prefix enabled is then validated using the search_near call without prefix enabled.
         */
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        std::map<uint64_t, scoped_cursor> cursors;
        // TODO - Should this be a fixed value ? Should it be from the configuration ?
        const uint64_t prefix_size = 3;
        const char *key_prefix, *key_default;
        int exact_prefix, exact_default;
        int ret;

        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();

            /* Find a cached cursor or create one if none exists. */
            if (cursors.find(coll.id) == cursors.end()) {
                cursors.emplace(
                  coll.id, std::move(tc->session.open_scoped_cursor(coll.name.c_str())));
                auto &cursor_prefix = cursors[coll.id];
                /* The cached cursors have the prefix configuration enabled. */
                testutil_check(
                  cursor_prefix.get()->reconfigure(cursor_prefix.get(), "prefix_key=true"));
            }

            auto &cursor_prefix = cursors[coll.id];

            /*
             * Select a random timestamp between the oldest and now and start the transaction at
             * that time.
             */
            wt_timestamp_t ts = random_generator::instance().generate_integer(
              tc->tsm->get_oldest_ts(), tc->tsm->get_next_ts());
            tc->transaction.begin("read_timestamp=" + tc->tsm->decimal_to_hex(ts));

            /*
             * The oldest timestamp might move ahead and the reading timestamp might become invalid.
             * If this happens, we can exit the current loop.
             */
            while (tc->transaction.active() && tc->running() && ts >= tc->tsm->get_oldest_ts()) {

                /*
                 * Generate a random prefix. For this, we start by generating a random size and then
                 * its value.
                 */
                const uint64_t prefix_size_tmp =
                  random_generator::instance().generate_integer(1UL, prefix_size);
                const std::string prefix = random_generator::instance().generate_random_string(
                  prefix_size_tmp, characters_type::ALPHABET);

                /* Open a cursor with the default configuration on the selected collection. */
                scoped_cursor cursor_default(tc->session.open_scoped_cursor(coll.name.c_str()));

                /* Call the search_near api using the two cursors. */
                search_near(cursor_default, cursor_prefix, prefix);

                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            testutil_check(cursor_prefix->reset(cursor_prefix.get()));
        }
        /* Roll back the last transaction if still active now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }

    private:
    /*
     * Perform search_near calls using a cursor with prefix key enabled and a cursor without it.
     * Validate the output of the former with the latter.
     */
    void
    search_near(
      scoped_cursor &cursor_default, scoped_cursor &cursor_prefix, const std::string &prefix)
    {
        /* Call search near with both cursors using the given prefix. */
        cursor_default->set_key(cursor_default.get(), prefix.c_str());
        cursor_prefix->set_key(cursor_prefix.get(), prefix.c_str());

        int exact_default, exact_prefix;
        int ret_default = cursor_default->search_near(cursor_default.get(), &exact_default);
        int ret_prefix = cursor_prefix->search_near(cursor_prefix.get(), &exact_prefix);

        /*
         * It is not possible to have a prefix search near call successful and the default search
         * near call unsuccessful.
         */
        testutil_assert(
          ret_default == ret_prefix || (ret_default == 0 && ret_prefix == WT_NOTFOUND));

        /*
         * We only have to perform validation when the default search near call is successful.
         */
        if (ret_default == 0) {
            /* Both calls are successful. */
            if (ret_prefix == 0)
                validate_successful_calls(
                  cursor_default, cursor_prefix, prefix, exact_default, exact_prefix);
            /* The prefix search near call failed.*/
            else
                validate_unsuccessful_prefix_call(
                  cursor_default, cursor_prefix, prefix, exact_default, exact_prefix);
        }
    }

    void
    validate_successful_calls(scoped_cursor &cursor_default, scoped_cursor &cursor_prefix,
      const std::string &prefix, int exact_default, int exact_prefix)
    {
        /*
         * The prefix search near call cannot retrieve a key with a smaller value than the prefix we
         * searched.
         */
        testutil_assert(exact_prefix == 0 || exact_prefix == 1);

        /* Retrieve the keys each cursor is pointing at. */
        const char *key_default;
        testutil_check(cursor_default->get_key(cursor_default.get(), &key_default));
        std::string key_default_str = key_default;

        const char *key_prefix;
        testutil_check(cursor_prefix->get_key(cursor_prefix.get(), &key_prefix));
        std::string key_prefix_str = key_prefix;

        logger::log_msg(LOG_TRACE,
          "search_near (normal) exact " + std::to_string(exact_default) + " key " + key_default);
        logger::log_msg(LOG_TRACE,
          "search_near (prefix) exact " + std::to_string(exact_prefix) + " key " + key_prefix);

        /* They key from the prefix search near needs to contain the prefix. */
        testutil_assert(key_prefix_str.substr(0, prefix.size()) == prefix);

        /*
         * If the exact value from the default search near call is -1, the key found by the prefix
         * search near has to be the next key.
         */
        if (exact_default == -1) {
            testutil_check(cursor_default->next(cursor_default.get()));
            const char *k;
            testutil_check(cursor_default->get_key(cursor_default.get(), &k));
            testutil_assert(std::string(k) == key_prefix_str);
        }
        /*
         * If the exact value from the default search near call is set to 0, we expect both search
         * near calls to return the same output.
         */
        else if (exact_default == 0)
            testutil_assert(exact_prefix == exact_default && key_default_str == key_prefix_str);
        /*
         * If the exact value from the default search near call is 1, the validation depends on the
         * exact value set by the prefix search near.
         */
        else {
            /* Both search near calls should have returned the same key. */
            if (exact_prefix == 1)
                testutil_assert(key_default_str == key_prefix_str);
            /*
             * The exact value from the default search near is 1 and the exact value from the prefix
             * enabled search near call is 0. This means the latter has found the exact same key. We
             * only need to check the previous key using the default cursor.
             */
            else {
                testutil_check(cursor_default->prev(cursor_default.get()));
                const char *k;
                testutil_check(cursor_default->get_key(cursor_default.get(), &k));
                testutil_assert(std::string(k) == key_prefix_str);
            }
        }
    }

    /*
     * Validate that no keys with the prefix used for the search have been found. To validate this,
     * we can use the exact value set by the default search near.
     * Since the prefix search near failed, the exact value set by the default
     * search near call has to be either -1 or 1:
     * - If it is -1, we need to check the next keys until we reach the end of the
     * table or a key that is greater than the prefix we looked for.
     * - If it is 1, we need to check the previous keys until we reach the end of
     * the table or a key that is smaller than the prefix we looked for.
     */
    void
    validate_unsuccessful_prefix_call(scoped_cursor &cursor_default, scoped_cursor &cursor_prefix,
      const std::string &prefix, int exact_default, int exact_prefix)
    {
        /*
         * The exact value from the default search near call cannot be 0, otherwise the prefix
         * search near should be successful too.
         */
        testutil_assert(exact_default == -1 || exact_default == 1);

        /* Check the key returned by the default search near does not contain the prefix. */
        const char *key_default;
        testutil_check(cursor_default->get_key(cursor_default.get(), &key_default));
        std::string key_default_str = key_default;
        testutil_assert(key_default_str.substr(0, prefix.size()) != prefix);

        /*
         * If the default search near call sets exact to -1, make sure no following keys in the
         * table contains the prefix.
         */
        if (exact_default == -1) {
            // TODO - Is checking the next key (if it exists) enough ?
            while (cursor_default->next(cursor_default.get()) == 0) {
                const char *k;
                testutil_check(cursor_default->get_key(cursor_default.get(), &k));
                std::string k_str = k;
                /*
                 * We can stop searching if the current key is greater than the prefix.
                 */
                if (!std::lexicographical_compare(
                      k_str.begin(), k_str.end(), prefix.begin(), prefix.end()))
                    break;
                /* Check the key does not contain the prefix. */
                testutil_assert(k_str.substr(0, prefix.size()) != prefix);
            }
            /* We have reached the end of the table or we did an early exit. */
        }
        /*
         * If the default search near call sets exact to 1, make sure the previous key is
         * lexicographically smaller than prefix.
         */
        else {
            int ret = cursor_default->prev(cursor_default.get());
            if (ret == 0) {
                const char *k;
                testutil_check(cursor_default->get_key(cursor_default.get(), &k));
                std::string k_str = k;
                testutil_assert(std::lexicographical_compare(
                  k_str.begin(), k_str.end(), prefix.begin(), prefix.end()));
            } else
                /* Check we have reached the end of the table. */
                testutil_assert(ret == WT_NOTFOUND);
        }
    }
};
