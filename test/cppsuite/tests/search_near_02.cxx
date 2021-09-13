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
 * During the test duration, threads will keep inserting new random keys while other threads will
 * execute search_near calls with prefix enabled using random prefixes as well. In order to verify
 * if a search_near call returns the correct key, we will perform another search_near using the same
 * prefix but with prefix_key disabled. If both calls return the same key, the result is correct.
 */
class search_near_02 : public test_harness::test {
    public:
    search_near_02(const test_harness::test_args &args) : test(args) {}

    void
    populate(test_harness::database &database, test_harness::timestamp_manager *tsm,
      test_harness::configuration *config, test_harness::workload_tracking *tracking) override final
    {
        /* Configuration parsing. */
        int64_t collection_count = config->get_int(COLLECTION_COUNT);
        testutil_assert(collection_count > 0);

        logger::log_msg(
          LOG_INFO, "Populate: " + std::to_string(collection_count) + " creating collections.");

        /* Create empty collections. */
        for (int64_t i = 0; i < collection_count; ++i)
            database.add_collection();

        logger::log_msg(LOG_INFO, "Populate: finished.");
    }

    void
    insert_operation(test_harness::thread_context *tc) override final
    {
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

        const std::string alphabet("abcdefghijklmnopqrstuvwxyz");
        std::string key;
        uint64_t counter = 0;
        // TODO - From configuration
        uint64_t prefix_size = 1;

        testutil_assert(tc->key_size >= prefix_size);

        while (tc->running()) {

            auto &cc = ccv[counter];
            tc->transaction.begin();

            while (tc->transaction.active() && tc->running()) {

                /* Generate a prefix. */
                key = "";
                for (int i = 0; i < prefix_size; ++i)
                    key += random_generator::instance().generate_pseudo_random_string(
                      prefix_size, characters_type::ALPHABET);

                // To remove
                key = "a";
                std::cout << "Generated prefix is " << key << std::endl;

                /* Generate the remaining part of the key. */
                key += random_generator::instance().generate_pseudo_random_string(
                  tc->key_size - prefix_size);
                std::cout << "Generated key is " << key << std::endl;

                /* Insert a key value pair. */
                bool rollback_required = tc->insert(cc.cursor, cc.coll.id, key);
                if (!rollback_required) {
                    if (tc->transaction.can_commit())
                        rollback_required = tc->transaction.commit();
                }

                if (rollback_required)
                    tc->transaction.rollback();

                /* Sleep the duration defined by the op_rate. */
                tc->sleep();
            }

            // TODO - Do I need to rollback my transaction ? The test can be stopped and the
            // transaction started

            /* Reset our cursor to avoid pinning content. */
            testutil_check(cc.cursor->reset(cc.cursor.get()));
            if (++counter >= collections_per_thread)
                counter = 0;
        }
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        std::cout << "read_operation: running..." << std::endl;

        std::map<uint64_t, scoped_cursor> cursors;
        // TODO - From configuration
        uint64_t prefix_size = 1;
        const char *retrieved_key;
        int exact, res;

        while (tc->running()) {
            /* Get a collection and find a cached cursor. */
            collection &coll = tc->db.get_random_collection();

            if (cursors.find(coll.id) == cursors.end()) {
                cursors.emplace(
                  coll.id, std::move(tc->session.open_scoped_cursor(coll.name.c_str())));
                auto &cursor = cursors[coll.id];
                /* The first search_near will use the prefix configuration. */
                testutil_check(cursor.get()->reconfigure(cursor.get(), "prefix_key=true"));
            }

            auto &cursor = cursors[coll.id];

            /* Select a random timestamp between the oldest and now. */
            // wt_timestamp_t ts = random_generator::instance().generate_integer(
            //   tc->tsm->get_oldest_ts(), tc->tsm->get_next_ts());
            std::string ts = tc->tsm->decimal_to_hex(tc->tsm->get_oldest_ts());
            std::cout << "ts I am reading at " << ts << std::endl;
            tc->transaction.begin("read_timestamp=" + ts);
            while (tc->transaction.active() && tc->running()) {

                /* Generate a prefix. */
                uint64_t prefix_size_tmp =
                  random_generator::instance().generate_integer(1UL, prefix_size);
                // std::cout << "Prefix size will be " << prefix_size_tmp << std::endl;
                std::string prefix = random_generator::instance().generate_string(
                  prefix_size_tmp, characters_type::ALPHABET);

                // TO remove
                prefix = "a";
                std::cout << "prefix to look for is " << prefix << std::endl;

                cursor->set_key(cursor.get(), prefix.c_str());
                /*
                 * FIXME-WT-7912 The error occurs here. We current have WT_NOTFOUND while we should
                 * get the only visible key.
                 */
                int ret = cursor->search_near(cursor.get(), &exact);
                std::cout << "ret search_near is " << ret << std::endl;

                tc->transaction.add_op();
                tc->transaction.try_rollback();
                tc->sleep();
            }
            testutil_check(cursor->reset(cursor.get()));
        }
    }
};
