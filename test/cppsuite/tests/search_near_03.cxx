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
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overriden and customized.
 */
class search_near_03 : public test_harness::test {
    public:
    search_near_03(const test_harness::test_args &args) : test(args) {}

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customized your test. */
        test::run();
    }

    // static void perform_unique_index_insertions(test_harness::thread_context *tc, ) {

    // }

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
        int64_t collection_count = tc->db.get_collection_count();
        int64_t collections_per_thread = collection_count / tc->thread_count;

        /* Must have unique collections for each thread. */
        testutil_assert(collection_count % tc->thread_count == 0);
        const uint64_t thread_offset = tc->id * collections_per_thread;
        for (uint64_t i = thread_offset;
             i < thread_offset + collections_per_thread && tc->running(); ++i) {
            collection &coll = tc->db.get_collection(i);
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            ccv.push_back({coll, std::move(cursor)});
        }

        std::string key;
        const uint64_t MAX_ROLLBACKS = 100;
        uint64_t counter = 0;
        uint32_t rollback_retries = 0;

        while (tc->running()) {

            auto &cc = ccv[counter];
            tc->transaction.begin();

            while (tc->transaction.active() && tc->running()) {

                /* Generate a random key. */
                key = random_generator::instance().generate_random_string(tc->key_size);

                /* Insert a key value pair. */
                if (tc->insert(cc.cursor, cc.coll.id, key)) {
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
            testutil_check(cc.cursor->reset(cc.cursor.get()));
            if (++counter == ccv.size())
                counter = 0;
            testutil_assert(counter < collections_per_thread);
        }
    }

    void
    read_operation(test_harness::thread_context *) override final
    {
        std::cout << "read_operation: nothing done." << std::endl;
    }

    void
    update_operation(test_harness::thread_context *) override final
    {
        std::cout << "update_operation: nothing done." << std::endl;
    }
};
