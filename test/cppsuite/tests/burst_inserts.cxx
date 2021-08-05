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
#include "test_harness/workload/random_generator.h"

using namespace test_harness;

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overriden and customized.
 */
class burst_inserts : public test {
    public:
    burst_inserts(const test_args &args) : test(args) {}


    void
    read_operation(thread_context *tc) override final
    {
        logger::log_msg(
            LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");
        uint64_t collection_count = tc->db.get_collection_count();
        uint64_t coll_num = 10;
        while (tc->running()) {
            /* Collection cursor vector. */
            std::vector<scoped_cursor> cursors;
            /*
            * Algo:1 open 10 cursors for 10 collections. Read random documents for each of them for a
            * period then close the cursors.
            */
            for (uint64_t i = 0; i < coll_num; ++i) {
                auto& coll = tc->db.get_random_collection();
                cursors.push_back(std::move(tc->session.open_scoped_cursor(coll.name.c_str(), "next_random=true")));
            }
            uint64_t counter = 0;
            auto burst_start = std::chrono::system_clock::now();
            while (tc->running() && std::chrono::system_clock::now() - burst_start < std::chrono::seconds(60)) {
                tc->transaction.try_begin();
                if (tc->next(cursors[counter]) != 0)
                    continue;
                tc->transaction.try_commit();
                counter++;
                if (counter == cursors.size())
                    counter = 0;
                std::this_thread::sleep_for(std::chrono::milliseconds(random_generator::instance().generate_integer<uint64_t>(5,15)));
            }
            tc->sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }


    /*
     * Insert operation that inserts continuously for insert_duration with no throttling.
     * It then sleeps for op_rate.
     */
    void
    insert_operation(thread_context *tc) override final
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


        uint64_t counter = 0;
        while (tc->running()) {
            uint64_t start_key = ccv[counter].coll.get_key_count();
            uint64_t added_count = 0;
            bool committed = true;
            auto &cc = ccv[counter];
            auto burst_start = std::chrono::system_clock::now();
            while (tc->running() && std::chrono::system_clock::now() - burst_start < std::chrono::seconds(60)) {
                tc->transaction.try_begin();
                if (!tc->insert(cc.cursor, cc.coll.id, start_key + added_count)) {
                    added_count = 0;
                    continue;
                }
                added_count++;

                tc->transaction.try_commit();

                if (!tc->transaction.active()) {
                    cc.coll.increase_key_count(added_count);
                    start_key = cc.coll.get_key_count();
                    added_count = 0;
                }
            }
            testutil_check(cc.cursor->reset(cc.cursor.get()));
            counter++;
            if (counter == collections_per_thread)
                counter = 0;

            tc->sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }
};
