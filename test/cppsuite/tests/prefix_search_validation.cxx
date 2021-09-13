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
#include "test_harness/workload/thread_context.h"
#include "test_harness/thread_manager.h"
#include "test_harness/workload/random_generator.h"
#include "test_harness/util/api_const.h"
/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overriden and customized.
 */
using namespace test_harness;
class prefix_search_validation : public test_harness::test {

    wt_timestamp_t start_ts = UINT64_MAX;
    wt_timestamp_t end_ts = 0;
    uint64_t keys_per_prefix = 0;

    std::string alphabet{"abcdefghijklmnopqrstuvwxyz"};
    const int ALPHABET_SIZE = 26;
    void
    populate_worker(thread_context *tc)
    {
        logger::log_msg(LOG_INFO, "Populate: prefix key: " + std::to_string(tc->id));
        std::string prefix_key;
        int cmpp;
        uint64_t collections_per_thread = tc->collection_count;

        for (int64_t i = 0; i < collections_per_thread; ++i) {
            collection &coll = tc->db.get_collection(i);
            /*
            * WiredTiger lets you open a cursor on a collection using the same pointer. When a session
            * is closed, WiredTiger APIs close the cursors too.
            */
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name.c_str());
            scoped_cursor evict_cursor =
            tc->session.open_scoped_cursor(coll.name.c_str(), "debug=(release_evict=true)");
            /* Start a txn. */
            tc->transaction.begin();
            for (uint64_t j = 0; j < ALPHABET_SIZE; ++j) {
                for (uint64_t k = 0; k < ALPHABET_SIZE; ++k) {
                    for (uint64_t count = 0; count < tc->key_count; ++count) {
                        prefix_key = {alphabet.at(tc->id), alphabet.at(i), alphabet.at(j)};
                        prefix_key += random_generator::instance().generate_string(tc->key_size);
                        if (tc->insert(cursor, coll.id, prefix_key)) {
                            /* We failed to insert, rollback our transaction and retry. */
                            tc->transaction.rollback();
                            --count;
                            continue;
                        }
                    }
                }
            }
            tc->transaction.commit("commit_timestamp=" + timestamp_str(100));

            for (uint64_t j = 0; j < ALPHABET_SIZE; ++j) {
                for (uint64_t k = 0; k < ALPHABET_SIZE; ++k) {
                    std::string key = {alphabet.at(tc->id), alphabet.at(i), alphabet.at(j)};
                    evict_cursor->set_key(evict_cursor.get(), key.c_str());
                    evict_cursor->search_near(evict_cursor.get(), &cmpp);
                    testutil_check(evict_cursor->reset(evict_cursor.get()));
                }
            }
        }
    }

    std::string generate_random_search_key() {
        char a = alphabet.at(random_generator::instance().generate_integer(0, ALPHABET_SIZE - 1));
        char b = alphabet.at(random_generator::instance().generate_integer(0, ALPHABET_SIZE - 1));

        return {a, b};
    }

    public:
    prefix_search_validation(const test_harness::test_args &args) : test(args) {}

    void
    populate(test_harness::database &database, test_harness::timestamp_manager *tsm,
      test_harness::configuration *config, test_harness::workload_tracking *tracking) override final
    {
        int64_t collection_count, key_count, key_size;
        std::vector<thread_context *> workers;
        thread_manager tm;

        /* Validate our config. */
        collection_count = config->get_int(COLLECTION_COUNT);
        key_count = keys_per_prefix = config->get_int(KEY_COUNT_PER_COLLECTION);
        key_size = config->get_int(KEY_SIZE);
        testutil_assert(key_size > 0);
        /* Keys must be unique. */
        testutil_assert(key_count <= pow(10, key_size));


        std::cout << key_size << " " << key_count << " " << collection_count << std::endl;
        
        /* Create n collections as per the configuration. */
        for (int64_t i = 0; i < collection_count; ++i)
            /*
            * The database model will call into the API and create the collection, with its own
            * session.
            */
            database.add_collection();

        /*
         * Spawn thread_count threads to populate the database, theoretically we should be IO bound
         * here.
         */
        for (int64_t i = 0; i < ALPHABET_SIZE; ++i) {
            thread_context *tc = new thread_context(i, thread_type::INSERT, config,
              connection_manager::instance().create_session(), tsm, tracking, database);
            workers.push_back(tc);
            tm.add_thread(
              std::bind(&prefix_search_validation::populate_worker, this, std::placeholders::_1),
              tc);
        }

        /* Wait for our populate threads to finish and then join them. */
        logger::log_msg(LOG_INFO, "Populate: waiting for threads to complete.");
        tm.join();

        /* Cleanup our workers. */
        for (auto &it : workers) {
            delete it;
            it = nullptr;
        }
        logger::log_msg(LOG_INFO, "Populate: finished.");
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {

        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        scoped_cursor cursor;
        int cmpp;
        int64_t entries_stat, prefix_stat, prev_entries_stat, prev_prefix_stat;

        cmpp = 0;
        entries_stat = prev_entries_stat = get_stat(tc, WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100);;
        prefix_stat = prev_prefix_stat = get_stat(tc, WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS);

        std::map<uint64_t, scoped_cursor> cursors;
        while (tc->running()) {
            tc->transaction.begin("read_timestamp=" + timestamp_str(10));

            /* Get a collection and find a cached cursor. */
            collection  &coll = tc->db.get_random_collection();
            if (cursors.find(coll.id) == cursors.end()) {
                scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name.c_str());
                cursor->reconfigure(cursor.get(), "prefix_key=true");
                cursors.emplace(coll.id, std::move(cursor));
            }

            std::string srch_key = generate_random_search_key();  


            logger::log_msg(LOG_ERROR, srch_key);
            /* Do a second lookup now that we know it exists. */
            auto &cursor = cursors[coll.id];
            if (tc->transaction.active()) {

                cursor->set_key(cursor.get(), "aa");
                auto ret = cursor->search_near(cursor.get(), &cmpp);
                testutil_assert(ret == WT_NOTFOUND);
                entries_stat = get_stat(tc, WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100);
                prefix_stat = get_stat(tc, WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS);

                logger::log_msg(LOG_ERROR,
                  "Read working: skipped entries " + std::to_string(entries_stat) +
                    " prefix fash path  " + std::to_string(prefix_stat));
                testutil_assert(keys_per_prefix * ALPHABET_SIZE * 2 >= entries_stat - prev_entries_stat);
                testutil_assert(prefix_stat > prev_prefix_stat);
                prev_entries_stat = entries_stat;
                prev_prefix_stat = prefix_stat;
                tc->transaction.add_op();
                // tc->transaction.try_rollback();
                tc->sleep();
            }
            tc->transaction.commit();
            /* Reset our cursor to avoid pinning content. */
            testutil_check(cursor->reset(cursor.get()));
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }

    int64_t
    get_stat(test_harness::thread_context *tc, int stat_field)
    {
        int64_t valuep;
        /* Open our statistic cursor. */
        scoped_cursor cursor =
          tc->session.open_scoped_cursor(STATISTICS_URI);

        const char *desc, *pvalue;
        cursor->set_key(cursor.get(), stat_field);
        testutil_check(cursor->search(cursor.get()));
        testutil_check(cursor->get_value(cursor.get(), &desc, &pvalue, &valuep));
        testutil_check(cursor->reset(cursor.get()));
        return valuep;
    }

    std::string
    timestamp_str(wt_timestamp_t ts)
    {
        std::stringstream stream;
        stream << std::hex << ts;
        return stream.str();
    }
};
