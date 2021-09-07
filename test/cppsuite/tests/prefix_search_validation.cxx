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
class prefix_search_validation : public test_harness::test {

    wt_timestamp_t start_ts = UINT64_MAX;
    wt_timestamp_t end_ts = 0;

    std::string ALPHABET_ARRAY = "abcdefghijklmnopqrstuvwxyz";
    const int ALPHABET_SIZE = 26;
    std::map<std::string, int> map;
    void
    populate_worker(test_harness::thread_context *tc)
    {
        test_harness::logger::log_msg(LOG_ERROR, "Populate: prefix key: " + std::to_string(tc->id));
        test_harness::collection &coll = tc->db.get_collection(0);
        std::string prefix_key;


        /*
         * WiredTiger lets you open a cursor on a collection using the same pointer. When a session
         * is closed, WiredTiger APIs close the cursors too.
         */
        test_harness::scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name.c_str());
        test_harness::scoped_cursor evict_cursor =
          tc->session.open_scoped_cursor(coll.name.c_str(), "debug=(release_evict=true)");
        /* Start a txn. */

        tc->transaction.begin();
        for (uint64_t i = 0; i < ALPHABET_SIZE; ++i) {
            for (uint64_t j = 0; j < ALPHABET_SIZE; ++j) {
                prefix_key = generate_prefix_key(tc, i, j);
                
                if (tc->insert(cursor, coll.id, prefix_key)) {
                    /* We failed to insert, rollback our transaction and retry. */
                    tc->transaction.rollback();
                    --i;
                    continue;
                }
            }
        }
        wt_timestamp_t ts = tc->tsm->get_next_ts();
        //std::cout << ts << std::endl;
        tc->transaction.set_commit_timestamp(ts);
        if (tc->transaction.commit() == true) {
            testutil_die(0, "ERROR", "rollback");
        }

        for (uint64_t i = 0; i < ALPHABET_SIZE; ++i) {
            for (uint64_t j = 0; j < ALPHABET_SIZE; ++j) {
                std::string key = generate_prefix_key(tc, i, j);
                evict_cursor->set_key(evict_cursor.get(), key.c_str());
                testutil_check(evict_cursor->search(evict_cursor.get()));
                testutil_check(evict_cursor->reset(evict_cursor.get()));
            }
        }
    }

    std::string generate_prefix_key(test_harness::thread_context *tc, int i, int j) {
        std::string prefix_key = "";
        prefix_key += ALPHABET_ARRAY[tc->id];
        prefix_key += ALPHABET_ARRAY[i];
        prefix_key += ALPHABET_ARRAY[j];
        return prefix_key;
    }

    public:
    prefix_search_validation(const test_harness::test_args &args) : test(args) {}

    void
    populate(test_harness::database &database, test_harness::timestamp_manager *_timestamp_manager,
      test_harness::configuration *_config,
      test_harness::workload_tracking *tracking) override final
    {
        std::vector<test_harness::thread_context *> workers;
        test_harness::thread_manager tm;

        database.add_collection();

        /*
         * Spawn thread_count threads to populate the database, theoretically we should be IO bound
         * here.
         */
        start_ts = _timestamp_manager->get_next_ts();
        for (int64_t i = 0; i < ALPHABET_SIZE; ++i) {
            test_harness::thread_context *tc =
              new test_harness::thread_context(i, test_harness::thread_type::INSERT, _config,
                test_harness::connection_manager::instance().create_session(), _timestamp_manager,
                tracking, database);
            workers.push_back(tc);
            tm.add_thread(
              std::bind(&prefix_search_validation::populate_worker, this, std::placeholders::_1),
              tc);
        }

        /* Wait for our populate threads to finish and then join them. */
        test_harness::logger::log_msg(LOG_ERROR, "Populate: waiting for threads to complete.");
        tm.join();

        /* Cleanup our workers. */
        for (auto &it : workers) {
            delete it;
            it = nullptr;
        }
        end_ts =_timestamp_manager->get_next_ts();
        std::cout << "start: " << start_ts << " end: " << end_ts << std::endl;
        test_harness::logger::log_msg(LOG_ERROR, "Populate: finished.");
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {

        test_harness::logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        test_harness::scoped_cursor cursor;
        int cmpp;
        int64_t entries_stat, prefix_stat;
        std::string prefix_key = "";
        /* Open our statistic cursor. */
        
        char a = ALPHABET_ARRAY[test_harness::random_generator::instance().generate_integer(0, ALPHABET_SIZE)];
        char b = ALPHABET_ARRAY[test_harness::random_generator::instance().generate_integer(0, ALPHABET_SIZE)];
        prefix_key += a;
        prefix_key += b;

        cmpp = 0;
        while (tc->running()) {
            wt_timestamp_t read_ts = test_harness::random_generator::instance().generate_integer(start_ts, end_ts);
            /* Get a collection. */
            test_harness::collection &coll = tc->db.get_random_collection();
            std::cout << read_ts << std::endl;
            tc->transaction.begin("read_timestamp=" + timestamp_str(start_ts));
            cursor = tc->session.open_scoped_cursor(coll.name.c_str());
            if (tc->transaction.active()) {
                cursor->reconfigure(cursor.get(), "prefix_key=true");
                cursor->set_key(cursor.get(), "xt");
                auto ret = cursor->search_near(cursor.get(), &cmpp);

                const char *tracked_key;
                cursor->get_key(cursor.get(), &tracked_key);
                std::cout << prefix_key << std::endl;
                test_harness::logger::log_msg(LOG_ERROR, "Ret? " + std::to_string(ret));
                if (ret != 0) {
                    if (ret == WT_NOTFOUND) {
                        cursor->reset(cursor.get());
                    } else if (ret == WT_ROLLBACK) {
                        tc->transaction.rollback();
                        tc->sleep();
                        continue;
                    } else
                        testutil_die(ret, "Unexpected error returned from cursor->next()");
                }
                entries_stat = 0;
                get_stat(tc, WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100, &entries_stat);
                get_stat(tc, WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS, &prefix_stat);

                test_harness::logger::log_msg(LOG_ERROR,
                  "Read working: skipped entries " + std::to_string(entries_stat) +
                    " prefix fash path  " + std::to_string(prefix_stat));
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

    void
    get_stat(test_harness::thread_context *tc, int stat_field, int64_t *valuep)
    {
        test_harness::scoped_cursor cursor =
          tc->session.open_scoped_cursor(test_harness::STATISTICS_URI);

        const char *desc, *pvalue;
        cursor->set_key(cursor.get(), stat_field);
        testutil_check(cursor->search(cursor.get()));
        testutil_check(cursor->get_value(cursor.get(), &desc, &pvalue, valuep));
        testutil_check(cursor->reset(cursor.get()));
    }

    std::string
    timestamp_str(wt_timestamp_t ts)
    {
        std::stringstream stream;
        stream << std::hex << ts;
        return stream.str();
    }
};
