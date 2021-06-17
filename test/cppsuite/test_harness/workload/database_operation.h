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

#ifndef DATABASE_OPERATION_H
#define DATABASE_OPERATION_H

#include <map>
#include <memory>
#include <thread>

#include "database_model.h"
#include "workload_tracking.h"
#include "thread_context.h"
#include "random_generator.h"
#include "../thread_manager.h"

namespace test_harness {
class database_operation {
    public:
    /*
     * Function that performs the following steps using the configuration that is defined by the
     * test:
     *  - Create the working dir.
     *  - Open a connection.
     *  - Open a session.
     *  - Create n collections as per the configuration.
     *      - Open a cursor on each collection.
     *      - Insert m key/value pairs in each collection. Values are random strings which size is
     * defined by the configuration.
     *      - Store in memory the created collections.
     */
    virtual void
    populate(database &database, timestamp_manager *tsm, configuration *config,
      workload_tracking *tracking)
    {
        WT_SESSION *session;
        int64_t collection_count, key_count, key_size, thread_count, value_size;
        std::string collection_name;
        thread_manager tm;

        /* Get a session. */
        session = connection_manager::instance().create_session();

        /* Get our configuration values, validating that they make sense. */
        collection_count = config->get_int(COLLECTION_COUNT);
        key_count = config->get_int(KEY_COUNT_PER_COLLECTION);
        value_size = config->get_int(VALUE_SIZE);
        thread_count = config->get_int(THREAD_COUNT);
        testutil_assert(collection_count % thread_count == 0);
        testutil_assert(value_size > 0);
        key_size = config->get_int(KEY_SIZE);
        testutil_assert(key_size > 0);

        /* Keys must be unique. */
        testutil_assert(key_count <= pow(10, key_size));

        /* Create n collections as per the configuration and store each collection name. */
        for (int64_t i = 0; i < collection_count; ++i) {
            database.add_collection(key_count);
        }
        debug_print(
          "Populate: " + std::to_string(collection_count) + " collections created.", DEBUG_INFO);

        /*
         * Spawn thread_count threads to populate the database, theoretically we should be IO bound
         * here.
         */
        int64_t collections_per_thread = collection_count / thread_count;
        for (int64_t i = 0; i < thread_count; ++i) {
            tm.add_thread(populate_worker, tsm, tracking, database, i, collections_per_thread, tsm,
              tracking, key_count, key_size, value_size);
        }

        /* Wait for our populate threads to finish and then join them. */
        debug_print("Populate: waiting for threads to complete.", DEBUG_INFO);
        tm.join();

        debug_print("Populate: finished.", DEBUG_INFO);
    }

    /* Basic insert operation that adds a new key every rate tick. */
    virtual void
    insert_operation(thread_context *tc)
    {
        debug_print(type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.",
          DEBUG_INFO);
    }

    /* Basic read operation that chooses a random collection and walks a cursor. */
    virtual void
    read_operation(thread_context *tc)
    {
        debug_print(type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.",
          DEBUG_INFO);
        WT_CURSOR *cursor = nullptr;
        WT_DECL_RET;
        std::map<uint64_t, WT_CURSOR *> cursors;
        while (tc->running()) {
            {
                /* Get a collection and find a cached cursor. */
                std::shared_ptr<collection> coll = tc->database.get_random_collection();
                const auto &it = cursors.find(coll->id);
                if (it == cursors.end()) {
                    testutil_check(tc->session->open_cursor(
                      tc->session, coll->name.c_str(), NULL, NULL, &cursor));
                    cursors.emplace(coll->id, nullptr);
                } else
                    cursor = it->second;
            }
            /* Walk the cursor. */
            tc->transaction.begin(tc->session, "");
            while (tc->transaction.active() && tc->running()) {
                ret = cursor->next(cursor);
                if (ret == WT_NOTFOUND) {
                    testutil_check(cursor->reset(cursor));
                    tc->transaction.rollback(tc->session, "");
                } else if (ret != 0)
                    testutil_die(ret, "cursor->next() failed");
                tc->transaction.try_rollback(tc->session, "");
                tc->sleep();
            }
        }
        /* Make sure the last operation is committed now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback(tc->session, "");
    }

    /*
     * Basic update operation that uses a random cursor to update values in a randomly chosen
     * collection.
     */
    virtual void
    update_operation(thread_context *tc)
    {
        debug_print(type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.",
          DEBUG_INFO);

        /* A structure that's used to track which cursors we've opened for which collection. */
        struct collection_cursors {
            const std::string collection_name;
            WT_CURSOR *random_cursor;
            WT_CURSOR *update_cursor;
        };

        WT_DECL_RET;
        wt_timestamp_t ts;
        std::map<uint64_t, collection_cursors> collections;
        key_value_t key, generated_value;
        const char *key_tmp;
        uint64_t collection_id = 0;
        bool using_timestamps = tc->timestamp_manager->enabled();

        /*
         * Loop while the test is running.
         */
        while (tc->running()) {
            /*
             * Sleep the period defined by the op_rate in the configuration. Do this at the start of
             * the loop as it could be skipped by a subsequent continue call.
             */
            tc->sleep();

            /* Pick a random collection to update. */
            {
                std::shared_ptr<collection> coll = tc->database.get_random_collection();
                collection_id = coll->id;

                /* Look for existing cursors in our cursor cache. */
                if (collections.find(collection_id) == collections.end()) {
                    WT_CURSOR *random_cursor = nullptr, *update_cursor = nullptr;
                    debug_print("Thread {" + std::to_string(tc->id) +
                        "} Creating cursor for collection: " + coll->name,
                      DEBUG_TRACE);

                    /* Open a random cursor for that collection. */
                    tc->session->open_cursor(
                      tc->session, coll->name.c_str(), nullptr, "next_random=true", &random_cursor);
                    /*
                     * We can't call update on a random cursor so we open two cursors here, one to
                     * do the randomized next and one to subsequently update the key.
                     */
                    tc->session->open_cursor(
                      tc->session, coll->name.c_str(), nullptr, nullptr, &update_cursor);

                    collections.emplace(
                      collection_id, collection_cursors{coll->name, random_cursor, update_cursor});
                }
            }

            /* Start a transaction if possible. */
            tc->transaction.try_begin(tc->session, "");

            /* Get the random cursor associated with the collection. */
            auto collection = collections[collection_id];
            /* Call next to pick a new random record. */
            ret = collection.random_cursor->next(collection.random_cursor);
            if (ret == WT_NOTFOUND)
                continue;
            else if (ret != 0)
                testutil_die(ret, "unhandled error returned by cursor->next()");

            /* Get the record's key. */
            testutil_check(collection.random_cursor->get_key(collection.random_cursor, &key_tmp));

            /*
             * The retrieved key needs to be passed inside the update function. However, the update
             * API doesn't guarantee our buffer will still be valid once it is called, as such we
             * copy the buffer and then pass it into the API.
             */
            key = key_value_t(key_tmp);

            /* Generate a new value for the record. */
            generated_value =
              random_generator::random_generator::instance().generate_string(tc->value_size);

            /*
             * Get a timestamp to apply to the update. We still do this even if timestamps aren't
             * enabled as it will return WT_TS_NONE, which is then inserted into the tracking table.
             */
            ts = tc->timestamp_manager->get_next_ts();
            if (using_timestamps)
                tc->transaction.set_commit_timestamp(
                  tc->session, timestamp_manager::decimal_to_hex(ts));

            /*
             * Update the record but take care to handle WT_ROLLBACK as we may conflict with another
             * running transaction. Here we call the pre-defined wrappers as they also update the
             * tracking table, which is later used for validation.
             *
             * Additionally first get the update_cursor.
             */
            ret = update(tc->tracking, collection.update_cursor, collection.collection_name,
              key.c_str(), generated_value.c_str(), ts);

            /* Increment the current op count for the current transaction. */
            tc->transaction.op_count++;

            /*
             * If the wiredtiger API has returned rollback, comply. This will need to rollback
             * tracking table operations in the future but currently won't.
             */
            if (ret == WT_ROLLBACK)
                tc->transaction.rollback(tc->session, "");

            /* Commit the current transaction if we're able to. */
            tc->transaction.try_commit(tc->session, "");
        }

        /* Make sure the last operation is committed now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.commit(tc->session, "");
    }

    protected:
    /* WiredTiger APIs wrappers for single operations. */
    template <typename K, typename V>
    static int
    insert(WT_CURSOR *cursor, workload_tracking *tracking, const std::string &collection_name,
      const K &key, const V &value, wt_timestamp_t ts)
    {
        WT_DECL_RET;
        testutil_assert(cursor != nullptr);

        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);

        ret = cursor->insert(cursor);
        if (ret != 0) {
            if (ret == WT_ROLLBACK)
                return (ret);
            else
                testutil_die(ret, "unhandled error while trying to insert a key.");
        }

        debug_print("key/value inserted", DEBUG_TRACE);
        tracking->save_operation(tracking_operation::INSERT, collection_name, key, value, ts);
        return (0);
    }

    template <typename K, typename V>
    static int
    update(workload_tracking *tracking, WT_CURSOR *cursor, const std::string &collection_name,
      K key, V value, wt_timestamp_t ts)
    {
        WT_DECL_RET;
        testutil_assert(tracking != nullptr);
        testutil_assert(cursor != nullptr);

        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);

        ret = cursor->update(cursor);
        if (ret != 0) {
            if (ret == WT_ROLLBACK)
                return (ret);
            else
                testutil_die(ret, "unhandled error while trying to update a key");
        }

        debug_print("key/value updated", DEBUG_TRACE);
        tracking->save_operation(tracking_operation::UPDATE, collection_name, key, value, ts);
        return (0);
    }

    /*
     * Convert a number to a string. If the resulting string is less than the given length, padding
     * of '0' is added.
     */
    static std::string
    number_to_string(uint64_t size, uint64_t value)
    {
        std::string str, value_str = std::to_string(value);
        testutil_assert(size >= value_str.size());
        uint64_t diff = size - value_str.size();
        std::string s(diff, '0');
        str = s.append(value_str);
        return (str);
    }

    private:
    static void
    populate_worker(timestamp_manager *tsm, workload_tracking *tracking, database &database,
      uint64_t worker_id, int64_t coll_count, int64_t key_count, int64_t key_size,
      int64_t value_size)
    {
        WT_DECL_RET;
        WT_CURSOR *cursor;
        WT_SESSION *session;
        std::string cfg;
        wt_timestamp_t ts;
        key_value_t generated_key, generated_value;

        session = connection_manager::instance().create_session();

        for (int64_t i = 0; i < coll_count; ++i) {
            int64_t next_coll_id = (worker_id * coll_count) + i;
            std::shared_ptr<collection> coll = database.get_collection(next_coll_id);
            /*
             * WiredTiger lets you open a cursor on a collection using the same pointer. When a
             * session is closed, WiredTiger APIs close the cursors too.
             */
            testutil_check(session->open_cursor(session, coll->name.c_str(), NULL, NULL, &cursor));
            for (uint64_t i = 0; i < key_count; ++i) {
                /* Generation of a unique key. */
                generated_key = number_to_string(key_size, i);
                /*
                 * Generation of a random string value using the size defined in the test
                 * configuration.
                 */
                generated_value =
                  random_generator::random_generator::instance().generate_string(value_size);
                ts = tsm->get_next_ts();

                /* Start a txn. */
                testutil_check(session->begin_transaction(session, nullptr));

                ret = insert(cursor, tracking, coll->name.c_str(), generated_key.c_str(),
                  generated_value.c_str(), ts);

                /* This may require some sort of "stuck" mechanism but for now is fine. */
                if (ret == WT_ROLLBACK)
                    testutil_die(-1, "Got a rollback in populate, this is currently not handled.");

                if (tsm->enabled())
                    cfg = std::string(COMMIT_TS) + "=" + timestamp_manager::decimal_to_hex(ts);
                else
                    cfg = "";

                testutil_check(session->commit_transaction(session, cfg.c_str()));
            }
        }

        session->close(session, nullptr);
        debug_print("Populate: thread {" + std::to_string(worker_id) + "} finished", DEBUG_TRACE);
    }
};
} // namespace test_harness
#endif
