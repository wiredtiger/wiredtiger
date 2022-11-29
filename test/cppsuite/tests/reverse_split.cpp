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

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/main/test.h"

namespace test_harness {
/* Defines what data is written to the tracking table for use in custom validation. */
class operation_tracker_reverse_split : public operation_tracker {

    public:
    operation_tracker_reverse_split(
      configuration *config, const bool use_compression, timestamp_manager &tsm)
        : operation_tracker(config, use_compression, tsm)
    {
    }

    void
    set_tracking_cursor(WT_SESSION *session, const tracking_operation &operation,
      const uint64_t &collection_id, const std::string &key, const std::string &value,
      wt_timestamp_t ts, scoped_cursor &op_track_cursor) override final
    {
        /* You can replace this call to define your own tracking table contents. */
        operation_tracker::set_tracking_cursor(
          session, operation, collection_id, key, value, ts, op_track_cursor);
    }
};

/*
 * This test inserts data at the end of the collection and truncates off from the start of the
 * collection. In doing so pages at the start of the tree are gradually emptied while pages are
 * added at the end of the tree. This means the test frequently executes the reverse split path.
 */
class reverse_split : public test {
    public:
    reverse_split(test_args &args) : test(args)
    {
        /* Add split timing stresses to the conn_open config. */
        if (args.wt_open_config == "") {
            std::string stress;
            if (random_generator::instance().generate_bool())
                stress = "timing_stress_for_test=[split_3]";
            else
                stress = "timing_stress_for_test=[split_4]";
            logger::log_msg(LOG_WARN, "Adding config to WiredTiger open: " + stress);
            args.wt_open_config = stress;
        }
        init_operation_tracker();
    }

    /*
     * Insert operation that inserts a new K/V pair every op_rate.
     */
    void
    insert_operation(thread_worker *tc) override final
    {
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        /* Must have unique collection for each thread. */
        testutil_assert(tc->db.get_collection_count() == tc->thread_count);
        collection &coll = tc->db.get_collection(tc->id);
        scoped_cursor write_cursor = std::move(tc->session.open_scoped_cursor(coll.name));
        while (tc->running()) {
            tc->txn.begin();
            int added_count = 0;
            uint64_t start_key = coll.get_key_count();
            while (tc->txn.active()) {
                auto key = tc->pad_string(std::to_string(start_key + added_count), tc->key_size);
                auto value =
                  random_generator::instance().generate_pseudo_random_string(tc->value_size);
                /* A return value of true implies the insert was successful. */
                if (!tc->insert(write_cursor, coll.id, key, value)) {
                    tc->txn.rollback();
                    break;
                }
                added_count++;
                if (tc->txn.can_commit()) {
                    if (tc->txn.commit()) {
                        coll.increase_key_count(added_count);
                        start_key = coll.get_key_count();
                    }
                }
            }
            /* Reset the write cursor every iteration. */
            testutil_check(write_cursor->reset(write_cursor.get()));
            tc->sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        tc->txn.try_rollback();
    }

    /* Remove operation simulates bursty deletes. */
    void
    remove_operation(thread_worker *tc) override
    {
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");
        /* Must have unique collection for each thread. */
        testutil_assert(tc->db.get_collection_count() == tc->thread_count);
        collection &coll = tc->db.get_collection(tc->id);
        scoped_cursor write_cursor = std::move(tc->session.open_scoped_cursor(coll.name));

        while (tc->running()) {
            testutil_check(write_cursor->reset(write_cursor.get()));
            tc->txn.begin();
            int ret = write_cursor->next(write_cursor.get());
            if (ret != 0) {
                tc->txn.rollback();
                continue;
            }
            const char *key;
            testutil_check(write_cursor->get_key(write_cursor.get(), &key));
            const std::string key_str(key);
            /* Truncate up to 20% of the range. */
            uint64_t min_key_id = std::stoi(key_str);
            uint64_t key_count = coll.get_key_count();
            // logger::log_msg(LOG_TRACE, "colleciton has key count" + std::to_string(key_count));
            uint64_t end_key_id = random_generator::instance().generate_integer<uint64_t>(
              min_key_id, min_key_id + ((key_count - min_key_id) / 1.2));
            // logger::log_msg(LOG_TRACE, "thread {" + std::to_string(tc->id) + "} truncating range:
            // " + std::to_string(min_key_id) + "->" + std::to_string(end_key_id));
            std::string end_key = tc->pad_string(std::to_string(end_key_id), tc->key_size);
            if (!tc->truncate(coll.id, key_str, end_key, "")) {
                tc->txn.rollback();
                continue;
            }
            if (tc->txn.commit())
                logger::log_msg(LOG_INFO,
                  "thread {" + std::to_string(tc->id) + "} committed truncation of " +
                    std::to_string(end_key_id - min_key_id) + " records.");
            else
                logger::log_msg(LOG_WARN,
                  "thread {" + std::to_string(tc->id) + "} failed to commit truncation of " +
                    std::to_string(end_key_id - min_key_id) + " records.");
            tc->sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        tc->txn.try_rollback();
    }
};
}