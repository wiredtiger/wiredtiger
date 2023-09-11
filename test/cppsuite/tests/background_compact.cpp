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
class operation_tracker_background_compact : public operation_tracker {

public:
    operation_tracker_background_compact(
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
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class background_compact : public test {
    bool maintenance_window = false;

public:
    background_compact(const test_args &args) : test(args)
    {
        init_operation_tracker(
          new operation_tracker_background_compact(_config->get_subconfig(OPERATION_TRACKER),
            _config->get_bool(COMPRESSION_ENABLED), *_timestamp_manager));
    }

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customize your test. */
        test::run();
    }

    void
    custom_operation(thread_worker *tw) override final
    {
        int64_t bytes_avail_reuse, pages_reviewed, pages_rewritten, size;

        const int64_t megabyte = 1024 * 1024;

        std::string log_prefix =
          type_string(tw->type) + " thread {" + std::to_string(tw->id) + "}: ";
        logger::log_msg(
          LOG_INFO, type_string(tw->type) + " thread {" + std::to_string(tw->id) + "} commencing.");

        uint64_t collection_count = tw->db.get_collection_count();

        while (tw->running()) {
            logger::log_msg(LOG_INFO, log_prefix + "=== Toggle maintanence window ===");

            for (int i = 0; i < collection_count; i++) {
                /* Make sure that thread statistics cursor is null before we open it. */
                // testutil_assert(tw->stat_cursor.get() == nullptr);

                collection &coll = tw->db.get_collection(i);
                std::string uri = STATISTICS_URI + coll.name;

                logger::log_msg(LOG_INFO, "custom thread uri: " + uri);
                tw->stat_cursor = tw->session.open_scoped_cursor(uri);

                metrics_monitor::get_stat(
                  tw->stat_cursor, WT_STAT_DSRC_BLOCK_REUSE_BYTES, &bytes_avail_reuse);
                metrics_monitor::get_stat(
                  tw->stat_cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED, &pages_reviewed);
                metrics_monitor::get_stat(
                  tw->stat_cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REWRITTEN, &pages_rewritten);
                metrics_monitor::get_stat(tw->stat_cursor, WT_STAT_DSRC_BLOCK_SIZE, &size);

                logger::log_msg(LOG_INFO,
                  log_prefix +
                    "block reuse bytes = " + std::to_string(bytes_avail_reuse / megabyte));
                logger::log_msg(
                  LOG_INFO, log_prefix + "pages_reviewed = " + std::to_string(pages_reviewed));
                logger::log_msg(
                  LOG_INFO, log_prefix + "pages_rewritten = " + std::to_string(pages_rewritten));
                logger::log_msg(LOG_INFO, log_prefix + "size = " + std::to_string(size / megabyte));
            }

            maintenance_window = !maintenance_window;
            tw->sleep();
        }
    }

    void
    read_operation(thread_worker *) override final
    {
        logger::log_msg(LOG_WARN, "read_operation: nothing done");
    }

    void
    remove_operation(thread_worker *tw) override final
    {
        logger::log_msg(
          LOG_INFO, type_string(tw->type) + " thread {" + std::to_string(tw->id) + "} commencing.");

        /*
         * We need two types of cursors. One cursor is a random cursor to randomly select a key
         * and the other one is a standard cursor to remove the random key. This is required as
         * the random cursor does not support the remove operation.
         */
        std::map<uint64_t, scoped_cursor> rnd_cursors, cursors;

        /* Loop while the test is running. */
        while (tw->running()) {
            if (maintenance_window) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            /*
             * Sleep the period defined by the op_rate in the configuration. Do this at the
             * start of the loop as it could be skipped by a subsequent continue call.
             */
            tw->sleep();

            /* Choose a random collection to update. */
            collection &coll = tw->db.get_random_collection();

            /* Look for existing cursors in our cursor cache. */
            if (cursors.find(coll.id) == cursors.end()) {
                logger::log_msg(LOG_TRACE,
                  "Thread {" + std::to_string(tw->id) +
                    "} Creating cursor for collection: " + coll.name);
                /* Open the two cursors for the chosen collection. */
                scoped_cursor rnd_cursor =
                  tw->session.open_scoped_cursor(coll.name, "next_random=true");
                rnd_cursors.emplace(coll.id, std::move(rnd_cursor));
                scoped_cursor cursor = tw->session.open_scoped_cursor(coll.name);
                cursors.emplace(coll.id, std::move(cursor));
            }

            /* Start a transaction if possible. */
            tw->txn.try_begin();

            /* Get the cursor associated with the collection. */
            scoped_cursor &rnd_cursor = rnd_cursors[coll.id];
            scoped_cursor &cursor = cursors[coll.id];

            /* Choose a random key to delete. */
            int ret = rnd_cursor->next(rnd_cursor.get());

            if (ret != 0) {
                /*
                 * It is possible not to find anything if the collection is empty. In that case,
                 * finish the current transaction as we might be able to see new records after
                 * starting a new one.
                 */
                if (ret == WT_NOTFOUND) {
                    WT_IGNORE_RET_BOOL(tw->txn.commit());
                } else if (ret == WT_ROLLBACK) {
                    tw->txn.rollback();
                } else {
                    testutil_die(ret, "Unexpected error returned from cursor->next()");
                }
                testutil_check(rnd_cursor->reset(rnd_cursor.get()));
                continue;
            }

            const char *key_str;
            testutil_check(rnd_cursor->get_key(rnd_cursor.get(), &key_str));

            std::string first_key = key_str;
            uint64_t key_count = coll.get_key_count();
            uint64_t n_keys_to_delete =
              random_generator::instance().generate_integer<uint64_t>(0, key_count / 20);
            std::string end_key = tw->pad_string(
              std::to_string(std::stoi(first_key) + n_keys_to_delete), first_key.size());

            /* If we generate an invalid range or our truncate fails rollback the transaction.
             */
            if (end_key == first_key || !tw->truncate(coll.id, first_key, end_key, "")) {
                tw->txn.rollback();
                continue;
            }
            /* Commit the current transaction if we're able to. */
            if (tw->txn.can_commit()) {
                logger::log_msg(LOG_INFO,
                  type_string(tw->type) + " thread {" + std::to_string(tw->id) +
                    "} committing removed keys from " + first_key + " from table: [" + coll.name +
                    "]");
                WT_IGNORE_RET_BOOL(tw->txn.commit());
            }
            // if (tw->txn.commit())
            //     logger::log_msg(LOG_INFO,
            //       "thread {" + std::to_string(tw->id) + "} committed truncation from " +
            //       first_key +
            //         " to " + end_key + " on table [" + coll.name + "]");
            // else
            //     logger::log_msg(LOG_INFO,
            //       "thread {" + std::to_string(tw->id) + "} failed to commit truncation of " +
            //         std::to_string(std::stoi(end_key) - std::stoi(first_key)) + " records.");

            /* Reset our cursors to avoid pinning content. */
            testutil_check(cursor->reset(cursor.get()));
            testutil_check(rnd_cursor->reset(rnd_cursor.get()));
        }

        /* Make sure the last operation is rolled back now the work is finished. */
        tw->txn.try_rollback();
    }

    void
    update_operation(thread_worker *) override final
    {
        logger::log_msg(LOG_WARN, "update_operation: nothing done");
    }

    void
    insert_operation(thread_worker *tc) override final
    {
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        /* Helper struct which stores a pointer to a collection and a cursor associated with it.
         */
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
        testutil_assert(collection_count != 0);
        uint64_t collections_per_thread = collection_count / tc->thread_count;
        /* Must have unique collections for each thread. */
        testutil_assert(collection_count % tc->thread_count == 0);
        for (int i = tc->id * collections_per_thread;
             i < (tc->id * collections_per_thread) + collections_per_thread && tc->running(); ++i) {
            collection &coll = tc->db.get_collection(i);
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            ccv.push_back({coll, std::move(cursor)});
        }

        uint64_t counter = 0;
        while (tc->running()) {
            if (maintenance_window) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            uint64_t start_key = ccv[counter].coll.get_key_count();
            uint64_t added_count = 0;
            tc->txn.begin();

            /* Collection cursor. */
            auto &cc = ccv[counter];
            while (tc->txn.active() && tc->running()) {
                /* Insert a key value pair, rolling back the transaction if required. */
                auto key = tc->pad_string(std::to_string(start_key + added_count), tc->key_size);
                auto value =
                  random_generator::instance().generate_pseudo_random_string(tc->value_size);
                if (!tc->insert(cc.cursor, cc.coll.id, key, value)) {
                    added_count = 0;
                    tc->txn.rollback();
                } else {
                    added_count++;
                    if (tc->txn.can_commit()) {
                        if (tc->txn.commit()) {
                            /*
                             * We need to inform the database model that we've added these keys
                             * as some other thread may rely on the key_count data. Only do so
                             * if we successfully committed.
                             */
                            cc.coll.increase_key_count(added_count);
                        } else {
                            added_count = 0;
                        }
                    }
                }

                /* Sleep the duration defined by the op_rate. */
                tc->sleep();
            }
            /* Reset our cursor to avoid pinning content. */
            testutil_check(cc.cursor->reset(cc.cursor.get()));
            counter++;
            if (counter == collections_per_thread)
                counter = 0;
            testutil_assert(counter < collections_per_thread);
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        tc->txn.try_rollback();
    }

    void
    validate(const std::string &, const std::string &, database &) override final
    {
        logger::log_msg(LOG_WARN, "validate: nothing done");
    }
};

} // namespace test_harness
