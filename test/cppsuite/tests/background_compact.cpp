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

        /* Make sure that thread statistics cursor is null before we open it. */
        testutil_assert(tw->stat_cursor.get() == nullptr);

        collection &coll = tw->db.get_collection(tw->id);
        std::string uri = STATISTICS_URI + coll.name;

        logger::log_msg(LOG_INFO, "custom thread uri: " + uri);
        tw->stat_cursor = tw->session.open_scoped_cursor(uri);

        while (tw->running()) {
            metrics_monitor::get_stat(
              tw->stat_cursor, WT_STAT_DSRC_BLOCK_REUSE_BYTES, &bytes_avail_reuse);
            metrics_monitor::get_stat(
              tw->stat_cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED, &pages_reviewed);
            metrics_monitor::get_stat(
              tw->stat_cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REWRITTEN, &pages_rewritten);
            metrics_monitor::get_stat(tw->stat_cursor, WT_STAT_DSRC_BLOCK_SIZE, &size);

            logger::log_msg(LOG_INFO,
              log_prefix + "block reuse bytes = " + std::to_string(bytes_avail_reuse / megabyte));
            logger::log_msg(
              LOG_INFO, log_prefix + "pages_reviewed = " + std::to_string(pages_reviewed));
            logger::log_msg(
              LOG_INFO, log_prefix + "pages_rewrittenn = " + std::to_string(pages_rewritten));
            logger::log_msg(LOG_INFO, log_prefix + "size = " + std::to_string(size / megabyte));
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
         * We need two types of cursors. One cursor is a random cursor to randomly select a key and
         * the other one is a standard cursor to remove the random key. This is required as the
         * random cursor does not support the remove operation.
         */
        std::map<uint64_t, scoped_cursor> rnd_cursors, cursors;

        /* Loop while the test is running. */
        while (tw->running()) {
            /*
             * Sleep the period defined by the op_rate in the configuration. Do this at the start of
             * the loop as it could be skipped by a subsequent continue call.
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

            /* If we generate an invalid range or our truncate fails rollback the transaction. */
            if (end_key == first_key || !tw->truncate(coll.id, first_key, end_key, "")) {
                tw->txn.rollback();
                continue;
            }
            if (tw->txn.commit())
                logger::log_msg(LOG_INFO,
                  "thread {" + std::to_string(tw->id) + "} committed truncation from " + first_key +
                    " to " + end_key + " on table [" + coll.name + "]");
            else
                logger::log_msg(LOG_INFO,
                  "thread {" + std::to_string(tw->id) + "} failed to commit truncation of " +
                    std::to_string(std::stoi(end_key) - std::stoi(first_key)) + " records.");

            /* Reset our cursors to avoid pinning content. */
            testutil_check(cursor->reset(cursor.get()));
            testutil_check(rnd_cursor->reset(rnd_cursor.get()));

            /* Commit the current transaction if we're able to. */
            if (tw->txn.can_commit()) {
                logger::log_msg(LOG_INFO,
                  type_string(tw->type) + " thread {" + std::to_string(tw->id) +
                    "} committing removed keys from " + first_key + " from table: [" + coll.name +
                    "]");
                WT_IGNORE_RET_BOOL(tw->txn.commit());
            }
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
    validate(const std::string &, const std::string &, database &) override final
    {
        logger::log_msg(LOG_WARN, "validate: nothing done");
    }
};

} // namespace test_harness
