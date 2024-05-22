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

#include "src/util/execution_timer.h"
#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/main/test.h"

namespace test_harness {

/* Insert a key into the database. */
static void
make_insert(thread_worker *tc, const std::string &id)
{
    std::string cursor_uri = tc->db.get_collection(0).name;

    auto cursor = tc->session.open_scoped_cursor(cursor_uri);
    cursor->set_key(cursor.get(), std::string("key" + id).c_str());
    cursor->set_value(cursor.get(), std::string("value1" + id).c_str());
    testutil_assert(cursor->insert(cursor.get()) == 0);
}

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class session_microbenchmarks : public test {
public:
    session_microbenchmarks(const test_args &args) : test(args)
    {
        init_operation_tracker(NULL);
    }

    void
    checkpoint_operation(thread_worker *) override final
    {
        logger::log_msg(LOG_WARN, "checkpoint_operation: not done as this is a performance test");
    }

    void
    custom_operation(thread_worker *tc) override final
    {
        /* Assert there is only one collection. */
        testutil_assert(tc->collection_count == 1);

        /* Create the necessary timers. */
        execution_timer begin_transaction_timer("begin_transaction", test::_args.test_name);
        execution_timer commit_transaction_timer("commit_transaction", test::_args.test_name);
        execution_timer rollback_transaction_timer("rollback_transaction", test::_args.test_name);
        execution_timer open_cursor_cached_timer(
          "open_cursor_cached", test::_args.test_name, false);
        execution_timer open_cursor_uncached_timer(
          "open_cursor_uncached", test::_args.test_name, false);
        execution_timer timestamp_transaction_uint_timer(
          "timestamp_transaction_uint", test::_args.test_name);
        std::string cursor_uri = tc->db.get_collection(0).name;

        /*
         * Time begin transaction and commit transaction. In order for commit to do work we need at
         * least one modification on the transaction.
         */
        scoped_session &session = tc->session;
        int result;
        for (int i = 0; i < 30; i++) {
            result = begin_transaction_timer.track(
              [&session]() -> int { return session->begin_transaction(session.get(), NULL); });
            testutil_assert(result == 0);

            /* Add the modification. */
            make_insert(tc, std::to_string(i + 1));

            result = commit_transaction_timer.track(
              [&session]() -> int { return session->commit_transaction(session.get(), NULL); });
            testutil_assert(result == 0);
        }

        /* Time rollback transaction. */
        for (int i = 0; i < 30; i++) {
            result = begin_transaction_timer.track(
              [&session]() -> int { return session->begin_transaction(session.get(), NULL); });
            testutil_assert(result == 0);
            result = rollback_transaction_timer.track(
              [&session]() -> int { return session->rollback_transaction(session.get(), NULL); });
            testutil_assert(result == 0);
        }

        /* Time timestamp transaction_uint. */
        testutil_assert(session->begin_transaction(session.get(), NULL) == 0);
        for (int i = 0; i < 30; i++) {
            auto timestamp = tc->tsm->get_next_ts();
            result = timestamp_transaction_uint_timer.track([&session, &timestamp]() -> int {
                return session->timestamp_transaction_uint(
                  session.get(), WT_TS_TXN_TYPE_COMMIT, timestamp);
            });
        }
        testutil_assert(result == 0);
        testutil_assert(session->rollback_transaction(session.get(), NULL) == 0);

        /* Count instructions taken to open a cursor, this should use a cached cursor. */
        WT_CURSOR *cursorp = NULL;
        result = open_cursor_cached_timer.track([&session, &cursor_uri, &cursorp]() -> int {
            return session->open_cursor(session.get(), cursor_uri.c_str(), NULL, NULL, &cursorp);
        });
        testutil_assert(result == 0);

        cursorp->close(cursorp);
        cursorp = NULL;

        /* Count instructions taken to open a cursor without using the cache. */
        session->reconfigure(session.get(), "cache_cursors=false");
        result = open_cursor_uncached_timer.track([&session, &cursor_uri, &cursorp]() -> int {
            return session->open_cursor(session.get(), cursor_uri.c_str(), NULL, NULL, &cursorp);
        });
        testutil_assert(result == 0);

        cursorp->close(cursorp);
        cursorp = NULL;
    }
};

} // namespace test_harness
