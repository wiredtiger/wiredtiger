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
#include "src/util/instruction_counter.h"
#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/main/test.h"

namespace test_harness {
/*
 * Benchmark various frequently called session and cursor APIs, measuring both wall-clock time and
 * (on Linux) hardware instruction counts.
 *
 * Each operation is measured with an execution_timer (all platforms) and an instruction_counter
 * (Linux only, on other platforms instruction_counter is a no-op stub with the same interface).
 * Both counters are applied via nesting so the API under test is called exactly once per
 * measurement point.
 *
 * The test has measures in place to prevent background threads from taking resources:
 *  - Sweep server interval set greater than the test duration.
 *  - Logging and the log manager thread are disabled per the connection open configuration.
 *  - Prefetch is off by default.
 *  - Background compact, capacity server, checkpoint server, and checkpoint cleanup are all
 *    disabled by in_memory mode.
 *  - Eviction still runs but ideally we don't cross any threshold to trigger it.
 *
 * Additionally, to avoid I/O the connection is set to in_memory.
 */
class api_benchmarks : public test {
public:
    api_benchmarks(const test_args &args) : test(args)
    {
        init_operation_tracker(nullptr);
    }

    void
    custom_operation(thread_worker *tc) override final
    {
        /* The test expects exactly one collection. */
        testutil_assert(tc->collection_count == 1);

        /* Assert that we are running in memory. */
        testutil_assert(_config->get_bool(IN_MEMORY));

        execution_timer begin_transaction_timer("begin_transaction", test::_args.test_name);
        execution_timer commit_transaction_timer("commit_transaction", test::_args.test_name);
        execution_timer cursor_insert_timer("cursor_insert", test::_args.test_name);
        execution_timer cursor_modify_timer("cursor_modify", test::_args.test_name);
        execution_timer cursor_remove_timer("cursor_remove", test::_args.test_name);
        execution_timer cursor_reset_timer("cursor_reset", test::_args.test_name);
        execution_timer cursor_search_timer("cursor_search", test::_args.test_name);
        execution_timer cursor_update_timer("cursor_update", test::_args.test_name);
        execution_timer open_cursor_cached_timer("open_cursor_cached", test::_args.test_name);
        execution_timer open_cursor_uncached_timer("open_cursor_uncached", test::_args.test_name);
        execution_timer rollback_transaction_timer("rollback_transaction", test::_args.test_name);
        execution_timer timestamp_transaction_uint_timer(
          "timestamp_transaction_uint", test::_args.test_name);

        instruction_counter begin_transaction_ic("begin_transaction", test::_args.test_name);
        instruction_counter commit_transaction_ic("commit_transaction", test::_args.test_name);
        instruction_counter cursor_insert_ic("cursor_insert", test::_args.test_name);
        instruction_counter cursor_modify_ic("cursor_modify", test::_args.test_name);
        instruction_counter cursor_remove_ic("cursor_remove", test::_args.test_name);
        instruction_counter cursor_reset_ic("cursor_reset", test::_args.test_name);
        instruction_counter cursor_search_ic("cursor_search", test::_args.test_name);
        instruction_counter cursor_update_ic("cursor_update", test::_args.test_name);
        instruction_counter open_cursor_cached_ic("open_cursor_cached", test::_args.test_name);
        instruction_counter open_cursor_uncached_ic("open_cursor_uncached", test::_args.test_name);
        instruction_counter rollback_transaction_ic("rollback_transaction", test::_args.test_name);
        instruction_counter timestamp_transaction_uint_ic(
          "timestamp_transaction_uint", test::_args.test_name);

        collection &coll = tc->db.get_collection(0);
        scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);

        /*
         * We don't want to measure the getter overhead. For consistency only use wt_cursor and
         * wt_session from this point forward.
         */
        WT_CURSOR *wt_cursor = cursor.get();
        WT_SESSION *wt_session = tc->session.get();

        std::string key = tc->pad_string(std::to_string(coll.get_key_count() - 1), tc->key_size);

        /* Benchmark cursor->search and cursor->reset. */
        wt_cursor->set_key(wt_cursor, key.c_str());
        testutil_check(cursor_search_timer.track([&]() -> int {
            return cursor_search_ic.track([&]() -> int { return wt_cursor->search(wt_cursor); });
        }));
        testutil_check(cursor_reset_timer.track([&]() -> int {
            return cursor_reset_ic.track([&]() -> int { return wt_cursor->reset(wt_cursor); });
        }));

        /* Benchmark session->begin_transaction. */
        testutil_check(begin_transaction_timer.track([&]() -> int {
            return begin_transaction_ic.track(
              [&]() -> int { return wt_session->begin_transaction(wt_session, nullptr); });
        }));

        /* Benchmark session->timestamp_transaction_uint. */
        auto timestamp = tc->tsm->get_next_ts();
        testutil_check(timestamp_transaction_uint_timer.track([&]() -> int {
            return timestamp_transaction_uint_ic.track([&]() -> int {
                return wt_session->timestamp_transaction_uint(
                  wt_session, WT_TS_TXN_TYPE_COMMIT, timestamp);
            });
        }));

        /* Benchmark session->rollback_transaction. */
        testutil_check(rollback_transaction_timer.track([&]() -> int {
            return rollback_transaction_ic.track(
              [&]() -> int { return wt_session->rollback_transaction(wt_session, nullptr); });
        }));

        /* Begin a transaction that we will later commit. */
        testutil_check(wt_session->begin_transaction(wt_session, nullptr));

        /*
         * Search before making modifications to avoid triggering a search internally on the
         * update/modify/remove operations below.
         */
        wt_cursor->set_key(wt_cursor, key.c_str());
        testutil_check(wt_cursor->search(wt_cursor));

        /*
         * Benchmark cursor->update. We need to be careful here: setting a key on the cursor
         * triggers a fresh search from root, so we rely on the search above to position the cursor
         * first.
         */
        wt_cursor->set_value(wt_cursor, "b");
        testutil_check(cursor_update_timer.track([&]() -> int {
            return cursor_update_ic.track([&]() -> int { return wt_cursor->update(wt_cursor); });
        }));

        /*
         * Benchmark session->commit_transaction. We need at least one modification on the
         * transaction to actually commit.
         */
        testutil_check(commit_transaction_timer.track([&]() -> int {
            return commit_transaction_ic.track(
              [&]() -> int { return wt_session->commit_transaction(wt_session, nullptr); });
        }));

        /* Re-search to position the cursor for modify. */
        wt_cursor->set_key(wt_cursor, key.c_str());
        testutil_check(wt_cursor->search(wt_cursor));

        /* Benchmark cursor->modify. Again we position with a search to avoid measuring search. */
        testutil_check(wt_session->begin_transaction(wt_session, nullptr));
        WT_MODIFY mod;
        mod.data.data = "c";
        mod.data.size = 1;
        mod.offset = 0;
        mod.size = mod.data.size;
        testutil_check(cursor_modify_timer.track([&]() -> int {
            return cursor_modify_ic.track(
              [&]() -> int { return wt_cursor->modify(wt_cursor, &mod, 1); });
        }));
        testutil_check(wt_session->rollback_transaction(wt_session, nullptr));

        /* Re-search. Set up overwrite so cursor->insert doesn't trigger an internal search. */
        testutil_check(wt_cursor->reconfigure(wt_cursor, "overwrite=true"));
        wt_cursor->set_key(wt_cursor, key.c_str());
        testutil_check(wt_cursor->search(wt_cursor));

        /* Benchmark cursor->insert with overwrite=true to avoid an internal search. */
        wt_cursor->set_value(wt_cursor, "a");
        testutil_check(cursor_insert_timer.track([&]() -> int {
            return cursor_insert_ic.track([&]() -> int { return wt_cursor->insert(wt_cursor); });
        }));

        /* Re-search to position the cursor for remove. */
        wt_cursor->set_key(wt_cursor, key.c_str());
        testutil_check(wt_cursor->search(wt_cursor));

        /* Benchmark cursor->remove. We are positioned from the search above. */
        testutil_check(cursor_remove_timer.track([&]() -> int {
            return cursor_remove_ic.track([&]() -> int { return wt_cursor->remove(wt_cursor); });
        }));

        /* Benchmark session->open_cursor without cursor cache. */
        WT_CURSOR *cursorp = nullptr;
        const char *cursor_uri = tc->db.get_collection(0).name.c_str();
        testutil_check(wt_session->reconfigure(wt_session, "cache_cursors=false"));
        testutil_check(open_cursor_uncached_timer.track([&]() -> int {
            return open_cursor_uncached_ic.track([&]() -> int {
                return wt_session->open_cursor(wt_session, cursor_uri, nullptr, nullptr, &cursorp);
            });
        }));
        testutil_check(wt_session->reconfigure(wt_session, "cache_cursors=true"));
        testutil_check(cursorp->close(cursorp));
        cursorp = nullptr;

        /* Open and close a cursor to seed the cursor cache. */
        testutil_check(wt_session->open_cursor(wt_session, cursor_uri, nullptr, nullptr, &cursorp));
        testutil_check(cursorp->close(cursorp));
        cursorp = nullptr;

        /* Benchmark session->open_cursor from the cursor cache. */
        testutil_check(open_cursor_cached_timer.track([&]() -> int {
            return open_cursor_cached_ic.track([&]() -> int {
                return wt_session->open_cursor(wt_session, cursor_uri, nullptr, nullptr, &cursorp);
            });
        }));
        testutil_check(cursorp->close(cursorp));
        cursorp = nullptr;

        /*
         * Loop the timer.track sites to gather enough samples for a stable signal.
         * instruction_counter is deterministic on Linux and overwrites its stored count on each
         * call, so looping the counter adds no information.
         */
        constexpr int LOOP_COUNTER = 1000;
        auto key_count = coll.get_key_count();

        for (int i = 0; i < LOOP_COUNTER / 10; i++) {
            testutil_check(begin_transaction_timer.track(
              [&]() -> int { return wt_session->begin_transaction(wt_session, nullptr); }));
            auto loop_key = tc->pad_string(std::to_string(key_count + i), tc->key_size);
            if (!tc->insert(cursor, 0, loop_key, "a")) {
                i--;
                testutil_check(wt_session->rollback_transaction(wt_session, nullptr));
                continue;
            }
            testutil_check(commit_transaction_timer.track(
              [&]() -> int { return wt_session->commit_transaction(wt_session, nullptr); }));
        }

        for (int i = 0; i < LOOP_COUNTER; i++) {
            testutil_check(begin_transaction_timer.track(
              [&]() -> int { return wt_session->begin_transaction(wt_session, nullptr); }));
            testutil_check(rollback_transaction_timer.track(
              [&]() -> int { return wt_session->rollback_transaction(wt_session, nullptr); }));
        }

        testutil_check(wt_session->begin_transaction(wt_session, nullptr));
        for (int i = 0; i < LOOP_COUNTER; i++) {
            auto timestamp = tc->tsm->get_next_ts();
            testutil_check(timestamp_transaction_uint_timer.track([&]() -> int {
                return wt_session->timestamp_transaction_uint(
                  wt_session, WT_TS_TXN_TYPE_COMMIT, timestamp);
            }));
        }
        testutil_check(wt_session->rollback_transaction(wt_session, nullptr));
    }
};

} // namespace test_harness
