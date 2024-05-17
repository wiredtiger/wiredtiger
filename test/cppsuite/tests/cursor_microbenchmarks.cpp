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
#include "src/util/execution_timer.h"

namespace test_harness {
/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
/*
 * This test aims to measure the number of instructions cursor API calls take. The test has measures
 * in place to prevent background threads from taking resources:
 *  - We set the sweep server interval to be greater than the test duration. This means it never
 *    triggers.
 *  - Logging, and the log manager thread are disabled per the connection open configuration.
 *  - Prefetch, off by default
 *  - Background compact, disabled by in_memory.
 *  - capacity server, disabled by in_memory.
 *  - checkpoint server, disabled by in_memory.
 *  - Eviction
 *  - checkpoint cleanup, disabled by in_memory.
 *
 * Additionally to avoid I/O the connection is set to in_memory.
 */
class cursor_microbenchmarks : public test {
public:
    cursor_microbenchmarks(const test_args &args) : test(args)
    {
        init_operation_tracker();
    }

    void
    checkpoint_operation(thread_worker *) override final
    {
        logger::log_msg(LOG_WARN, "Skipping checkpoint as this a performance test.");
    }

    void
    custom_operation(thread_worker *tc) override final
    {
        /* The test expects no more than one collection. */
        testutil_assert(tc->collection_count == 1);

        /* Assert that we are running in memory. */
        testutil_assert(_config->get_bool(IN_MEMORY));

        /* Test a single cursor insertion. */
        execution_timer cursor_insert_timer("cursor_insert", test::_args.test_name, false);
        execution_timer cursor_update_timer("cursor_update", test::_args.test_name, false);
        execution_timer cursor_modify_timer("cursor_modify", test::_args.test_name, false);
        execution_timer cursor_remove_timer("cursor_remove", test::_args.test_name, false);
        execution_timer cursor_reset_timer("cursor_reset", test::_args.test_name);
        execution_timer cursor_search_timer("cursor_search", test::_args.test_name);

        collection &coll = tc->db.get_collection(0);
        scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
        auto key_count = coll.get_key_count();
        tc->session->begin_transaction(tc->session.get(), NULL);

        /* Benchmark cursor->search. */
        std::string key;
        int ret;
        for (int i = 0; i < 1000; i++) {
            key = tc->pad_string(std::to_string(key_count - 1), tc->key_size);
            cursor->set_key(cursor.get(), key.c_str());
            ret = cursor_search_timer.track(
              [&cursor]() -> int { return cursor->search(cursor.get()); });
            testutil_assert(ret == 0);
            ret =
              cursor_reset_timer.track([&cursor]() -> int { return cursor->reset(cursor.get()); });
            testutil_assert(ret == 0);
        }

        /* Re-search. */
        cursor->set_key(cursor.get(), key.c_str());
        cursor->search(cursor.get());

        /*
         * Benchmark cursor->update.
         *
         * We need to be careful here, if we don't call search before this we will unintentionally
         * be benchmarking search + update. Additionally setting a key on the cursor will trigger a
         * fresh search from the root.
         */
        cursor->set_value(cursor.get(), "b");
        ret =
          cursor_update_timer.track([&cursor]() -> int { return cursor->update(cursor.get()); });
        testutil_assert(ret == 0);

        /* Re-search. */
        cursor->set_key(cursor.get(), key.c_str());
        cursor->search(cursor.get());

        /*
         * Benchmark cursor->modify. Again we've positioned using a search to avoid searching,
         * internally.
         */
        WT_MODIFY mod;
        mod.data.data = "c";
        mod.data.size = 1;
        mod.offset = 0;
        mod.size = mod.data.size;
        ret = cursor_modify_timer.track(
          [&cursor, &mod]() -> int { return cursor->modify(cursor.get(), &mod, 1); });
        testutil_assert(ret == 0);

        /* Re-search. */
        testutil_assert(cursor->reconfigure(cursor.get(), "overwrite=true") == 0);
        cursor->set_key(cursor.get(), key.c_str());
        testutil_assert(cursor->search(cursor.get()) == 0);

        /*
         * Benchmark cursor->insert. Provide the overwrite configuration to avoid triggering a
         * search.
         */
        cursor->set_value(cursor.get(), "a");
        ret =
          cursor_insert_timer.track([&cursor]() -> int { return cursor->insert(cursor.get()); });
        testutil_assert(ret == 0);

        cursor->set_key(cursor.get(), key.c_str());
        testutil_assert(cursor->search(cursor.get()) == 0);

        /*
         * Benchmark cursor->remove. Again we've positioned using a search to avoid searching,
         * internally.
         */
        ret =
          cursor_remove_timer.track([&cursor]() -> int { return cursor->remove(cursor.get()); });
        testutil_assert(ret == 0);
    }
};

} // namespace test_harness
