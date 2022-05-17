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
#include "test_harness/util/api_const.h"
#include "test_harness/workload/random_generator.h"
#include <time.h>
#include "test_harness/core/op_tracker.cpp"

/* TO-DO LIST
 *   1) Add prev support
 *   2) Create op tracker component
 *   3) Aggregate stats and perf
 *   4) Remove overwritten populate, change key generation to generate random number btwn 0 and
 * key_count, pad with zeroes 5)
 */
namespace test_harness {

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class bounded_cursor_perf : public test {

    enum set_bounds { SET_LOWER, SET_UPPER, SET_ALL };
    std::string test_name;

    public:
    bounded_cursor_perf(const test_args &args) : test(args) {
        this->test_name = args.test_name;
    }

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customize your test. */
        test::run();
    }

    /* Ensures that both cursors are positioned at the same place. */
    void
    cursor_traversal(thread_context *tc, scoped_cursor &cursor,
      scoped_cursor &range_cursor, std::string operation, int set_bounds, uint64_t key,
      op_tracker &bounded, op_tracker &unbounded)
    {

        int exact, ret, range_ret = 0;
        // Move the range cursor first
        // range_ret = range_cursor->search(range_cursor.get());

        if (operation == "next") {
            // Check if lower bounds have been set, then move the default cursor with search_near
            if (set_bounds == SET_LOWER || set_bounds == SET_ALL) {
                cursor->set_key(cursor.get(), tc->key_to_string(key).c_str());
                ret = cursor->search_near(cursor.get(), &exact);
            } else // If only upper bound set, move the default cursor to the start as well
                ret = cursor->next(cursor.get());
        } else {
            if (set_bounds == SET_UPPER || set_bounds == SET_ALL) {
                cursor->set_key(cursor.get(), tc->key_to_string(key).c_str());
                ret = cursor->search_near(cursor.get(), &exact);
            } else // If only lower bound set, move the default cursor to the end
                ret = cursor->prev(cursor.get());
        }

        testutil_assert(ret ==  0 || ret == WT_NOTFOUND);

        // Realign the range cursor in the event an exact match of lower bound is not found
        // (search_near randomly places) range_ret = range_cursor->next(range_cursor.get());
        if ((operation == "next" && exact < 0) || (operation == "prev" && exact < 0))
            range_ret = range_cursor->prev(range_cursor.get());
        else if ((operation == "next" && exact > 0) || (operation == "prev" && exact > 0))
            range_ret = range_cursor->next(range_cursor.get());

        while (range_ret != WT_NOTFOUND) {
            testutil_assert(ret == 0 || ret == WT_NOTFOUND);
            testutil_assert(range_ret == 0 || range_ret == WT_NOTFOUND);

            // Do the timing
            if (operation == "next") {
                range_ret = bounded.track(
                  [&range_cursor]() -> int { return range_cursor->next(range_cursor.get()); });
                ret = unbounded.track([&cursor]() -> int { cursor->next(cursor.get()); });
            } else {
                range_ret = bounded.track(
                  [&range_cursor]() -> int { return range_cursor->prev(range_cursor.get()); });
                ret = unbounded.track([&cursor]() -> int { return cursor->prev(cursor.get()); });
            }
        }
    }

    void
    read_operation(thread_context *tc) override final
    {

        /* This test will only work with one read thread. */
        testutil_assert(tc->thread_count == 1);
        /*
         * Each read operation performs next() and prev() calls with both normal cursors and bounded
         * cursors. Each read operation is timed and the performance stats get sent to perf tracking
         * component [].
         */

        int range_ret = 0;

        // Initialise the op trackers
        op_tracker bounded_next("bounded_next", this->test_name);
        op_tracker default_next("default_next", this->test_name);
        op_tracker bounded_prev("bounded_prev", this->test_name);
        op_tracker default_prev("default_prev", this->test_name);

        while (tc->running()) {
            /* Get a random collection to work on. */
            collection &coll = tc->db.get_random_collection();
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            scoped_cursor range_cursor = tc->session.open_scoped_cursor(coll.name);

            int set_bounds = random_generator::instance().generate_integer(0, 2);
            int ret = 0;
            uint64_t lower_key, upper_key;

            if (set_bounds == SET_LOWER || set_bounds == SET_ALL) {
                lower_key = random_generator::instance().generate_integer<uint64_t>(
                  0, coll.get_key_count() - 1);
                range_cursor->bound(range_cursor.get(), "bound=lower");
                range_cursor->set_key(range_cursor.get(), std::to_string(lower_key).c_str());
            }
            if (set_bounds == SET_UPPER || set_bounds == SET_ALL) {
                upper_key = random_generator::instance().generate_integer<uint64_t>(
                  0, coll.get_key_count() - 1);
                range_cursor->bound(range_cursor.get(), "bound=upper");
                range_cursor->set_key(range_cursor.get(), std::to_string(upper_key).c_str());
            }

            // if (ret == EINVAL)
            //     continue;

            // Position the cursors for next
            cursor_traversal(
              tc, cursor, range_cursor, "next", set_bounds, lower_key, bounded_next, default_next);
            cursor_traversal(
              tc, cursor, range_cursor, "prev", set_bounds, upper_key, bounded_prev, default_prev);

            testutil_assert(ret == 0 || ret == WT_NOTFOUND);
            testutil_assert(range_ret == 0 || range_ret == WT_NOTFOUND);

        }
    }

};
} // namespace test_harness
