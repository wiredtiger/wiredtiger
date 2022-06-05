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

#include "src/component/execution_timer.cpp"
#include "src/main/test.h"

using namespace test_harness;

/*
 * This test performs cursor traversal operations next() and prev() on a collection with both
 * bounded and normal cursors. The performance of both cursors are tracked and the average time
 * taken is added to the perf file. The test traverses all keys in the collection.
 */
class bounded_cursor_perf : public Test {
    public:
    bounded_cursor_perf(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    static void
    set_bounds(ScopedCursor &cursor)
    {
        std::string lower_bound(1, ('0' - 1));
        cursor->set_key(cursor.Get(), lower_bound.c_str());
        cursor->bound(cursor.Get(), "bound=lower");
        std::string upper_bound(1, ('9' + 1));
        cursor->set_key(cursor.Get(), upper_bound.c_str());
        cursor->bound(cursor.Get(), "bound=upper");
    }

    void
    ReadOperation(thread_worker *tc) override final
    {
        /* This test will only work with one read thread. */
        testutil_assert(tc->thread_count == 1);
        /*
         * Each read operation performs next() and prev() calls with both normal cursors and bounded
         * cursors.
         */
        int range_ret_next, range_ret_prev, ret_next, ret_prev;

        /* Initialize the different timers for each function. */
        ExecutionTimer bounded_next("bounded_next", Test::_args.testName);
        ExecutionTimer default_next("default_next", Test::_args.testName);
        ExecutionTimer bounded_prev("bounded_prev", Test::_args.testName);
        ExecutionTimer default_prev("default_prev", Test::_args.testName);

        /* Get the collection to work on. */
        testutil_assert(tc->collection_count == 1);
        Collection &coll = tc->db.GetCollection(0);

        /* Opening the cursors. */
        ScopedCursor next_cursor = tc->session.open_scoped_cursor(coll.name);
        ScopedCursor next_range_cursor = tc->session.open_scoped_cursor(coll.name);
        ScopedCursor prev_cursor = tc->session.open_scoped_cursor(coll.name);
        ScopedCursor prev_range_cursor = tc->session.open_scoped_cursor(coll.name);

        /*
         * The keys in the collection are contiguous from 0 -> key_count -1. Applying the range
         * cursor bounds outside of the key range for the purpose of this test.
         */
        set_bounds(next_range_cursor);
        set_bounds(prev_range_cursor);

        while (tc->running()) {
            while (ret_next != WT_NOTFOUND && ret_prev != WT_NOTFOUND && tc->running()) {
                range_ret_next = bounded_next.Track([&next_range_cursor]() -> int {
                    return next_range_cursor->next(next_range_cursor.Get());
                });
                ret_next = default_next.Track(
                  [&next_cursor]() -> int { return next_cursor->next(next_cursor.Get()); });

                range_ret_prev = bounded_prev.Track([&prev_range_cursor]() -> int {
                    return prev_range_cursor->prev(prev_range_cursor.Get());
                });
                ret_prev = default_prev.Track(
                  [&prev_cursor]() -> int { return prev_cursor->prev(prev_cursor.Get()); });

                testutil_assert((ret_next == 0 || ret_next == WT_NOTFOUND) &&
                  (ret_prev == 0 || ret_prev == WT_NOTFOUND));
                testutil_assert((range_ret_prev == 0 || range_ret_prev == WT_NOTFOUND) &&
                  (range_ret_next == 0 || range_ret_next == WT_NOTFOUND));
            }
            set_bounds(next_range_cursor);
            set_bounds(prev_range_cursor);
        }
    }
};
