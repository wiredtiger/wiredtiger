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

#include <time.h>

#include "test_harness/core/op_tracker.cpp"
#include "test_harness/test.h"
#include "test_harness/util/api_const.h"
#include "test_harness/workload/random_generator.h"

namespace test_harness {

/*
 * This test performs cursor traversal operations next() and prev() on a collection with both
 * bounded and normal cursors. The performance of both cursors are tracked and the average time
 * taken is added to the perf file. The test traverses all keys in the collection.
 */
class bounded_cursor_perf : public test {
    public:
    bounded_cursor_perf(const test_args &args) : test(args) {}

    void
    read_operation(thread_context *tc) override final
    {
        /* This test will only work with one read thread. */
        testutil_assert(tc->thread_count == 1);
        /*
         * Each read operation performs next() and prev() calls with both normal cursors and bounded
         * cursors.
         */
        int range_ret_next, range_ret_prev, ret_next, ret_prev;

        /* Initialize the op trackers. */
        op_tracker bounded_next("bounded_next", test::_args.test_name);
        op_tracker default_next("default_next", test::_args.test_name);
        op_tracker bounded_prev("bounded_prev", test::_args.test_name);
        op_tracker default_prev("default_prev", test::_args.test_name);

        /* Get the collection to work on. */
        collection &coll = tc->db.get_collection(0);

        /* Opening the cursors. */
        scoped_cursor next_cursor = tc->session.open_scoped_cursor(coll.name);
        scoped_cursor next_range_cursor = tc->session.open_scoped_cursor(coll.name);
        scoped_cursor prev_cursor = tc->session.open_scoped_cursor(coll.name);
        scoped_cursor prev_range_cursor = tc->session.open_scoped_cursor(coll.name);

        /*
         * The keys in the collection are contiguous from 0 -> key_count -1. Applying the range
         * cursor bounds outside of the key range for the purpose of this test.
         */
        next_range_cursor->set_key(next_range_cursor.get(), tc->key_to_string(0).c_str());
        next_range_cursor->bound(next_range_cursor.get(), "bound=lower");
        prev_range_cursor->set_key(prev_range_cursor.get(), tc->key_to_string('0' - 1).c_str());
        prev_range_cursor->bound(prev_range_cursor.get(), "bound=upper");

        while (tc->running()) {
            while (ret_next != WT_NOTFOUND && ret_prev != WT_NOTFOUND && tc->running()) {
                range_ret_next = bounded_next.track([&next_range_cursor]() -> int {
                    return next_range_cursor->next(next_range_cursor.get());
                });
                ret_next = default_next.track(
                  [&next_cursor]() -> int { return next_cursor->next(next_cursor.get()); });

                range_ret_prev = bounded_prev.track([&prev_range_cursor]() -> int {
                    return prev_range_cursor->prev(prev_range_cursor.get());
                });
                ret_prev = default_prev.track(
                  [&prev_cursor]() -> int { return prev_cursor->prev(prev_cursor.get()); });

                int ret = (ret_next == 0 || ret_next == WT_NOTFOUND) &&
                  (ret_prev == 0 || ret_prev == WT_NOTFOUND);
                int range_ret = (range_ret_prev == 0 || range_ret_prev == WT_NOTFOUND) &&
                  (range_ret_next == 0 || range_ret_next == WT_NOTFOUND);
                testutil_assert(ret == 1);
                testutil_assert(range_ret == 1);
            }
            next_range_cursor->set_key(next_range_cursor.get(), tc->key_to_string(0).c_str());
            next_range_cursor->bound(next_range_cursor.get(), "bound=lower");
            prev_range_cursor->set_key(prev_range_cursor.get(), tc->key_to_string('0' - 1).c_str());
            prev_range_cursor->bound(prev_range_cursor.get(), "bound=upper");
        }
    }
};
} // namespace test_harness
