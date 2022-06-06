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
class BoundedCursorPerf : public Test {
    public:
    BoundedCursorPerf(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    static void
    SetBounds(ScopedCursor &cursor)
    {
        std::string lower_bound(1, ('0' - 1));
        cursor->set_key(cursor.Get(), lower_bound.c_str());
        cursor->bound(cursor.Get(), "bound=lower");
        std::string upper_bound(1, ('9' + 1));
        cursor->set_key(cursor.Get(), upper_bound.c_str());
        cursor->bound(cursor.Get(), "bound=upper");
    }

    void
    ReadOperation(thread_worker *threadWorker) override final
    {
        /* This test will only work with one read thread. */
        testutil_assert(threadWorker->thread_count == 1);
        /*
         * Each read operation performs next() and prev() calls with both normal cursors and bounded
         * cursors.
         */
        int rangeRetNext, rangeRetPrev, retNext, retPrev;

        /* Initialize the different timers for each function. */
        ExecutionTimer boundedNext("boundedNext", Test::_args.testName);
        ExecutionTimer defaultNext("defaultNext", Test::_args.testName);
        ExecutionTimer boundedPrev("boundedPrev", Test::_args.testName);
        ExecutionTimer defaultPrev("defaultPrev", Test::_args.testName);

        /* Get the collection to work on. */
        testutil_assert(threadWorker->collection_count == 1);
        Collection &coll = threadWorker->db.GetCollection(0);

        /* Opening the cursors. */
        ScopedCursor nextCursor = threadWorker->session.OpenScopedCursor(coll.name);
        ScopedCursor nextRangeCursor = threadWorker->session.OpenScopedCursor(coll.name);
        ScopedCursor prevCursor = threadWorker->session.OpenScopedCursor(coll.name);
        ScopedCursor prevRangeCursor = threadWorker->session.OpenScopedCursor(coll.name);

        /*
         * The keys in the collection are contiguous from 0 -> key_count -1. Applying the range
         * cursor bounds outside of the key range for the purpose of this test.
         */
        SetBounds(nextRangeCursor);
        SetBounds(prevRangeCursor);

        while (threadWorker->running()) {
            while (retNext != WT_NOTFOUND && retPrev != WT_NOTFOUND && threadWorker->running()) {
                rangeRetNext = boundedNext.Track([&nextRangeCursor]() -> int {
                    return nextRangeCursor->next(nextRangeCursor.Get());
                });
                retNext = defaultNext.Track(
                  [&nextCursor]() -> int { return nextCursor->next(nextCursor.Get()); });

                rangeRetPrev = boundedPrev.Track([&prevRangeCursor]() -> int {
                    return prevRangeCursor->prev(prevRangeCursor.Get());
                });
                retPrev = defaultPrev.Track(
                  [&prevCursor]() -> int { return prevCursor->prev(prevCursor.Get()); });

                testutil_assert((retNext == 0 || retNext == WT_NOTFOUND) &&
                  (retPrev == 0 || retPrev == WT_NOTFOUND));
                testutil_assert((rangeRetPrev == 0 || rangeRetPrev == WT_NOTFOUND) &&
                  (rangeRetNext == 0 || rangeRetNext == WT_NOTFOUND));
            }
            SetBounds(nextRangeCursor);
            SetBounds(prevRangeCursor);
        }
    }
};
