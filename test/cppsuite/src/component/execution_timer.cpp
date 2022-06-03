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

#include "execution_timer.h"

#include "metrics_writer.h"

namespace test_harness {
ExecutionTimer::ExecutionTimer(const std::string id, const std::string &testname)
    : _id(id), _testName(testname), _iterationCount(0), _totalClockTime(0)
{
}

void
ExecutionTimer::AppendStatistics()
{
    uint64_t avg = (uint64_t)_totalClockTime / _iterationCount;
    std::string stat = "{\"name\":\"" + _id + "\",\"value\":" + std::to_string(avg) + "}";
    MetricsWriter::GetInstance().AddStatistics(stat);
}

template <typename T>
auto
ExecutionTimer::Track(T lambda)
{
    auto start = std::chrono::steady_clock::now();
    int ret = lambda();
    auto end = std::chrono::steady_clock::now();
    _totalClockTime += (end - start).count();
    _iterationCount += 1;

    return ret;
}

ExecutionTimer::~ExecutionTimer()
{
    if (_iterationCount != 0)
        AppendStatistics();
}
}; // namespace test_harness
