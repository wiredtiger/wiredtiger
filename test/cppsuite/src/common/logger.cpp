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

#include "logger.h"

#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "test_util.h"
}

/* Define helpful functions related to debugging. */
namespace test_harness {
/* Order of elements in this vector corresponds to the definitions in the header. */
const std::vector<std::string> loggingLevels = {"ERROR", "WARN", "INFO", "TRACE"};

/* Mutex used by logger to synchronize printing. */
static std::mutex _loggerMutex;

/* Set default log level. */
int64_t Logger::traceLevel = LOG_WARN;

void
GetTime(char *timeBuffer, size_t bufferSize)
{
    size_t allocatedSize;
    struct tm *tm, _tm;

    /* Get time since epoch in nanoseconds. */
    uint64_t epochNano = std::chrono::high_resolution_clock::now().time_since_epoch().count();

    /* Calculate time since epoch in seconds. */
    time_t timeEpochSecs = epochNano / WT_BILLION;

    tm = localtime_r(&timeEpochSecs, &_tm);
    testutil_assert(tm != nullptr);

    allocatedSize = strftime(timeBuffer, bufferSize, "[%Y-%m-%dT%H:%M:%S", tm);

    testutil_assert(allocatedSize <= bufferSize);
    WT_IGNORE_RET(__wt_snprintf(&timeBuffer[allocatedSize], bufferSize - allocatedSize,
      ".%" PRIu64 "Z]", (uint64_t)epochNano % WT_BILLION));
}

/* Used to print out traces for debugging purpose. */
void
Logger::LogMessage(int64_t traceType, const std::string &str)
{
    if (Logger::traceLevel >= traceType) {
        testutil_assert(traceType >= LOG_ERROR && traceType < loggingLevels.size());

        char timeBuffer[64];
        GetTime(timeBuffer, sizeof(timeBuffer));

        std::ostringstream ss;
        ss << timeBuffer << "[TID:" << std::this_thread::get_id() << "]["
           << loggingLevels[traceType] << "]: " << str << std::endl;

        std::lock_guard<std::mutex> lg(_loggerMutex);
        if (traceType == LOG_ERROR)
            std::cerr << ss.str();
        else
            std::cout << ss.str();
    }
}
} // namespace test_harness
