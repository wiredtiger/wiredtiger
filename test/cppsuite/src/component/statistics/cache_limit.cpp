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

#include "cache_limit.h"

#include "src/common/logger.h"
#include "src/component/metrics_monitor.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {

CacheLimit::CacheLimit(Configuration &config, const std::string &name)
    : Statistics(config, name, -1)
{
}

void
CacheLimit::Check(ScopedCursor &cursor)
{
    double cacheUsage = GetCacheUsagePercentage(cursor);
    if (cacheUsage > max) {
        const std::string error =
          "MetricsMonitor: Cache usage exceeded during test! Limit: " + std::to_string(max) +
          " usage: " + std::to_string(cacheUsage);
        testutil_die(-1, error.c_str());
    } else
        Logger::LogMessage(LOG_TRACE, name + " usage: " + std::to_string(cacheUsage));
}

std::string
CacheLimit::GetValueString(ScopedCursor &cursor)
{
    return std::to_string(GetCacheUsagePercentage(cursor));
}

double
CacheLimit::GetCacheUsagePercentage(ScopedCursor &cursor)
{
    int64_t cacheBytesImage, cacheBytesOther, cacheBytesMax;
    /* Three statistics are required to compute cache use percentage. */
    MetricsMonitor::GetStatistics(cursor, WT_STAT_CONN_CACHE_BYTES_IMAGE, &cacheBytesImage);
    MetricsMonitor::GetStatistics(cursor, WT_STAT_CONN_CACHE_BYTES_OTHER, &cacheBytesOther);
    MetricsMonitor::GetStatistics(cursor, WT_STAT_CONN_CACHE_BYTES_MAX, &cacheBytesMax);
    /*
     * Assert that we never exceed our configured limit for cache usage. Add 0.0 to avoid floating
     * point conversion errors.
     */
    testutil_assert(cacheBytesMax > 0);
    double cacheUsage = ((cacheBytesImage + cacheBytesOther + 0.0) / cacheBytesMax) * 100;
    return cacheUsage;
}
} // namespace test_harness
