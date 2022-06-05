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

#include "statistics.h"

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/component/metrics_monitor.h"

namespace test_harness {

Statistics::Statistics(Configuration &config, const std::string &statName, int statField)
    : field(statField), max(config.GetInt(maxConfig)), min(config.GetInt(minConfig)),
      name(statName), postrun(config.GetBool(postrunStatistics)),
      runtime(config.GetBool(runtimeStatistics)), save(config.GetBool(saveConfig))
{
}

void
Statistics::Check(ScopedCursor &cursor)
{
    int64_t value;
    MetricsMonitor::GetStatistics(cursor, field, &value);
    if (value < min || value > max) {
        const std::string error = "MetricsMonitor: Postrun stat \"" + name +
          "\" was outside of the specified limits. Min=" + std::to_string(min) +
          " Max=" + std::to_string(max) + " Actual=" + std::to_string(value);
        testutil_die(-1, error.c_str());
    } else
        Logger::LogMessage(LOG_TRACE, name + " usage: " + std::to_string(value));
}

std::string
Statistics::GetValueString(ScopedCursor &cursor)
{
    int64_t value;
    MetricsMonitor::GetStatistics(cursor, field, &value);
    return std::to_string(value);
}

int
Statistics::GetField() const
{
    return field;
}

int64_t
Statistics::GetMax() const
{
    return max;
}

int64_t
Statistics::GetMin() const
{
    return min;
}

const std::string &
Statistics::GetName() const
{
    return name;
}

bool
Statistics::IsPostRunCheckEnabled() const
{
    return postrun;
}

bool
Statistics::IsRuntimeCheckEnabled() const
{
    return runtime;
}

bool
Statistics::IsSaveEnabled() const
{
    return save;
}
} // namespace test_harness
