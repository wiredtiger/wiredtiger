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

#include "metrics_monitor.h"

#include <fstream>

#include "metrics_writer.h"
#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/storage/connection_manager.h"
#include "statistics/cache_limit.h"
#include "statistics/database_size.h"
#include "statistics/statistics.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {

/*
 * The WiredTiger configuration API doesn't accept string statistic names when retrieving statistic
 * values. This function provides the required mapping to statistic id. We should consider
 * generating it programmatically in `stat.py` to avoid having to manually add a condition every
 * time we want to observe a new postrun statistic.
 */
inline int
GetStatisticsField(const std::string &name)
{
    if (name == cacheHsInsert)
        return (WT_STAT_CONN_CACHE_HS_INSERT);
    else if (name == ccPagesRemoved)
        return (WT_STAT_CONN_CC_PAGES_REMOVED);
    testutil_die(EINVAL, "get_stat_field: Stat \"%s\" is unrecognized", name.c_str());
}

void
MetricsMonitor::GetStatistics(scoped_cursor &cursor, int statisticsField, int64_t *valuep)
{
    const char *desc, *pvalue;
    cursor->set_key(cursor.get(), statisticsField);
    testutil_check(cursor->search(cursor.get()));
    testutil_check(cursor->get_value(cursor.get(), &desc, &pvalue, valuep));
    testutil_check(cursor->reset(cursor.get()));
}

MetricsMonitor::MetricsMonitor(
  const std::string &testName, configuration *config, database &database)
    : Component(metricsMonitor, config), _testName(testName), _database(database)
{
}

void
MetricsMonitor::Load()
{
    /* Load the general component things. */
    Component::Load();

    /* If the component is enabled, load all the known statistics. */
    if (_enabled) {

        std::unique_ptr<configuration> stat_config(_config->get_subconfig(statisticsCacheSize));
        _stats.push_back(
          std::unique_ptr<CacheLimit>(new CacheLimit(*stat_config, statisticsCacheSize)));

        stat_config.reset(_config->get_subconfig(statisticsDatabaseSize));
        _stats.push_back(std::unique_ptr<DatabaseSize>(
          new DatabaseSize(*stat_config, statisticsDatabaseSize, _database)));

        stat_config.reset(_config->get_subconfig(cacheHsInsert));
        _stats.push_back(std::unique_ptr<Statistics>(
          new Statistics(*stat_config, cacheHsInsert, GetStatisticsField(cacheHsInsert))));

        stat_config.reset(_config->get_subconfig(ccPagesRemoved));
        _stats.push_back(std::unique_ptr<Statistics>(
          new Statistics(*stat_config, ccPagesRemoved, GetStatisticsField(ccPagesRemoved))));

        /* Open our statistic cursor. */
        _session = connection_manager::instance().create_session();
        _cursor = _session.open_scoped_cursor(statisticsURI);
    }
}

void
MetricsMonitor::DoWork()
{
    /* Check runtime statistics. */
    for (const auto &stat : _stats) {
        if (stat->IsRuntimeCheckEnabled())
            stat->Check(_cursor);
    }
}

void
MetricsMonitor::Finish()
{
    Component::Finish();

    bool success = true;

    for (const auto &stat : _stats) {

        const std::string statisticsName = stat->GetName();

        /* Append stats to the statistics writer if it needs to be saved. */
        if (stat->IsSaveEnabled()) {
            auto json = "{\"name\":\"" + statisticsName +
              "\",\"value\":" + stat->GetValueString(_cursor) + "}";
            metrics_writer::instance().add_stat(json);
        }

        if (!stat->IsPostRunCheckEnabled())
            continue;

        int64_t max = stat->GetMax();
        int64_t min = stat->GetMin();
        int64_t value = std::stoi(stat->GetValueString(_cursor));

        if (value < min || value > max) {
            const std::string error_string = "MetricsMonitor: Postrun stat \"" + statisticsName +
              "\" was outside of the specified limits. Min=" + std::to_string(min) +
              " Max=" + std::to_string(max) + " Actual=" + std::to_string(value);
            Logger::LogMessage(LOG_ERROR, error_string);
            success = false;
        }

        Logger::LogMessage(LOG_INFO,
          "MetricsMonitor: Final value of stat " + statisticsName +
            " is: " + std::to_string(value));
    }

    if (!success)
        testutil_die(-1,
          "MetricsMonitor: One or more postrun statistics were outside of their specified "
          "limits.");
}
} // namespace test_harness
