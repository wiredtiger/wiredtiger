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

#include "constants.h"

/* Define all constants related to WiredTiger APIs and testing. */
namespace test_harness {

/* Component names. */
const std::string operationTracker = "operation_tracker";
const std::string metricsMonitor = "metrics_monitor";
const std::string timestampManager = "timestamp_manager";
const std::string workloadManager = "workload_manager";

/* Configuration API consts. */
const std::string cacheHsInsert = "cache_hs_insert";
const std::string cacheMaxWaitMs = "cache_max_wait_ms";
const std::string cacheSizeMB = "cache_size_mb";
const std::string ccPagesRemoved = "cc_pages_removed";
const std::string collectionCount = "collection_count";
const std::string checkpointOpConfig = "checkpoint_config";
const std::string compressionEnabled = "compression_enabled";
const std::string customOpConfig = "custom_config";
const std::string durationSecs = "duration_seconds";
const std::string enabledConfig = "enabled";
const std::string enableLogging = "enable_logging";
const std::string insertOpConfig = "insert_config";
const std::string keyCountPerCollection = "key_count_per_collection";
const std::string keySize = "key_size";
const std::string limit = "limit";
const std::string maxConfig = "max";
const std::string minConfig = "min";
const std::string oldestLag = "oldest_lag";
const std::string opRate = "op_rate";
const std::string opsPerTransaction = "ops_per_transaction";
const std::string populateConfig = "populate_config";
const std::string postrunStatistics = "postrun";
const std::string readOpConfig = "read_config";
const std::string removeOpConfig = "remove_config";
const std::string reverseCollator = "reverse_collator";
const std::string runtimeStatistics = "runtime";
const std::string saveConfig = "save";
const std::string stableLag = "stable_lag";
const std::string statisticsCacheSize = "stat_cache_size";
const std::string statisticsDatabaseSize = "stat_db_size";
const std::string statisticsConfig = "statistics_config";
const std::string threadCount = "thread_count";
const std::string trackingKeyFormat = "tracking_key_format";
const std::string trackingValueFormat = "tracking_value_format";
const std::string type = "type";
const std::string updateOpConfig = "update_config";
const std::string valueSize = "value_size";

/* WiredTiger API consts. */
const std::string commitTimestamp = "commit_timestamp";
const std::string connectionCreate = "create";
const std::string oldestTimestamp = "oldest_timestamp";
const std::string stableTimestamp = "stable_timestamp";
const std::string statisticsLog = "statistics_log=(json,wait=1)";

/* Test harness consts. */
const std::string defaultFrameworkSchema = "key_format=S,value_format=S,";
const std::string tableNameOpWorkloadTracker = "table:operation_tracker";
const std::string tableNameSchemaWorkloadTracker = "table:schema_tracking";
const std::string statisticsURI = "statistics:";

} // namespace test_harness
