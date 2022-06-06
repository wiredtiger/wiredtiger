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
const std::string kOperationTracker = "operation_tracker";
const std::string kMetricsMonitor = "metrics_monitor";
const std::string kTimestampManager = "timestamp_manager";
const std::string kWorkloadManager = "workload_manager";

/* Configuration API consts. */
const std::string kCacheHsInsert = "cache_hs_insert";
const std::string kCacheMaxWaitMs = "cache_max_wait_ms";
const std::string kCacheSizeMB = "cache_size_mb";
const std::string kCcPagesRemoved = "cc_pages_removed";
const std::string kCollectionCount = "collection_count";
const std::string kCheckpointOpConfig = "checkpoint_config";
const std::string kCompressionEnabled = "compression_enabled";
const std::string kCustomOpConfig = "custom_config";
const std::string kDurationSecs = "duration_seconds";
const std::string kEnabledConfig = "enabled";
const std::string kEnableLogging = "enable_logging";
const std::string kInsertOpConfig = "insert_config";
const std::string kKeyCountPerCollection = "key_count_per_collection";
const std::string kKeySize = "key_size";
const std::string kLimit = "limit";
const std::string kMaxConfig = "max";
const std::string kMinConfig = "min";
const std::string kOldestLag = "oldest_lag";
const std::string kOpRate = "op_rate";
const std::string kOpsPerTransaction = "ops_per_transaction";
const std::string kPopulateConfig = "populate_config";
const std::string kPostrunStatistics = "postrun";
const std::string kReadOpConfig = "read_config";
const std::string kRemoveOpConfig = "remove_config";
const std::string kReverseCollator = "reverse_collator";
const std::string kRuntimeStatistics = "runtime";
const std::string kSaveConfig = "save";
const std::string kStableLag = "stable_lag";
const std::string kStatisticsCacheSize = "stat_cache_size";
const std::string kStatisticsDatabaseSize = "stat_db_size";
const std::string kStatisticsConfig = "statistics_config";
const std::string kThreadCount = "thread_count";
const std::string kTrackingKeyFormat = "tracking_key_format";
const std::string kTrackingValueFormat = "tracking_value_format";
const std::string kType = "type";
const std::string kUpdateOpConfig = "update_config";
const std::string kValueSize = "value_size";

/* WiredTiger API consts. */
const std::string kCommitTimestamp = "commit_timestamp";
const std::string kConnectionCreate = "create";
const std::string kOldestTimestamp = "oldest_timestamp";
const std::string kStableTimestamp = "stable_timestamp";
const std::string kStatisticsLog = "statistics_log=(json,wait=1)";

/* Test harness consts. */
const std::string kDefaultFrameworkSchema = "key_format=S,value_format=S,";
const std::string kTableNameOpWorkloadTracker = "table:operation_tracker";
const std::string kTableNameSchemaWorkloadTracker = "table:schema_tracking";
const std::string kStatisticsURI = "statistics:";

} // namespace test_harness
