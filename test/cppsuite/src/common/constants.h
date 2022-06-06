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

#ifndef API_CONST_H
#define API_CONST_H

#include <string>

/* Define all constants related to WiredTiger APIs and testing. */
namespace test_harness {

/* Component names. */
extern const std::string kOperationTracker;
extern const std::string kMetricsMonitor;
extern const std::string kTimestampManager;
extern const std::string kWorkloadManager;

/* Configuration API consts. */
extern const std::string kCacheHsInsert;
extern const std::string kCacheMaxWaitMs;
extern const std::string kCacheSizeMB;
extern const std::string kCcPagesRemoved;
extern const std::string kCollectionCount;
extern const std::string kCheckpointOpConfig;
extern const std::string kCompressionEnabled;
extern const std::string kCustomOpConfig;
extern const std::string kDurationSecs;
extern const std::string kEnabledConfig;
extern const std::string kEnableLogging;
extern const std::string kInsertOpConfig;
extern const std::string kKeyCountPerCollection;
extern const std::string kKeySize;
extern const std::string kLimit;
extern const std::string kMaxConfig;
extern const std::string kMinConfig;
extern const std::string kOldestLag;
extern const std::string kOpRate;
extern const std::string kOpsPerTransaction;
extern const std::string kPopulateConfig;
extern const std::string kPostrunStatistics;
extern const std::string kReadOpConfig;
extern const std::string kRemoveOpConfig;
extern const std::string kReverseCollator;
extern const std::string kRuntimeStatistics;
extern const std::string kSaveConfig;
extern const std::string kStableLag;
extern const std::string kStatisticsCacheSize;
extern const std::string kStatisticsDatabaseSize;
extern const std::string kStatisticsConfig;
extern const std::string kThreadCount;
extern const std::string kTrackingKeyFormat;
extern const std::string kTrackingValueFormat;
extern const std::string kType;
extern const std::string kUpdateOpConfig;
extern const std::string kValueSize;

/* WiredTiger API consts. */
extern const std::string kCommitTimestamp;
extern const std::string kConnectionCreate;
extern const std::string kOldestTimestamp;
extern const std::string kStableTimestamp;
extern const std::string kStatisticsLog;

/*
 * Use the Snappy compressor for stress testing to avoid excessive disk space usage. Our builds can
 * pre-specify 'EXTSUBPATH' to indicate any special sub-directories the module is located. If unset
 * we fallback to the '.libs' sub-directory used by autoconf.
 */
#define BLKCMP_PFX "block_compressor="
#define SNAPPY_BLK BLKCMP_PFX "snappy"
#define EXTPATH "../../ext/"
#ifndef EXTSUBPATH
#define EXTSUBPATH ".libs/"
#endif

/* Use reverse collator to test changes that deal different table sorting orders. */
#define REVERSE_COL_CFG "collator=reverse"

#define SNAPPY_PATH EXTPATH "compressors/snappy/" EXTSUBPATH "libwiredtiger_snappy.so"
#define REVERSE_COLLATOR_PATH \
    EXTPATH "collators/reverse/" EXTSUBPATH "libwiredtiger_reverse_collator.so"

/* Test harness consts. */
extern const std::string kDefaultFrameworkSchema;
extern const std::string kTableNameOpWorkloadTracker;
extern const std::string kTableNameSchemaWorkloadTracker;
extern const std::string kStatisticsURI;

} // namespace test_harness

#endif
