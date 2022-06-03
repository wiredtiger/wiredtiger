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
extern const std::string operationTracker;
extern const std::string metricsMonitor;
extern const std::string timestampManager;
extern const std::string workloadManager;

/* Configuration API consts. */
extern const std::string cacheHsInsert;
extern const std::string cacheMaxWaitMs;
extern const std::string cacheSizeMB;
extern const std::string ccPagesRemoved;
extern const std::string collectionCount;
extern const std::string checkpointOpConfig;
extern const std::string compressionEnabled;
extern const std::string customOpConfig;
extern const std::string durationSecs;
extern const std::string enabledConfig;
extern const std::string enableLogging;
extern const std::string insertOpConfig;
extern const std::string keyCountPerCollection;
extern const std::string keySize;
extern const std::string limit;
extern const std::string maxConfig;
extern const std::string minConfig;
extern const std::string oldestLag;
extern const std::string opRate;
extern const std::string opsPerTransaction;
extern const std::string populateConfig;
extern const std::string postrunStatistics;
extern const std::string readOpConfig;
extern const std::string removeOpConfig;
extern const std::string reverseCollator;
extern const std::string runtimeStatistics;
extern const std::string saveConfig;
extern const std::string stableLag;
extern const std::string statisticsCacheSize;
extern const std::string statisticsDatabaseSize;
extern const std::string statisticsConfig;
extern const std::string threadCount;
extern const std::string trackingKeyFormat;
extern const std::string trackingValueFormat;
extern const std::string type;
extern const std::string updateOpConfig;
extern const std::string valueSize;

/* WiredTiger API consts. */
extern const std::string commitTimestamp;
extern const std::string connectionCreate;
extern const std::string oldestTimestamp;
extern const std::string stableTimestamp;
extern const std::string statisticsLog;

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
extern const std::string defaultFrameworkSchema;
extern const std::string tableNameOpWorkloadTracker;
extern const std::string tableNameSchemaWorkloadTracker;
extern const std::string statisticsURI;

} // namespace test_harness

#endif
