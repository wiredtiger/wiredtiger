/* DO NOT EDIT: automatically built by format/config.sh. */

#include "format.h"

CONFIG configuration_list[] = {
  {"assert.read_timestamp", "assert read_timestamp",
    C_BOOL, 2, 0, 0, V_GLOBAL_ASSERT_READ_TIMESTAMP},

  {"assert.write_timestamp", "set write_timestamp_usage and assert write_timestamp",
    C_BOOL, 2, 0, 0, V_GLOBAL_ASSERT_WRITE_TIMESTAMP},

  {"backup", "configure backups",
    C_BOOL, 20, 0, 0, V_GLOBAL_BACKUP},

  {"backup.incremental", "backup type (off | block | log)",
    C_IGNORE | C_STRING, 0, 0, 0, V_GLOBAL_BACKUP_INCREMENTAL},

  {"backup.incr_granularity", "incremental backup block granularity (KB)",
    0x0, 4, 16384, 16384, V_GLOBAL_BACKUP_INCR_GRANULARITY},

  {"block_cache", "enable the block cache",
    C_BOOL, 10, 0, 0, V_GLOBAL_BLOCK_CACHE},

  {"block_cache.cache_on_checkpoint", "block cache: cache checkpoint writes",
    C_BOOL, 30, 0, 0, V_GLOBAL_BLOCK_CACHE_CACHE_ON_CHECKPOINT},

  {"block_cache.cache_on_writes", "block cache: populate the cache on writes",
    C_BOOL, 60, 0, 0, V_GLOBAL_BLOCK_CACHE_CACHE_ON_WRITES},

  {"block_cache.size", "block cache size (MB)",
    0x0, 1, 100, 100 * 1024, V_GLOBAL_BLOCK_CACHE_SIZE},

  {"btree.bitcnt", "fixed-length column-store object size (number of bits)",
    C_TABLE | C_TYPE_FIX, 1, 8, 8, V_TABLE_BTREE_BITCNT},

  {"btree.compression", "data compression (off | lz4 | snappy | zlib | zstd)",
    C_IGNORE | C_STRING | C_TABLE, 0, 0, 0, V_TABLE_BTREE_COMPRESSION},

  {"btree.dictionary", "configure dictionary compressed values",
    C_BOOL | C_TABLE | C_TYPE_ROW | C_TYPE_VAR, 20, 0, 0, V_TABLE_BTREE_DICTIONARY},

  {"btree.huffman_value", "configure huffman encoded values",
    C_BOOL | C_TABLE | C_TYPE_ROW | C_TYPE_VAR, 20, 0, 0, V_TABLE_BTREE_HUFFMAN_VALUE},

  {"btree.internal_key_truncation", "truncate internal keys",
    C_BOOL | C_TABLE, 95, 0, 0, V_TABLE_BTREE_INTERNAL_KEY_TRUNCATION},

  {"btree.internal_page_max", "btree internal node maximum size",
    C_TABLE, 9, 17, 27, V_TABLE_BTREE_INTERNAL_PAGE_MAX},

  {"btree.key_max", "maximum key size",
    C_TABLE | C_TYPE_ROW, 20, 128, MEGABYTE(10), V_TABLE_BTREE_KEY_MAX},

  {"btree.key_min", "minimum key size",
    C_TABLE | C_TYPE_ROW, KEY_LEN_CONFIG_MIN, 32, 256, V_TABLE_BTREE_KEY_MIN},

  {"btree.leaf_page_max", "btree leaf node maximum size",
    C_TABLE, 9, 17, 27, V_TABLE_BTREE_LEAF_PAGE_MAX},

  {"btree.memory_page_max", "maximum cache page size",
    C_TABLE, 1, 10, 128, V_TABLE_BTREE_MEMORY_PAGE_MAX},

  {"btree.prefix_len", "common key prefix",
    C_TABLE | C_TYPE_ROW | C_ZERO_NOTSET, PREFIX_LEN_CONFIG_MIN, PREFIX_LEN_CONFIG_MAX, PREFIX_LEN_CONFIG_MAX, V_TABLE_BTREE_PREFIX_LEN},

  {"btree.prefix_compression", "configure prefix compressed keys",
    C_BOOL | C_TABLE | C_TYPE_ROW, 80, 0, 0, V_TABLE_BTREE_PREFIX_COMPRESSION},

  {"btree.prefix_compression_min", "minimum gain before prefix compression is used (bytes)",
    C_TABLE | C_TYPE_ROW, 0, 8, 256, V_TABLE_BTREE_PREFIX_COMPRESSION_MIN},

  {"btree.repeat_data_pct", "duplicate values (percentage)",
    C_TABLE | C_TYPE_VAR, 0, 90, 90, V_TABLE_BTREE_REPEAT_DATA_PCT},

  {"btree.reverse", "reverse order collation",
    C_BOOL | C_TABLE | C_TYPE_ROW, 10, 0, 0, V_TABLE_BTREE_REVERSE},

  {"btree.split_pct", "page split size as a percentage of the maximum page size",
    C_TABLE, 50, 100, 100, V_TABLE_BTREE_SPLIT_PCT},

  {"btree.value_max", "maximum value size",
    C_TABLE | C_TYPE_ROW | C_TYPE_VAR, 32, 4096, MEGABYTE(10), V_TABLE_BTREE_VALUE_MAX},

  {"btree.value_min", "minimum value size",
    C_TABLE | C_TYPE_ROW | C_TYPE_VAR, 0, 20, 4096, V_TABLE_BTREE_VALUE_MIN},

  {"cache", "cache size (MB)",
    0x0, 1, 100, 100 * 1024, V_GLOBAL_CACHE},

  {"cache.evict_max", "maximum number of eviction workers",
    0x0, 0, 5, 100, V_GLOBAL_CACHE_EVICT_MAX},

  {"cache.minimum", "minimum cache size (MB)",
    C_IGNORE, 0, 0, 100 * 1024, V_GLOBAL_CACHE_MINIMUM},

  {"checkpoint", "checkpoint type (on | off | wiredtiger)",
    C_IGNORE | C_STRING, 0, 0, 0, V_GLOBAL_CHECKPOINT},

  {"checkpoint.log_size", "MB of log to wait if wiredtiger checkpoints configured",
    0x0, 20, 200, 1024, V_GLOBAL_CHECKPOINT_LOG_SIZE},

  {"checkpoint.wait", "seconds to wait if wiredtiger checkpoints configured",
    0x0, 5, 100, 3600, V_GLOBAL_CHECKPOINT_WAIT},

  {"disk.checksum", "checksum type (on | off | uncompressed | unencrypted)",
    C_IGNORE | C_STRING | C_TABLE, 0, 0, 0, V_TABLE_DISK_CHECKSUM},

  {"disk.data_extend", "configure data file extension",
    C_BOOL, 5, 0, 0, V_GLOBAL_DISK_DATA_EXTEND},

  {"disk.direct_io", "configure direct I/O for data objects",
    C_BOOL | C_IGNORE, 0, 0, 1, V_GLOBAL_DISK_DIRECT_IO},

  {"disk.encryption", "encryption type (off | rotn-7)",
    C_IGNORE | C_STRING, 0, 0, 0, V_GLOBAL_DISK_ENCRYPTION},

  {"disk.firstfit", "configure first-fit allocation",
    C_BOOL | C_TABLE, 10, 0, 0, V_TABLE_DISK_FIRSTFIT},

  {"disk.mmap", "configure mmap operations (reads only)",
    C_BOOL, 90, 0, 0, V_GLOBAL_DISK_MMAP},

  {"disk.mmap_all", "configure mmap operations (read and write)",
    C_BOOL, 5, 0, 0, V_GLOBAL_DISK_MMAP_ALL},

  {"format.abort", "drop core during timed run",
    C_BOOL, 0, 0, 0, V_GLOBAL_FORMAT_ABORT},

  {"format.independent_thread_rng", "configure independent thread RNG space",
    C_BOOL, 75, 0, 0, V_GLOBAL_FORMAT_INDEPENDENT_THREAD_RNG},

  {"format.major_timeout", "long-running operations timeout (minutes)",
    C_IGNORE, 0, 0, 1000, V_GLOBAL_FORMAT_MAJOR_TIMEOUT},

/*
 * 0%
 * FIXME-WT-7418: Temporarily disable import until WT_ROLLBACK error and wt_copy_and_sync error is
 * fixed. It should be (C_BOOL, 20, 0, 0).
 */
  {"import", "import table from newly created database",
    C_BOOL, 0, 0, 0, V_GLOBAL_IMPORT},

  {"logging", "configure logging",
    C_BOOL, 50, 0, 0, V_GLOBAL_LOGGING},

  {"logging.compression", "logging compression (off | lz4 | snappy | zlib | zstd)",
    C_IGNORE | C_STRING, 0, 0, 0, V_GLOBAL_LOGGING_COMPRESSION},

  {"logging.file_max", "maximum log file size (KB)",
    0x0, 100, 512000, 2097152, V_GLOBAL_LOGGING_FILE_MAX},

  {"logging.prealloc", "configure log file pre-allocation",
    C_BOOL, 50, 0, 0, V_GLOBAL_LOGGING_PREALLOC},

  {"logging.remove", "configure log file removal",
    C_BOOL, 50, 0, 0, V_GLOBAL_LOGGING_REMOVE},

  {"lsm.auto_throttle", "throttle LSM inserts",
    C_BOOL | C_TABLE | C_TYPE_LSM, 90, 0, 0, V_TABLE_LSM_AUTO_THROTTLE},

  {"lsm.bloom", "configure bloom filters",
    C_BOOL | C_TABLE | C_TYPE_LSM, 95, 0, 0, V_TABLE_LSM_BLOOM},

  {"lsm.bloom_bit_count", "number of bits per item for bloom filters",
    C_TABLE | C_TYPE_LSM, 4, 64, 1000, V_TABLE_LSM_BLOOM_BIT_COUNT},

  {"lsm.bloom_hash_count", "number of hash values per item for bloom filters",
    C_TABLE | C_TYPE_LSM, 4, 32, 100, V_TABLE_LSM_BLOOM_HASH_COUNT},

  {"lsm.bloom_oldest", "configure bloom_oldest=true",
    C_BOOL | C_TABLE | C_TYPE_LSM, 10, 0, 0, V_TABLE_LSM_BLOOM_OLDEST},

  {"lsm.chunk_size", "LSM chunk size (MB)",
    C_TABLE | C_TYPE_LSM, 1, 10, 100, V_TABLE_LSM_CHUNK_SIZE},

  {"lsm.merge_max", "maximum number of chunks to include in an LSM merge operation",
    C_TABLE | C_TYPE_LSM, 4, 20, 100, V_TABLE_LSM_MERGE_MAX},

  {"lsm.worker_threads", "number of LSM worker threads",
    C_TYPE_LSM, 3, 4, 20, V_GLOBAL_LSM_WORKER_THREADS},

  {"ops.alter", "configure table alterations",
    C_BOOL, 10, 0, 0, V_GLOBAL_OPS_ALTER},

  {"ops.compaction", "configure compaction",
    C_BOOL, 10, 0, 0, V_GLOBAL_OPS_COMPACTION},

  {"ops.hs_cursor", "configure history store cursor reads",
    C_BOOL, 50, 0, 0, V_GLOBAL_OPS_HS_CURSOR},

  {"ops.pct.delete", "delete operations (percentage)",
    C_IGNORE | C_TABLE, 0, 0, 100, V_TABLE_OPS_PCT_DELETE},

  {"ops.pct.insert", "insert operations (percentage)",
    C_IGNORE | C_TABLE, 0, 0, 100, V_TABLE_OPS_PCT_INSERT},

  {"ops.pct.modify", "modify operations (percentage)",
    C_IGNORE | C_TABLE, 0, 0, 100, V_TABLE_OPS_PCT_MODIFY},

  {"ops.pct.read", "read operations (percentage)",
    C_IGNORE | C_TABLE, 0, 0, 100, V_TABLE_OPS_PCT_READ},

  {"ops.pct.write", "update operations (percentage)",
    C_IGNORE | C_TABLE, 0, 0, 100, V_TABLE_OPS_PCT_WRITE},

  {"ops.prepare", "configure transaction prepare",
    C_BOOL, 5, 0, 0, V_GLOBAL_OPS_PREPARE},

  {"ops.random_cursor", "configure random cursor reads",
    C_BOOL, 10, 0, 0, V_GLOBAL_OPS_RANDOM_CURSOR},

  {"ops.salvage", "configure salvage",
    C_BOOL, 100, 1, 0, V_GLOBAL_OPS_SALVAGE},

  {"ops.truncate", "configure truncation",
    C_BOOL | C_TABLE, 100, 0, 0, V_TABLE_OPS_TRUNCATE},

  {"ops.verify", "configure verify",
    C_BOOL, 100, 1, 0, V_GLOBAL_OPS_VERIFY},

  {"quiet", "quiet run (same as -q)",
    C_BOOL | C_IGNORE, 0, 0, 1, V_GLOBAL_QUIET},

  {"runs.in_memory", "configure in-memory",
    C_BOOL | C_IGNORE, 0, 0, 1, V_GLOBAL_RUNS_IN_MEMORY},

  {"runs.ops", "operations per run",
    0x0, 0, M(2), M(100), V_GLOBAL_RUNS_OPS},

  {"runs.rows", "number of rows",
    C_TABLE, 10, M(1), M(100), V_TABLE_RUNS_ROWS},

  {"runs.source", "data source type (file | lsm | table)",
    C_IGNORE | C_STRING | C_TABLE, 0, 0, 0, V_TABLE_RUNS_SOURCE},

  {"runs.tables", "number of tables",
    0x0, 1, 32, V_MAX_TABLES_CONFIG, V_GLOBAL_RUNS_TABLES},

  {"runs.threads", "number of worker threads",
    0x0, 1, 32, 128, V_GLOBAL_RUNS_THREADS},

  {"runs.timer", "run time (minutes)",
    C_IGNORE, 0, 0, UINT_MAX, V_GLOBAL_RUNS_TIMER},

  {"runs.type", "object type (fix | row | var)",
    C_IGNORE | C_STRING | C_TABLE, 0, 0, 0, V_TABLE_RUNS_TYPE},

  {"runs.verify_failure_dump", "configure page dump on repeatable read error",
    C_BOOL | C_IGNORE, 0, 0, 1, V_GLOBAL_RUNS_VERIFY_FAILURE_DUMP},

  {"statistics", "configure statistics",
    C_BOOL, 20, 0, 0, V_GLOBAL_STATISTICS},

  {"statistics.server", "configure statistics server thread",
    C_BOOL, 5, 0, 0, V_GLOBAL_STATISTICS_SERVER},

  {"stress.aggressive_sweep", "stress aggressive sweep",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_AGGRESSIVE_SWEEP},

  {"stress.checkpoint", "stress checkpoints",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_CHECKPOINT},

  {"stress.checkpoint_reserved_txnid_delay", "stress checkpoint invisible transaction id delay",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_CHECKPOINT_RESERVED_TXNID_DELAY},

  {"stress.checkpoint_prepare", "stress checkpoint prepare",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_CHECKPOINT_PREPARE},

  {"stress.evict_reposition", "stress evict reposition",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_EVICT_REPOSITION},

  {"stress.failpoint_hs_delete_key_from_ts", "stress failpoint history store delete key from ts",
    C_BOOL, 30, 0, 0, V_GLOBAL_STRESS_FAILPOINT_HS_DELETE_KEY_FROM_TS},

  {"stress.hs_checkpoint_delay", "stress history store checkpoint delay",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_HS_CHECKPOINT_DELAY},

  {"stress.hs_search", "stress history store search",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_HS_SEARCH},

  {"stress.hs_sweep", "stress history store sweep",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_HS_SWEEP},

  {"stress.split_1", "stress splits (#1)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_1},

  {"stress.split_2", "stress splits (#2)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_2},

  {"stress.split_3", "stress splits (#3)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_3},

  {"stress.split_4", "stress splits (#4)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_4},

  {"stress.split_5", "stress splits (#5)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_5},

  {"stress.split_6", "stress splits (#6)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_6},

  {"stress.split_7", "stress splits (#7)",
    C_BOOL, 2, 0, 0, V_GLOBAL_STRESS_SPLIT_7},

  {"transaction.implicit", "implicit, without timestamps, transactions (percentage)",
    0, 0, 100, 100, V_GLOBAL_TRANSACTION_IMPLICIT},

  {"transaction.timestamps", "all transactions (or none), have timestamps",
    C_BOOL, 80, 0, 0, V_GLOBAL_TRANSACTION_TIMESTAMPS},

  {"wiredtiger.config", "wiredtiger_open API configuration string",
    C_IGNORE | C_STRING, 0, 0, 0, V_GLOBAL_WIREDTIGER_CONFIG},

  {"wiredtiger.rwlock", "configure wiredtiger read/write mutexes",
    C_BOOL, 80, 0, 0, V_GLOBAL_WIREDTIGER_RWLOCK},

  {"wiredtiger.leak_memory", "leak memory on wiredtiger shutdown",
    C_BOOL, 0, 0, 0, V_GLOBAL_WIREDTIGER_LEAK_MEMORY},

  {NULL, NULL, 0x0, 0, 0, 0, 0}
};
