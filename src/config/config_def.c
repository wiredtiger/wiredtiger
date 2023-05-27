/* DO NOT EDIT: automatically built by dist/api_config.py. */

#include "wt_internal.h"

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_close[] = {
  {"final_flush", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 159, 0, 0, NULL,
    0},
  {"leak_memory", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 160, 0, 0, NULL,
    0},
  {"use_timestamp", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 158, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_debug_info[] = {
  {"cache", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 161, 0, 0, NULL, 0},
  {"cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 162, 0, 0, NULL, 0},
  {"handles", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 163, 0, 0, NULL, 0},
  {"log", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 36, 0, 0, NULL, 0},
  {"sessions", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 164, 0, 0, NULL, 0},
  {"txn", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 165, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_load_extension[] = {
  {"config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 246, 0, 0, NULL, 0},
  {"early_load", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 247, 0, 0, NULL,
    0},
  {"entry", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 248, 0, 0, NULL, 0},
  {"terminate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 249, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_open_session_debug_subconfigs[] = {
  {"release_evict_page", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 251, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_open_session[] = {
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"debug", "category", NULL, NULL, confchk_WT_CONNECTION_open_session_debug_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 117, 0, 0, NULL, 0},
  {"ignore_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 252, 0, 0,
    NULL, 0},
  {"isolation", "string", NULL,
    "choices=[\"read-uncommitted\",\"read-committed\","
    "\"snapshot\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 148, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_query_timestamp[] = {
  {"get", "string", NULL,
    "choices=[\"all_durable\",\"last_checkpoint\",\"oldest\","
    "\"oldest_reader\",\"oldest_timestamp\",\"pinned\",\"recovery\","
    "\"stable\",\"stable_timestamp\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 136, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_block_cache_subconfigs[] = {
  {"blkcache_eviction_aggression", "int", NULL, "min=1,max=7200", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 169, 1, 7200, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_on_checkpoint", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 167, 0,
    0, NULL, 0},
  {"cache_on_writes", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 168, 0, 0,
    NULL, 0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"full_target", "int", NULL, "min=30,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 170, 30, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"hashsize", "int", NULL, "min=512,max=256K", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 172, 512,
    256LL * WT_KILOBYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"max_percent_overhead", "int", NULL, "min=1,max=500", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 173,
    1, 500, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"nvram_path", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 174, 0, 0, NULL, 0},
  {"percent_file_in_dram", "int", NULL, "min=0,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 175,
    0, 100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"size", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 171, 0,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"system_ram", "int", NULL, "min=0,max=1024GB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 176, 0,
    1024LL * WT_GIGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 9, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_checkpoint_subconfigs[] = {
  {"log_size", "int", NULL, "min=0,max=2GB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 181, 0,
    2LL * WT_GIGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"wait", "int", NULL, "min=0,max=100000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 182, 0, 100000,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_chunk_cache_subconfigs[] = {
  {"capacity", "int", NULL, "min=0,max=100TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 185, 0,
    100LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"chunk_cache_evict_trigger", "int", NULL, "min=0,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    186, 0, 100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"chunk_size", "int", NULL, "min=512KB,max=100GB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 77,
    512LL * WT_KILOBYTE, 100LL * WT_GIGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"device_path", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 187, 0, 0, NULL,
    0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"hashsize", "int", NULL, "min=64,max=1048576", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 172, 64,
    1048576LL, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 9, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_reconfigure_compatibility_subconfigs[] = {
  {"release", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 189, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_debug_mode_subconfigs[] = {
  {"checkpoint_retention", "int", NULL, "min=0,max=1024", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 192,
    0, 1024, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"corruption_abort", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 191, 0, 0,
    NULL, 0},
  {"cursor_copy", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 193, 0, 0, NULL,
    0},
  {"cursor_reposition", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 194, 0, 0,
    NULL, 0},
  {"eviction", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 195, 0, 0, NULL, 0},
  {"log_retention", "int", NULL, "min=0,max=1024", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 196, 0,
    1024, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"realloc_exact", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 197, 0, 0,
    NULL, 0},
  {"realloc_malloc", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 198, 0, 0,
    NULL, 0},
  {"rollback_error", "int", NULL, "min=0,max=10M", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 199, 0,
    10LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"slow_checkpoint", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 200, 0, 0,
    NULL, 0},
  {"stress_skiplist", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 201, 0, 0,
    NULL, 0},
  {"table_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 202, 0, 0,
    NULL, 0},
  {"tiered_flush_error_continue", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    203, 0, 0, NULL, 0},
  {"update_restore_evict", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 204, 0,
    0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_eviction_subconfigs[] = {
  {"threads_max", "int", NULL, "min=1,max=20", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 206, 1, 20,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"threads_min", "int", NULL, "min=1,max=20", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 207, 1, 20,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_file_manager_subconfigs[] = {
  {"close_handle_minimum", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 217, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"close_idle_time", "int", NULL, "min=0,max=100000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 218, 0,
    100000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"close_scan_interval", "int", NULL, "min=1,max=100000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    219, 1, 100000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_history_store_subconfigs[] = {
  {"file_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 222, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_io_capacity_subconfigs[] = {
  {"total", "int", NULL, "min=0,max=1TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 224, 0,
    1LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_reconfigure_log_subconfigs[] = {
  {"archive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 226, 0, 0, NULL, 0},
  {"os_cache_dirty_pct", "int", NULL, "min=0,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 227, 0,
    100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"prealloc", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 228, 0, 0, NULL, 0},
  {"remove", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 229, 0, 0, NULL, 0},
  {"zero_fill", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 230, 0, 0, NULL,
    0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_lsm_manager_subconfigs[] = {
  {"merge", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 233, 0, 0, NULL, 0},
  {"worker_thread_max", "int", NULL, "min=3,max=20", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 232, 3,
    20, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_operation_tracking_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"path", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 235, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_shared_cache_subconfigs[] = {
  {"chunk", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 237,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"quota", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 238, 0, 0, NULL, 0},
  {"reserve", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 239, 0, 0, NULL, 0},
  {"size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 171,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_reconfigure_statistics_log_subconfigs[] = {
  {"json", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 241, 0, 0, NULL, 0},
  {"on_close", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 242, 0, 0, NULL, 0},
  {"sources", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 243, 0, 0, NULL, 0},
  {"timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 244, 0, 0, NULL, 0},
  {"wait", "int", NULL, "min=0,max=100000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 182, 0, 100000,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_reconfigure_tiered_storage_subconfigs[] = {
  {"local_retention", "int", NULL, "min=0,max=10000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 52, 0,
    10000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_reconfigure[] = {
  {"block_cache", "category", NULL, NULL, confchk_wiredtiger_open_block_cache_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 166, 0, 0, NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_overhead", "int", NULL, "min=0,max=30", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 178, 0, 30,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 179,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_stuck_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 180, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"checkpoint", "category", NULL, NULL, confchk_wiredtiger_open_checkpoint_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 56, 0, 0, NULL, 0},
  {"checkpoint_cleanup", "string", NULL, "choices=[\"none\",\"reclaim_space\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 183, 0, 0, NULL, 0},
  {"chunk_cache", "category", NULL, NULL, confchk_wiredtiger_open_chunk_cache_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 184, 0, 0, NULL, 0},
  {"compatibility", "category", NULL, NULL,
    confchk_WT_CONNECTION_reconfigure_compatibility_subconfigs, 1, WT_CONFIG_COMPILED_TYPE_CATEGORY,
    188, 0, 0, NULL, 0},
  {"debug_mode", "category", NULL, NULL, confchk_wiredtiger_open_debug_mode_subconfigs, 14,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 190, 0, 0, NULL, 0},
  {"error_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 205, 0, 0, NULL,
    0},
  {"eviction", "category", NULL, NULL, confchk_wiredtiger_open_eviction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 195, 0, 0, NULL, 0},
  {"eviction_checkpoint_target", "int", NULL, "min=0,max=10TB", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 208, 0, 10LL * WT_TERABYTE, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_target", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    209, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_trigger", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    210, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_target", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 211, 10,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_trigger", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 212,
    10, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_target", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    213, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_trigger", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    214, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"extra_diagnostics", "list", NULL,
    "choices=[\"all\",\"checkpoint_validate\",\"cursor_check\""
    ",\"disk_validate\",\"eviction_check\",\"generation_check\","
    "\"hs_validate\",\"key_out_of_order\",\"log_validate\","
    "\"prepared\",\"slow_operation\",\"txn_visibility\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 215, 0, 0, NULL, 0},
  {"file_manager", "category", NULL, NULL, confchk_wiredtiger_open_file_manager_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 216, 0, 0, NULL, 0},
  {"generation_drain_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 220,
    0, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"history_store", "category", NULL, NULL, confchk_wiredtiger_open_history_store_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 221, 0, 0, NULL, 0},
  {"io_capacity", "category", NULL, NULL, confchk_wiredtiger_open_io_capacity_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 223, 0, 0, NULL, 0},
  {"json_output", "list", NULL, "choices=[\"error\",\"message\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 225, 0, 0, NULL, 0},
  {"log", "category", NULL, NULL, confchk_WT_CONNECTION_reconfigure_log_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm_manager", "category", NULL, NULL, confchk_wiredtiger_open_lsm_manager_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 231, 0, 0, NULL, 0},
  {"operation_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"operation_tracking", "category", NULL, NULL,
    confchk_wiredtiger_open_operation_tracking_subconfigs, 2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 234,
    0, 0, NULL, 0},
  {"shared_cache", "category", NULL, NULL, confchk_wiredtiger_open_shared_cache_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 236, 0, 0, NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"none\","
    "\"clear\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"statistics_log", "category", NULL, NULL,
    confchk_WT_CONNECTION_reconfigure_statistics_log_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 240, 0, 0, NULL, 0},
  {"tiered_storage", "category", NULL, NULL,
    confchk_WT_CONNECTION_reconfigure_tiered_storage_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"timing_stress_for_test", "list", NULL,
    "choices=[\"aggressive_sweep\",\"backup_rename\","
    "\"checkpoint_evict_page\",\"checkpoint_handle\","
    "\"checkpoint_slow\",\"checkpoint_stop\",\"compact_slow\","
    "\"evict_reposition\","
    "\"failpoint_eviction_fail_after_reconciliation\","
    "\"failpoint_history_store_delete_key_from_ts\","
    "\"history_store_checkpoint_delay\",\"history_store_search\","
    "\"history_store_sweep_race\",\"prepare_checkpoint_delay\","
    "\"prepare_resolution\",\"sleep_before_read_overflow_onpage\","
    "\"split_1\",\"split_2\",\"split_3\",\"split_4\",\"split_5\","
    "\"split_6\",\"split_7\",\"split_8\",\"tiered_flush_finish\","
    "\"txn_read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 245, 0, 0, NULL, 0},
  {"verbose", "list", NULL,
    "choices=[\"api\",\"backup\",\"block\",\"block_cache\","
    "\"checkpoint\",\"checkpoint_cleanup\",\"checkpoint_progress\","
    "\"chunkcache\",\"compact\",\"compact_progress\","
    "\"error_returns\",\"evict\",\"evict_stuck\",\"evictserver\","
    "\"fileops\",\"generation\",\"handleops\",\"history_store\","
    "\"history_store_activity\",\"log\",\"lsm\",\"lsm_manager\","
    "\"metadata\",\"mutex\",\"out_of_order\",\"overflow\",\"read\","
    "\"reconcile\",\"recovery\",\"recovery_progress\",\"rts\","
    "\"salvage\",\"shared_cache\",\"split\",\"temporary\","
    "\"thread_group\",\"tiered\",\"timestamp\",\"transaction\","
    "\"verify\",\"version\",\"write\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 10, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_rollback_to_stable[] = {
  {"dryrun", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 254, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CONNECTION_set_timestamp[] = {
  {"durable_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 3, 0, 0,
    NULL, 0},
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {"oldest_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 253, 0, 0,
    NULL, 0},
  {"stable_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 145, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CURSOR_bound[] = {
  {"action", "string", NULL, "choices=[\"clear\",\"set\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING,
    93, 0, 0, NULL, 0},
  {"bound", "string", NULL, "choices=[\"lower\",\"upper\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 94, 0, 0, NULL, 0},
  {"inclusive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 95, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_CURSOR_reconfigure[] = {
  {"append", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 90, 0, 0, NULL, 0},
  {"overwrite", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 91, 0, 0, NULL, 0},
  {"prefix_search", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 92, 0, 0, NULL,
    0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_assert_subconfigs[] = {
  {"commit_timestamp", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"never\","
    "\"none\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 2, 0, 0, NULL, 0},
  {"durable_timestamp", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"never\","
    "\"none\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 3, 0, 0, NULL, 0},
  {"read_timestamp", "string", NULL, "choices=[\"always\",\"never\",\"none\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 4, 0, 0, NULL, 0},
  {"write_timestamp", "string", NULL, "choices=[\"off\",\"on\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 5, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_log_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_alter[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"exclusive_refreshed", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 96, 0, 0,
    NULL, 0},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_begin_transaction_roundup_timestamps_subconfigs[] =
  {{"prepared", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 153, 0, 0, NULL,
     0},
    {"read", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 154, 0, 0, NULL, 0},
    {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_begin_transaction[] = {
  {"ignore_prepare", "string", NULL, "choices=[\"false\",\"force\",\"true\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 147, 0, 0, NULL, 0},
  {"isolation", "string", NULL,
    "choices=[\"read-uncommitted\",\"read-committed\","
    "\"snapshot\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 148, 0, 0, NULL, 0},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"no_timestamp", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 149, 0, 0, NULL,
    0},
  {"operation_timeout_ms", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 1, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"priority", "int", NULL, "min=-100,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 151, 100, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"read_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 4, 0, 0, NULL,
    0},
  {"roundup_timestamps", "category", NULL, NULL,
    confchk_WT_SESSION_begin_transaction_roundup_timestamps_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 152, 0, 0, NULL, 0},
  {"sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 114, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_checkpoint_flush_tier_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {"sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 114, 0, 0, NULL, 0},
  {"timeout", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 97, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_checkpoint[] = {
  {"drop", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 156, 0, 0, NULL, 0},
  {"flush_tier", "category", NULL, NULL, confchk_WT_SESSION_checkpoint_flush_tier_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 157, 0, 0, NULL, 0},
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 135, 0, 0, NULL, 0},
  {"use_timestamp", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 158, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_commit_transaction[] = {
  {"commit_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 2, 0, 0, NULL,
    0},
  {"durable_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 3, 0, 0,
    NULL, 0},
  {"operation_timeout_ms", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 1, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"sync", "string", NULL, "choices=[\"off\",\"on\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 114,
    0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_compact[] = {
  {"timeout", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 97, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_encryption_subconfigs[] = {
  {"keyid", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 21, 0, 0, NULL, 0},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_import_subconfigs[] = {
  {"compare_timestamp", "string", NULL,
    "choices=[\"oldest\",\"oldest_timestamp\",\"stable\","
    "\"stable_timestamp\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 100, 0, 0, NULL, 0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"file_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 102, 0, 0, NULL,
    0},
  {"metadata_file", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 103, 0, 0, NULL,
    0},
  {"repair", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 101, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_merge_custom_subconfigs[] = {
  {"prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 79, 0, 0, NULL, 0},
  {"start_generation", "int", NULL, "min=0,max=10", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 80, 0, 10,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"suffix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 81, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_lsm_subconfigs[] = {
  {"auto_throttle", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 69, 0, 0, NULL,
    0},
  {"bloom", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 70, 0, 0, NULL, 0},
  {"bloom_bit_count", "int", NULL, "min=2,max=1000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 71, 2,
    1000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"bloom_config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 72, 0, 0, NULL,
    0},
  {"bloom_hash_count", "int", NULL, "min=2,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 73, 2,
    100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"bloom_oldest", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 74, 0, 0, NULL,
    0},
  {"chunk_count_limit", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 75, 0, 0, NULL, 0},
  {"chunk_max", "int", NULL, "min=100MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 76,
    100LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"chunk_size", "int", NULL, "min=512K,max=500MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 77,
    512LL * WT_KILOBYTE, 500LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"merge_custom", "category", NULL, NULL, confchk_WT_SESSION_create_merge_custom_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 78, 0, 0, NULL, 0},
  {"merge_max", "int", NULL, "min=2,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 82, 2, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"merge_min", "int", NULL, "max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 83, 0, 100, NULL,
    WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create_tiered_storage_subconfigs[] = {
  {"auth_token", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 48, 0, 0, NULL, 0},
  {"bucket", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 49, 0, 0, NULL, 0},
  {"bucket_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 50, 0, 0, NULL,
    0},
  {"cache_directory", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 51, 0, 0, NULL,
    0},
  {"local_retention", "int", NULL, "min=0,max=10000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 52, 0,
    10000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"object_target_size", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 53, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"shared", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 54, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_create[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"colgroups", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 87, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"exclusive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 98, 0, 0, NULL, 0},
  {"extractor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 63, 0, 0, NULL, 0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"immutable", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 64, 0, 0, NULL, 0},
  {"import", "category", NULL, NULL, confchk_WT_SESSION_create_import_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 99, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm", "category", NULL, NULL, confchk_WT_SESSION_create_lsm_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 68, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"source", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 8, 0, 0, NULL, 0},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 9, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_drop[] = {
  {"checkpoint_wait", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 104, 0, 0,
    NULL, 0},
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {"lock_wait", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 106, 0, 0, NULL,
    0},
  {"remove_files", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 107, 0, 0, NULL,
    0},
  {"remove_shared", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 108, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_flush_tier[] = {
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {"lock_wait", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 106, 0, 0, NULL,
    0},
  {"sync", "string", NULL, "choices=[\"off\",\"on\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 114,
    0, 0, NULL, 0},
  {"timeout", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 97, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_join[] = {
  {"bloom_bit_count", "int", NULL, "min=2,max=1000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 71, 2,
    1000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"bloom_false_positives", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 109, 0,
    0, NULL, 0},
  {"bloom_hash_count", "int", NULL, "min=2,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 73, 2,
    100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compare", "string", NULL, "choices=[\"eq\",\"ge\",\"gt\",\"le\",\"lt\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 110, 0, 0, NULL, 0},
  {"count", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 111, 0, 0, NULL, 0},
  {"operation", "string", NULL, "choices=[\"and\",\"or\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING,
    112, 0, 0, NULL, 0},
  {"strategy", "string", NULL, "choices=[\"bloom\",\"default\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 113, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_log_flush[] = {
  {"sync", "string", NULL, "choices=[\"off\",\"on\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 114,
    0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_open_cursor_debug_subconfigs[] = {
  {"checkpoint_read_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 118,
    0, 0, NULL, 0},
  {"dump_version", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 119, 0, 0, NULL,
    0},
  {"release_evict", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 120, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_open_cursor_incremental_subconfigs[] = {
  {"consolidate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 123, 0, 0, NULL,
    0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"file", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 124, 0, 0, NULL, 0},
  {"force_stop", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 125, 0, 0, NULL,
    0},
  {"granularity", "int", NULL, "min=4KB,max=2GB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 126,
    4LL * WT_KILOBYTE, 2LL * WT_GIGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"src_id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 127, 0, 0, NULL, 0},
  {"this_id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 128, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_open_cursor[] = {
  {"append", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 90, 0, 0, NULL, 0},
  {"bulk", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 115, 0, 0, NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"checkpoint_use_history", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 116,
    0, 0, NULL, 0},
  {"checkpoint_wait", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 104, 0, 0,
    NULL, 0},
  {"debug", "category", NULL, NULL, confchk_WT_SESSION_open_cursor_debug_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 117, 0, 0, NULL, 0},
  {"dump", "string", NULL,
    "choices=[\"hex\",\"json\",\"pretty\",\"pretty_hex\","
    "\"print\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 121, 0, 0, NULL, 0},
  {"incremental", "category", NULL, NULL, confchk_WT_SESSION_open_cursor_incremental_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 122, 0, 0, NULL, 0},
  {"next_random", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 129, 0, 0, NULL,
    0},
  {"next_random_sample_size", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 130, 0,
    0, NULL, 0},
  {"overwrite", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 91, 0, 0, NULL, 0},
  {"prefix_search", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 92, 0, 0, NULL,
    0},
  {"raw", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 131, 0, 0, NULL, 0},
  {"read_once", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 132, 0, 0, NULL,
    0},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"skip_sort_check", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 133, 0, 0,
    NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"clear\","
    "\"size\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 135, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_prepare_transaction[] = {
  {"prepare_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 155, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_query_timestamp[] = {
  {"get", "string", NULL,
    "choices=[\"commit\",\"first_commit\",\"prepare\","
    "\"read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 136, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_reconfigure[] = {
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"debug", "category", NULL, NULL, confchk_WT_CONNECTION_open_session_debug_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 117, 0, 0, NULL, 0},
  {"ignore_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 252, 0, 0,
    NULL, 0},
  {"isolation", "string", NULL,
    "choices=[\"read-uncommitted\",\"read-committed\","
    "\"snapshot\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 148, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_rollback_transaction[] = {
  {"operation_timeout_ms", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 1, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_salvage[] = {
  {"force", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 105, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_timestamp_transaction[] = {
  {"commit_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 2, 0, 0, NULL,
    0},
  {"durable_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 3, 0, 0,
    NULL, 0},
  {"prepare_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 155, 0, 0,
    NULL, 0},
  {"read_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 4, 0, 0, NULL,
    0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_WT_SESSION_verify[] = {
  {"do_not_clear_txn_id", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 137, 0,
    0, NULL, 0},
  {"dump_address", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 138, 0, 0, NULL,
    0},
  {"dump_app_data", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 139, 0, 0,
    NULL, 0},
  {"dump_blocks", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 140, 0, 0, NULL,
    0},
  {"dump_layout", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 141, 0, 0, NULL,
    0},
  {"dump_offsets", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 142, 0, 0, NULL, 0},
  {"dump_pages", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 143, 0, 0, NULL,
    0},
  {"read_corrupt", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 144, 0, 0, NULL,
    0},
  {"stable_timestamp", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 145, 0, 0,
    NULL, 0},
  {"strict", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 146, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_colgroup_meta[] = {
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"source", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 8, 0, 0, NULL, 0},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 9, 0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_file_config[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_file_meta[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"checkpoint_backup_info", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 57, 0,
    0, NULL, 0},
  {"checkpoint_lsn", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 58, 0, 0, NULL,
    0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 59, 0, 0, NULL, 0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_object", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 61, 0, 0, NULL,
    0},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_index_meta[] = {
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"extractor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 63, 0, 0, NULL, 0},
  {"immutable", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 64, 0, 0, NULL, 0},
  {"index_key_columns", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 65, 0, 0, NULL, 0},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"source", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 8, 0, 0, NULL, 0},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 9, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_lsm_meta[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"chunks", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 66, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"last", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 67, 0, 0, NULL, 0},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm", "category", NULL, NULL, confchk_WT_SESSION_create_lsm_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 68, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"old_chunks", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 84, 0, 0, NULL, 0},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_object_meta[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"checkpoint_backup_info", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 57, 0,
    0, NULL, 0},
  {"checkpoint_lsn", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 58, 0, 0, NULL,
    0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"flush_time", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 85, 0, 0, NULL, 0},
  {"flush_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 86, 0, 0, NULL,
    0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 59, 0, 0, NULL, 0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_object", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 61, 0, 0, NULL,
    0},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_table_meta[] = {
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"colgroups", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 87, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_tier_meta[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"bucket", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 49, 0, 0, NULL, 0},
  {"bucket_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 50, 0, 0, NULL,
    0},
  {"cache_directory", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 51, 0, 0, NULL,
    0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"checkpoint_backup_info", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 57, 0,
    0, NULL, 0},
  {"checkpoint_lsn", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 58, 0, 0, NULL,
    0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 59, 0, 0, NULL, 0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_object", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 61, 0, 0, NULL,
    0},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_tiered_meta[] = {
  {"access_pattern_hint", "string", NULL, "choices=[\"none\",\"random\",\"sequential\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 12, 0, 0, NULL, 0},
  {"allocation_size", "int", NULL, "min=512B,max=128MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 13,
    512, 128LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"app_metadata", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 0, 0, 0, NULL, 0},
  {"assert", "category", NULL, NULL, confchk_WT_SESSION_create_assert_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 1, 0, 0, NULL, 0},
  {"block_allocation", "string", NULL, "choices=[\"best\",\"first\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 14, 0, 0, NULL, 0},
  {"block_compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 15, 0, 0,
    NULL, 0},
  {"cache_resident", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 16, 0, 0,
    NULL, 0},
  {"checkpoint", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 56, 0, 0, NULL, 0},
  {"checkpoint_backup_info", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 57, 0,
    0, NULL, 0},
  {"checkpoint_lsn", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 58, 0, 0, NULL,
    0},
  {"checksum", "string", NULL,
    "choices=[\"on\",\"off\",\"uncompressed\","
    "\"unencrypted\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 17, 0, 0, NULL, 0},
  {"collator", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 6, 0, 0, NULL, 0},
  {"columns", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 7, 0, 0, NULL, 0},
  {"dictionary", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 18, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"encryption", "category", NULL, NULL, confchk_WT_SESSION_create_encryption_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"flush_time", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 85, 0, 0, NULL, 0},
  {"flush_timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 86, 0, 0, NULL,
    0},
  {"format", "string", NULL, "choices=[\"btree\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0,
    0, NULL, 0},
  {"huffman_key", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 23, 0, 0, NULL, 0},
  {"huffman_value", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 24, 0, 0, NULL,
    0},
  {"id", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 59, 0, 0, NULL, 0},
  {"ignore_in_memory_cache_size", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN,
    25, 0, 0, NULL, 0},
  {"internal_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 26, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 27, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"internal_key_truncate", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 28, 0,
    0, NULL, 0},
  {"internal_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 29,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 30,
    0, 0, NULL, 0},
  {"key_gap", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"last", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 67, 0, 0, NULL, 0},
  {"leaf_item_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_key_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 33, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"leaf_page_max", "int", NULL, "min=512B,max=512MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 34,
    512, 512LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"leaf_value_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"log", "category", NULL, NULL, confchk_WT_SESSION_create_log_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"memory_page_image_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 38, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"memory_page_max", "int", NULL, "min=512B,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 39,
    512, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"oldest", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 88, 0, 0, NULL, 0},
  {"os_cache_dirty_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 40, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"os_cache_max", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 41, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"prefix_compression", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 42, 0, 0,
    NULL, 0},
  {"prefix_compression_min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 43, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"split_deepen_min_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 44, 0, 0,
    NULL, 0},
  {"split_deepen_per_child", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 45, 0, 0,
    NULL, 0},
  {"split_pct", "int", NULL, "min=50,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 46, 50, 100,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"tiered_object", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 61, 0, 0, NULL,
    0},
  {"tiered_storage", "category", NULL, NULL, confchk_WT_SESSION_create_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"tiers", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 89, 0, 0, NULL, 0},
  {"value_format", "format", __wt_struct_confchk, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_FORMAT, 55,
    0, 0, NULL, 0},
  {"verbose", "list", NULL, "choices=[\"write_timestamp\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    10, 0, 0, NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_timestamp_usage", "string", NULL,
    "choices=[\"always\",\"key_consistent\",\"mixed_mode\","
    "\"never\",\"none\",\"ordered\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 11, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_compatibility_subconfigs[] = {
  {"release", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 189, 0, 0, NULL, 0},
  {"require_max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 259, 0, 0, NULL,
    0},
  {"require_min", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 260, 0, 0, NULL,
    0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_encryption_subconfigs[] = {
  {"keyid", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 21, 0, 0, NULL, 0},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"secretkey", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 265, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_hash_subconfigs[] = {
  {"buckets", "int", NULL, "min=64,max=65536", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 269, 64, 65536,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"dhandle_buckets", "int", NULL, "min=64,max=65536", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 270,
    64, 65536, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_log_subconfigs[] = {
  {"archive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 226, 0, 0, NULL, 0},
  {"compressor", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 273, 0, 0, NULL, 0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"file_max", "int", NULL, "min=100KB,max=2GB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 222,
    100LL * WT_KILOBYTE, 2LL * WT_GIGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"force_write_wait", "int", NULL, "min=1,max=60", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 274, 1,
    60, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"os_cache_dirty_pct", "int", NULL, "min=0,max=100", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 227, 0,
    100, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"path", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 235, 0, 0, NULL, 0},
  {"prealloc", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 228, 0, 0, NULL, 0},
  {"recover", "string", NULL, "choices=[\"error\",\"on\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING,
    275, 0, 0, NULL, 0},
  {"remove", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 229, 0, 0, NULL, 0},
  {"zero_fill", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 230, 0, 0, NULL,
    0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_statistics_log_subconfigs[] = {
  {"json", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 241, 0, 0, NULL, 0},
  {"on_close", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 242, 0, 0, NULL, 0},
  {"path", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 235, 0, 0, NULL, 0},
  {"sources", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 243, 0, 0, NULL, 0},
  {"timestamp", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 244, 0, 0, NULL, 0},
  {"wait", "int", NULL, "min=0,max=100000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 182, 0, 100000,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_tiered_storage_subconfigs[] = {
  {"auth_token", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 48, 0, 0, NULL, 0},
  {"bucket", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 49, 0, 0, NULL, 0},
  {"bucket_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 50, 0, 0, NULL,
    0},
  {"cache_directory", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 51, 0, 0, NULL,
    0},
  {"interval", "int", NULL, "min=1,max=1000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 283, 1, 1000,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"local_retention", "int", NULL, "min=0,max=10000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 52, 0,
    10000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"name", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 20, 0, 0, NULL, 0},
  {"shared", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 54, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_transaction_sync_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 37, 0, 0, NULL, 0},
  {"method", "string", NULL, "choices=[\"dsync\",\"fsync\",\"none\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 285, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open[] = {
  {"backup_restore_target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 255, 0, 0,
    NULL, 0},
  {"block_cache", "category", NULL, NULL, confchk_wiredtiger_open_block_cache_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 166, 0, 0, NULL, 0},
  {"buffer_alignment", "int", NULL, "min=-1,max=1MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 256, 1,
    1LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"builtin_extension_config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 257,
    0, 0, NULL, 0},
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_overhead", "int", NULL, "min=0,max=30", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 178, 0, 30,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 179,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_stuck_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 180, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"checkpoint", "category", NULL, NULL, confchk_wiredtiger_open_checkpoint_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 56, 0, 0, NULL, 0},
  {"checkpoint_cleanup", "string", NULL, "choices=[\"none\",\"reclaim_space\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 183, 0, 0, NULL, 0},
  {"checkpoint_sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 258, 0, 0,
    NULL, 0},
  {"chunk_cache", "category", NULL, NULL, confchk_wiredtiger_open_chunk_cache_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 184, 0, 0, NULL, 0},
  {"compatibility", "category", NULL, NULL, confchk_wiredtiger_open_compatibility_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 188, 0, 0, NULL, 0},
  {"compile_configuration_count", "int", NULL, "min=500", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 261,
    500, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"config_base", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 262, 0, 0, NULL,
    0},
  {"create", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 263, 0, 0, NULL, 0},
  {"debug_mode", "category", NULL, NULL, confchk_wiredtiger_open_debug_mode_subconfigs, 14,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 190, 0, 0, NULL, 0},
  {"direct_io", "list", NULL, "choices=[\"checkpoint\",\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 264, 0, 0, NULL, 0},
  {"encryption", "category", NULL, NULL, confchk_wiredtiger_open_encryption_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"error_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 205, 0, 0, NULL,
    0},
  {"eviction", "category", NULL, NULL, confchk_wiredtiger_open_eviction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 195, 0, 0, NULL, 0},
  {"eviction_checkpoint_target", "int", NULL, "min=0,max=10TB", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 208, 0, 10LL * WT_TERABYTE, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_target", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    209, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_trigger", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    210, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_target", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 211, 10,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_trigger", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 212,
    10, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_target", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    213, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_trigger", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    214, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"exclusive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 98, 0, 0, NULL, 0},
  {"extensions", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 266, 0, 0, NULL, 0},
  {"extra_diagnostics", "list", NULL,
    "choices=[\"all\",\"checkpoint_validate\",\"cursor_check\""
    ",\"disk_validate\",\"eviction_check\",\"generation_check\","
    "\"hs_validate\",\"key_out_of_order\",\"log_validate\","
    "\"prepared\",\"slow_operation\",\"txn_visibility\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 215, 0, 0, NULL, 0},
  {"file_extend", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    267, 0, 0, NULL, 0},
  {"file_manager", "category", NULL, NULL, confchk_wiredtiger_open_file_manager_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 216, 0, 0, NULL, 0},
  {"generation_drain_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 220,
    0, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"hash", "category", NULL, NULL, confchk_wiredtiger_open_hash_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 268, 0, 0, NULL, 0},
  {"hazard_max", "int", NULL, "min=15", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 271, 15, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"history_store", "category", NULL, NULL, confchk_wiredtiger_open_history_store_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 221, 0, 0, NULL, 0},
  {"in_memory", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 272, 0, 0, NULL,
    0},
  {"io_capacity", "category", NULL, NULL, confchk_wiredtiger_open_io_capacity_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 223, 0, 0, NULL, 0},
  {"json_output", "list", NULL, "choices=[\"error\",\"message\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 225, 0, 0, NULL, 0},
  {"log", "category", NULL, NULL, confchk_wiredtiger_open_log_subconfigs, 11,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm_manager", "category", NULL, NULL, confchk_wiredtiger_open_lsm_manager_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 231, 0, 0, NULL, 0},
  {"mmap", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 276, 0, 0, NULL, 0},
  {"mmap_all", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 277, 0, 0, NULL, 0},
  {"multiprocess", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 278, 0, 0, NULL,
    0},
  {"operation_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"operation_tracking", "category", NULL, NULL,
    confchk_wiredtiger_open_operation_tracking_subconfigs, 2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 234,
    0, 0, NULL, 0},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"salvage", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 279, 0, 0, NULL, 0},
  {"session_max", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 280, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"session_scratch_max", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 281, 0, 0, NULL,
    0},
  {"session_table_cache", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 282, 0,
    0, NULL, 0},
  {"shared_cache", "category", NULL, NULL, confchk_wiredtiger_open_shared_cache_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 236, 0, 0, NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"none\","
    "\"clear\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"statistics_log", "category", NULL, NULL, confchk_wiredtiger_open_statistics_log_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 240, 0, 0, NULL, 0},
  {"tiered_storage", "category", NULL, NULL, confchk_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"timing_stress_for_test", "list", NULL,
    "choices=[\"aggressive_sweep\",\"backup_rename\","
    "\"checkpoint_evict_page\",\"checkpoint_handle\","
    "\"checkpoint_slow\",\"checkpoint_stop\",\"compact_slow\","
    "\"evict_reposition\","
    "\"failpoint_eviction_fail_after_reconciliation\","
    "\"failpoint_history_store_delete_key_from_ts\","
    "\"history_store_checkpoint_delay\",\"history_store_search\","
    "\"history_store_sweep_race\",\"prepare_checkpoint_delay\","
    "\"prepare_resolution\",\"sleep_before_read_overflow_onpage\","
    "\"split_1\",\"split_2\",\"split_3\",\"split_4\",\"split_5\","
    "\"split_6\",\"split_7\",\"split_8\",\"tiered_flush_finish\","
    "\"txn_read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 245, 0, 0, NULL, 0},
  {"transaction_sync", "category", NULL, NULL, confchk_wiredtiger_open_transaction_sync_subconfigs,
    2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 284, 0, 0, NULL, 0},
  {"use_environment", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 286, 0, 0,
    NULL, 0},
  {"use_environment_priv", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 287, 0,
    0, NULL, 0},
  {"verbose", "list", NULL,
    "choices=[\"api\",\"backup\",\"block\",\"block_cache\","
    "\"checkpoint\",\"checkpoint_cleanup\",\"checkpoint_progress\","
    "\"chunkcache\",\"compact\",\"compact_progress\","
    "\"error_returns\",\"evict\",\"evict_stuck\",\"evictserver\","
    "\"fileops\",\"generation\",\"handleops\",\"history_store\","
    "\"history_store_activity\",\"log\",\"lsm\",\"lsm_manager\","
    "\"metadata\",\"mutex\",\"out_of_order\",\"overflow\",\"read\","
    "\"reconcile\",\"recovery\",\"recovery_progress\",\"rts\","
    "\"salvage\",\"shared_cache\",\"split\",\"temporary\","
    "\"thread_group\",\"tiered\",\"timestamp\",\"transaction\","
    "\"verify\",\"version\",\"write\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 10, 0, 0, NULL, 0},
  {"verify_metadata", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 288, 0, 0,
    NULL, 0},
  {"write_through", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 289, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_all[] = {
  {"backup_restore_target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 255, 0, 0,
    NULL, 0},
  {"block_cache", "category", NULL, NULL, confchk_wiredtiger_open_block_cache_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 166, 0, 0, NULL, 0},
  {"buffer_alignment", "int", NULL, "min=-1,max=1MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 256, 1,
    1LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"builtin_extension_config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 257,
    0, 0, NULL, 0},
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_overhead", "int", NULL, "min=0,max=30", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 178, 0, 30,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 179,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_stuck_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 180, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"checkpoint", "category", NULL, NULL, confchk_wiredtiger_open_checkpoint_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 56, 0, 0, NULL, 0},
  {"checkpoint_cleanup", "string", NULL, "choices=[\"none\",\"reclaim_space\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 183, 0, 0, NULL, 0},
  {"checkpoint_sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 258, 0, 0,
    NULL, 0},
  {"chunk_cache", "category", NULL, NULL, confchk_wiredtiger_open_chunk_cache_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 184, 0, 0, NULL, 0},
  {"compatibility", "category", NULL, NULL, confchk_wiredtiger_open_compatibility_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 188, 0, 0, NULL, 0},
  {"compile_configuration_count", "int", NULL, "min=500", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 261,
    500, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"config_base", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 262, 0, 0, NULL,
    0},
  {"create", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 263, 0, 0, NULL, 0},
  {"debug_mode", "category", NULL, NULL, confchk_wiredtiger_open_debug_mode_subconfigs, 14,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 190, 0, 0, NULL, 0},
  {"direct_io", "list", NULL, "choices=[\"checkpoint\",\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 264, 0, 0, NULL, 0},
  {"encryption", "category", NULL, NULL, confchk_wiredtiger_open_encryption_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"error_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 205, 0, 0, NULL,
    0},
  {"eviction", "category", NULL, NULL, confchk_wiredtiger_open_eviction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 195, 0, 0, NULL, 0},
  {"eviction_checkpoint_target", "int", NULL, "min=0,max=10TB", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 208, 0, 10LL * WT_TERABYTE, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_target", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    209, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_trigger", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    210, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_target", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 211, 10,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_trigger", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 212,
    10, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_target", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    213, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_trigger", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    214, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"exclusive", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 98, 0, 0, NULL, 0},
  {"extensions", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 266, 0, 0, NULL, 0},
  {"extra_diagnostics", "list", NULL,
    "choices=[\"all\",\"checkpoint_validate\",\"cursor_check\""
    ",\"disk_validate\",\"eviction_check\",\"generation_check\","
    "\"hs_validate\",\"key_out_of_order\",\"log_validate\","
    "\"prepared\",\"slow_operation\",\"txn_visibility\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 215, 0, 0, NULL, 0},
  {"file_extend", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    267, 0, 0, NULL, 0},
  {"file_manager", "category", NULL, NULL, confchk_wiredtiger_open_file_manager_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 216, 0, 0, NULL, 0},
  {"generation_drain_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 220,
    0, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"hash", "category", NULL, NULL, confchk_wiredtiger_open_hash_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 268, 0, 0, NULL, 0},
  {"hazard_max", "int", NULL, "min=15", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 271, 15, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"history_store", "category", NULL, NULL, confchk_wiredtiger_open_history_store_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 221, 0, 0, NULL, 0},
  {"in_memory", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 272, 0, 0, NULL,
    0},
  {"io_capacity", "category", NULL, NULL, confchk_wiredtiger_open_io_capacity_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 223, 0, 0, NULL, 0},
  {"json_output", "list", NULL, "choices=[\"error\",\"message\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 225, 0, 0, NULL, 0},
  {"log", "category", NULL, NULL, confchk_wiredtiger_open_log_subconfigs, 11,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm_manager", "category", NULL, NULL, confchk_wiredtiger_open_lsm_manager_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 231, 0, 0, NULL, 0},
  {"mmap", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 276, 0, 0, NULL, 0},
  {"mmap_all", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 277, 0, 0, NULL, 0},
  {"multiprocess", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 278, 0, 0, NULL,
    0},
  {"operation_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"operation_tracking", "category", NULL, NULL,
    confchk_wiredtiger_open_operation_tracking_subconfigs, 2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 234,
    0, 0, NULL, 0},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"salvage", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 279, 0, 0, NULL, 0},
  {"session_max", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 280, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"session_scratch_max", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 281, 0, 0, NULL,
    0},
  {"session_table_cache", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 282, 0,
    0, NULL, 0},
  {"shared_cache", "category", NULL, NULL, confchk_wiredtiger_open_shared_cache_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 236, 0, 0, NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"none\","
    "\"clear\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"statistics_log", "category", NULL, NULL, confchk_wiredtiger_open_statistics_log_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 240, 0, 0, NULL, 0},
  {"tiered_storage", "category", NULL, NULL, confchk_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"timing_stress_for_test", "list", NULL,
    "choices=[\"aggressive_sweep\",\"backup_rename\","
    "\"checkpoint_evict_page\",\"checkpoint_handle\","
    "\"checkpoint_slow\",\"checkpoint_stop\",\"compact_slow\","
    "\"evict_reposition\","
    "\"failpoint_eviction_fail_after_reconciliation\","
    "\"failpoint_history_store_delete_key_from_ts\","
    "\"history_store_checkpoint_delay\",\"history_store_search\","
    "\"history_store_sweep_race\",\"prepare_checkpoint_delay\","
    "\"prepare_resolution\",\"sleep_before_read_overflow_onpage\","
    "\"split_1\",\"split_2\",\"split_3\",\"split_4\",\"split_5\","
    "\"split_6\",\"split_7\",\"split_8\",\"tiered_flush_finish\","
    "\"txn_read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 245, 0, 0, NULL, 0},
  {"transaction_sync", "category", NULL, NULL, confchk_wiredtiger_open_transaction_sync_subconfigs,
    2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 284, 0, 0, NULL, 0},
  {"use_environment", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 286, 0, 0,
    NULL, 0},
  {"use_environment_priv", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 287, 0,
    0, NULL, 0},
  {"verbose", "list", NULL,
    "choices=[\"api\",\"backup\",\"block\",\"block_cache\","
    "\"checkpoint\",\"checkpoint_cleanup\",\"checkpoint_progress\","
    "\"chunkcache\",\"compact\",\"compact_progress\","
    "\"error_returns\",\"evict\",\"evict_stuck\",\"evictserver\","
    "\"fileops\",\"generation\",\"handleops\",\"history_store\","
    "\"history_store_activity\",\"log\",\"lsm\",\"lsm_manager\","
    "\"metadata\",\"mutex\",\"out_of_order\",\"overflow\",\"read\","
    "\"reconcile\",\"recovery\",\"recovery_progress\",\"rts\","
    "\"salvage\",\"shared_cache\",\"split\",\"temporary\","
    "\"thread_group\",\"tiered\",\"timestamp\",\"transaction\","
    "\"verify\",\"version\",\"write\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 10, 0, 0, NULL, 0},
  {"verify_metadata", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 288, 0, 0,
    NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_through", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 289, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_basecfg[] = {
  {"backup_restore_target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 255, 0, 0,
    NULL, 0},
  {"block_cache", "category", NULL, NULL, confchk_wiredtiger_open_block_cache_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 166, 0, 0, NULL, 0},
  {"buffer_alignment", "int", NULL, "min=-1,max=1MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 256, 1,
    1LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"builtin_extension_config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 257,
    0, 0, NULL, 0},
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_overhead", "int", NULL, "min=0,max=30", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 178, 0, 30,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 179,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_stuck_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 180, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"checkpoint", "category", NULL, NULL, confchk_wiredtiger_open_checkpoint_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 56, 0, 0, NULL, 0},
  {"checkpoint_cleanup", "string", NULL, "choices=[\"none\",\"reclaim_space\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 183, 0, 0, NULL, 0},
  {"checkpoint_sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 258, 0, 0,
    NULL, 0},
  {"chunk_cache", "category", NULL, NULL, confchk_wiredtiger_open_chunk_cache_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 184, 0, 0, NULL, 0},
  {"compatibility", "category", NULL, NULL, confchk_wiredtiger_open_compatibility_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 188, 0, 0, NULL, 0},
  {"compile_configuration_count", "int", NULL, "min=500", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 261,
    500, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"debug_mode", "category", NULL, NULL, confchk_wiredtiger_open_debug_mode_subconfigs, 14,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 190, 0, 0, NULL, 0},
  {"direct_io", "list", NULL, "choices=[\"checkpoint\",\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 264, 0, 0, NULL, 0},
  {"encryption", "category", NULL, NULL, confchk_wiredtiger_open_encryption_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"error_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 205, 0, 0, NULL,
    0},
  {"eviction", "category", NULL, NULL, confchk_wiredtiger_open_eviction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 195, 0, 0, NULL, 0},
  {"eviction_checkpoint_target", "int", NULL, "min=0,max=10TB", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 208, 0, 10LL * WT_TERABYTE, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_target", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    209, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_trigger", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    210, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_target", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 211, 10,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_trigger", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 212,
    10, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_target", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    213, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_trigger", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    214, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"extensions", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 266, 0, 0, NULL, 0},
  {"extra_diagnostics", "list", NULL,
    "choices=[\"all\",\"checkpoint_validate\",\"cursor_check\""
    ",\"disk_validate\",\"eviction_check\",\"generation_check\","
    "\"hs_validate\",\"key_out_of_order\",\"log_validate\","
    "\"prepared\",\"slow_operation\",\"txn_visibility\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 215, 0, 0, NULL, 0},
  {"file_extend", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    267, 0, 0, NULL, 0},
  {"file_manager", "category", NULL, NULL, confchk_wiredtiger_open_file_manager_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 216, 0, 0, NULL, 0},
  {"generation_drain_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 220,
    0, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"hash", "category", NULL, NULL, confchk_wiredtiger_open_hash_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 268, 0, 0, NULL, 0},
  {"hazard_max", "int", NULL, "min=15", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 271, 15, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"history_store", "category", NULL, NULL, confchk_wiredtiger_open_history_store_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 221, 0, 0, NULL, 0},
  {"io_capacity", "category", NULL, NULL, confchk_wiredtiger_open_io_capacity_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 223, 0, 0, NULL, 0},
  {"json_output", "list", NULL, "choices=[\"error\",\"message\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 225, 0, 0, NULL, 0},
  {"log", "category", NULL, NULL, confchk_wiredtiger_open_log_subconfigs, 11,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm_manager", "category", NULL, NULL, confchk_wiredtiger_open_lsm_manager_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 231, 0, 0, NULL, 0},
  {"mmap", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 276, 0, 0, NULL, 0},
  {"mmap_all", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 277, 0, 0, NULL, 0},
  {"multiprocess", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 278, 0, 0, NULL,
    0},
  {"operation_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"operation_tracking", "category", NULL, NULL,
    confchk_wiredtiger_open_operation_tracking_subconfigs, 2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 234,
    0, 0, NULL, 0},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"salvage", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 279, 0, 0, NULL, 0},
  {"session_max", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 280, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"session_scratch_max", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 281, 0, 0, NULL,
    0},
  {"session_table_cache", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 282, 0,
    0, NULL, 0},
  {"shared_cache", "category", NULL, NULL, confchk_wiredtiger_open_shared_cache_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 236, 0, 0, NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"none\","
    "\"clear\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"statistics_log", "category", NULL, NULL, confchk_wiredtiger_open_statistics_log_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 240, 0, 0, NULL, 0},
  {"tiered_storage", "category", NULL, NULL, confchk_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"timing_stress_for_test", "list", NULL,
    "choices=[\"aggressive_sweep\",\"backup_rename\","
    "\"checkpoint_evict_page\",\"checkpoint_handle\","
    "\"checkpoint_slow\",\"checkpoint_stop\",\"compact_slow\","
    "\"evict_reposition\","
    "\"failpoint_eviction_fail_after_reconciliation\","
    "\"failpoint_history_store_delete_key_from_ts\","
    "\"history_store_checkpoint_delay\",\"history_store_search\","
    "\"history_store_sweep_race\",\"prepare_checkpoint_delay\","
    "\"prepare_resolution\",\"sleep_before_read_overflow_onpage\","
    "\"split_1\",\"split_2\",\"split_3\",\"split_4\",\"split_5\","
    "\"split_6\",\"split_7\",\"split_8\",\"tiered_flush_finish\","
    "\"txn_read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 245, 0, 0, NULL, 0},
  {"transaction_sync", "category", NULL, NULL, confchk_wiredtiger_open_transaction_sync_subconfigs,
    2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 284, 0, 0, NULL, 0},
  {"verbose", "list", NULL,
    "choices=[\"api\",\"backup\",\"block\",\"block_cache\","
    "\"checkpoint\",\"checkpoint_cleanup\",\"checkpoint_progress\","
    "\"chunkcache\",\"compact\",\"compact_progress\","
    "\"error_returns\",\"evict\",\"evict_stuck\",\"evictserver\","
    "\"fileops\",\"generation\",\"handleops\",\"history_store\","
    "\"history_store_activity\",\"log\",\"lsm\",\"lsm_manager\","
    "\"metadata\",\"mutex\",\"out_of_order\",\"overflow\",\"read\","
    "\"reconcile\",\"recovery\",\"recovery_progress\",\"rts\","
    "\"salvage\",\"shared_cache\",\"split\",\"temporary\","
    "\"thread_group\",\"tiered\",\"timestamp\",\"transaction\","
    "\"verify\",\"version\",\"write\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 10, 0, 0, NULL, 0},
  {"verify_metadata", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 288, 0, 0,
    NULL, 0},
  {"version", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 62, 0, 0, NULL, 0},
  {"write_through", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 289, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_wiredtiger_open_usercfg[] = {
  {"backup_restore_target", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 255, 0, 0,
    NULL, 0},
  {"block_cache", "category", NULL, NULL, confchk_wiredtiger_open_block_cache_subconfigs, 12,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 166, 0, 0, NULL, 0},
  {"buffer_alignment", "int", NULL, "min=-1,max=1MB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 256, 1,
    1LL * WT_MEGABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"builtin_extension_config", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 257,
    0, 0, NULL, 0},
  {"cache_cursors", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 250, 0, 0,
    NULL, 0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 177, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_overhead", "int", NULL, "min=0,max=30", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 178, 0, 30,
    NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_size", "int", NULL, "min=1MB,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 179,
    1LL * WT_MEGABYTE, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"cache_stuck_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 180, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"checkpoint", "category", NULL, NULL, confchk_wiredtiger_open_checkpoint_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 56, 0, 0, NULL, 0},
  {"checkpoint_cleanup", "string", NULL, "choices=[\"none\",\"reclaim_space\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_STRING, 183, 0, 0, NULL, 0},
  {"checkpoint_sync", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 258, 0, 0,
    NULL, 0},
  {"chunk_cache", "category", NULL, NULL, confchk_wiredtiger_open_chunk_cache_subconfigs, 7,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 184, 0, 0, NULL, 0},
  {"compatibility", "category", NULL, NULL, confchk_wiredtiger_open_compatibility_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 188, 0, 0, NULL, 0},
  {"compile_configuration_count", "int", NULL, "min=500", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 261,
    500, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"debug_mode", "category", NULL, NULL, confchk_wiredtiger_open_debug_mode_subconfigs, 14,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 190, 0, 0, NULL, 0},
  {"direct_io", "list", NULL, "choices=[\"checkpoint\",\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 264, 0, 0, NULL, 0},
  {"encryption", "category", NULL, NULL, confchk_wiredtiger_open_encryption_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 19, 0, 0, NULL, 0},
  {"error_prefix", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 205, 0, 0, NULL,
    0},
  {"eviction", "category", NULL, NULL, confchk_wiredtiger_open_eviction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 195, 0, 0, NULL, 0},
  {"eviction_checkpoint_target", "int", NULL, "min=0,max=10TB", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 208, 0, 10LL * WT_TERABYTE, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_target", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    209, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_dirty_trigger", "int", NULL, "min=1,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    210, 1, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_target", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 211, 10,
    10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_trigger", "int", NULL, "min=10,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 212,
    10, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_target", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    213, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"eviction_updates_trigger", "int", NULL, "min=0,max=10TB", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT,
    214, 0, 10LL * WT_TERABYTE, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"extensions", "list", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 266, 0, 0, NULL, 0},
  {"extra_diagnostics", "list", NULL,
    "choices=[\"all\",\"checkpoint_validate\",\"cursor_check\""
    ",\"disk_validate\",\"eviction_check\",\"generation_check\","
    "\"hs_validate\",\"key_out_of_order\",\"log_validate\","
    "\"prepared\",\"slow_operation\",\"txn_visibility\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 215, 0, 0, NULL, 0},
  {"file_extend", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST,
    267, 0, 0, NULL, 0},
  {"file_manager", "category", NULL, NULL, confchk_wiredtiger_open_file_manager_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 216, 0, 0, NULL, 0},
  {"generation_drain_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 220,
    0, 0, NULL, WT_CONFIG_MIN_LIMIT},
  {"hash", "category", NULL, NULL, confchk_wiredtiger_open_hash_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 268, 0, 0, NULL, 0},
  {"hazard_max", "int", NULL, "min=15", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 271, 15, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"history_store", "category", NULL, NULL, confchk_wiredtiger_open_history_store_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 221, 0, 0, NULL, 0},
  {"io_capacity", "category", NULL, NULL, confchk_wiredtiger_open_io_capacity_subconfigs, 1,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 223, 0, 0, NULL, 0},
  {"json_output", "list", NULL, "choices=[\"error\",\"message\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 225, 0, 0, NULL, 0},
  {"log", "category", NULL, NULL, confchk_wiredtiger_open_log_subconfigs, 11,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 36, 0, 0, NULL, 0},
  {"lsm_manager", "category", NULL, NULL, confchk_wiredtiger_open_lsm_manager_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 231, 0, 0, NULL, 0},
  {"mmap", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 276, 0, 0, NULL, 0},
  {"mmap_all", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 277, 0, 0, NULL, 0},
  {"multiprocess", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 278, 0, 0, NULL,
    0},
  {"operation_timeout_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 150, 0, 0,
    NULL, WT_CONFIG_MIN_LIMIT},
  {"operation_tracking", "category", NULL, NULL,
    confchk_wiredtiger_open_operation_tracking_subconfigs, 2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 234,
    0, 0, NULL, 0},
  {"readonly", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 60, 0, 0, NULL, 0},
  {"salvage", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 279, 0, 0, NULL, 0},
  {"session_max", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 280, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"session_scratch_max", "int", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 281, 0, 0, NULL,
    0},
  {"session_table_cache", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 282, 0,
    0, NULL, 0},
  {"shared_cache", "category", NULL, NULL, confchk_wiredtiger_open_shared_cache_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 236, 0, 0, NULL, 0},
  {"statistics", "list", NULL,
    "choices=[\"all\",\"cache_walk\",\"fast\",\"none\","
    "\"clear\",\"tree_walk\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 134, 0, 0, NULL, 0},
  {"statistics_log", "category", NULL, NULL, confchk_wiredtiger_open_statistics_log_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 240, 0, 0, NULL, 0},
  {"tiered_storage", "category", NULL, NULL, confchk_tiered_storage_subconfigs, 8,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 47, 0, 0, NULL, 0},
  {"timing_stress_for_test", "list", NULL,
    "choices=[\"aggressive_sweep\",\"backup_rename\","
    "\"checkpoint_evict_page\",\"checkpoint_handle\","
    "\"checkpoint_slow\",\"checkpoint_stop\",\"compact_slow\","
    "\"evict_reposition\","
    "\"failpoint_eviction_fail_after_reconciliation\","
    "\"failpoint_history_store_delete_key_from_ts\","
    "\"history_store_checkpoint_delay\",\"history_store_search\","
    "\"history_store_sweep_race\",\"prepare_checkpoint_delay\","
    "\"prepare_resolution\",\"sleep_before_read_overflow_onpage\","
    "\"split_1\",\"split_2\",\"split_3\",\"split_4\",\"split_5\","
    "\"split_6\",\"split_7\",\"split_8\",\"tiered_flush_finish\","
    "\"txn_read\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 245, 0, 0, NULL, 0},
  {"transaction_sync", "category", NULL, NULL, confchk_wiredtiger_open_transaction_sync_subconfigs,
    2, WT_CONFIG_COMPILED_TYPE_CATEGORY, 284, 0, 0, NULL, 0},
  {"verbose", "list", NULL,
    "choices=[\"api\",\"backup\",\"block\",\"block_cache\","
    "\"checkpoint\",\"checkpoint_cleanup\",\"checkpoint_progress\","
    "\"chunkcache\",\"compact\",\"compact_progress\","
    "\"error_returns\",\"evict\",\"evict_stuck\",\"evictserver\","
    "\"fileops\",\"generation\",\"handleops\",\"history_store\","
    "\"history_store_activity\",\"log\",\"lsm\",\"lsm_manager\","
    "\"metadata\",\"mutex\",\"out_of_order\",\"overflow\",\"read\","
    "\"reconcile\",\"recovery\",\"recovery_progress\",\"rts\","
    "\"salvage\",\"shared_cache\",\"split\",\"temporary\","
    "\"thread_group\",\"tiered\",\"timestamp\",\"transaction\","
    "\"verify\",\"version\",\"write\"]",
    NULL, 0, WT_CONFIG_COMPILED_TYPE_LIST, 10, 0, 0, NULL, 0},
  {"verify_metadata", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 288, 0, 0,
    NULL, 0},
  {"write_through", "list", NULL, "choices=[\"data\",\"log\"]", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_LIST, 289, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_ENTRY config_entries[] = {
  {"WT_CONNECTION.add_collator", "", NULL, 0, 1, true},
  {"WT_CONNECTION.add_compressor", "", NULL, 0, 2, true},
  {"WT_CONNECTION.add_data_source", "", NULL, 0, 3, true},
  {"WT_CONNECTION.add_encryptor", "", NULL, 0, 4, true},
  {"WT_CONNECTION.add_extractor", "", NULL, 0, 5, true},
  {"WT_CONNECTION.add_storage_source", "", NULL, 0, 6, true},
  {"WT_CONNECTION.close", "final_flush=false,leak_memory=false,use_timestamp=true",
    confchk_WT_CONNECTION_close, 3, 7, true},
  {"WT_CONNECTION.debug_info",
    "cache=false,cursors=false,handles=false,log=false,sessions=false"
    ",txn=false",
    confchk_WT_CONNECTION_debug_info, 6, 8, true},
  {"WT_CONNECTION.load_extension",
    "config=,early_load=false,entry=wiredtiger_extension_init,"
    "terminate=wiredtiger_extension_terminate",
    confchk_WT_CONNECTION_load_extension, 4, 9, true},
  {"WT_CONNECTION.open_session",
    "cache_cursors=true,cache_max_wait_ms=0,"
    "debug=(release_evict_page=false),ignore_cache_size=false,"
    "isolation=snapshot",
    confchk_WT_CONNECTION_open_session, 5, 10, true},
  {"WT_CONNECTION.query_timestamp", "get=all_durable", confchk_WT_CONNECTION_query_timestamp, 1, 11,
    true},
  {"WT_CONNECTION.reconfigure",
    "block_cache=(blkcache_eviction_aggression=1800,"
    "cache_on_checkpoint=true,cache_on_writes=true,enabled=false,"
    "full_target=95,hashsize=32768,max_percent_overhead=10,"
    "nvram_path=,percent_file_in_dram=50,size=0,system_ram=0,type=),"
    "cache_max_wait_ms=0,cache_overhead=8,cache_size=100MB,"
    "cache_stuck_timeout_ms=300000,checkpoint=(log_size=0,wait=0),"
    "checkpoint_cleanup=none,chunk_cache=(capacity=10GB,"
    "chunk_cache_evict_trigger=90,chunk_size=1MB,device_path=,"
    "enabled=false,hashsize=1024,type=),compatibility=(release=),"
    "debug_mode=(checkpoint_retention=0,corruption_abort=true,"
    "cursor_copy=false,cursor_reposition=false,eviction=false,"
    "log_retention=0,realloc_exact=false,realloc_malloc=false,"
    "rollback_error=0,slow_checkpoint=false,stress_skiplist=false,"
    "table_logging=false,tiered_flush_error_continue=false,"
    "update_restore_evict=false),error_prefix=,"
    "eviction=(threads_max=8,threads_min=1),"
    "eviction_checkpoint_target=1,eviction_dirty_target=5,"
    "eviction_dirty_trigger=20,eviction_target=80,eviction_trigger=95"
    ",eviction_updates_target=0,eviction_updates_trigger=0,"
    "extra_diagnostics=[],file_manager=(close_handle_minimum=250,"
    "close_idle_time=30,close_scan_interval=10),"
    "generation_drain_timeout_ms=240000,history_store=(file_max=0),"
    "io_capacity=(total=0),json_output=[],log=(archive=true,"
    "os_cache_dirty_pct=0,prealloc=true,remove=true,zero_fill=false),"
    "lsm_manager=(merge=true,worker_thread_max=4),"
    "operation_timeout_ms=0,operation_tracking=(enabled=false,"
    "path=\".\"),shared_cache=(chunk=10MB,name=,quota=0,reserve=0,"
    "size=500MB),statistics=none,statistics_log=(json=false,"
    "on_close=false,sources=,timestamp=\"%b %d %H:%M:%S\",wait=0),"
    "tiered_storage=(local_retention=300),timing_stress_for_test=,"
    "verbose=[]",
    confchk_WT_CONNECTION_reconfigure, 35, 12, true},
  {"WT_CONNECTION.rollback_to_stable", "dryrun=false", confchk_WT_CONNECTION_rollback_to_stable, 1,
    13, true},
  {"WT_CONNECTION.set_file_system", "", NULL, 0, 14, true},
  {"WT_CONNECTION.set_timestamp",
    "durable_timestamp=,force=false,oldest_timestamp=,"
    "stable_timestamp=",
    confchk_WT_CONNECTION_set_timestamp, 4, 15, true},
  {"WT_CURSOR.bound", "action=set,bound=,inclusive=true", confchk_WT_CURSOR_bound, 3, 16, true},
  {"WT_CURSOR.close", "", NULL, 0, 17, true},
  {"WT_CURSOR.reconfigure", "append=false,overwrite=true,prefix_search=false",
    confchk_WT_CURSOR_reconfigure, 3, 18, true},
  {"WT_SESSION.alter",
    "access_pattern_hint=none,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),cache_resident=false,"
    "checkpoint=,exclusive_refreshed=true,log=(enabled=true),"
    "os_cache_dirty_max=0,os_cache_max=0,verbose=[],"
    "write_timestamp_usage=none",
    confchk_WT_SESSION_alter, 11, 19, true},
  {"WT_SESSION.begin_transaction",
    "ignore_prepare=false,isolation=,name=,no_timestamp=false,"
    "operation_timeout_ms=0,priority=0,read_timestamp=,"
    "roundup_timestamps=(prepared=false,read=false),sync=",
    confchk_WT_SESSION_begin_transaction, 9, 20, true},
  {"WT_SESSION.checkpoint",
    "drop=,flush_tier=(enabled=false,force=false,sync=true,timeout=0)"
    ",force=false,name=,target=,use_timestamp=true",
    confchk_WT_SESSION_checkpoint, 6, 21, true},
  {"WT_SESSION.close", "", NULL, 0, 22, true},
  {"WT_SESSION.commit_transaction",
    "commit_timestamp=,durable_timestamp=,operation_timeout_ms=0,"
    "sync=",
    confchk_WT_SESSION_commit_transaction, 4, 23, true},
  {"WT_SESSION.compact", "timeout=1200", confchk_WT_SESSION_compact, 1, 24, true},
  {"WT_SESSION.create",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checksum=on,colgroups=,"
    "collator=,columns=,dictionary=0,encryption=(keyid=,name=),"
    "exclusive=false,extractor=,format=btree,huffman_key=,"
    "huffman_value=,ignore_in_memory_cache_size=false,immutable=false"
    ",import=(compare_timestamp=oldest_timestamp,enabled=false,"
    "file_metadata=,metadata_file=,repair=false),internal_item_max=0,"
    "internal_key_max=0,internal_key_truncate=true,"
    "internal_page_max=4KB,key_format=u,key_gap=10,leaf_item_max=0,"
    "leaf_key_max=0,leaf_page_max=32KB,leaf_value_max=0,"
    "log=(enabled=true),lsm=(auto_throttle=true,bloom=true,"
    "bloom_bit_count=16,bloom_config=,bloom_hash_count=8,"
    "bloom_oldest=false,chunk_count_limit=0,chunk_max=5GB,"
    "chunk_size=10MB,merge_custom=(prefix=,start_generation=0,"
    "suffix=),merge_max=15,merge_min=0),memory_page_image_max=0,"
    "memory_page_max=5MB,os_cache_dirty_max=0,os_cache_max=0,"
    "prefix_compression=false,prefix_compression_min=4,source=,"
    "split_deepen_min_child=0,split_deepen_per_child=0,split_pct=90,"
    "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,local_retention=300,name=,object_target_size=0,"
    "shared=false),type=file,value_format=u,verbose=[],"
    "write_timestamp_usage=none",
    confchk_WT_SESSION_create, 48, 25, true},
  {"WT_SESSION.drop",
    "checkpoint_wait=true,force=false,lock_wait=true,"
    "remove_files=true,remove_shared=false",
    confchk_WT_SESSION_drop, 5, 26, true},
  {"WT_SESSION.flush_tier", "force=false,lock_wait=true,sync=on,timeout=0",
    confchk_WT_SESSION_flush_tier, 4, 27, true},
  {"WT_SESSION.join",
    "bloom_bit_count=16,bloom_false_positives=false,"
    "bloom_hash_count=8,compare=\"eq\",count=0,operation=\"and\","
    "strategy=",
    confchk_WT_SESSION_join, 7, 28, true},
  {"WT_SESSION.log_flush", "sync=on", confchk_WT_SESSION_log_flush, 1, 29, true},
  {"WT_SESSION.log_printf", "", NULL, 0, 30, true},
  {"WT_SESSION.open_cursor",
    "append=false,bulk=false,checkpoint=,checkpoint_use_history=true,"
    "checkpoint_wait=true,debug=(checkpoint_read_timestamp=,"
    "dump_version=false,release_evict=false),dump=,"
    "incremental=(consolidate=false,enabled=false,file=,"
    "force_stop=false,granularity=16MB,src_id=,this_id=),"
    "next_random=false,next_random_sample_size=0,overwrite=true,"
    "prefix_search=false,raw=false,read_once=false,readonly=false,"
    "skip_sort_check=false,statistics=,target=",
    confchk_WT_SESSION_open_cursor, 18, 31, true},
  {"WT_SESSION.prepare_transaction", "prepare_timestamp=", confchk_WT_SESSION_prepare_transaction,
    1, 32, true},
  {"WT_SESSION.query_timestamp", "get=read", confchk_WT_SESSION_query_timestamp, 1, 33, true},
  {"WT_SESSION.reconfigure",
    "cache_cursors=true,cache_max_wait_ms=0,"
    "debug=(release_evict_page=false),ignore_cache_size=false,"
    "isolation=snapshot",
    confchk_WT_SESSION_reconfigure, 5, 34, true},
  {"WT_SESSION.rename", "", NULL, 0, 35, true}, {"WT_SESSION.reset", "", NULL, 0, 36, true},
  {"WT_SESSION.reset_snapshot", "", NULL, 0, 37, true},
  {"WT_SESSION.rollback_transaction", "operation_timeout_ms=0",
    confchk_WT_SESSION_rollback_transaction, 1, 38, true},
  {"WT_SESSION.salvage", "force=false", confchk_WT_SESSION_salvage, 1, 39, true},
  {"WT_SESSION.strerror", "", NULL, 0, 40, true},
  {"WT_SESSION.timestamp_transaction",
    "commit_timestamp=,durable_timestamp=,prepare_timestamp=,"
    "read_timestamp=",
    confchk_WT_SESSION_timestamp_transaction, 4, 41, true},
  {"WT_SESSION.timestamp_transaction_uint", "", NULL, 0, 42, true},
  {"WT_SESSION.truncate", "", NULL, 0, 43, true}, {"WT_SESSION.upgrade", "", NULL, 0, 44, true},
  {"WT_SESSION.verify",
    "do_not_clear_txn_id=false,dump_address=false,dump_app_data=false"
    ",dump_blocks=false,dump_layout=false,dump_offsets=,"
    "dump_pages=false,read_corrupt=false,stable_timestamp=false,"
    "strict=false",
    confchk_WT_SESSION_verify, 10, 45, true},
  {"colgroup.meta",
    "app_metadata=,assert=(commit_timestamp=none,"
    "durable_timestamp=none,read_timestamp=none,write_timestamp=off),"
    "collator=,columns=,source=,type=file,verbose=[],"
    "write_timestamp_usage=none",
    confchk_colgroup_meta, 8, 46, true},
  {"file.config",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checksum=on,collator=,"
    "columns=,dictionary=0,encryption=(keyid=,name=),format=btree,"
    "huffman_key=,huffman_value=,ignore_in_memory_cache_size=false,"
    "internal_item_max=0,internal_key_max=0,"
    "internal_key_truncate=true,internal_page_max=4KB,key_format=u,"
    "key_gap=10,leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,"
    "leaf_value_max=0,log=(enabled=true),memory_page_image_max=0,"
    "memory_page_max=5MB,os_cache_dirty_max=0,os_cache_max=0,"
    "prefix_compression=false,prefix_compression_min=4,"
    "split_deepen_min_child=0,split_deepen_per_child=0,split_pct=90,"
    "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,local_retention=300,name=,object_target_size=0,"
    "shared=false),value_format=u,verbose=[],"
    "write_timestamp_usage=none",
    confchk_file_config, 40, 47, true},
  {"file.meta",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checkpoint=,"
    "checkpoint_backup_info=,checkpoint_lsn=,checksum=on,collator=,"
    "columns=,dictionary=0,encryption=(keyid=,name=),format=btree,"
    "huffman_key=,huffman_value=,id=,"
    "ignore_in_memory_cache_size=false,internal_item_max=0,"
    "internal_key_max=0,internal_key_truncate=true,"
    "internal_page_max=4KB,key_format=u,key_gap=10,leaf_item_max=0,"
    "leaf_key_max=0,leaf_page_max=32KB,leaf_value_max=0,"
    "log=(enabled=true),memory_page_image_max=0,memory_page_max=5MB,"
    "os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,"
    "prefix_compression_min=4,readonly=false,split_deepen_min_child=0"
    ",split_deepen_per_child=0,split_pct=90,tiered_object=false,"
    "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,local_retention=300,name=,object_target_size=0,"
    "shared=false),value_format=u,verbose=[],version=(major=0,"
    "minor=0),write_timestamp_usage=none",
    confchk_file_meta, 47, 48, true},
  {"index.meta",
    "app_metadata=,assert=(commit_timestamp=none,"
    "durable_timestamp=none,read_timestamp=none,write_timestamp=off),"
    "collator=,columns=,extractor=,immutable=false,index_key_columns="
    ",key_format=u,source=,type=file,value_format=u,verbose=[],"
    "write_timestamp_usage=none",
    confchk_index_meta, 13, 49, true},
  {"lsm.meta",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checksum=on,chunks=,"
    "collator=,columns=,dictionary=0,encryption=(keyid=,name=),"
    "format=btree,huffman_key=,huffman_value=,"
    "ignore_in_memory_cache_size=false,internal_item_max=0,"
    "internal_key_max=0,internal_key_truncate=true,"
    "internal_page_max=4KB,key_format=u,key_gap=10,last=0,"
    "leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,"
    "leaf_value_max=0,log=(enabled=true),lsm=(auto_throttle=true,"
    "bloom=true,bloom_bit_count=16,bloom_config=,bloom_hash_count=8,"
    "bloom_oldest=false,chunk_count_limit=0,chunk_max=5GB,"
    "chunk_size=10MB,merge_custom=(prefix=,start_generation=0,"
    "suffix=),merge_max=15,merge_min=0),memory_page_image_max=0,"
    "memory_page_max=5MB,old_chunks=,os_cache_dirty_max=0,"
    "os_cache_max=0,prefix_compression=false,prefix_compression_min=4"
    ",split_deepen_min_child=0,split_deepen_per_child=0,split_pct=90,"
    "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,local_retention=300,name=,object_target_size=0,"
    "shared=false),value_format=u,verbose=[],"
    "write_timestamp_usage=none",
    confchk_lsm_meta, 44, 50, true},
  {"object.meta",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checkpoint=,"
    "checkpoint_backup_info=,checkpoint_lsn=,checksum=on,collator=,"
    "columns=,dictionary=0,encryption=(keyid=,name=),flush_time=0,"
    "flush_timestamp=0,format=btree,huffman_key=,huffman_value=,id=,"
    "ignore_in_memory_cache_size=false,internal_item_max=0,"
    "internal_key_max=0,internal_key_truncate=true,"
    "internal_page_max=4KB,key_format=u,key_gap=10,leaf_item_max=0,"
    "leaf_key_max=0,leaf_page_max=32KB,leaf_value_max=0,"
    "log=(enabled=true),memory_page_image_max=0,memory_page_max=5MB,"
    "os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,"
    "prefix_compression_min=4,readonly=false,split_deepen_min_child=0"
    ",split_deepen_per_child=0,split_pct=90,tiered_object=false,"
    "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,local_retention=300,name=,object_target_size=0,"
    "shared=false),value_format=u,verbose=[],version=(major=0,"
    "minor=0),write_timestamp_usage=none",
    confchk_object_meta, 49, 51, true},
  {"table.meta",
    "app_metadata=,assert=(commit_timestamp=none,"
    "durable_timestamp=none,read_timestamp=none,write_timestamp=off),"
    "colgroups=,collator=,columns=,key_format=u,value_format=u,"
    "verbose=[],write_timestamp_usage=none",
    confchk_table_meta, 9, 52, true},
  {"tier.meta",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,bucket=,bucket_prefix=,cache_directory=,"
    "cache_resident=false,checkpoint=,checkpoint_backup_info=,"
    "checkpoint_lsn=,checksum=on,collator=,columns=,dictionary=0,"
    "encryption=(keyid=,name=),format=btree,huffman_key=,"
    "huffman_value=,id=,ignore_in_memory_cache_size=false,"
    "internal_item_max=0,internal_key_max=0,"
    "internal_key_truncate=true,internal_page_max=4KB,key_format=u,"
    "key_gap=10,leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,"
    "leaf_value_max=0,log=(enabled=true),memory_page_image_max=0,"
    "memory_page_max=5MB,os_cache_dirty_max=0,os_cache_max=0,"
    "prefix_compression=false,prefix_compression_min=4,readonly=false"
    ",split_deepen_min_child=0,split_deepen_per_child=0,split_pct=90,"
    "tiered_object=false,tiered_storage=(auth_token=,bucket=,"
    "bucket_prefix=,cache_directory=,local_retention=300,name=,"
    "object_target_size=0,shared=false),value_format=u,verbose=[],"
    "version=(major=0,minor=0),write_timestamp_usage=none",
    confchk_tier_meta, 50, 53, true},
  {"tiered.meta",
    "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
    "assert=(commit_timestamp=none,durable_timestamp=none,"
    "read_timestamp=none,write_timestamp=off),block_allocation=best,"
    "block_compressor=,cache_resident=false,checkpoint=,"
    "checkpoint_backup_info=,checkpoint_lsn=,checksum=on,collator=,"
    "columns=,dictionary=0,encryption=(keyid=,name=),flush_time=0,"
    "flush_timestamp=0,format=btree,huffman_key=,huffman_value=,id=,"
    "ignore_in_memory_cache_size=false,internal_item_max=0,"
    "internal_key_max=0,internal_key_truncate=true,"
    "internal_page_max=4KB,key_format=u,key_gap=10,last=0,"
    "leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,"
    "leaf_value_max=0,log=(enabled=true),memory_page_image_max=0,"
    "memory_page_max=5MB,oldest=1,os_cache_dirty_max=0,os_cache_max=0"
    ",prefix_compression=false,prefix_compression_min=4,"
    "readonly=false,split_deepen_min_child=0,split_deepen_per_child=0"
    ",split_pct=90,tiered_object=false,tiered_storage=(auth_token=,"
    "bucket=,bucket_prefix=,cache_directory=,local_retention=300,"
    "name=,object_target_size=0,shared=false),tiers=,value_format=u,"
    "verbose=[],version=(major=0,minor=0),write_timestamp_usage=none",
    confchk_tiered_meta, 52, 54, true},
  {"wiredtiger_open",
    "backup_restore_target=,"
    "block_cache=(blkcache_eviction_aggression=1800,"
    "cache_on_checkpoint=true,cache_on_writes=true,enabled=false,"
    "full_target=95,hashsize=32768,max_percent_overhead=10,"
    "nvram_path=,percent_file_in_dram=50,size=0,system_ram=0,type=),"
    "buffer_alignment=-1,builtin_extension_config=,cache_cursors=true"
    ",cache_max_wait_ms=0,cache_overhead=8,cache_size=100MB,"
    "cache_stuck_timeout_ms=300000,checkpoint=(log_size=0,wait=0),"
    "checkpoint_cleanup=none,checkpoint_sync=true,"
    "chunk_cache=(capacity=10GB,chunk_cache_evict_trigger=90,"
    "chunk_size=1MB,device_path=,enabled=false,hashsize=1024,type=),"
    "compatibility=(release=,require_max=,require_min=),"
    "compile_configuration_count=1000,config_base=true,create=false,"
    "debug_mode=(checkpoint_retention=0,corruption_abort=true,"
    "cursor_copy=false,cursor_reposition=false,eviction=false,"
    "log_retention=0,realloc_exact=false,realloc_malloc=false,"
    "rollback_error=0,slow_checkpoint=false,stress_skiplist=false,"
    "table_logging=false,tiered_flush_error_continue=false,"
    "update_restore_evict=false),direct_io=,encryption=(keyid=,name=,"
    "secretkey=),error_prefix=,eviction=(threads_max=8,threads_min=1)"
    ",eviction_checkpoint_target=1,eviction_dirty_target=5,"
    "eviction_dirty_trigger=20,eviction_target=80,eviction_trigger=95"
    ",eviction_updates_target=0,eviction_updates_trigger=0,"
    "exclusive=false,extensions=,extra_diagnostics=[],file_extend=,"
    "file_manager=(close_handle_minimum=250,close_idle_time=30,"
    "close_scan_interval=10),generation_drain_timeout_ms=240000,"
    "hash=(buckets=512,dhandle_buckets=512),hazard_max=1000,"
    "history_store=(file_max=0),in_memory=false,io_capacity=(total=0)"
    ",json_output=[],log=(archive=true,compressor=,enabled=false,"
    "file_max=100MB,force_write_wait=0,os_cache_dirty_pct=0,"
    "path=\".\",prealloc=true,recover=on,remove=true,zero_fill=false)"
    ",lsm_manager=(merge=true,worker_thread_max=4),mmap=true,"
    "mmap_all=false,multiprocess=false,operation_timeout_ms=0,"
    "operation_tracking=(enabled=false,path=\".\"),readonly=false,"
    "salvage=false,session_max=100,session_scratch_max=2MB,"
    "session_table_cache=true,shared_cache=(chunk=10MB,name=,quota=0,"
    "reserve=0,size=500MB),statistics=none,statistics_log=(json=false"
    ",on_close=false,path=\".\",sources=,timestamp=\"%b %d %H:%M:%S\""
    ",wait=0),tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,interval=60,local_retention=300,name=,"
    "shared=false),timing_stress_for_test=,"
    "transaction_sync=(enabled=false,method=fsync),"
    "use_environment=true,use_environment_priv=false,verbose=[],"
    "verify_metadata=false,write_through=",
    confchk_wiredtiger_open, 64, 55, true},
  {"wiredtiger_open_all",
    "backup_restore_target=,"
    "block_cache=(blkcache_eviction_aggression=1800,"
    "cache_on_checkpoint=true,cache_on_writes=true,enabled=false,"
    "full_target=95,hashsize=32768,max_percent_overhead=10,"
    "nvram_path=,percent_file_in_dram=50,size=0,system_ram=0,type=),"
    "buffer_alignment=-1,builtin_extension_config=,cache_cursors=true"
    ",cache_max_wait_ms=0,cache_overhead=8,cache_size=100MB,"
    "cache_stuck_timeout_ms=300000,checkpoint=(log_size=0,wait=0),"
    "checkpoint_cleanup=none,checkpoint_sync=true,"
    "chunk_cache=(capacity=10GB,chunk_cache_evict_trigger=90,"
    "chunk_size=1MB,device_path=,enabled=false,hashsize=1024,type=),"
    "compatibility=(release=,require_max=,require_min=),"
    "compile_configuration_count=1000,config_base=true,create=false,"
    "debug_mode=(checkpoint_retention=0,corruption_abort=true,"
    "cursor_copy=false,cursor_reposition=false,eviction=false,"
    "log_retention=0,realloc_exact=false,realloc_malloc=false,"
    "rollback_error=0,slow_checkpoint=false,stress_skiplist=false,"
    "table_logging=false,tiered_flush_error_continue=false,"
    "update_restore_evict=false),direct_io=,encryption=(keyid=,name=,"
    "secretkey=),error_prefix=,eviction=(threads_max=8,threads_min=1)"
    ",eviction_checkpoint_target=1,eviction_dirty_target=5,"
    "eviction_dirty_trigger=20,eviction_target=80,eviction_trigger=95"
    ",eviction_updates_target=0,eviction_updates_trigger=0,"
    "exclusive=false,extensions=,extra_diagnostics=[],file_extend=,"
    "file_manager=(close_handle_minimum=250,close_idle_time=30,"
    "close_scan_interval=10),generation_drain_timeout_ms=240000,"
    "hash=(buckets=512,dhandle_buckets=512),hazard_max=1000,"
    "history_store=(file_max=0),in_memory=false,io_capacity=(total=0)"
    ",json_output=[],log=(archive=true,compressor=,enabled=false,"
    "file_max=100MB,force_write_wait=0,os_cache_dirty_pct=0,"
    "path=\".\",prealloc=true,recover=on,remove=true,zero_fill=false)"
    ",lsm_manager=(merge=true,worker_thread_max=4),mmap=true,"
    "mmap_all=false,multiprocess=false,operation_timeout_ms=0,"
    "operation_tracking=(enabled=false,path=\".\"),readonly=false,"
    "salvage=false,session_max=100,session_scratch_max=2MB,"
    "session_table_cache=true,shared_cache=(chunk=10MB,name=,quota=0,"
    "reserve=0,size=500MB),statistics=none,statistics_log=(json=false"
    ",on_close=false,path=\".\",sources=,timestamp=\"%b %d %H:%M:%S\""
    ",wait=0),tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,interval=60,local_retention=300,name=,"
    "shared=false),timing_stress_for_test=,"
    "transaction_sync=(enabled=false,method=fsync),"
    "use_environment=true,use_environment_priv=false,verbose=[],"
    "verify_metadata=false,version=(major=0,minor=0),write_through=",
    confchk_wiredtiger_open_all, 65, 56, true},
  {"wiredtiger_open_basecfg",
    "backup_restore_target=,"
    "block_cache=(blkcache_eviction_aggression=1800,"
    "cache_on_checkpoint=true,cache_on_writes=true,enabled=false,"
    "full_target=95,hashsize=32768,max_percent_overhead=10,"
    "nvram_path=,percent_file_in_dram=50,size=0,system_ram=0,type=),"
    "buffer_alignment=-1,builtin_extension_config=,cache_cursors=true"
    ",cache_max_wait_ms=0,cache_overhead=8,cache_size=100MB,"
    "cache_stuck_timeout_ms=300000,checkpoint=(log_size=0,wait=0),"
    "checkpoint_cleanup=none,checkpoint_sync=true,"
    "chunk_cache=(capacity=10GB,chunk_cache_evict_trigger=90,"
    "chunk_size=1MB,device_path=,enabled=false,hashsize=1024,type=),"
    "compatibility=(release=,require_max=,require_min=),"
    "compile_configuration_count=1000,"
    "debug_mode=(checkpoint_retention=0,corruption_abort=true,"
    "cursor_copy=false,cursor_reposition=false,eviction=false,"
    "log_retention=0,realloc_exact=false,realloc_malloc=false,"
    "rollback_error=0,slow_checkpoint=false,stress_skiplist=false,"
    "table_logging=false,tiered_flush_error_continue=false,"
    "update_restore_evict=false),direct_io=,encryption=(keyid=,name=,"
    "secretkey=),error_prefix=,eviction=(threads_max=8,threads_min=1)"
    ",eviction_checkpoint_target=1,eviction_dirty_target=5,"
    "eviction_dirty_trigger=20,eviction_target=80,eviction_trigger=95"
    ",eviction_updates_target=0,eviction_updates_trigger=0,"
    "extensions=,extra_diagnostics=[],file_extend=,"
    "file_manager=(close_handle_minimum=250,close_idle_time=30,"
    "close_scan_interval=10),generation_drain_timeout_ms=240000,"
    "hash=(buckets=512,dhandle_buckets=512),hazard_max=1000,"
    "history_store=(file_max=0),io_capacity=(total=0),json_output=[],"
    "log=(archive=true,compressor=,enabled=false,file_max=100MB,"
    "force_write_wait=0,os_cache_dirty_pct=0,path=\".\",prealloc=true"
    ",recover=on,remove=true,zero_fill=false),lsm_manager=(merge=true"
    ",worker_thread_max=4),mmap=true,mmap_all=false,"
    "multiprocess=false,operation_timeout_ms=0,"
    "operation_tracking=(enabled=false,path=\".\"),readonly=false,"
    "salvage=false,session_max=100,session_scratch_max=2MB,"
    "session_table_cache=true,shared_cache=(chunk=10MB,name=,quota=0,"
    "reserve=0,size=500MB),statistics=none,statistics_log=(json=false"
    ",on_close=false,path=\".\",sources=,timestamp=\"%b %d %H:%M:%S\""
    ",wait=0),tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,interval=60,local_retention=300,name=,"
    "shared=false),timing_stress_for_test=,"
    "transaction_sync=(enabled=false,method=fsync),verbose=[],"
    "verify_metadata=false,version=(major=0,minor=0),write_through=",
    confchk_wiredtiger_open_basecfg, 59, 57, true},
  {"wiredtiger_open_usercfg",
    "backup_restore_target=,"
    "block_cache=(blkcache_eviction_aggression=1800,"
    "cache_on_checkpoint=true,cache_on_writes=true,enabled=false,"
    "full_target=95,hashsize=32768,max_percent_overhead=10,"
    "nvram_path=,percent_file_in_dram=50,size=0,system_ram=0,type=),"
    "buffer_alignment=-1,builtin_extension_config=,cache_cursors=true"
    ",cache_max_wait_ms=0,cache_overhead=8,cache_size=100MB,"
    "cache_stuck_timeout_ms=300000,checkpoint=(log_size=0,wait=0),"
    "checkpoint_cleanup=none,checkpoint_sync=true,"
    "chunk_cache=(capacity=10GB,chunk_cache_evict_trigger=90,"
    "chunk_size=1MB,device_path=,enabled=false,hashsize=1024,type=),"
    "compatibility=(release=,require_max=,require_min=),"
    "compile_configuration_count=1000,"
    "debug_mode=(checkpoint_retention=0,corruption_abort=true,"
    "cursor_copy=false,cursor_reposition=false,eviction=false,"
    "log_retention=0,realloc_exact=false,realloc_malloc=false,"
    "rollback_error=0,slow_checkpoint=false,stress_skiplist=false,"
    "table_logging=false,tiered_flush_error_continue=false,"
    "update_restore_evict=false),direct_io=,encryption=(keyid=,name=,"
    "secretkey=),error_prefix=,eviction=(threads_max=8,threads_min=1)"
    ",eviction_checkpoint_target=1,eviction_dirty_target=5,"
    "eviction_dirty_trigger=20,eviction_target=80,eviction_trigger=95"
    ",eviction_updates_target=0,eviction_updates_trigger=0,"
    "extensions=,extra_diagnostics=[],file_extend=,"
    "file_manager=(close_handle_minimum=250,close_idle_time=30,"
    "close_scan_interval=10),generation_drain_timeout_ms=240000,"
    "hash=(buckets=512,dhandle_buckets=512),hazard_max=1000,"
    "history_store=(file_max=0),io_capacity=(total=0),json_output=[],"
    "log=(archive=true,compressor=,enabled=false,file_max=100MB,"
    "force_write_wait=0,os_cache_dirty_pct=0,path=\".\",prealloc=true"
    ",recover=on,remove=true,zero_fill=false),lsm_manager=(merge=true"
    ",worker_thread_max=4),mmap=true,mmap_all=false,"
    "multiprocess=false,operation_timeout_ms=0,"
    "operation_tracking=(enabled=false,path=\".\"),readonly=false,"
    "salvage=false,session_max=100,session_scratch_max=2MB,"
    "session_table_cache=true,shared_cache=(chunk=10MB,name=,quota=0,"
    "reserve=0,size=500MB),statistics=none,statistics_log=(json=false"
    ",on_close=false,path=\".\",sources=,timestamp=\"%b %d %H:%M:%S\""
    ",wait=0),tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
    "cache_directory=,interval=60,local_retention=300,name=,"
    "shared=false),timing_stress_for_test=,"
    "transaction_sync=(enabled=false,method=fsync),verbose=[],"
    "verify_metadata=false,write_through=",
    confchk_wiredtiger_open_usercfg, 58, 58, true},
  {NULL, NULL, NULL, 0, 0, false}};

int
__wt_conn_config_init(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    const WT_CONFIG_ENTRY *ep, **epp;

    conn = S2C(session);

    /* Build a list of pointers to the configuration information. */
    WT_RET(__wt_calloc_def(session, WT_ELEMENTS(config_entries), &epp));
    conn->config_entries = epp;

    /* Fill in the list to reference the default information. */
    for (ep = config_entries;;) {
        *epp++ = ep++;
        if (ep->method == NULL)
            break;
    }
    return (0);
}

void
__wt_conn_config_discard(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    __wt_free(session, conn->config_entries);
}

/*
 * __wt_conn_config_match --
 *     Return the static configuration entry for a method.
 */
const WT_CONFIG_ENTRY *
__wt_conn_config_match(const char *method)
{
    const WT_CONFIG_ENTRY *ep;

    for (ep = config_entries; ep->method != NULL; ++ep)
        if (strcmp(method, ep->method) == 0)
            return (ep);
    return (NULL);
}
