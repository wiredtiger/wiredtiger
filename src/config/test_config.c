/* DO NOT EDIT: automatically built by dist/api_config.py. */

#include "wt_internal.h"

static const WT_CONFIG_CHECK confchk_cache_hs_insert_subconfigs[] = {
  {"max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 10, 0, 0, NULL, 0},
  {"min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 9, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"postrun", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 11, 0, 0, NULL, 0},
  {"runtime", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 12, 0, 0, NULL, 0},
  {"save", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 13, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_cc_pages_removed_subconfigs[] = {
  {"max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 10, 0, 0, NULL, 0},
  {"min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 9, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"postrun", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 11, 0, 0, NULL, 0},
  {"runtime", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 12, 0, 0, NULL, 0},
  {"save", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 13, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_stat_cache_size_subconfigs[] = {
  {"max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 10, 0, 0, NULL, 0},
  {"min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 9, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"postrun", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 11, 0, 0, NULL, 0},
  {"runtime", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 12, 0, 0, NULL, 0},
  {"save", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 13, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_stat_db_size_subconfigs[] = {
  {"max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 10, 0, 0, NULL, 0},
  {"min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 9, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"postrun", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 11, 0, 0, NULL, 0},
  {"runtime", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 12, 0, 0, NULL, 0},
  {"save", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 13, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_metrics_monitor_subconfigs[] = {
  {"cache_hs_insert", "category", NULL, NULL, confchk_cache_hs_insert_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 8, 0, 0, NULL, 0},
  {"cc_pages_removed", "category", NULL, NULL, confchk_cc_pages_removed_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 14, 0, 0, NULL, 0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 6, 0, 0, NULL, 0},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"stat_cache_size", "category", NULL, NULL, confchk_stat_cache_size_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 15, 0, 0, NULL, 0},
  {"stat_db_size", "category", NULL, NULL, confchk_stat_db_size_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 16, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_operation_tracker_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 6, 0, 0, NULL, 0},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"tracking_key_format", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 18, 0, 0,
    NULL, 0},
  {"tracking_value_format", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 19, 0, 0,
    NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_statistics_config_subconfigs[] = {
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"type", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 22, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_timestamp_manager_subconfigs[] = {
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 6, 0, 0, NULL, 0},
  {"oldest_lag", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 24, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"stable_lag", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 25, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_checkpoint_config_subconfigs[] = {
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0,max=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 1, NULL,
    WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_ops_per_transaction_subconfigs[] = {
  {"max", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 10, 0, 0, NULL, 0},
  {"min", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 9, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_custom_config_subconfigs[] = {
  {"key_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"ops_per_transaction", "category", NULL, NULL, confchk_ops_per_transaction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 30, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"value_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_insert_config_subconfigs[] = {
  {"key_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"ops_per_transaction", "category", NULL, NULL, confchk_ops_per_transaction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 30, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"value_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_populate_config_subconfigs[] = {
  {"collection_count", "int", NULL, "min=0,max=200000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 35, 0,
    200000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_count_per_collection", "int", NULL, "min=0,max=1000000", NULL, 0,
    WT_CONFIG_COMPILED_TYPE_INT, 36, 0, 1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"key_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"thread_count", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 28, 0, 0, NULL,
    0},
  {"value_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_read_config_subconfigs[] = {
  {"key_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"ops_per_transaction", "category", NULL, NULL, confchk_ops_per_transaction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 30, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"value_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_remove_config_subconfigs[] = {
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"ops_per_transaction", "category", NULL, NULL, confchk_ops_per_transaction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 30, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_update_config_subconfigs[] = {
  {"key_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 31, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"ops_per_transaction", "category", NULL, NULL, confchk_ops_per_transaction_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 30, 0, 0, NULL, 0},
  {"thread_count", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 28, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"value_size", "int", NULL, "min=1", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 32, 1, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_workload_manager_subconfigs[] = {
  {"checkpoint_config", "category", NULL, NULL, confchk_checkpoint_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 27, 0, 0, NULL, 0},
  {"custom_config", "category", NULL, NULL, confchk_custom_config_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 29, 0, 0, NULL, 0},
  {"enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 6, 0, 0, NULL, 0},
  {"insert_config", "category", NULL, NULL, confchk_insert_config_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 33, 0, 0, NULL, 0},
  {"op_rate", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 7, 0, 0, NULL, 0},
  {"populate_config", "category", NULL, NULL, confchk_populate_config_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 34, 0, 0, NULL, 0},
  {"read_config", "category", NULL, NULL, confchk_read_config_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 38, 0, 0, NULL, 0},
  {"remove_config", "category", NULL, NULL, confchk_remove_config_subconfigs, 3,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 37, 0, 0, NULL, 0},
  {"update_config", "category", NULL, NULL, confchk_update_config_subconfigs, 5,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 39, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_bounded_cursor_perf[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_bounded_cursor_prefix_indices[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_bounded_cursor_prefix_search_near[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_bounded_cursor_prefix_stat[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"search_near_threads", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 40, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_bounded_cursor_stress[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_burst_inserts[] = {
  {"burst_duration", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 41, 0, 0, NULL,
    0},
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_cache_resize[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_hs_cleanup[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_operations_test[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_reverse_split[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_search_near_01[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"search_near_threads", "string", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_STRING, 40, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_search_near_02[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_search_near_03[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_CHECK confchk_test_template[] = {
  {"cache_max_wait_ms", "int", NULL, "min=0", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 0, 0, 0, NULL,
    WT_CONFIG_MIN_LIMIT},
  {"cache_size_mb", "int", NULL, "min=0,max=100000000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 1,
    0, 100000000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"compression_enabled", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 2, 0, 0,
    NULL, 0},
  {"duration_seconds", "int", NULL, "min=0,max=1000000", NULL, 0, WT_CONFIG_COMPILED_TYPE_INT, 3, 0,
    1000000, NULL, WT_CONFIG_MIN_LIMIT | WT_CONFIG_MAX_LIMIT},
  {"enable_logging", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 4, 0, 0, NULL,
    0},
  {"metrics_monitor", "category", NULL, NULL, confchk_metrics_monitor_subconfigs, 6,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 5, 0, 0, NULL, 0},
  {"operation_tracker", "category", NULL, NULL, confchk_operation_tracker_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 17, 0, 0, NULL, 0},
  {"reverse_collator", "boolean", NULL, NULL, NULL, 0, WT_CONFIG_COMPILED_TYPE_BOOLEAN, 20, 0, 0,
    NULL, 0},
  {"statistics_config", "category", NULL, NULL, confchk_statistics_config_subconfigs, 2,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 21, 0, 0, NULL, 0},
  {"timestamp_manager", "category", NULL, NULL, confchk_timestamp_manager_subconfigs, 4,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 23, 0, 0, NULL, 0},
  {"workload_manager", "category", NULL, NULL, confchk_workload_manager_subconfigs, 9,
    WT_CONFIG_COMPILED_TYPE_CATEGORY, 26, 0, 0, NULL, 0},
  {NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0}};

static const WT_CONFIG_ENTRY config_entries[] = {
  {"bounded_cursor_perf",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_bounded_cursor_perf, 11, 1, true},
  {"bounded_cursor_prefix_indices",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_bounded_cursor_prefix_indices, 11, 2, true},
  {"bounded_cursor_prefix_search_near",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_bounded_cursor_prefix_search_near, 11, 3, true},
  {"bounded_cursor_prefix_stat",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,search_near_threads=10,"
    "statistics_config=(enable_logging=true,type=all),"
    "timestamp_manager=(enabled=true,oldest_lag=1,op_rate=1s,"
    "stable_lag=1),workload_manager=(checkpoint_config=(op_rate=60s,"
    "thread_count=1),custom_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "enabled=true,insert_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "op_rate=1s,populate_config=(collection_count=1,"
    "key_count_per_collection=0,key_size=5,thread_count=1,"
    "value_size=5),read_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "remove_config=(op_rate=1s,ops_per_transaction=(max=1,min=0),"
    "thread_count=0),update_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5))",
    confchk_bounded_cursor_prefix_stat, 12, 4, true},
  {"bounded_cursor_stress",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_bounded_cursor_stress, 11, 5, true},
  {"burst_inserts",
    "burst_duration=90,cache_max_wait_ms=0,cache_size_mb=0,"
    "compression_enabled=false,duration_seconds=0,"
    "enable_logging=false,metrics_monitor=(cache_hs_insert=(max=1,"
    "min=0,postrun=false,runtime=false,save=false),"
    "cc_pages_removed=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),enabled=true,op_rate=1s,stat_cache_size=(max=1,min=0"
    ",postrun=false,runtime=false,save=false),stat_db_size=(max=1,"
    "min=0,postrun=false,runtime=false,save=false)),"
    "operation_tracker=(enabled=true,op_rate=1s,"
    "tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_burst_inserts, 12, 6, true},
  {"cache_resize",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_cache_resize, 11, 7, true},
  {"hs_cleanup",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_hs_cleanup, 11, 8, true},
  {"operations_test",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_operations_test, 11, 9, true},
  {"reverse_split",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_reverse_split, 11, 10, true},
  {"search_near_01",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,search_near_threads=10,"
    "statistics_config=(enable_logging=true,type=all),"
    "timestamp_manager=(enabled=true,oldest_lag=1,op_rate=1s,"
    "stable_lag=1),workload_manager=(checkpoint_config=(op_rate=60s,"
    "thread_count=1),custom_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "enabled=true,insert_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "op_rate=1s,populate_config=(collection_count=1,"
    "key_count_per_collection=0,key_size=5,thread_count=1,"
    "value_size=5),read_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5),"
    "remove_config=(op_rate=1s,ops_per_transaction=(max=1,min=0),"
    "thread_count=0),update_config=(key_size=5,op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0,value_size=5))",
    confchk_search_near_01, 12, 11, true},
  {"search_near_02",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_search_near_02, 11, 12, true},
  {"search_near_03",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_search_near_03, 11, 13, true},
  {"test_template",
    "cache_max_wait_ms=0,cache_size_mb=0,compression_enabled=false,"
    "duration_seconds=0,enable_logging=false,"
    "metrics_monitor=(cache_hs_insert=(max=1,min=0,postrun=false,"
    "runtime=false,save=false),cc_pages_removed=(max=1,min=0,"
    "postrun=false,runtime=false,save=false),enabled=true,op_rate=1s,"
    "stat_cache_size=(max=1,min=0,postrun=false,runtime=false,"
    "save=false),stat_db_size=(max=1,min=0,postrun=false,"
    "runtime=false,save=false)),operation_tracker=(enabled=true,"
    "op_rate=1s,tracking_key_format=QSQ,tracking_value_format=iS),"
    "reverse_collator=false,statistics_config=(enable_logging=true,"
    "type=all),timestamp_manager=(enabled=true,oldest_lag=1,"
    "op_rate=1s,stable_lag=1),"
    "workload_manager=(checkpoint_config=(op_rate=60s,thread_count=1)"
    ",custom_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1"
    ",min=0),thread_count=0,value_size=5),enabled=true,"
    "insert_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5),op_rate=1s,"
    "populate_config=(collection_count=1,key_count_per_collection=0,"
    "key_size=5,thread_count=1,value_size=5),read_config=(key_size=5,"
    "op_rate=1s,ops_per_transaction=(max=1,min=0),thread_count=0,"
    "value_size=5),remove_config=(op_rate=1s,"
    "ops_per_transaction=(max=1,min=0),thread_count=0),"
    "update_config=(key_size=5,op_rate=1s,ops_per_transaction=(max=1,"
    "min=0),thread_count=0,value_size=5))",
    confchk_test_template, 11, 14, true},
  {NULL, NULL, NULL, 0, 0, false}};

/*
 * __wt_test_config_match --
 *     Return the static configuration entry for a test.
 */
const WT_CONFIG_ENTRY *
__wt_test_config_match(const char *test_name)
{
    const WT_CONFIG_ENTRY *ep;

    for (ep = config_entries; ep->method != NULL; ++ep)
        if (strcmp(test_name, ep->method) == 0)
            return (ep);
    return (NULL);
}
