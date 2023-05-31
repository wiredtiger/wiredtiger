/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Sample usage:
 *
 * __wt_conf_gets(session, cfg, statistics, &cval);
 * __wt_conf_gets(session, cfg, operation_tracking.enabled, &cval);
 */
#define __wt_conf_gets(s, cfg, key, cval) \
    __wt_conf_gets_fcn(s, cfg, WT_CONF_KEY_STRUCTURE.key, 0, false, cval)

#define __wt_conf_gets_def(s, cfg, key, def, cval) \
    __wt_conf_gets_fcn(s, cfg, WT_CONF_KEY_STRUCTURE.key, def, true, cval)

/*******************************************
 * API configuration keys.
 *******************************************/
/*
 * DO NOT EDIT: automatically built by dist/api_config.py.
 * API configuration keys: BEGIN
 */
#define WT_CONF_KEY_access_pattern_hint 12ULL
#define WT_CONF_KEY_action 93ULL
#define WT_CONF_KEY_allocation_size 13ULL
#define WT_CONF_KEY_app_metadata 0ULL
#define WT_CONF_KEY_append 90ULL
#define WT_CONF_KEY_archive 226ULL
#define WT_CONF_KEY_assert 1ULL
#define WT_CONF_KEY_auth_token 48ULL
#define WT_CONF_KEY_auto_throttle 69ULL
#define WT_CONF_KEY_backup_restore_target 255ULL
#define WT_CONF_KEY_blkcache_eviction_aggression 169ULL
#define WT_CONF_KEY_block_allocation 14ULL
#define WT_CONF_KEY_block_cache 166ULL
#define WT_CONF_KEY_block_compressor 15ULL
#define WT_CONF_KEY_bloom 70ULL
#define WT_CONF_KEY_bloom_bit_count 71ULL
#define WT_CONF_KEY_bloom_config 72ULL
#define WT_CONF_KEY_bloom_false_positives 109ULL
#define WT_CONF_KEY_bloom_hash_count 73ULL
#define WT_CONF_KEY_bloom_oldest 74ULL
#define WT_CONF_KEY_bound 94ULL
#define WT_CONF_KEY_bucket 49ULL
#define WT_CONF_KEY_bucket_prefix 50ULL
#define WT_CONF_KEY_buckets 269ULL
#define WT_CONF_KEY_buffer_alignment 256ULL
#define WT_CONF_KEY_builtin_extension_config 257ULL
#define WT_CONF_KEY_bulk 115ULL
#define WT_CONF_KEY_cache 161ULL
#define WT_CONF_KEY_cache_cursors 250ULL
#define WT_CONF_KEY_cache_directory 51ULL
#define WT_CONF_KEY_cache_max_wait_ms 177ULL
#define WT_CONF_KEY_cache_on_checkpoint 167ULL
#define WT_CONF_KEY_cache_on_writes 168ULL
#define WT_CONF_KEY_cache_overhead 178ULL
#define WT_CONF_KEY_cache_resident 16ULL
#define WT_CONF_KEY_cache_size 179ULL
#define WT_CONF_KEY_cache_stuck_timeout_ms 180ULL
#define WT_CONF_KEY_capacity 185ULL
#define WT_CONF_KEY_checkpoint 56ULL
#define WT_CONF_KEY_checkpoint_backup_info 57ULL
#define WT_CONF_KEY_checkpoint_cleanup 183ULL
#define WT_CONF_KEY_checkpoint_lsn 58ULL
#define WT_CONF_KEY_checkpoint_read_timestamp 118ULL
#define WT_CONF_KEY_checkpoint_retention 192ULL
#define WT_CONF_KEY_checkpoint_sync 258ULL
#define WT_CONF_KEY_checkpoint_use_history 116ULL
#define WT_CONF_KEY_checkpoint_wait 104ULL
#define WT_CONF_KEY_checksum 17ULL
#define WT_CONF_KEY_chunk 237ULL
#define WT_CONF_KEY_chunk_cache 184ULL
#define WT_CONF_KEY_chunk_cache_evict_trigger 186ULL
#define WT_CONF_KEY_chunk_count_limit 75ULL
#define WT_CONF_KEY_chunk_max 76ULL
#define WT_CONF_KEY_chunk_size 77ULL
#define WT_CONF_KEY_chunks 66ULL
#define WT_CONF_KEY_close_handle_minimum 217ULL
#define WT_CONF_KEY_close_idle_time 218ULL
#define WT_CONF_KEY_close_scan_interval 219ULL
#define WT_CONF_KEY_colgroups 87ULL
#define WT_CONF_KEY_collator 6ULL
#define WT_CONF_KEY_columns 7ULL
#define WT_CONF_KEY_commit_timestamp 2ULL
#define WT_CONF_KEY_compare 110ULL
#define WT_CONF_KEY_compare_timestamp 100ULL
#define WT_CONF_KEY_compatibility 188ULL
#define WT_CONF_KEY_compile_configuration_count 261ULL
#define WT_CONF_KEY_compressor 273ULL
#define WT_CONF_KEY_config 246ULL
#define WT_CONF_KEY_config_base 262ULL
#define WT_CONF_KEY_consolidate 123ULL
#define WT_CONF_KEY_corruption_abort 191ULL
#define WT_CONF_KEY_count 111ULL
#define WT_CONF_KEY_create 263ULL
#define WT_CONF_KEY_cursor_copy 193ULL
#define WT_CONF_KEY_cursor_reposition 194ULL
#define WT_CONF_KEY_cursors 162ULL
#define WT_CONF_KEY_debug 117ULL
#define WT_CONF_KEY_debug_mode 190ULL
#define WT_CONF_KEY_device_path 187ULL
#define WT_CONF_KEY_dhandle_buckets 270ULL
#define WT_CONF_KEY_dictionary 18ULL
#define WT_CONF_KEY_direct_io 264ULL
#define WT_CONF_KEY_do_not_clear_txn_id 137ULL
#define WT_CONF_KEY_drop 156ULL
#define WT_CONF_KEY_dryrun 254ULL
#define WT_CONF_KEY_dump 121ULL
#define WT_CONF_KEY_dump_address 138ULL
#define WT_CONF_KEY_dump_app_data 139ULL
#define WT_CONF_KEY_dump_blocks 140ULL
#define WT_CONF_KEY_dump_layout 141ULL
#define WT_CONF_KEY_dump_offsets 142ULL
#define WT_CONF_KEY_dump_pages 143ULL
#define WT_CONF_KEY_dump_version 119ULL
#define WT_CONF_KEY_durable_timestamp 3ULL
#define WT_CONF_KEY_early_load 247ULL
#define WT_CONF_KEY_enabled 37ULL
#define WT_CONF_KEY_encryption 19ULL
#define WT_CONF_KEY_entry 248ULL
#define WT_CONF_KEY_error_prefix 205ULL
#define WT_CONF_KEY_eviction 195ULL
#define WT_CONF_KEY_eviction_checkpoint_target 208ULL
#define WT_CONF_KEY_eviction_dirty_target 209ULL
#define WT_CONF_KEY_eviction_dirty_trigger 210ULL
#define WT_CONF_KEY_eviction_target 211ULL
#define WT_CONF_KEY_eviction_trigger 212ULL
#define WT_CONF_KEY_eviction_updates_target 213ULL
#define WT_CONF_KEY_eviction_updates_trigger 214ULL
#define WT_CONF_KEY_exclusive 98ULL
#define WT_CONF_KEY_exclusive_refreshed 96ULL
#define WT_CONF_KEY_extensions 266ULL
#define WT_CONF_KEY_extra_diagnostics 215ULL
#define WT_CONF_KEY_extractor 63ULL
#define WT_CONF_KEY_file 124ULL
#define WT_CONF_KEY_file_extend 267ULL
#define WT_CONF_KEY_file_manager 216ULL
#define WT_CONF_KEY_file_max 222ULL
#define WT_CONF_KEY_file_metadata 102ULL
#define WT_CONF_KEY_final_flush 159ULL
#define WT_CONF_KEY_flush_tier 157ULL
#define WT_CONF_KEY_flush_time 85ULL
#define WT_CONF_KEY_flush_timestamp 86ULL
#define WT_CONF_KEY_force 105ULL
#define WT_CONF_KEY_force_stop 125ULL
#define WT_CONF_KEY_force_write_wait 274ULL
#define WT_CONF_KEY_format 22ULL
#define WT_CONF_KEY_full_target 170ULL
#define WT_CONF_KEY_generation_drain_timeout_ms 220ULL
#define WT_CONF_KEY_get 136ULL
#define WT_CONF_KEY_granularity 126ULL
#define WT_CONF_KEY_handles 163ULL
#define WT_CONF_KEY_hash 268ULL
#define WT_CONF_KEY_hashsize 172ULL
#define WT_CONF_KEY_hazard_max 271ULL
#define WT_CONF_KEY_history_store 221ULL
#define WT_CONF_KEY_huffman_key 23ULL
#define WT_CONF_KEY_huffman_value 24ULL
#define WT_CONF_KEY_id 59ULL
#define WT_CONF_KEY_ignore_cache_size 252ULL
#define WT_CONF_KEY_ignore_in_memory_cache_size 25ULL
#define WT_CONF_KEY_ignore_prepare 147ULL
#define WT_CONF_KEY_immutable 64ULL
#define WT_CONF_KEY_import 99ULL
#define WT_CONF_KEY_in_memory 272ULL
#define WT_CONF_KEY_inclusive 95ULL
#define WT_CONF_KEY_incremental 122ULL
#define WT_CONF_KEY_index_key_columns 65ULL
#define WT_CONF_KEY_internal_item_max 26ULL
#define WT_CONF_KEY_internal_key_max 27ULL
#define WT_CONF_KEY_internal_key_truncate 28ULL
#define WT_CONF_KEY_internal_page_max 29ULL
#define WT_CONF_KEY_interval 283ULL
#define WT_CONF_KEY_io_capacity 223ULL
#define WT_CONF_KEY_isolation 148ULL
#define WT_CONF_KEY_json 241ULL
#define WT_CONF_KEY_json_output 225ULL
#define WT_CONF_KEY_key_format 30ULL
#define WT_CONF_KEY_key_gap 31ULL
#define WT_CONF_KEY_keyid 21ULL
#define WT_CONF_KEY_last 67ULL
#define WT_CONF_KEY_leaf_item_max 32ULL
#define WT_CONF_KEY_leaf_key_max 33ULL
#define WT_CONF_KEY_leaf_page_max 34ULL
#define WT_CONF_KEY_leaf_value_max 35ULL
#define WT_CONF_KEY_leak_memory 160ULL
#define WT_CONF_KEY_local_retention 52ULL
#define WT_CONF_KEY_lock_wait 106ULL
#define WT_CONF_KEY_log 36ULL
#define WT_CONF_KEY_log_retention 196ULL
#define WT_CONF_KEY_log_size 181ULL
#define WT_CONF_KEY_lsm 68ULL
#define WT_CONF_KEY_lsm_manager 231ULL
#define WT_CONF_KEY_max_percent_overhead 173ULL
#define WT_CONF_KEY_memory_page_image_max 38ULL
#define WT_CONF_KEY_memory_page_max 39ULL
#define WT_CONF_KEY_merge 233ULL
#define WT_CONF_KEY_merge_custom 78ULL
#define WT_CONF_KEY_merge_max 82ULL
#define WT_CONF_KEY_merge_min 83ULL
#define WT_CONF_KEY_metadata_file 103ULL
#define WT_CONF_KEY_method 285ULL
#define WT_CONF_KEY_mmap 276ULL
#define WT_CONF_KEY_mmap_all 277ULL
#define WT_CONF_KEY_multiprocess 278ULL
#define WT_CONF_KEY_name 20ULL
#define WT_CONF_KEY_next_random 129ULL
#define WT_CONF_KEY_next_random_sample_size 130ULL
#define WT_CONF_KEY_no_timestamp 149ULL
#define WT_CONF_KEY_nvram_path 174ULL
#define WT_CONF_KEY_object_target_size 53ULL
#define WT_CONF_KEY_old_chunks 84ULL
#define WT_CONF_KEY_oldest 88ULL
#define WT_CONF_KEY_oldest_timestamp 253ULL
#define WT_CONF_KEY_on_close 242ULL
#define WT_CONF_KEY_operation 112ULL
#define WT_CONF_KEY_operation_timeout_ms 150ULL
#define WT_CONF_KEY_operation_tracking 234ULL
#define WT_CONF_KEY_os_cache_dirty_max 40ULL
#define WT_CONF_KEY_os_cache_dirty_pct 227ULL
#define WT_CONF_KEY_os_cache_max 41ULL
#define WT_CONF_KEY_overwrite 91ULL
#define WT_CONF_KEY_path 235ULL
#define WT_CONF_KEY_percent_file_in_dram 175ULL
#define WT_CONF_KEY_prealloc 228ULL
#define WT_CONF_KEY_prefix 79ULL
#define WT_CONF_KEY_prefix_compression 42ULL
#define WT_CONF_KEY_prefix_compression_min 43ULL
#define WT_CONF_KEY_prefix_search 92ULL
#define WT_CONF_KEY_prepare_timestamp 155ULL
#define WT_CONF_KEY_prepared 153ULL
#define WT_CONF_KEY_priority 151ULL
#define WT_CONF_KEY_quota 238ULL
#define WT_CONF_KEY_raw 131ULL
#define WT_CONF_KEY_read 154ULL
#define WT_CONF_KEY_read_corrupt 144ULL
#define WT_CONF_KEY_read_once 132ULL
#define WT_CONF_KEY_read_timestamp 4ULL
#define WT_CONF_KEY_readonly 60ULL
#define WT_CONF_KEY_realloc_exact 197ULL
#define WT_CONF_KEY_realloc_malloc 198ULL
#define WT_CONF_KEY_recover 275ULL
#define WT_CONF_KEY_release 189ULL
#define WT_CONF_KEY_release_evict 120ULL
#define WT_CONF_KEY_release_evict_page 251ULL
#define WT_CONF_KEY_remove 229ULL
#define WT_CONF_KEY_remove_files 107ULL
#define WT_CONF_KEY_remove_shared 108ULL
#define WT_CONF_KEY_repair 101ULL
#define WT_CONF_KEY_require_max 259ULL
#define WT_CONF_KEY_require_min 260ULL
#define WT_CONF_KEY_reserve 239ULL
#define WT_CONF_KEY_rollback_error 199ULL
#define WT_CONF_KEY_roundup_timestamps 152ULL
#define WT_CONF_KEY_salvage 279ULL
#define WT_CONF_KEY_secretkey 265ULL
#define WT_CONF_KEY_session_max 280ULL
#define WT_CONF_KEY_session_scratch_max 281ULL
#define WT_CONF_KEY_session_table_cache 282ULL
#define WT_CONF_KEY_sessions 164ULL
#define WT_CONF_KEY_shared 54ULL
#define WT_CONF_KEY_shared_cache 236ULL
#define WT_CONF_KEY_size 171ULL
#define WT_CONF_KEY_skip_sort_check 133ULL
#define WT_CONF_KEY_slow_checkpoint 200ULL
#define WT_CONF_KEY_source 8ULL
#define WT_CONF_KEY_sources 243ULL
#define WT_CONF_KEY_split_deepen_min_child 44ULL
#define WT_CONF_KEY_split_deepen_per_child 45ULL
#define WT_CONF_KEY_split_pct 46ULL
#define WT_CONF_KEY_src_id 127ULL
#define WT_CONF_KEY_stable_timestamp 145ULL
#define WT_CONF_KEY_start_generation 80ULL
#define WT_CONF_KEY_statistics 134ULL
#define WT_CONF_KEY_statistics_log 240ULL
#define WT_CONF_KEY_strategy 113ULL
#define WT_CONF_KEY_stress_skiplist 201ULL
#define WT_CONF_KEY_strict 146ULL
#define WT_CONF_KEY_suffix 81ULL
#define WT_CONF_KEY_sync 114ULL
#define WT_CONF_KEY_system_ram 176ULL
#define WT_CONF_KEY_table_logging 202ULL
#define WT_CONF_KEY_target 135ULL
#define WT_CONF_KEY_terminate 249ULL
#define WT_CONF_KEY_this_id 128ULL
#define WT_CONF_KEY_threads_max 206ULL
#define WT_CONF_KEY_threads_min 207ULL
#define WT_CONF_KEY_tiered_flush_error_continue 203ULL
#define WT_CONF_KEY_tiered_object 61ULL
#define WT_CONF_KEY_tiered_storage 47ULL
#define WT_CONF_KEY_tiers 89ULL
#define WT_CONF_KEY_timeout 97ULL
#define WT_CONF_KEY_timestamp 244ULL
#define WT_CONF_KEY_timing_stress_for_test 245ULL
#define WT_CONF_KEY_total 224ULL
#define WT_CONF_KEY_transaction_sync 284ULL
#define WT_CONF_KEY_txn 165ULL
#define WT_CONF_KEY_type 9ULL
#define WT_CONF_KEY_update_restore_evict 204ULL
#define WT_CONF_KEY_use_environment 286ULL
#define WT_CONF_KEY_use_environment_priv 287ULL
#define WT_CONF_KEY_use_timestamp 158ULL
#define WT_CONF_KEY_value_format 55ULL
#define WT_CONF_KEY_verbose 10ULL
#define WT_CONF_KEY_verify_metadata 288ULL
#define WT_CONF_KEY_version 62ULL
#define WT_CONF_KEY_wait 182ULL
#define WT_CONF_KEY_worker_thread_max 232ULL
#define WT_CONF_KEY_write_through 289ULL
#define WT_CONF_KEY_write_timestamp 5ULL
#define WT_CONF_KEY_write_timestamp_usage 11ULL
#define WT_CONF_KEY_zero_fill 230ULL

#define WT_CONF_KEY_COUNT 290
/*
 * API configuration keys: END
 */

/*******************************************
 * Configuration key structure.
 *******************************************/
/*
 * DO NOT EDIT: automatically built by dist/api_config.py.
 * Configuration key structure: BEGIN
 */
static const struct {
    uint64_t access_pattern_hint;
    uint64_t action;
    uint64_t allocation_size;
    uint64_t app_metadata;
    uint64_t append;
    struct {
        uint64_t commit_timestamp;
        uint64_t durable_timestamp;
        uint64_t read_timestamp;
        uint64_t write_timestamp;
    } assert;
    uint64_t backup_restore_target;
    uint64_t block_allocation;
    struct {
        uint64_t blkcache_eviction_aggression;
        uint64_t cache_on_checkpoint;
        uint64_t cache_on_writes;
        uint64_t enabled;
        uint64_t full_target;
        uint64_t hashsize;
        uint64_t max_percent_overhead;
        uint64_t nvram_path;
        uint64_t percent_file_in_dram;
        uint64_t size;
        uint64_t system_ram;
        uint64_t type;
    } block_cache;
    uint64_t block_compressor;
    uint64_t bloom_bit_count;
    uint64_t bloom_false_positives;
    uint64_t bloom_hash_count;
    uint64_t bound;
    uint64_t bucket;
    uint64_t bucket_prefix;
    uint64_t buffer_alignment;
    uint64_t builtin_extension_config;
    uint64_t bulk;
    uint64_t cache;
    uint64_t cache_cursors;
    uint64_t cache_directory;
    uint64_t cache_max_wait_ms;
    uint64_t cache_overhead;
    uint64_t cache_resident;
    uint64_t cache_size;
    uint64_t cache_stuck_timeout_ms;
    struct {
        uint64_t log_size;
        uint64_t wait;
    } checkpoint;
    uint64_t checkpoint_backup_info;
    uint64_t checkpoint_cleanup;
    uint64_t checkpoint_lsn;
    uint64_t checkpoint_sync;
    uint64_t checkpoint_use_history;
    uint64_t checkpoint_wait;
    uint64_t checksum;
    struct {
        uint64_t capacity;
        uint64_t chunk_cache_evict_trigger;
        uint64_t chunk_size;
        uint64_t device_path;
        uint64_t enabled;
        uint64_t hashsize;
        uint64_t type;
    } chunk_cache;
    uint64_t chunks;
    uint64_t colgroups;
    uint64_t collator;
    uint64_t columns;
    uint64_t commit_timestamp;
    uint64_t compare;
    struct {
        uint64_t release;
    } compatibility;
    uint64_t compile_configuration_count;
    uint64_t config;
    uint64_t config_base;
    uint64_t count;
    uint64_t create;
    uint64_t cursors;
    struct {
        uint64_t release_evict_page;
    } debug;
    struct {
        uint64_t checkpoint_retention;
        uint64_t corruption_abort;
        uint64_t cursor_copy;
        uint64_t cursor_reposition;
        uint64_t eviction;
        uint64_t log_retention;
        uint64_t realloc_exact;
        uint64_t realloc_malloc;
        uint64_t rollback_error;
        uint64_t slow_checkpoint;
        uint64_t stress_skiplist;
        uint64_t table_logging;
        uint64_t tiered_flush_error_continue;
        uint64_t update_restore_evict;
    } debug_mode;
    uint64_t dictionary;
    uint64_t direct_io;
    uint64_t do_not_clear_txn_id;
    uint64_t drop;
    uint64_t dryrun;
    uint64_t dump;
    uint64_t dump_address;
    uint64_t dump_app_data;
    uint64_t dump_blocks;
    uint64_t dump_layout;
    uint64_t dump_offsets;
    uint64_t dump_pages;
    uint64_t durable_timestamp;
    uint64_t early_load;
    struct {
        uint64_t keyid;
        uint64_t name;
    } encryption;
    uint64_t entry;
    uint64_t error_prefix;
    struct {
        uint64_t threads_max;
        uint64_t threads_min;
    } eviction;
    uint64_t eviction_checkpoint_target;
    uint64_t eviction_dirty_target;
    uint64_t eviction_dirty_trigger;
    uint64_t eviction_target;
    uint64_t eviction_trigger;
    uint64_t eviction_updates_target;
    uint64_t eviction_updates_trigger;
    uint64_t exclusive;
    uint64_t exclusive_refreshed;
    uint64_t extensions;
    uint64_t extra_diagnostics;
    uint64_t extractor;
    uint64_t file_extend;
    struct {
        uint64_t close_handle_minimum;
        uint64_t close_idle_time;
        uint64_t close_scan_interval;
    } file_manager;
    uint64_t final_flush;
    struct {
        uint64_t enabled;
        uint64_t force;
        uint64_t sync;
        uint64_t timeout;
    } flush_tier;
    uint64_t flush_time;
    uint64_t flush_timestamp;
    uint64_t force;
    uint64_t format;
    uint64_t generation_drain_timeout_ms;
    uint64_t get;
    uint64_t handles;
    struct {
        uint64_t buckets;
        uint64_t dhandle_buckets;
    } hash;
    uint64_t hazard_max;
    struct {
        uint64_t file_max;
    } history_store;
    uint64_t huffman_key;
    uint64_t huffman_value;
    uint64_t id;
    uint64_t ignore_cache_size;
    uint64_t ignore_in_memory_cache_size;
    uint64_t ignore_prepare;
    uint64_t immutable;
    struct {
        uint64_t compare_timestamp;
        uint64_t enabled;
        uint64_t file_metadata;
        uint64_t metadata_file;
        uint64_t repair;
    } import;
    uint64_t in_memory;
    uint64_t inclusive;
    struct {
        uint64_t consolidate;
        uint64_t enabled;
        uint64_t file;
        uint64_t force_stop;
        uint64_t granularity;
        uint64_t src_id;
        uint64_t this_id;
    } incremental;
    uint64_t index_key_columns;
    uint64_t internal_item_max;
    uint64_t internal_key_max;
    uint64_t internal_key_truncate;
    uint64_t internal_page_max;
    struct {
        uint64_t total;
    } io_capacity;
    uint64_t isolation;
    uint64_t json_output;
    uint64_t key_format;
    uint64_t key_gap;
    uint64_t last;
    uint64_t leaf_item_max;
    uint64_t leaf_key_max;
    uint64_t leaf_page_max;
    uint64_t leaf_value_max;
    uint64_t leak_memory;
    uint64_t lock_wait;
    uint64_t log;
    struct {
        uint64_t auto_throttle;
        uint64_t bloom;
        uint64_t bloom_bit_count;
        uint64_t bloom_config;
        uint64_t bloom_hash_count;
        uint64_t bloom_oldest;
        uint64_t chunk_count_limit;
        uint64_t chunk_max;
        uint64_t chunk_size;
        struct {
            uint64_t prefix;
            uint64_t start_generation;
            uint64_t suffix;
        } merge_custom;
        uint64_t merge_max;
        uint64_t merge_min;
    } lsm;
    struct {
        uint64_t merge;
        uint64_t worker_thread_max;
    } lsm_manager;
    uint64_t memory_page_image_max;
    uint64_t memory_page_max;
    uint64_t mmap;
    uint64_t mmap_all;
    uint64_t multiprocess;
    uint64_t name;
    uint64_t next_random;
    uint64_t next_random_sample_size;
    uint64_t no_timestamp;
    uint64_t old_chunks;
    uint64_t oldest;
    uint64_t oldest_timestamp;
    uint64_t operation;
    uint64_t operation_timeout_ms;
    struct {
        uint64_t enabled;
        uint64_t path;
    } operation_tracking;
    uint64_t os_cache_dirty_max;
    uint64_t os_cache_max;
    uint64_t overwrite;
    uint64_t prefix_compression;
    uint64_t prefix_compression_min;
    uint64_t prefix_search;
    uint64_t prepare_timestamp;
    uint64_t priority;
    uint64_t raw;
    uint64_t read_corrupt;
    uint64_t read_once;
    uint64_t read_timestamp;
    uint64_t readonly;
    uint64_t remove_files;
    uint64_t remove_shared;
    struct {
        uint64_t prepared;
        uint64_t read;
    } roundup_timestamps;
    uint64_t salvage;
    uint64_t session_max;
    uint64_t session_scratch_max;
    uint64_t session_table_cache;
    uint64_t sessions;
    struct {
        uint64_t chunk;
        uint64_t name;
        uint64_t quota;
        uint64_t reserve;
        uint64_t size;
    } shared_cache;
    uint64_t skip_sort_check;
    uint64_t source;
    uint64_t split_deepen_min_child;
    uint64_t split_deepen_per_child;
    uint64_t split_pct;
    uint64_t stable_timestamp;
    uint64_t statistics;
    struct {
        uint64_t json;
        uint64_t on_close;
        uint64_t sources;
        uint64_t timestamp;
        uint64_t wait;
    } statistics_log;
    uint64_t strategy;
    uint64_t strict;
    uint64_t sync;
    uint64_t target;
    uint64_t terminate;
    uint64_t tiered_object;
    struct {
        uint64_t local_retention;
    } tiered_storage;
    uint64_t tiers;
    uint64_t timeout;
    uint64_t timing_stress_for_test;
    struct {
        uint64_t enabled;
        uint64_t method;
    } transaction_sync;
    uint64_t txn;
    uint64_t type;
    uint64_t use_environment;
    uint64_t use_environment_priv;
    uint64_t use_timestamp;
    uint64_t value_format;
    uint64_t verbose;
    uint64_t verify_metadata;
    uint64_t version;
    uint64_t write_through;
    uint64_t write_timestamp_usage;
} WT_CONF_KEY_STRUCTURE = {
  WT_CONF_KEY_access_pattern_hint,
  WT_CONF_KEY_action,
  WT_CONF_KEY_allocation_size,
  WT_CONF_KEY_app_metadata,
  WT_CONF_KEY_append,
  {
    WT_CONF_KEY_assert | (WT_CONF_KEY_commit_timestamp << 16),
    WT_CONF_KEY_assert | (WT_CONF_KEY_durable_timestamp << 16),
    WT_CONF_KEY_assert | (WT_CONF_KEY_read_timestamp << 16),
    WT_CONF_KEY_assert | (WT_CONF_KEY_write_timestamp << 16),
  },
  WT_CONF_KEY_backup_restore_target,
  WT_CONF_KEY_block_allocation,
  {
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_blkcache_eviction_aggression << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_cache_on_checkpoint << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_cache_on_writes << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_full_target << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_hashsize << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_max_percent_overhead << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_nvram_path << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_percent_file_in_dram << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_size << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_system_ram << 16),
    WT_CONF_KEY_block_cache | (WT_CONF_KEY_type << 16),
  },
  WT_CONF_KEY_block_compressor,
  WT_CONF_KEY_bloom_bit_count,
  WT_CONF_KEY_bloom_false_positives,
  WT_CONF_KEY_bloom_hash_count,
  WT_CONF_KEY_bound,
  WT_CONF_KEY_bucket,
  WT_CONF_KEY_bucket_prefix,
  WT_CONF_KEY_buffer_alignment,
  WT_CONF_KEY_builtin_extension_config,
  WT_CONF_KEY_bulk,
  WT_CONF_KEY_cache,
  WT_CONF_KEY_cache_cursors,
  WT_CONF_KEY_cache_directory,
  WT_CONF_KEY_cache_max_wait_ms,
  WT_CONF_KEY_cache_overhead,
  WT_CONF_KEY_cache_resident,
  WT_CONF_KEY_cache_size,
  WT_CONF_KEY_cache_stuck_timeout_ms,
  {
    WT_CONF_KEY_checkpoint | (WT_CONF_KEY_log_size << 16),
    WT_CONF_KEY_checkpoint | (WT_CONF_KEY_wait << 16),
  },
  WT_CONF_KEY_checkpoint_backup_info,
  WT_CONF_KEY_checkpoint_cleanup,
  WT_CONF_KEY_checkpoint_lsn,
  WT_CONF_KEY_checkpoint_sync,
  WT_CONF_KEY_checkpoint_use_history,
  WT_CONF_KEY_checkpoint_wait,
  WT_CONF_KEY_checksum,
  {
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_capacity << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_chunk_cache_evict_trigger << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_chunk_size << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_device_path << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_hashsize << 16),
    WT_CONF_KEY_chunk_cache | (WT_CONF_KEY_type << 16),
  },
  WT_CONF_KEY_chunks,
  WT_CONF_KEY_colgroups,
  WT_CONF_KEY_collator,
  WT_CONF_KEY_columns,
  WT_CONF_KEY_commit_timestamp,
  WT_CONF_KEY_compare,
  {
    WT_CONF_KEY_compatibility | (WT_CONF_KEY_release << 16),
  },
  WT_CONF_KEY_compile_configuration_count,
  WT_CONF_KEY_config,
  WT_CONF_KEY_config_base,
  WT_CONF_KEY_count,
  WT_CONF_KEY_create,
  WT_CONF_KEY_cursors,
  {
    WT_CONF_KEY_debug | (WT_CONF_KEY_release_evict_page << 16),
  },
  {
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_checkpoint_retention << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_corruption_abort << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_cursor_copy << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_cursor_reposition << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_eviction << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_log_retention << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_realloc_exact << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_realloc_malloc << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_rollback_error << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_slow_checkpoint << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_stress_skiplist << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_table_logging << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_tiered_flush_error_continue << 16),
    WT_CONF_KEY_debug_mode | (WT_CONF_KEY_update_restore_evict << 16),
  },
  WT_CONF_KEY_dictionary,
  WT_CONF_KEY_direct_io,
  WT_CONF_KEY_do_not_clear_txn_id,
  WT_CONF_KEY_drop,
  WT_CONF_KEY_dryrun,
  WT_CONF_KEY_dump,
  WT_CONF_KEY_dump_address,
  WT_CONF_KEY_dump_app_data,
  WT_CONF_KEY_dump_blocks,
  WT_CONF_KEY_dump_layout,
  WT_CONF_KEY_dump_offsets,
  WT_CONF_KEY_dump_pages,
  WT_CONF_KEY_durable_timestamp,
  WT_CONF_KEY_early_load,
  {
    WT_CONF_KEY_encryption | (WT_CONF_KEY_keyid << 16),
    WT_CONF_KEY_encryption | (WT_CONF_KEY_name << 16),
  },
  WT_CONF_KEY_entry,
  WT_CONF_KEY_error_prefix,
  {
    WT_CONF_KEY_eviction | (WT_CONF_KEY_threads_max << 16),
    WT_CONF_KEY_eviction | (WT_CONF_KEY_threads_min << 16),
  },
  WT_CONF_KEY_eviction_checkpoint_target,
  WT_CONF_KEY_eviction_dirty_target,
  WT_CONF_KEY_eviction_dirty_trigger,
  WT_CONF_KEY_eviction_target,
  WT_CONF_KEY_eviction_trigger,
  WT_CONF_KEY_eviction_updates_target,
  WT_CONF_KEY_eviction_updates_trigger,
  WT_CONF_KEY_exclusive,
  WT_CONF_KEY_exclusive_refreshed,
  WT_CONF_KEY_extensions,
  WT_CONF_KEY_extra_diagnostics,
  WT_CONF_KEY_extractor,
  WT_CONF_KEY_file_extend,
  {
    WT_CONF_KEY_file_manager | (WT_CONF_KEY_close_handle_minimum << 16),
    WT_CONF_KEY_file_manager | (WT_CONF_KEY_close_idle_time << 16),
    WT_CONF_KEY_file_manager | (WT_CONF_KEY_close_scan_interval << 16),
  },
  WT_CONF_KEY_final_flush,
  {
    WT_CONF_KEY_flush_tier | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_flush_tier | (WT_CONF_KEY_force << 16),
    WT_CONF_KEY_flush_tier | (WT_CONF_KEY_sync << 16),
    WT_CONF_KEY_flush_tier | (WT_CONF_KEY_timeout << 16),
  },
  WT_CONF_KEY_flush_time,
  WT_CONF_KEY_flush_timestamp,
  WT_CONF_KEY_force,
  WT_CONF_KEY_format,
  WT_CONF_KEY_generation_drain_timeout_ms,
  WT_CONF_KEY_get,
  WT_CONF_KEY_handles,
  {
    WT_CONF_KEY_hash | (WT_CONF_KEY_buckets << 16),
    WT_CONF_KEY_hash | (WT_CONF_KEY_dhandle_buckets << 16),
  },
  WT_CONF_KEY_hazard_max,
  {
    WT_CONF_KEY_history_store | (WT_CONF_KEY_file_max << 16),
  },
  WT_CONF_KEY_huffman_key,
  WT_CONF_KEY_huffman_value,
  WT_CONF_KEY_id,
  WT_CONF_KEY_ignore_cache_size,
  WT_CONF_KEY_ignore_in_memory_cache_size,
  WT_CONF_KEY_ignore_prepare,
  WT_CONF_KEY_immutable,
  {
    WT_CONF_KEY_import | (WT_CONF_KEY_compare_timestamp << 16),
    WT_CONF_KEY_import | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_import | (WT_CONF_KEY_file_metadata << 16),
    WT_CONF_KEY_import | (WT_CONF_KEY_metadata_file << 16),
    WT_CONF_KEY_import | (WT_CONF_KEY_repair << 16),
  },
  WT_CONF_KEY_in_memory,
  WT_CONF_KEY_inclusive,
  {
    WT_CONF_KEY_incremental | (WT_CONF_KEY_consolidate << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_file << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_force_stop << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_granularity << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_src_id << 16),
    WT_CONF_KEY_incremental | (WT_CONF_KEY_this_id << 16),
  },
  WT_CONF_KEY_index_key_columns,
  WT_CONF_KEY_internal_item_max,
  WT_CONF_KEY_internal_key_max,
  WT_CONF_KEY_internal_key_truncate,
  WT_CONF_KEY_internal_page_max,
  {
    WT_CONF_KEY_io_capacity | (WT_CONF_KEY_total << 16),
  },
  WT_CONF_KEY_isolation,
  WT_CONF_KEY_json_output,
  WT_CONF_KEY_key_format,
  WT_CONF_KEY_key_gap,
  WT_CONF_KEY_last,
  WT_CONF_KEY_leaf_item_max,
  WT_CONF_KEY_leaf_key_max,
  WT_CONF_KEY_leaf_page_max,
  WT_CONF_KEY_leaf_value_max,
  WT_CONF_KEY_leak_memory,
  WT_CONF_KEY_lock_wait,
  WT_CONF_KEY_log,
  {
    WT_CONF_KEY_lsm | (WT_CONF_KEY_auto_throttle << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_bloom << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_bloom_bit_count << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_bloom_config << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_bloom_hash_count << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_bloom_oldest << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_chunk_count_limit << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_chunk_max << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_chunk_size << 16),
    {
      WT_CONF_KEY_lsm | (WT_CONF_KEY_merge_custom << 16) | (WT_CONF_KEY_prefix << 32),
      WT_CONF_KEY_lsm | (WT_CONF_KEY_merge_custom << 16) | (WT_CONF_KEY_start_generation << 32),
      WT_CONF_KEY_lsm | (WT_CONF_KEY_merge_custom << 16) | (WT_CONF_KEY_suffix << 32),
    },
    WT_CONF_KEY_lsm | (WT_CONF_KEY_merge_max << 16),
    WT_CONF_KEY_lsm | (WT_CONF_KEY_merge_min << 16),
  },
  {
    WT_CONF_KEY_lsm_manager | (WT_CONF_KEY_merge << 16),
    WT_CONF_KEY_lsm_manager | (WT_CONF_KEY_worker_thread_max << 16),
  },
  WT_CONF_KEY_memory_page_image_max,
  WT_CONF_KEY_memory_page_max,
  WT_CONF_KEY_mmap,
  WT_CONF_KEY_mmap_all,
  WT_CONF_KEY_multiprocess,
  WT_CONF_KEY_name,
  WT_CONF_KEY_next_random,
  WT_CONF_KEY_next_random_sample_size,
  WT_CONF_KEY_no_timestamp,
  WT_CONF_KEY_old_chunks,
  WT_CONF_KEY_oldest,
  WT_CONF_KEY_oldest_timestamp,
  WT_CONF_KEY_operation,
  WT_CONF_KEY_operation_timeout_ms,
  {
    WT_CONF_KEY_operation_tracking | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_operation_tracking | (WT_CONF_KEY_path << 16),
  },
  WT_CONF_KEY_os_cache_dirty_max,
  WT_CONF_KEY_os_cache_max,
  WT_CONF_KEY_overwrite,
  WT_CONF_KEY_prefix_compression,
  WT_CONF_KEY_prefix_compression_min,
  WT_CONF_KEY_prefix_search,
  WT_CONF_KEY_prepare_timestamp,
  WT_CONF_KEY_priority,
  WT_CONF_KEY_raw,
  WT_CONF_KEY_read_corrupt,
  WT_CONF_KEY_read_once,
  WT_CONF_KEY_read_timestamp,
  WT_CONF_KEY_readonly,
  WT_CONF_KEY_remove_files,
  WT_CONF_KEY_remove_shared,
  {
    WT_CONF_KEY_roundup_timestamps | (WT_CONF_KEY_prepared << 16),
    WT_CONF_KEY_roundup_timestamps | (WT_CONF_KEY_read << 16),
  },
  WT_CONF_KEY_salvage,
  WT_CONF_KEY_session_max,
  WT_CONF_KEY_session_scratch_max,
  WT_CONF_KEY_session_table_cache,
  WT_CONF_KEY_sessions,
  {
    WT_CONF_KEY_shared_cache | (WT_CONF_KEY_chunk << 16),
    WT_CONF_KEY_shared_cache | (WT_CONF_KEY_name << 16),
    WT_CONF_KEY_shared_cache | (WT_CONF_KEY_quota << 16),
    WT_CONF_KEY_shared_cache | (WT_CONF_KEY_reserve << 16),
    WT_CONF_KEY_shared_cache | (WT_CONF_KEY_size << 16),
  },
  WT_CONF_KEY_skip_sort_check,
  WT_CONF_KEY_source,
  WT_CONF_KEY_split_deepen_min_child,
  WT_CONF_KEY_split_deepen_per_child,
  WT_CONF_KEY_split_pct,
  WT_CONF_KEY_stable_timestamp,
  WT_CONF_KEY_statistics,
  {
    WT_CONF_KEY_statistics_log | (WT_CONF_KEY_json << 16),
    WT_CONF_KEY_statistics_log | (WT_CONF_KEY_on_close << 16),
    WT_CONF_KEY_statistics_log | (WT_CONF_KEY_sources << 16),
    WT_CONF_KEY_statistics_log | (WT_CONF_KEY_timestamp << 16),
    WT_CONF_KEY_statistics_log | (WT_CONF_KEY_wait << 16),
  },
  WT_CONF_KEY_strategy,
  WT_CONF_KEY_strict,
  WT_CONF_KEY_sync,
  WT_CONF_KEY_target,
  WT_CONF_KEY_terminate,
  WT_CONF_KEY_tiered_object,
  {
    WT_CONF_KEY_tiered_storage | (WT_CONF_KEY_local_retention << 16),
  },
  WT_CONF_KEY_tiers,
  WT_CONF_KEY_timeout,
  WT_CONF_KEY_timing_stress_for_test,
  {
    WT_CONF_KEY_transaction_sync | (WT_CONF_KEY_enabled << 16),
    WT_CONF_KEY_transaction_sync | (WT_CONF_KEY_method << 16),
  },
  WT_CONF_KEY_txn,
  WT_CONF_KEY_type,
  WT_CONF_KEY_use_environment,
  WT_CONF_KEY_use_environment_priv,
  WT_CONF_KEY_use_timestamp,
  WT_CONF_KEY_value_format,
  WT_CONF_KEY_verbose,
  WT_CONF_KEY_verify_metadata,
  WT_CONF_KEY_version,
  WT_CONF_KEY_write_through,
  WT_CONF_KEY_write_timestamp_usage,
};
/*
 * Configuration key structure: END
 */

#define WT_CONF_BIND_VALUES_LEN 5

typedef WT_CONF_COMPILED WT_CONF_LIST;

/*
 * WT_CONF_BINDINGS --
 *	A set of values bound.
 */
struct __wt_conf_bindings {
    u_int bind_values; /* position of top of values (next available) */

    struct {
        WT_CONF_BIND_DESC *desc;
        WT_CONFIG_ITEM item;
    } values[WT_CONF_BIND_VALUES_LEN];
};

/*
 * WT_CONF_BIND_DESC --
 *	A descriptor about a value to be bound.
 */
struct __wt_conf_bind_desc {
    u_int type;   /* WT_CONFIG_ITEM.type */
    u_int offset; /* offset into WT_SESSION::conf_bindings.values table */
};

/*
 * WT_CONF_COMPILED --
 *	A compiled configuration string
 */
struct __wt_conf_compiled {
    enum { CONF_COMPILED_TEMP = 0, CONF_COMPILED_CALLER, CONF_COMPILED_BASE_API } compiled_type;
    const WT_CONFIG_ENTRY *compile_time_entry; /* May be used for diagnostic checks. */
    char *orig_config;

    uint8_t key_to_set_item[WT_CONF_KEY_COUNT]; /* For each key, a 1-based index into set_item */

    uint32_t set_item_count;
    size_t set_item_allocated;
    WT_CONF_SET_ITEM *set_item;

    uint32_t binding_count;
    size_t binding_allocated;
    WT_CONF_BIND_DESC **binding_descriptions;
};

struct __wt_conf_set_item {
    enum {
        CONF_SET_DEFAULT_ITEM,
        CONF_SET_NONDEFAULT_ITEM,
        CONF_SET_BIND_DESC,
        CONF_SET_SUB_INFO
    } set_type;
    union {
        WT_CONFIG_ITEM item;
        WT_CONF_BIND_DESC bind_desc;
        WT_CONF_COMPILED *sub;
    } u;
};
