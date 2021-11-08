#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

project(csuite)

include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

set(c_tests)

define_c_test(
    TARGET test_incr_backup
    SOURCES incr_backup/main.c
    DIR_NAME incr_backup
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/incr_backup/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_incr_backup>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_random_abort
    SOURCES random_abort/main.c
    DIR_NAME random_abort
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/random_abort/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_random_abort>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_random_directio
    SOURCES
        random_directio/main.c
        random_directio/util.c
    DIR_NAME random_directio
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/random_directio/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_random_directio>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_rwlock
    SOURCES rwlock/main.c
    DIR_NAME rwlock
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_rwlock>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_schema_abort
    SOURCES schema_abort/main.c
    DIR_NAME schema_abort
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/schema_abort/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_schema_abort>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_scope
    SOURCES scope/main.c
    DIR_NAME scope
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_scope>/WT_HOME>
)

define_c_test(
    TARGET test_tiered_abort
    SOURCES tiered_abort/main.c
    DIR_NAME tiered_abort
    # We need to manually specify the location of the local store library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_STORAGE_LIB=\"ext/storage_sources/local_store/libwiredtiger_local_store.so\""
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/tiered_abort/smoke.sh
    ARGUMENTS -b ${CMAKE_BINARY_DIR} $<TARGET_FILE:test_tiered_abort>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_timestamp_abort
    SOURCES timestamp_abort/main.c
    DIR_NAME timestamp_abort
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/timestamp_abort/smoke.sh
    ARGUMENTS -b $<TARGET_FILE:test_timestamp_abort>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_truncated_log
    SOURCES truncated_log/main.c
    DIR_NAME truncated_log
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/truncated_log/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_truncated_log>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt1965_col_efficiency
    SOURCES wt1965_col_efficiency/main.c
    DIR_NAME wt1965_col_efficiency
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt1965_col_efficiency>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2403_lsm_workload
    SOURCES wt2403_lsm_workload/main.c
    DIR_NAME wt2403_lsm_workload
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2403_lsm_workload>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2246_col_append
    SOURCES wt2246_col_append/main.c
    DIR_NAME wt2246_col_append
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2246_col_append>/WT_HOME>
)

define_c_test(
    TARGET test_wt2323_join_visibility
    SOURCES wt2323_join_visibility/main.c
    DIR_NAME wt2323_join_visibility
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2323_join_visibility>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2535_insert_race
    SOURCES wt2535_insert_race/main.c
    DIR_NAME wt2535_insert_race
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt2535_insert_race/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_wt2535_insert_race>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2447_join_main_table
    SOURCES wt2447_join_main_table/main.c
    DIR_NAME wt2447_join_main_table
    VARIANTS
        "test_wt2447_join_main_table_row;-t r"
        "test_wt2447_join_main_table_col;-t c"
)

define_c_test(
    TARGET test_wt2695_checksum
    SOURCES wt2695_checksum/main.c
    DIR_NAME wt2695_checksum
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2695_checksum>/WT_HOME>
)

define_c_test(
    TARGET test_wt2592_join_schema
    SOURCES wt2592_join_schema/main.c
    DIR_NAME wt2592_join_schema
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2592_join_schema>/WT_HOME>
)

define_c_test(
    TARGET test_wt2834_join_bloom_fix
    SOURCES wt2834_join_bloom_fix/main.c
    DIR_NAME wt2834_join_bloom_fix
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2834_join_bloom_fix>/WT_HOME>
)

define_c_test(
    TARGET test_wt2853_perf
    SOURCES wt2853_perf/main.c
    DIR_NAME wt2853_perf
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt2853_perf/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_wt2853_perf>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2909_checkpoint_integrity
    SOURCES wt2909_checkpoint_integrity/main.c
    DIR_NAME wt2909_checkpoint_integrity
    # We need to manually specify the location of the fail fs library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_FAIL_FS_LIB=\"ext/test/fail_fs/libwiredtiger_fail_fs.so\""
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt2909_checkpoint_integrity/smoke.sh
    ARGUMENTS -b ${CMAKE_BINARY_DIR} $<TARGET_FILE:test_wt2909_checkpoint_integrity>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2999_join_extractor
    SOURCES wt2999_join_extractor/main.c
    DIR_NAME wt2999_join_extractor
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt2999_join_extractor>/WT_HOME>
)

define_c_test(
    TARGET test_wt3120_filesys
    SOURCES wt3120_filesys/main.c
    DIR_NAME wt3120_filesys
    # We need to manually specify the location of the fail fs library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_FAIL_FS_LIB=\"ext/test/fail_fs/libwiredtiger_fail_fs.so\""
    ARGUMENTS -b ${CMAKE_BINARY_DIR} -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3120_filesys>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3135_search_near_collator
    SOURCES wt3135_search_near_collator/main.c
    DIR_NAME wt3135_search_near_collator
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3135_search_near_collator>/WT_HOME>
)

define_c_test(
    TARGET test_wt3184_dup_index_collator
    SOURCES wt3184_dup_index_collator/main.c
    DIR_NAME wt3184_dup_index_collator
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3184_dup_index_collator>/WT_HOME>
)

define_c_test(
    TARGET test_wt3338_partial_update
    SOURCES wt3338_partial_update/main.c
    DIR_NAME wt3338_partial_update
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3338_partial_update>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3363_checkpoint_op_races
    SOURCES wt3363_checkpoint_op_races/main.c
    DIR_NAME wt3363_checkpoint_op_races
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3363_checkpoint_op_races>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3874_pad_byte_collator
    SOURCES wt3874_pad_byte_collator/main.c
    DIR_NAME wt3874_pad_byte_collator
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt3874_pad_byte_collator>/WT_HOME>
)

define_c_test(
    TARGET test_wt4105_large_doc_small_upd
    SOURCES wt4105_large_doc_small_upd/main.c
    DIR_NAME wt4105_large_doc_small_upd
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt4105_large_doc_small_upd/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_wt4105_large_doc_small_upd>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4117_checksum
    SOURCES wt4117_checksum/main.c
    DIR_NAME wt4117_checksum
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4117_checksum>/WT_HOME>
)

define_c_test(
    TARGET test_wt4156_metadata_salvage
    SOURCES wt4156_metadata_salvage/main.c
    DIR_NAME wt4156_metadata_salvage
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4156_metadata_salvage>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4333_handle_locks
    SOURCES wt4333_handle_locks/main.c
    DIR_NAME wt4333_handle_locks
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4333_handle_locks>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4699_json
    SOURCES wt4699_json/main.c
    DIR_NAME wt4699_json
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4699_json>/WT_HOME>
)

define_c_test(
    TARGET test_wt4803_history_store_abort
    SOURCES wt4803_history_store_abort/main.c
    DIR_NAME wt4803_history_store_abort
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4803_history_store_abort>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4891_meta_ckptlist_get_alloc
    SOURCES wt4891_meta_ckptlist_get_alloc/main.c
    DIR_NAME wt4891_meta_ckptlist_get_alloc
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt4891_meta_ckptlist_get_alloc>/WT_HOME>
)

define_c_test(
    TARGET test_wt6185_modify_ts
    SOURCES wt6185_modify_ts/main.c
    DIR_NAME wt6185_modify_ts
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt6185_modify_ts/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_wt6185_modify_ts>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt6616_checkpoint_oldest_ts
    SOURCES wt6616_checkpoint_oldest_ts/main.c
    DIR_NAME wt6616_checkpoint_oldest_ts
    EXEC_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/wt6616_checkpoint_oldest_ts/smoke.sh
    ARGUMENTS $<TARGET_FILE:test_wt6616_checkpoint_oldest_ts>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt7989_compact_checkpoint
    SOURCES wt7989_compact_checkpoint/main.c
    DIR_NAME wt7989_compact_checkpoint
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt7989_compact_checkpoint>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt8057_compact_stress
    SOURCES wt8057_compact_stress/main.c
    DIR_NAME wt8057_compact_stress
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:test_wt8057_compact_stress>/WT_HOME>
    DEPENDS "WT_POSIX"
)
