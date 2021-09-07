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
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_random_abort
    SOURCES random_abort/main.c
    DIR_NAME random_abort
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_random_directio
    SOURCES
        random_directio/main.c
        random_directio/util.c
    DIR_NAME random_directio
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_rwlock
    SOURCES rwlock/main.c
    DIR_NAME rwlock
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_schema_abort
    SOURCES schema_abort/main.c
    DIR_NAME schema_abort
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_scope
    SOURCES scope/main.c
    DIR_NAME scope
)

define_c_test(
    TARGET test_timestamp_abort
    SOURCES timestamp_abort/main.c
    DIR_NAME timestamp_abort
    SMOKE
    ARGUMENTS -b $<TARGET_FILE:test_timestamp_abort>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_truncated_log
    SOURCES truncated_log/main.c
    DIR_NAME truncated_log
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt1965_col_efficiency
    SOURCES wt1965_col_efficiency/main.c
    DIR_NAME wt1965_col_efficiency
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2403_lsm_workload
    SOURCES wt2403_lsm_workload/main.c
    DIR_NAME wt2403_lsm_workload
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2246_col_append
    SOURCES wt2246_col_append/main.c
    DIR_NAME wt2246_col_append
)

define_c_test(
    TARGET test_wt2323_join_visibility
    SOURCES wt2323_join_visibility/main.c
    DIR_NAME wt2323_join_visibility
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2535_insert_race
    SOURCES wt2535_insert_race/main.c
    DIR_NAME wt2535_insert_race
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2447_join_main_table
    SOURCES wt2447_join_main_table/main.c
    DIR_NAME wt2447_join_main_table
    SMOKE
)

define_c_test(
    TARGET test_wt2695_checksum
    SOURCES wt2695_checksum/main.c
    DIR_NAME wt2695_checksum
)

define_c_test(
    TARGET test_wt2592_join_schema
    SOURCES wt2592_join_schema/main.c
    DIR_NAME wt2592_join_schema
)

define_c_test(
    TARGET test_wt2834_join_bloom_fix
    SOURCES wt2834_join_bloom_fix/main.c
    DIR_NAME wt2834_join_bloom_fix
)

define_c_test(
    TARGET test_wt2853_perf
    SOURCES wt2853_perf/main.c
    DIR_NAME wt2853_perf
    SMOKE
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
    ARGUMENTS -b ${CMAKE_BINARY_DIR}
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt2999_join_extractor
    SOURCES wt2999_join_extractor/main.c
    DIR_NAME wt2999_join_extractor
)

define_c_test(
    TARGET test_wt3120_filesys
    SOURCES wt3120_filesys/main.c
    DIR_NAME wt3120_filesys
    # We need to manually specify the location of the fail fs library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_FAIL_FS_LIB=\"ext/test/fail_fs/libwiredtiger_fail_fs.so\""
    ARGUMENTS -b ${CMAKE_BINARY_DIR}
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3135_search_near_collator
    SOURCES wt3135_search_near_collator/main.c
    DIR_NAME wt3135_search_near_collator
)

define_c_test(
    TARGET test_wt3184_dup_index_collator
    SOURCES wt3184_dup_index_collator/main.c
    DIR_NAME wt3184_dup_index_collator
)

define_c_test(
    TARGET test_wt3338_partial_update
    SOURCES wt3338_partial_update/main.c
    DIR_NAME wt3338_partial_update
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3363_checkpoint_op_races
    SOURCES wt3363_checkpoint_op_races/main.c
    DIR_NAME wt3363_checkpoint_op_races
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt3874_pad_byte_collator
    SOURCES wt3874_pad_byte_collator/main.c
    DIR_NAME wt3874_pad_byte_collator
)

define_c_test(
    TARGET test_wt4105_large_doc_small_upd
    SOURCES wt4105_large_doc_small_upd/main.c
    DIR_NAME wt4105_large_doc_small_upd
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4117_checksum
    SOURCES wt4117_checksum/main.c
    DIR_NAME wt4117_checksum
)

define_c_test(
    TARGET test_wt4156_metadata_salvage
    SOURCES wt4156_metadata_salvage/main.c
    DIR_NAME wt4156_metadata_salvage
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4333_handle_locks
    SOURCES wt4333_handle_locks/main.c
    DIR_NAME wt4333_handle_locks
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4699_json
    SOURCES wt4699_json/main.c
    DIR_NAME wt4699_json
)

define_c_test(
    TARGET test_wt4803_history_store_abort
    SOURCES wt4803_history_store_abort/main.c
    DIR_NAME wt4803_history_store_abort
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt4891_meta_ckptlist_get_alloc
    SOURCES wt4891_meta_ckptlist_get_alloc/main.c
    DIR_NAME wt4891_meta_ckptlist_get_alloc
)

define_c_test(
    TARGET test_wt6185_modify_ts
    SOURCES wt6185_modify_ts/main.c
    DIR_NAME wt6185_modify_ts
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt6616_checkpoint_oldest_ts
    SOURCES wt6616_checkpoint_oldest_ts/main.c
    DIR_NAME wt6616_checkpoint_oldest_ts
    SMOKE
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET test_wt7989_compact_checkpoint
    SOURCES wt7989_compact_checkpoint/main.c
    DIR_NAME wt7989_compact_checkpoint
    DEPENDS "WT_POSIX"
)

# Run this during a "ctest check" or "ctest csuite" smoke test
set_tests_properties(${c_tests} PROPERTIES LABELS "check;csuite")
