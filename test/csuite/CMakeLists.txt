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

set(csuite_tests)

macro(define_csuite_test)
    cmake_parse_arguments(
        "CSUITE"
        "SMOKE"
        "TARGET;DIR_NAME"
        "SOURCES;FLAGS;ARGUMENTS"
        ${ARGN}
    )
    if (NOT "${CSUITE_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Unknown arguments to define_csuite_test: ${CSUITE_UNPARSED_ARGUMENTS}")
    endif()
    if ("${CSUITE_TARGET}" STREQUAL "")
        message(FATAL_ERROR "No target name given to define_csuite_test")
    endif()
    if ("${CSUITE_SOURCES}" STREQUAL "")
        message(FATAL_ERROR "No sources given to define_csuite_test")
    endif()
    if ("${CSUITE_DIR_NAME}" STREQUAL "")
        message(FATAL_ERROR "No directory given to define_csuite_test")
    endif()
    set(additional_executable_args)
    if(NOT "${CSUITE_FLAGS}" STREQUAL "")
        list(APPEND additional_executable_args FLAGS ${CSUITE_FLAGS})
    endif()
    if (CSUITE_SMOKE)
        # csuite test comes with a smoke execution wrapper.
        create_test_executable(${CSUITE_TARGET}
            SOURCES ${CSUITE_SOURCES}
            ADDITIONAL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/${CSUITE_DIR_NAME}/smoke.sh
            BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}
            ${additional_executable_args}
        )
        add_test(NAME ${CSUITE_TARGET}
            COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}/smoke.sh $<TARGET_FILE:${CSUITE_TARGET}> ${CSUITE_ARGUMENTS}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}
        )
    else()
        create_test_executable(${CSUITE_TARGET}
            SOURCES ${CSUITE_SOURCES}
            BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}
            ${additional_executable_args}
        )
        # Take a CMake-based path and convert it to a platform-specfic path (/ for Unix, \ for Windows).
        set(wt_test_home_dir ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}/WT_HOME_${CSUITE_TARGET})
        file(TO_NATIVE_PATH "${wt_test_home_dir}" wt_test_home_dir)
        # Ensure each DB home directory is run under the tests working directory.
        set(command_args -h ${wt_test_home_dir})
        list(APPEND command_args ${CSUITE_ARGUMENTS})
        add_test(NAME ${CSUITE_TARGET}
            COMMAND ${CSUITE_TARGET} ${command_args}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CSUITE_DIR_NAME}
        )
    endif()
    list(APPEND csuite_tests ${CSUITE_TARGET})
endmacro(define_csuite_test)

define_csuite_test(
    TARGET test_incr_backup
    SOURCES incr_backup/main.c
    DIR_NAME incr_backup
    SMOKE
)

define_csuite_test(
    TARGET test_random_abort
    SOURCES random_abort/main.c
    DIR_NAME random_abort
    SMOKE
)

define_csuite_test(
    TARGET test_random_directio
    SOURCES
        random_directio/main.c
        random_directio/util.c
    DIR_NAME random_directio
    SMOKE
)

define_csuite_test(
    TARGET test_rwlock
    SOURCES rwlock/main.c
    DIR_NAME rwlock
)

define_csuite_test(
    TARGET test_schema_abort
    SOURCES schema_abort/main.c
    DIR_NAME schema_abort
    SMOKE
)

define_csuite_test(
    TARGET test_scope
    SOURCES scope/main.c
    DIR_NAME scope
)

define_csuite_test(
    TARGET test_timestamp_abort
    SOURCES timestamp_abort/main.c
    DIR_NAME timestamp_abort
    SMOKE
)

define_csuite_test(
    TARGET test_truncated_log
    SOURCES truncated_log/main.c
    DIR_NAME truncated_log
)

define_csuite_test(
    TARGET test_wt1965_col_efficiency
    SOURCES wt1965_col_efficiency/main.c
    DIR_NAME wt1965_col_efficiency
)

define_csuite_test(
    TARGET test_wt2403_lsm_workload
    SOURCES wt2403_lsm_workload/main.c
    DIR_NAME wt2403_lsm_workload
)

# Temporarily disabled (WT-5790).
#define_csuite_test(
#    TARGET test_wt2246_col_append
#    SOURCES wt2246_col_append/main.c
#    DIR_NAME wt2246_col_append
#)

define_csuite_test(
    TARGET test_wt2323_join_visibility
    SOURCES wt2323_join_visibility/main.c
    DIR_NAME wt2323_join_visibility
)

define_csuite_test(
    TARGET test_wt2535_insert_race
    SOURCES wt2535_insert_race/main.c
    DIR_NAME wt2535_insert_race
)

define_csuite_test(
    TARGET test_wt2447_join_main_table
    SOURCES wt2447_join_main_table/main.c
    DIR_NAME wt2447_join_main_table
)

define_csuite_test(
    TARGET test_wt2695_checksum
    SOURCES wt2695_checksum/main.c
    DIR_NAME wt2695_checksum
)

define_csuite_test(
    TARGET test_wt2592_join_schema
    SOURCES wt2592_join_schema/main.c
    DIR_NAME wt2592_join_schema
)

define_csuite_test(
    TARGET test_wt2834_join_bloom_fix
    SOURCES wt2834_join_bloom_fix/main.c
    DIR_NAME wt2834_join_bloom_fix
)

define_csuite_test(
    TARGET test_wt2853_perf
    SOURCES wt2853_perf/main.c
    DIR_NAME wt2853_perf
)

define_csuite_test(
    TARGET test_wt2909_checkpoint_integrity
    SOURCES wt2909_checkpoint_integrity/main.c
    DIR_NAME wt2909_checkpoint_integrity
    # We need to manually specify the location of the fail fs library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_FAIL_FS_LIB=\"ext/test/fail_fs/libwiredtiger_fail_fs.so\""
    ARGUMENTS -b ${CMAKE_BINARY_DIR}
)

define_csuite_test(
    TARGET test_wt2999_join_extractor
    SOURCES wt2999_join_extractor/main.c
    DIR_NAME wt2999_join_extractor
)

define_csuite_test(
    TARGET test_wt3120_filesys
    SOURCES wt3120_filesys/main.c
    DIR_NAME wt3120_filesys
    # We need to manually specify the location of the fail fs library
    # and build directory as this path is more dynamic compared to layout autoconf
    # produces in build_posix.
    FLAGS "-DWT_FAIL_FS_LIB=\"ext/test/fail_fs/libwiredtiger_fail_fs.so\""
    ARGUMENTS -b ${CMAKE_BINARY_DIR}
)

define_csuite_test(
    TARGET test_wt3135_search_near_collator
    SOURCES wt3135_search_near_collator/main.c
    DIR_NAME wt3135_search_near_collator
)

define_csuite_test(
    TARGET test_wt3184_dup_index_collator
    SOURCES wt3184_dup_index_collator/main.c
    DIR_NAME wt3184_dup_index_collator
)

define_csuite_test(
    TARGET test_wt3338_partial_update
    SOURCES wt3338_partial_update/main.c
    DIR_NAME wt3338_partial_update
)

define_csuite_test(
    TARGET test_wt3363_checkpoint_op_races
    SOURCES wt3363_checkpoint_op_races/main.c
    DIR_NAME wt3363_checkpoint_op_races
)

define_csuite_test(
    TARGET test_wt3874_pad_byte_collator
    SOURCES wt3874_pad_byte_collator/main.c
    DIR_NAME wt3874_pad_byte_collator
)

define_csuite_test(
    TARGET test_wt4105_large_doc_small_upd
    SOURCES wt4105_large_doc_small_upd/main.c
    DIR_NAME wt4105_large_doc_small_upd
)

define_csuite_test(
    TARGET test_wt4117_checksum
    SOURCES wt4117_checksum/main.c
    DIR_NAME wt4117_checksum
)

define_csuite_test(
    TARGET test_wt4156_metadata_salvage
    SOURCES wt4156_metadata_salvage/main.c
    DIR_NAME wt4156_metadata_salvage
)

define_csuite_test(
    TARGET test_wt4333_handle_locks
    SOURCES wt4333_handle_locks/main.c
    DIR_NAME wt4333_handle_locks
)

define_csuite_test(
    TARGET test_wt4699_json
    SOURCES wt4699_json/main.c
    DIR_NAME wt4699_json
)

define_csuite_test(
    TARGET test_wt4803_history_store_abort
    SOURCES wt4803_history_store_abort/main.c
    DIR_NAME wt4803_history_store_abort
)

define_csuite_test(
    TARGET test_wt4891_meta_ckptlist_get_alloc
    SOURCES wt4891_meta_ckptlist_get_alloc/main.c
    DIR_NAME wt4891_meta_ckptlist_get_alloc
)

define_csuite_test(
    TARGET test_wt6185_modify_ts
    SOURCES wt6185_modify_ts/main.c
    DIR_NAME wt6185_modify_ts
)

define_csuite_test(
    TARGET test_wt6616_checkpoint_oldest_ts
    SOURCES wt6616_checkpoint_oldest_ts/main.c
    DIR_NAME wt6616_checkpoint_oldest_ts
)

# Run this during a "ctest check" or "ctest csuite" smoke test
set_tests_properties(${csuite_tests} PROPERTIES LABELS "check;csuite")
