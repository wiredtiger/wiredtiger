project(unittest CXX)

# Assertion unit tests require overriding wt_abort such that WiredTiger no longer crashes.
# This change will break assertions for all other testing so these tests must be run on their own.
if(HAVE_UNITTEST_ASSERTS)
    set(unittests misc_tests/test_assertions.cpp)
else()
    set(unittests
        misc_tests/test_acquire_release_macros.cpp
        misc_tests/test_binary_search_string.cpp
        misc_tests/test_bitmap.cpp
        misc_tests/test_crc32.cpp
        misc_tests/test_error.cpp
        misc_tests/test_fnv.cpp
        misc_tests/test_futex.cpp
        misc_tests/test_intpack.cpp
        misc_tests/test_mock_session.cpp
        misc_tests/test_pow.cpp
        misc_tests/test_prepare_mod_sort.cpp
        misc_tests/test_reconciliation_tracking.cpp
        misc_tests/test_session_config.cpp
        misc_tests/test_string_match.cpp
        misc_tests/test_tailq.cpp
        block/api/test_block_api_misc.cpp
        block/api/test_block_api_write.cpp
        block/unit/test_bitstring.cpp
        block/unit/test_block_addr.cpp
        block/unit/test_block_ckpt.cpp
        block/unit/test_block_file.cpp
        block/unit/test_block_other.cpp
        block/unit/test_block_session_bms.cpp
        block/unit/test_block_session_ext.cpp
        block/unit/test_block_session_size.cpp
        block/unit/test_extent_list.cpp
        block/unit/test_extent_list_insert_wo_block.cpp
        block/unit/test_extent_list_search.cpp
        block/unit/test_extent_list_insert_block.cpp
        cursors/api/test_bulk_cursor.cpp
        cursors/unit/test_bounds_restore.cpp
        cursors/unit/test_cursor_get_raw_key_value.cpp
        sub_level_error/api/test_sub_level_error_session_get_last_error.cpp
        sub_level_error/api/test_sub_level_error_strerror.cpp
        sub_level_error/unit/test_sub_level_error_api_end.cpp
        sub_level_error/unit/test_sub_level_error_compact.cpp
        sub_level_error/unit/test_sub_level_error_drop_conflict.cpp
        sub_level_error/unit/test_sub_level_error_drop_uncommitted_dirty.cpp
        sub_level_error/unit/test_sub_level_error_is_valid_sub_level_error.cpp
        sub_level_error/unit/test_sub_level_error_msg_macros.cpp
        sub_level_error/unit/test_sub_level_error_rollback.cpp
        sub_level_error/unit/test_sub_level_error_session_set_last_error.cpp
    )
endif()

# FIXME-WT-14051: Live restore is not supported on Windows
if(NOT WT_WIN)
    set(unittests live_restore/api/test_live_restore_fh_lock_close_sync.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fh_read_write.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fh_size.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fh_truncate.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fs_directory_list.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fs_open_file.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fs_exist.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fs_remove_rename.cpp ${unittests})
    set(unittests live_restore/api/test_live_restore_fs_size.cpp ${unittests})
    set(unittests live_restore/unit/test_live_restore_bitmap_encode_decode.cpp ${unittests})
    set(unittests live_restore/unit/test_live_restore_bitmap_filling_bit_range.cpp ${unittests})
    set(unittests live_restore/unit/test_live_restore_compute_read_end_bit.cpp ${unittests})
    set(unittests live_restore/unit/test_live_restore_fill_hole.cpp ${unittests})
    set(unittests sub_level_error/unit/test_sub_level_error_live_restore_conflict.cpp ${unittests})
endif()

set(unittest_sources
    main.cpp
    utils.cpp
    block/util_block.cpp
    block/utils_extlist.cpp
    sub_level_error/utils_sub_level_error.cpp
    wrappers/block_mods.cpp
    wrappers/config_parser.cpp
    wrappers/connection_wrapper.cpp
    wrappers/item_wrapper.cpp
    wrappers/mock_connection.cpp
    wrappers/mock_session.cpp
    ${unittests}
)

# Live restore is not supported on Windows
if(NOT WT_WIN)
    set(unittest_sources
        live_restore/live_restore_test_env.cpp
        live_restore/utils_live_restore.cpp
        ${unittest_sources}
    )
endif()

# Disable 4200 warning on windows, its related to struct definitions with zero sized arrays.
if(WT_WIN)
    set(UNITTEST_FLAGS /wd4200)
else()
    set(UNITTEST_FLAGS -Wno-missing-declarations)
endif()

create_test_executable(catch2-unittests
    SOURCES ${unittest_sources}
    FLAGS ${UNITTEST_FLAGS}
    CXX
)

target_compile_options(
    catch2-unittests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(catch2-unittests Catch2::Catch2)

if(WT_POSIX)
    add_dependencies(catch2-unittests wiredtiger_dir_store)
endif()

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/catch2-unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
