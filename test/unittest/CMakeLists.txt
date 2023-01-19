project(unittest CXX)

# Assertion unit tests require overriding wt_abort such that WiredTiger no longer crashes.
# This change will break assertions for all other testing so these tests must be run on their own.
if(HAVE_UNITTEST_ASSERTS)
    set(unittests tests/test_assertions.cpp)
else()
    set(unittests
        tests/test_bounds_restore.cpp
        tests/test_crc32.cpp
        tests/test_extent_list.cpp
        tests/test_fnv.cpp
        tests/test_pow.cpp
        tests/test_reconciliation_tracking.cpp
        tests/test_tailq.cpp
        tests/block/test_bitstring.cpp
        tests/block/test_block_ckpt.cpp
        tests/cursors/test_cursor_get_raw_key_value.cpp
        tests/packing/test_intpack.cpp
    )
endif()

set(unittest_sources
    tests/main.cpp
    tests/utils.cpp
    tests/wrappers/block_mods.cpp
    tests/wrappers/connection_wrapper.cpp
    tests/wrappers/item_wrapper.cpp
    tests/wrappers/mock_connection.cpp
    tests/wrappers/mock_session.cpp
    ${unittests}
)

# Disable 4200 warning on windows, its related to struct definitions with zero sized arrays.
if(WT_WIN)
    set(UNITTEST_FLAGS /wd4200)
else()
    set(UNITTEST_FLAGS -Wno-missing-declarations)
endif()

create_test_executable(unittests
    SOURCES ${unittest_sources}
    FLAGS ${UNITTEST_FLAGS}
    CXX
)

target_compile_options(
    unittests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(unittests Catch2::Catch2)

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
