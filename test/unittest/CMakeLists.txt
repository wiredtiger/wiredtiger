project(unittest CXX)

set(unittest_sources
    tests/main.cpp
    tests/test_crc32.cpp
    tests/test_extent_list.cpp
    tests/test_fnv.cpp
    tests/test_pow.cpp
    tests/test_reconciliation_tracking.cpp
    tests/test_tailq.cpp
    tests/utils.cpp
    tests/block/test_bitstring.cpp
    tests/block/test_block_ckpt.cpp
    tests/packing/test_intpack.cpp
    tests/wrappers/block_mods.cpp
    tests/wrappers/connection_wrapper.cpp
    tests/wrappers/mock_connection.cpp
    tests/wrappers/mock_session.cpp
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
