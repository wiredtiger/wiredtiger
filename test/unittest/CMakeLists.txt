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

create_test_executable(unittests
    SOURCES ${unittest_sources}
    FLAGS -Wno-missing-declarations
    CXX
)

set_target_properties(unittests PROPERTIES CXX_STANDARD 17)
target_compile_options(
    unittests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(unittests Catch2::Catch2)
if(ENABLE_TCMALLOC)
    target_link_libraries(unittests wt::tcmalloc)
endif()

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
