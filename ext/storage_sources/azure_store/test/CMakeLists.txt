project(azure_unit_test CXX)
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Build the unit test binary.
create_test_executable(run_azure_unit_tests
    SOURCES test_azure_connection.cpp
    LIBS azure_container_util
    CXX
)

target_compile_options(
    run_azure_unit_tests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(run_azure_unit_tests Catch2::Catch2)

# Register the test with CTest.
add_test(NAME azure_unit_tests COMMAND run_azure_unit_tests)
# Label the tests so they can be run explicitly.
set_tests_properties(azure_unit_tests PROPERTIES LABELS "azure_store")
