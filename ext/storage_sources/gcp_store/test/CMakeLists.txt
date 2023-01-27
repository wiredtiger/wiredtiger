project(gcp_unit_test CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Build the unit test binary.
create_test_executable(run_gcp_unit_tests 
    SOURCES test_gcp_connection.cpp
    LIBS gcp_bucket_util
    CXX
)

target_compile_options(
    run_gcp_unit_tests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(run_gcp_unit_tests Catch2::Catch2)

# Register the test with CTest.
add_test(NAME gcp_unit_tests COMMAND run_gcp_unit_tests)
# Label the tests so they can be run explicitly.
set_tests_properties(gcp_unit_tests PROPERTIES LABELS "gcp_store")
