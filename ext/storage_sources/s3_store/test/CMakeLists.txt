project(s3_unit_test CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Build the unit test binary.
create_test_executable(run_s3_unit_tests 
    SOURCES test_s3_connection.cpp
    LIBS aws-sdk::s3-crt aws-sdk::core aws_bucket_util
    CXX
)

target_compile_options(
    run_s3_unit_tests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(run_s3_unit_tests Catch2::Catch2)
if(ENABLE_TCMALLOC)
    target_link_libraries(run_s3_unit_tests wt::tcmalloc)
endif()

# Register the test with CTest.
add_test(NAME s3_unit_tests COMMAND run_s3_unit_tests)
# Label the tests so they can be run explicitly.
set_tests_properties(s3_unit_tests PROPERTIES LABELS "s3_store")
