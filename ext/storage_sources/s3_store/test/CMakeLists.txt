project(s3_store_tests CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Build the unit test binary.
set(TEST_NAME s3_unit_tests)
set(TEST_EXE run_s3_unit_tests)
create_test_executable(${TEST_EXE} 
    SOURCES test_aws_bucket_conn.cxx
    LIBS aws-sdk::s3-crt aws-sdk::core aws_bucket_util
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${AWSSDK_INCLUDE_DIRS}
    CXX
)
# Register the test with CTest.
add_test(NAME ${TEST_NAME} COMMAND ${TEST_EXE})
# Label the tests so they can be run explicitly.
set_tests_properties(${TEST_NAME} PROPERTIES LABELS "s3_store")
