project(s3_store_tests CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Build the unit test binary.
create_test_executable(run_s3_unit_tests 
    SOURCES TestS3Connection.cpp
    LIBS aws-sdk::s3-crt aws-sdk::core aws_bucket_util
    CXX
)
# Register the test with CTest.
add_test(NAME s3_unit_tests COMMAND run_s3_unit_tests)
# Label the tests so they can be run explicitly.
set_tests_properties(s3_unit_tests PROPERTIES LABELS "s3_store")
