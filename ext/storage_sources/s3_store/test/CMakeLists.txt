project(s3_store_tests CXX)    

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Generate a test executable for the unit test.
create_test_executable(unitTest 
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_AwsBucketConn.cpp
    LIBS aws-sdk::s3 aws-sdk::s3-crt aws-sdk::core aws_bucket_util
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${AWSSDK_INCLUDE_DIRS}
    CXX)

# Register the test with cmake.
add_test(NAME listBucketTest COMMAND unitTest)