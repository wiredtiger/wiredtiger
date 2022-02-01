project(s3_store_tests CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

if(IMPORT_S3_SDK_PACKAGE AND ENABLE_S3)
    set(AWS_LIBS ${AWSSDK_LINK_LIBRARIES})
elseif(IMPORT_S3_SDK_EXTERNAL AND ENABLE_S3)
    set(AWS_LIBS aws-sdk::s3-crt aws-sdk::core)
endif()

# Build the unit test binary.
create_test_executable(run_s3_unit_tests 
    SOURCES test_aws_bucket_conn.cxx
    LIBS ${AWS_LIBS} aws_bucket_util
    CXX
)
# Register the test with CTest.
add_test(NAME s3_unit_tests COMMAND run_s3_unit_tests)
# Label the tests so they can be run explicitly.
set_tests_properties(s3_unit_tests PROPERTIES LABELS "s3_store")
