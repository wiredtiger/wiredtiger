project(s3_store_tests CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)
file(GLOB test_src "*.cpp")

# Build all tests within the /test subdirectory.
foreach(file ${test_src})
    # Extract the test's filename without the .cpp extension.
    get_filename_component(TEST_NAME_WE ${file} NAME)
    string(REPLACE ".cpp" "" TEST_NAME ${TEST_NAME_WE})

    # Generate a test executable for the unit test.
    set(TEST_EXE run_${TEST_NAME})
    create_test_executable(${TEST_EXE} 
    SOURCES ${file}
    LIBS aws-sdk::s3-crt aws-sdk::core aws_bucket_util
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${AWSSDK_INCLUDE_DIRS}
    CXX)
    # Register the test with CTest.
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_EXE})

endforeach()
