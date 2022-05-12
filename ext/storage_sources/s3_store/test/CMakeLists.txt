project(s3_unit_test CXX)    
include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

Include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.13.8
)

FetchContent_Populate(Catch2)
add_subdirectory(${catch2_SOURCE_DIR} ${catch2_BINARY_DIR})

# Build the unit test binary.
create_test_executable(run_s3_unit_tests 
    SOURCES test_unit.cpp
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

# # Register the test with CTest.
# add_test(NAME s3_unit_test COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run_s3_unit_tests)
# # Label the tests so they can be run explicitly.
# set_tests_properties(s3_unit_test PROPERTIES LABELS "s3_store")