project(tiered C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Create push_pull test executable.
create_test_executable(test_push_pull
    SOURCES push_pull.c
)
