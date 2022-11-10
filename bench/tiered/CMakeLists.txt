project(tiered C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Create intpack test executable.
create_test_executable(test_push_pull
    SOURCES push_pull.c
    #EXECUTABLE_NAME "intpack-test3"
)
