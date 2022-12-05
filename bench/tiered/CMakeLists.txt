project(tiered C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Create push_pull test executable.
create_test_executable(test_push_pull
    SOURCES push_pull.c
)

if(WT_POSIX)
    # Need to link math on POSIX systems.
    target_link_libraries(test_push_pull m)
endif()
