project(wt2853_perf C)

# Skip compiling wt2853_perf on non-Posix systems. Currently wt2853_perf isn't supported
# for our Windows builds.
if (NOT WT_POSIX)
    return()
endif()

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

create_test_executable(test_wt2853_perf
    SOURCES
        main.c
)
