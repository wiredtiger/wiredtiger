project(api_perf C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

create_test_executable(api_perf
    SOURCES
        api_perf.c
)

if(WT_POSIX)
    target_link_libraries(api_perf m)
endif()
