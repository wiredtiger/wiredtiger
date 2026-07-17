project(C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

if(WT_POSIX)
    create_test_executable(bench_checkpoint_cleanup
        SOURCES
            bench_checkpoint_cleanup.c
            ../dhandle/bench_timer.c
    )
    target_include_directories(bench_checkpoint_cleanup PRIVATE ../dhandle)
endif()
