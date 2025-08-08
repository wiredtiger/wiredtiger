project(C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

if(WT_POSIX)
  # Create the dhandle benchmark

  create_test_executable(bench_dhandle
    SOURCES
    bench_dhandle.c
    bench_timer.c
    )
endif()
