project(C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

if(WT_POSIX)
  # Create the dhandle benchmark

  create_test_executable(bench_dhandle
    SOURCES
    bench_dhandle.c
    bench_timer.c
    )

  # Create the data handle and schema lock benchmark; it uses only the public API.
  create_test_executable(bench_dhandle_lock
    SOURCES
    bench_dhandle_lock.c
    )
endif()
