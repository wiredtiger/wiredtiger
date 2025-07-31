project(C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

# Create push_pull test executable.
create_test_executable(bench_dhandle
  SOURCES
      bench_dhandle.c
      bench_timer.c
)

if(WT_POSIX)
    # Need to link math on POSIX systems.
    target_link_libraries(bench_dhandle m)
endif()
