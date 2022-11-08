project(wtperf C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

set(wt_perf_flags)
if(ENABLE_SNAPPY)
    list(APPEND wt_perf_flags "-DSNAPPY_PATH=\"snappy/libwiredtiger_snappy.so\"")
endif()
if(ENABLE_LZ4)
    list(APPEND wt_perf_flags "-DLZ4_PATH=\"lz4/libwiredtiger_lz4.so\"")
endif()
if(ENABLE_ZLIB)
    list(APPEND wt_perf_flags "-DZLIB_PATH=\"zlib/libwiredtiger_zlib.so\"")
endif()
if(ENABLE_ZSTD)
    list(APPEND wt_perf_flags "-DZSTD_PATH=\"zstd/libwiredtiger_zstd.so\"")
endif()

create_test_executable(wtperf
    SOURCES
        config.c
        idle_table_cycle.c
        misc.c
        track.c
        wtperf.c
        wtperf_throttle.c
        wtperf_truncate.c
    FLAGS
        ${wt_perf_flags}
)

if(WT_POSIX)
    # Need to link math on POSIX systems.
    target_link_libraries(wtperf m)
endif()

# Smoke-test wtperf as part of running "ctest check".
define_test_variants(wtperf
    VARIANTS
        "test_wtperf_small_lsm;-O \"${CMAKE_CURRENT_SOURCE_DIR}/runners/small-lsm.wtperf\" -o \"run_time=20\""
    LABELS
        check
        wtperf
)
