# Skip compiling wt2853_perf on non-Posix systems. Currently wt2853_perf isn't supported
# for our Windows builds.
if (NOT WT_POSIX)
    return()
endif()

create_test_executable(test_wt2853_perf
    SOURCES
        main.c
)
