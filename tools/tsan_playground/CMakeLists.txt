cmake_minimum_required(VERSION 3.12)

set(CMAKE_C_STANDARD 11)

# C11 variants
create_test_executable(tsan_playground_c11_atomics
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_C11_ATOMICS=1"
)

create_test_executable(tsan_playground_c11_acq_rel_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_C11_ACQ_REL_BARRIERS=1"
)

create_test_executable(tsan_playground_c11_full_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_C11_FULL_BARRIERS=1"
)

# GCC variants
create_test_executable(tsan_playground_gcc_atomics
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_GCC_ATOMICS=1"
)

create_test_executable(tsan_playground_gcc_acq_rel_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_GCC_ACQ_REL_BARRIERS=1"
)

create_test_executable(tsan_playground_gcc_full_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_GCC_FULL_BARRIERS=1"
)

# WT variants
create_test_executable(tsan_playground_wt_atomics
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_WT_ATOMICS=1"
)

create_test_executable(tsan_playground_wt_acq_rel_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_WT_ACQ_REL_BARRIERS=1"
)

create_test_executable(tsan_playground_wt_store_load_with_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_WT_STORE_LOAD_WITH_BARRIERS=1"
)

create_test_executable(tsan_playground_wt_full_barriers
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_WT_FULL_BARRIERS=1"
)

# Dummy variant (created to check that TSAN fails without synchronization)
create_test_executable(tsan_playground_dummy
    SOURCES
        tsan_playground.c
    FLAGS
        "-D_DUMMY_ATOMICS=1"
)

