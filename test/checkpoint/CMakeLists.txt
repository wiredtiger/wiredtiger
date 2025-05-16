project(test_checkpoint C)

create_test_executable(test_checkpoint
    SOURCES
        checkpointer.c
        workers.c
        test_checkpoint.c
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/recovery-test.sh
)

define_test_variants(test_checkpoint
    VARIANTS
        # 1. Mixed tables cases. Use four (or eight) tables because there are four table types.
        "test_checkpoint_4_row;-T 4"
        "test_checkpoint_6_row;-T 6"
        "test_checkpoint_8_row;-T 8"
        "test_checkpoint_4_row_sweep;-T 4 -W 3 -r 2 -n 100000 -k 100000 -s 1"
        "test_checkpoint_4_row_timestamps;-T 4 -W 3 -r 2 -x -n 100000 -k 100000"
        "test_checkpoint_6_row_named;-T 6 -c TeSt"
        "test_checkpoint_6_row_prepare;-T 6 -p"
        "test_checkpoint_6_row_named_prepare;-T 6 -c TeSt -p"
        "test_checkpoint_row_stress_sweep_timestamps;-W 3 -r 2 -s 1 -x -n 100000 -k 100000 -D"
        "test_checkpoint_row_sweep_timestamps;-W 3 -r 2 -s 1 -x -n 100000 -k 100000"

        # 1a. Mixed tables with 1/10 the ops and 10x the runs.
        "test_checkpoint_4_runs_sweep;-T 4 -W 3 -r 20 -n 1000 -s 1"
        "test_checkpoint_4_runs_timestamps;-T 4 -W 3 -r 20 -x -n 1000"
    LABELS
        check
        test_checkpoint
)

define_test_variants(test_checkpoint
    VARIANTS
        "test_checkpoint_row_server_93028_1;-W 50 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=250MB"
        "test_checkpoint_row_server_93028_2;-W 50 -r 2 -s 1 -n 100000 -k 100000 -C cache_size=250MB"
    LABELS
        check
        test_checkpoint
        long_running
)
