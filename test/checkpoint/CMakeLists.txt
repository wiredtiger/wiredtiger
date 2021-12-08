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
        "test_checkpoint_4_mixed;-t m -T 4"
        "test_checkpoint_8_mixed;-t m -T 8"
        "test_checkpoint_4_mixed_sweep;-t m -T 4 -W 3 -r 2 -n 100000 -k 100000 -s 1"
        "test_checkpoint_4_mixed_timestamps;-t m -T 4 -W 3 -r 2 -x -n 100000 -k 100000"

        # 2. FLCS cases.
        "test_checkpoint_6_fixed;-t f -T 6"
        "test_checkpoint_6_fixed_named;-t f -T 6 -c TeSt"
        "test_checkpoint_6_fixed_prepare;-t f -T 6 -p"
        "test_checkpoint_6_fixed_named_prepare;-t f -T 6 -c TeSt -p"
        "test_checkpoint_fixed_stress_sweep_timestamps;-t f -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB -D"
        "test_checkpoint_fixed_sweep_timestamps;-t f -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"

        # 3. VLCS cases.
        "test_checkpoint_6_column;-t c -T 6"
        "test_checkpoint_6_column_named;-t c -T 6 -c TeSt"
        "test_checkpoint_6_column_prepare;-t c -T 6 -p"
        "test_checkpoint_6_column_named_prepare;-t c -T 6 -c TeSt -p"
        "test_checkpoint_column_stress_sweep_timestamps;-t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB -D"
        "test_checkpoint_column_sweep_timestamps;-t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"

        # 4. Row-store cases.
        "test_checkpoint_6_row;-t r -T 6"
        "test_checkpoint_6_row_named;-t r -T 6 -c TeSt"
        "test_checkpoint_6_row_prepare;-t r -T 6 -p"
        "test_checkpoint_6_row_named_prepare;-t r -T 6 -c TeSt -p"
        "test_checkpoint_row_stress_sweep_timestamps;-t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB -D"
        "test_checkpoint_row_sweep_timestamps;-t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"

        # 5. LSM cases.
        "test_checkpoint_6_lsm;-t l -T 6"
    LABELS
        check
        test_checkpoint
)
