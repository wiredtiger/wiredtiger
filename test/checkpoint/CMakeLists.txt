create_test_executable(test_checkpoint
    SOURCES
        checkpointer.c
        workers.c
        test_checkpoint.c
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/recovery-test.sh
)

# Use C++ linker for building this target.
# When building sanitized builds (ASan, UBSan, etc) the C++ linker is required
# to link against the C++ sanitizer runtimes.
# Some extensions (PALite, tiered storage) require C++ runtime to be linked
# for sanitizers to work correctly.
set_target_properties(test_checkpoint PROPERTIES LINKER_LANGUAGE CXX)

# Define test variants for the checkpoint test
define_test_variants(test_checkpoint
    VARIANTS
        # 1. Mixed tables cases. Use four (or eight) tables because there are four table types.
        "4_mixed|-t m -T 4"
        "8_mixed|-t m -T 8"
        "4_mixed_sweep|-t m -T 4 -W 3 -r 2 -n 100000 -k 100000 -s 1"
        "4_mixed_timestamps|-t m -T 4 -W 3 -r 2 -x -n 100000 -k 100000"

        # 1a. Mixed tables with 1/10 the ops and 10x the runs.
        "4_runs_sweep|-t m -T 4 -W 3 -r 20 -n 1000 -s 1"
        "4_runs_timestamps|-t m -T 4 -W 3 -r 20 -x -n 1000"

        # 2. VLCS cases.
        "6_column|-t c -T 6"
        "6_column_named|-t c -T 6 -c TeSt"
        "6_column_prepare|-t c -T 6 -p"
        "6_column_named_prepare|-t c -T 6 -c TeSt -p"
        "column_stress_sweep_timestamps|-t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -D"
        "column_sweep_timestamps|-t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000"

        # 3. Row-store cases.
        "6_row|-t r -T 6"
        "6_row_named|-t r -T 6 -c TeSt"
        "6_row_prepare|-t r -T 6 -p"
        "6_row_named_prepare|-t r -T 6 -c TeSt -p"
        "row_stress_sweep_timestamps|-t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -D"
        "row_sweep_timestamps|-t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000"
    LABELS
        check
        test_checkpoint
)

# Define test variants for the disagg test
define_test_variants(test_checkpoint
    VARIANTS
        "disagg_leader_6_row|-d leader -e -t r -T 6 -x"
        "disagg_leader_6_row_long_running|-d leader -e -t r -W 3 -x -n 1000000 -k 1000000 -C cache_size=4000MB"
        # Prepare operations is not supported on Disagg yet.
        #"disagg_6_row_prepare|-d leader -e -t r -T 6 -p -x"
        "disagg_leader_row_stress_sweep|-d leader -e -t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -D"
        "disagg_leader_row_sweep|-d leader -e -t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000"
        "disagg_leader_row_failpoint_hs_delete_key_from_ts|-d leader -e -t r -W 3 -r 2 -s 2 -x -n 100000 -k 100000"
        "disagg_leader_row_hs_checkpoint_timing_stress|-d leader -e -t r -W 3 -r 2 -s 3 -x -n 100000 -k 100000"
        "disagg_leader_row_checkpoint_slow_timing_stress|-d leader -e -t r -W 3 -r 2 -s 5 -x -n 100000 -k 100000"
        "disagg_leader_row_evict_reposition_timing_stress|-d leader -e -t r -W 3 -r 2 -s 6 -x -n 100000 -k 100000"
        "disagg_leader_row_failpoint_eviction_split|-d leader -e -t r -W 3 -r 2 -s 7 -x -n 100000 -k 100000"
        "disagg_leader_row_failpoint_rec_before_wrapup|-d leader -e -t r -W 3 -r 2 -s 8 -x -n 100000 -k 100000"

        # FIXME-WT-16228: Re-evaluate whether setting large cache size is need after cache stuck issue is solved.
        "disagg_leader_row_server_93028|-d leader -e -t r -W 50 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=1500MB"
    LABELS
        check
        disagg_leader
        test_checkpoint
)

# Define long running test variants for the checkpoint test
# See SERVER-93028
define_test_variants(test_checkpoint
    VARIANTS
        "row_server_93028_1|-t r -W 50 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=250MB"
        "row_server_93028_2|-t r -W 50 -r 2 -s 1 -n 100000 -k 100000 -C cache_size=250MB"
    LABELS
        check
        test_checkpoint
        long_running
)
