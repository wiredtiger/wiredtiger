#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

project(test_checkpoint C)

create_test_executable(test_checkpoint
    SOURCES
        checkpointer.c
        workers.c
        test_checkpoint.c
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
