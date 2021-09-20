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
        "test_checkpoint_3_mixed;-T 3 -t m"
        "test_checkpoint_6_column;-T 6 -t c"
        "test_checkpoint_6_column_named;-c TeSt -T 6 -t c"
        "test_checkpoint_6_column_prepare;-T 6 -t c -p"
        "test_checkpoint_6_column_named_prepare;-c TeSt -T 6 -t c -p"
        "test_checkpoint_column_stress_sweep_timestamps;-t c -W 3 -r 2 -D -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"
        "test_checkpoint_column_sweep_timestamps;-t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"
        "test_checkpoint_6_lsm;-T 6 -t l"
        "test_checkpoint_6_mixed;-T 6 -t m"
        "test_checkpoint_6_row;-T 6 -t r"
        "test_checkpoint_6_row_named;-c TeSt -T 6 -t r"
        "test_checkpoint_6_row_prepare;-T 6 -t r -p"
        "test_checkpoint_6_row_named_prepare;-c TeSt -T 6 -t r -p"
        "test_checkpoint_row_stress_sweep_timestamps;-t r -W 3 -r 2 -D -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"
        "test_checkpoint_row_sweep_timestamps;-t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB"
        "test_checkpoint_3_mixed_sweep;-T 3 -t m -W 3 -r 2 -s 1 -n 100000 -k 100000"
        "test_checkpoint_3_mixed_timestamps;-T 3 -t m -W 3 -r 2 -x -n 100000 -k 100000"
    LABELS
        check
        test_checkpoint
)
