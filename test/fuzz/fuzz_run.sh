#!/bin/bash

# fuzz_run.sh - run a fuzz test
#
# Usage
# fuzz_run.sh <fuzz-test-binary> ...

if test "$#" -lt "1"; then
    echo "$0: must specify fuzz test to run"
    exit 1
fi

fuzz_test_bin="$1"

# Remove anything from previous runs.
rm -rf WT_TEST_*/ &> /dev/null
rm *.profraw &> /dev/null

# If we've compiled to emit coverage information, each worker process should write their own
# performance data.
export LLVM_PROFILE_FILE="WT_TEST_%p.profraw"

# The rationale for each flag is below:
#   - Choosing 8 workers is a reasonable starting point. Depending on their machine, they can bump
#     this number up but most machines will be able to handle this and it finishes jobs much faster
#     than without this flag (equivalent to jobs=1).
#   - Do 1,000,000 runs to make sure that we're stressing the system and hitting lots of branches.
#     Ideally, we'd just let the fuzzer run until the process is killed by the user but
#     unfortunately, coverage data won't get written out in that case.
./fuzz_test_bin -jobs=8 -runs=1000000
