#!/bin/bash

# fuzz_run.sh - run a fuzz test
#
# Usage
# fuzz_run.sh <fuzz-test-binary> ...

if test "$#" -lt "1"; then
	echo "$0: must specify fuzz test to run"
	exit 1
fi

# Take the binary name and shift.
# We don't want to forward this as an argument.
fuzz_test_bin="$1"
shift

# Remove anything from previous runs.
rm -rf WT_TEST_* &> /dev/null
rm *.profraw fuzz-*.log &> /dev/null

# If we've compiled to emit coverage information, each worker process should write their own
# performance data.
export LLVM_PROFILE_FILE="WT_TEST_%p.profraw"

# The rationale for each flag is below:
#   - Choosing 8 workers is a reasonable starting point. Depending on their machine, they can bump
#     this number up but most machines will be able to handle this and it finishes jobs much faster
#     than without this flag (equivalent to jobs=1).
#   - Do 1 billion runs to make sure that we're stressing the system and hitting lots of branches.
#     Ideally, we'd just let the fuzzer run until the process is killed by the user but
#     unfortunately, coverage data won't get written out in that case.
#   - Supress stdout and stderr. This isn't ideal but any fuzzing target that prints an error will
#     quickly fill up your disk. Better to just replay the input without this flag if you uncover a
#     bug.
$fuzz_test_bin -jobs=8 -runs=100000000 -close_fd_mask=3 "$@"
