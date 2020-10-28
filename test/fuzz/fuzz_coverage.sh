#!/bin/bash

# fuzz_coverage.sh - generate coverage information after running a fuzz test.
#
# This script assumes it is running in the directory that the fuzz test was executed in and requires
# that WiredTiger was compiled with "-fprofile-instr-generate" and "-fcoverage-mapping".
#
# Usage
# fuzz_coverage.sh <fuzz-test-binary>

if test "$#" -lt "1"; then
	echo "$0: must specify fuzz test to generate coverage for"
	exit 1
fi

fuzz_test_bin="$1"

if test -z "$PROFDATA_BINARY"; then
	profdata_bin="llvm-profdata"
	echo "$0: PROFDATA_BINARY is unset, defaulting to $profdata_bin"
else
	profdata_bin="$PROFDATA_BINARY"
fi

if test -z "$COV_BINARY"; then
	cov_bin="llvm-cov"
	echo "$0: COV_BINARY is unset, defaulting to $cov_bin"
else
	cov_bin="$COV_BINARY"
fi

fuzz_cov_name="${fuzz_test_bin}_cov"

$profdata_bin merge -sparse *.profraw -o default.profdata || exit 1
$cov_bin show $fuzz_test_bin -instr-profile=default.profdata > "${fuzz_cov_name}.txt"
$cov_bin show $fuzz_test_bin -instr-profile=default.profdata -format=html > "${fuzz_cov_name}.html"
