#! /bin/sh

set -e

# Smoke-test schema-disagg-abort as part of running "make check".

if [ -n "$1" ]
then
    test_bin=$1
else
    binary_dir=${binary_dir:-`dirname $0`}
    test_bin=$binary_dir/test_schema_disagg_abort
fi

# Resolve the build directory (two levels up from the test binary).
build_dir=$(cd "$(dirname "$test_bin")/../../../" && pwd)

$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -h WT_TEST.schema_disagg_abort.t2
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 4 -h WT_TEST.schema_disagg_abort.t4
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -s 4 -h WT_TEST.schema_disagg_abort.s4
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -s 16 -h WT_TEST.schema_disagg_abort.s16

# Multi-node: a follower tracks the leader via checkpoint pickup, then one or both nodes die.
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -k l -h WT_TEST.schema_disagg_abort.kl
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -k f -h WT_TEST.schema_disagg_abort.kf
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 10 -T 2 -k b -h WT_TEST.schema_disagg_abort.kb

# Switch mode: the crash always lands in phase 2, shallow or deep depending on the timeout.
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 5  -T 2 -m -h WT_TEST.schema_disagg_abort.switch_short
$TEST_WRAPPER "$test_bin" -b "$build_dir" -t 25 -T 2 -m -h WT_TEST.schema_disagg_abort.switch_long
