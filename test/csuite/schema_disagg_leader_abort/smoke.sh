#! /bin/sh

set -e

# Smoke-test schema-disagg-leader-abort as part of running "make check".

if [ -n "$1" ]
then
    test_bin=$1
else
    binary_dir=${binary_dir:-`dirname $0`}
    test_bin=$binary_dir/test_schema_disagg_leader_abort
fi

$TEST_WRAPPER "$test_bin" -t 10 -T 2 -h WT_TEST.schema_disagg_leader_abort.t2
$TEST_WRAPPER "$test_bin" -t 10 -T 4 -h WT_TEST.schema_disagg_leader_abort.t4
