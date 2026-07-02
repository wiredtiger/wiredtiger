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

$TEST_WRAPPER "$test_bin" -t 10 -T 2 -k l -h WT_TEST.schema_disagg_abort.kl
$TEST_WRAPPER "$test_bin" -t 10 -T 2 -k f -h WT_TEST.schema_disagg_abort.kf
$TEST_WRAPPER "$test_bin" -t 10 -T 2 -k b -h WT_TEST.schema_disagg_abort.kb
$TEST_WRAPPER "$test_bin" -t 10 -T 4 -k l -h WT_TEST.schema_disagg_abort.kl4
$TEST_WRAPPER "$test_bin" -t 10 -T 2 -w -h WT_TEST.schema_disagg_abort.w
