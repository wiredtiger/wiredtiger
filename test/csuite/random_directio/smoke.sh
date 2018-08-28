#! /bin/sh

set -e

# Smoke-test random-abort as part of running "make check".

echo TEST_WRAPPER = $TEST_WRAPPER
$TEST_WRAPPER ./test_random_directio  -t 5 -T 5
