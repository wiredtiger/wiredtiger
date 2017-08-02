#! /bin/sh

set -e

# Smoke-test recovery as part of running "make check".

$TEST_WRAPPER ./random-abort -t 10 -T 5
$TEST_WRAPPER ./random-abort -m -t 10 -T 5
$TEST_WRAPPER ./random-abort -C -t 10 -T 5
$TEST_WRAPPER ./random-abort -C -m -t 10 -T 5
$TEST_WRAPPER ./timestamp-abort -t 10 -T 5
$TEST_WRAPPER ./timestamp-abort -m -t 10 -T 5
$TEST_WRAPPER ./timestamp-abort -C -t 10 -T 5
$TEST_WRAPPER ./timestamp-abort -C -m -t 10 -T 5
$TEST_WRAPPER ./truncated-log
