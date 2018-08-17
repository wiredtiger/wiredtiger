#! /bin/sh

set -e

# Smoke-test random-abort as part of running "make check".

RUN_OS=$(uname -s)
if [ "$RUN_OS" = "Darwin" ]; then
    echo "Skipping test_random_abort, it is not reliable on OS/X" >&2
    exit 0
fi
$TEST_WRAPPER ./test_random_abort -t 10 -T 5
$TEST_WRAPPER ./test_random_abort -m -t 10 -T 5
$TEST_WRAPPER ./test_random_abort -C -t 10 -T 5
$TEST_WRAPPER ./test_random_abort -C -m -t 10 -T 5
