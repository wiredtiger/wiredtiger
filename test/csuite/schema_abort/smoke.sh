#! /bin/sh

set -e

# Smoke-test schema-abort as part of running "make check".

$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -t 10 -T 5
$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -m -t 10 -T 5
$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -C -t 10 -T 5
$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -C -m -t 10 -T 5
$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -m -t 10 -T 5 -z
$TEST_WRAPPER $top_builddir/test/csuite/test_schema_abort -m -t 10 -T 5 -x
