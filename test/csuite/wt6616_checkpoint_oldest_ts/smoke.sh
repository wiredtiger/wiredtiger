#! /bin/sh

set -e

# If $top_builddir/$top_srcdir aren't set, default to building in build_posix
# and running in test/csuite.
top_builddir=${top_builddir:-../../build_posix}
top_srcdir=${top_srcdir:-../..}

$TEST_WRAPPER $top_builddir/test/csuite/test_wt6616_checkpoint_oldest_ts
$TEST_WRAPPER $top_builddir/test/csuite/test_wt6616_checkpoint_oldest_ts -l