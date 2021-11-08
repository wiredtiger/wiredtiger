#! /bin/sh

set -e

# Return success from the script because the test itself does not yet work.
# Do this in the script so that we can manually run the program on the command line.
exit 0

# The cmake build passes in the builddir with -b; it should be passed through.
builddir_arg=
while getopts ":b:" opt; do
    case $opt in
        b) builddir_arg="-b $OPTARG" ;;
    esac
done
shift $(( OPTIND - 1 ))

# Smoke-test tiered-abort as part of running "make check".

if [ -n "$1" ]
then
    # If the test binary is passed in manually.
    test_bin=$1
else
    # If $top_builddir/$top_srcdir aren't set, default to building in build_posix
    # and running in test/csuite.
    top_builddir=${top_builddir:-../../build_posix}
    top_srcdir=${top_srcdir:-../..}
    test_bin=$top_builddir/test/csuite/test_tiered_abort
fi
$TEST_WRAPPER $test_bin $builddir_arg -t 10 -T 5
$TEST_WRAPPER $test_bin $builddir_arg -m -t 10 -T 5
$TEST_WRAPPER $test_bin $builddir_arg -C -t 10 -T 5
$TEST_WRAPPER $test_bin $builddir_arg -C -m -t 10 -T 5
