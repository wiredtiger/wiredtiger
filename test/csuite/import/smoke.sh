#! /bin/sh

set -e

# Bypass this test for slow machines, valgrind
test "$TESTUTIL_SLOW_MACHINE" = "1" && exit 0
test "$TESTUTIL_BYPASS_VALGRIND" = "1" && exit 0

# If $top_builddir/$top_srcdir aren't set, default to building in build_posix
# and running in test/csuite.
top_builddir=${top_builddir:-../../build_posix}
top_srcdir=${top_srcdir:-../..}

dir=WT_TEST.import
rm -rf $dir && mkdir $dir

rundir=$dir/RUNDIR
foreign=$dir/FOREIGN

EXT="extensions=[\
$top_builddir/ext/encryptors/rotn/.libs/libwiredtiger_rotn.so,\
$top_builddir/ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so]"

# Run test/format to create an object.
format()
{
	rm -rf $rundir || exit 1

	$top_builddir/test/format/t \
	    -1q \
	    -C "$EXT" \
	    -c $top_srcdir/test/format/CONFIG.stress \
	    -h $rundir \
	    backups=0 \
	    checkpoints=1 \
	    data_source=file \
	    ops=0 \
	    rebalance=0 \
	    salvage=0 \
	    timer=2 \
	    verify=1 || exit 1
}

verify()
{
	# Import and verify the object.
	egrep 'encryption=none' $rundir/CONFIG > /dev/null ||
	    EXT="encryption=(name=rotn,keyid=7),$EXT"
	wt="$top_builddir/wt"

	rm -rf $foreign && mkdir $foreign || exit 1
	$wt -C "$EXT" -h $foreign create file:xxx || exit 1
	mv $rundir/wt $foreign/yyy || exit 1
	$wt -C "$EXT" -h $foreign import file:yyy || exit 1
}

format
verify
exit 0
