#! /bin/sh

set -e

# Bypass this test for slow machines, valgrind
test "$TESTUTIL_SLOW_MACHINE" = "1" && exit 0
test "$TESTUTIL_BYPASS_VALGRIND" = "1" && exit 0

# Run test/format to create an object.
format()
{
	rm -rf RUNDIR || exit 1

	$top_builddir/test/format/t -1q -c $top_srcdir/test/format/CONFIG.stress \
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
	EXT="extensions=[\
	$top_builddir/ext/encryptors/rotn/.libs/libwiredtiger_rotn.so,\
	$top_builddir/ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so]"
	egrep 'encryption=none' RUNDIR/CONFIG > /dev/null ||
	    EXT="encryption=(name=rotn,keyid=7),$EXT"
	wt="$top_builddir/wt"

	rm -rf FOREIGN && mkdir FOREIGN || exit 1
	$wt -C "$EXT" -h FOREIGN create file:xxx || exit 1
	$wt -C "$EXT" -h FOREIGN import file:yyy $PWD/RUNDIR/wt || exit 1
}

format
verify
exit 0
