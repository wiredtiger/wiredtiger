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

rundir=$dir/RUNDIR
foreign=$dir/FOREIGN

mo=$dir/metadata.orig
mi=$dir/metadata.import
co=$dir/ckpt.orig
ci=$dir/ckpt.import

EXT="extensions=[\
$top_builddir/ext/encryptors/rotn/.libs/libwiredtiger_rotn.so,\
$top_builddir/ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so]"

wt="$top_builddir/wt"

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
	    threads=4 \
	    timer=2 \
	    verify=1
}

import()
{
	# Update the extensions if the run included encryption.
	egrep 'encryption=none' $rundir/CONFIG > /dev/null ||
	    EXT="encryption=(name=rotn,keyid=7),$EXT"

	# Dump the original metadata.
	echo; echo 'dumping the original metadata'
	$wt -C "$EXT" -h $rundir list -cv file:wt
	$wt -C "$EXT" -h $rundir list -v file:wt | sed 1d > $mo

	# Create a stub datbase and copy in the table.
	rm -rf $foreign && mkdir $foreign || exit 1
	$wt -C "$EXT" -h $foreign create file:xxx || exit 1
	cp $rundir/wt $foreign/yyy || exit 1

	# Import the table.
	$wt -C "$EXT" -h $foreign import file:yyy


	# Dump the imported metadata.
	echo; echo 'dumping the imported metadata'
	$wt -C "$EXT" -h $foreign list -cv file:yyy
	$wt -C "$EXT" -h $foreign list -v file:yyy | sed 1d > $mi
}

compare_checkpoints()
{
	echo 'comparing the original and imported checkpoints'
	sed -e 's/.*\(checkpoint=.*))\).*/\1/' < $mo > $co
	sed -e 's/.*\(checkpoint=.*))\).*/\1/' < $mi > $ci
	echo; echo 'original checkpoint'
	cat $co
	echo; echo 'imported checkpoint'
	cat $ci
	cmp $co $ci
}

verify()
{
	echo; echo 'verifying the imported file'
	$wt -C "$EXT" -h $foreign verify file:yyy || exit 1
}

# The checkpoints will differ in some ways, for example, the import clears the
# durable timestamp/transaction information in the checkpoint. If verify fails,
# you can repeatedly run the import and verify process using the -c option for
# debugging.
compare=0
while :
	do case "$1" in
	-c)
		compare=1
		shift;;
	*)
		break;;
	esac
done

if test $compare -eq 0; then
	rm -rf $dir && mkdir $dir
	format
fi
import
if test $compare -eq 1; then
	compare_checkpoints
fi
verify
exit 0
