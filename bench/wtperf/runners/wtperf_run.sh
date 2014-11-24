#!/bin/sh

# wtperf_run.sh - run wtperf regression tests on the Jenkins platform.
#
# The Jenkins machines show variability so we run this script to run
# each wtperf test several times.  We throw away the min and max
# number and average the remaining values.  That is the number we
# give to Jenkins for plotting.  We write these values to a
# test.average file in the current directory (which is 
# build_posix/bench/wtperf).
#
# This script should be invoked with the pathname of the wtperf test
# config to run.
#
if test "$#" -ne "1"; then
	echo "Must specify wtperf test to run"
	exit 1
fi
wttest=$1
home=./WT_TEST
outfile=./wtperf.out
rm -f $outfile

results=(0 0 0)
# Load needs floating point and bc, handle separately.
loadindex=4
results[$loadindex]=0
ops=(read insert update)
outp=("Read count:" "Insert count:" "Update count:")
outp[$loadindex]="Load time:"

rm -rf $home
mkdir $home
LD_PRELOAD=/usr/lib64/libjemalloc.so.1 LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib ./wtperf -O $wttest
if test "$?" -ne "0"; then
	exit 1
fi
# Load is always using floating point, so handle separately
l=`grep "^Load time:" ./WT_TEST/test.stat`
if test "$?" -eq "0"; then
	load=`echo $l | cut -d ' ' -f 3`
else
	load=0
fi
results[$loadindex]=$load
echo "cur ${results[$loadindex]}" >> $outfile
for i in ${!ops[*]}; do
	l=`grep "Executed.*${ops[$i]} operations" ./WT_TEST/test.stat`
	if test "$?" -eq "0"; then
		n=`echo $l | cut -d ' ' -f 2`
	else
		n=0
	fi
	results[$i]=$n
done

for i in ${!outp[*]}; do
	echo "${outp[$i]} ${results[$i]}" >> $outfile
done
exit 0
