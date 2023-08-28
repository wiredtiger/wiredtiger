#!/usr/bin/env bash

export WIREDTIGER_CONFIG='checkpoint_sync=0,transaction_sync=(method=none)'
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libeatmydata.so

#CMD='./t -q -c CONFIG.MIRROR -h $dir'
#CMD='./t -q -c CONFIG.MIRROR -h $dir -T bulk,txn,mirror_fail,retain=100'

CMD='./t -q -c CONFIG.MIRROR -h $dir -T bulk,txn,mirror_fail,retain=100'
TCMD='../../wt -h $dir/OPS.TRACE printlog -mux'

metaout=nohup.iter
rm -f $metaout
count=1
mydir=`pwd`
while true ; do
	d=`date`
	core=`ls core* | wc -l`
	echo "$d: Iteration $count ($mydir)" >> $metaout
	echo "$d: Iteration $count ($mydir)"
	`rm -rf nohup.out* WT_TEST* fulltrace*`
	for((t=0; t<5; t++)); do
		dir=WT_TEST.$t
		> nohup.out.$t ; eval nohup $CMD > nohup.out.$t &
	done
	wait
	#for((t=0; t<5; t++)); do
	#	dir=WT_TEST.$t
	#	> fulltrace.$t ; eval nohup $TCMD > fulltrace.$t
	#	err=`grep CURHS fulltrace*`
	#	if test "$?" -eq "0"; then
	#		echo "WARN: $err"
	#		echo "WARN: $err" >> $metaout
	#		exit 1
	#	fi
	#	rm fulltrace.$t
	#done
	#wait
	#for((t=0; t<5; t++)); do
	#	mv fulltrace.$t lasttrace.$t
	#done
	#err=`grep ERROR nohup*`
	#if test "$?" -eq "0"; then
	#	echo "ERROR: $err"
	#	echo "ERROR: $err" >> $metaout
	#	exit 1
	#fi
	err=`grep FAILED nohup*`
	if test "$?" -eq "0"; then
		echo "FAILED: $err"
		echo "FAILED: $err" >> $metaout
		exit 1
	fi
	err=`grep mismatch nohup*`
	if test "$?" -eq "0"; then
		echo "Mismatch: $err"
		echo "Mismatch: $err" >> $metaout
		exit 1
	fi
	err=`grep -i abort nohup*`
	if test "$?" -eq "0"; then
		echo "ERROR abort: $err"
		echo "ERROR abort: $err" >> $metaout
		exit 1
	fi
	success=`grep "successful run" nohup.out* | wc -l`
	if test "$success" -ne "5"; then
		echo "ERROR not all runs successful"
		exit 1
	fi
	core2=`ls core* | wc -l`
	if test "$core" -ne "$core2"; then
		echo "ERROR new core file generated"
		exit 1
	fi
	count=`expr $count + 1`
done
