#!/usr/bin/env bash

set -x

usage () {
    cat << EOF
Usage: recovery_test.sh {config} {loop_number_of_times}
Arguments:
    config    				# Configuration to the checkpoint binary
    loop_number_of_times       # Number of times to run the test in a loop
EOF
}

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters."
    usage
    echo FAILED
    exit 1
fi

#home=${1:-WT_TEST}
home="WT_TEST"
backup=$home.backup
recovery=$home.recovery
config=$1
loop_times=$2
#./t -t r -W 3 -D -X -n 100000 -k 100000 -C cache_size=100MB -h $home > $home.out 2>&1 &
#./t -t r -W 3 -D -n 100000 -k 100000 -C cache_size=100MB -h $home > $home.out 2>&1 &
./t $config -h $home > $home.out 2>&1 &
pid=$!

trap "kill -9 $pid" 0 1 2 3 13 15
outf=./outfile.txt
# Wait for the test to start running
while ! grep -q "Finished a checkpoint" $home.out ; do
	sleep 1
done

for i in $(seq ${loop_times}); do
	echo "$d: Iteration $i" >> $outf
	rm -rf $backup $recovery ; mkdir $backup ; mkdir $recovery
	# Make sure all threads are stopped before copying files
	sleep 1
	cp $home/* $backup
	#kill -CONT $pid
	cp $backup/* $recovery
	./t -t r -D -v -h $recovery || exit 1
done

exit 0
