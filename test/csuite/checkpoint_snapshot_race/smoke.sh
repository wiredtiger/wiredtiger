#! /bin/sh

set -e

# Smoke-test checkpoint snapshot race test program as part of running "make check".

if [ -n "$1" ]
then
    # If the test binary is passed in manually.
    test_bin=$1
else
    # If $binary_dir isn't set, default to using the build directory
    # this script resides under. Our CMake build will sync a copy of this
    # script to the build directory. Note this assumes we are executing a
    # copy of the script that lives under the build directory. Otherwise
    # passing the binary path is required.
    binary_dir=${binary_dir:-`dirname $0`}
    test_bin=$binary_dir/test_checkpoint_snapshot_race
fi
# $TEST_WRAPPER $test_bin -r 10

run_set_of_tests () {
    iteration=$1
    num_parallel=$2
    run_time=$3
    #echo "Running: it: $iteration, para: $num_parallel, time: $run_time"
    pids=""
    RESULT=0
    for i in `seq $num_parallel`
    do
        $TEST_WRAPPER $test_bin -r $run_time -h WT_TEST.$iteration.$i &
        pids="$pids $!"
    done

    for pid in $pids; do
        wait $pid || let "RESULT=1"
    done

    if [ "$RESULT" = "1" ]; then
        echo "AARGH"
        exit 1
    fi
}

# Run the program for an increasingly long time period
for i in `seq 100`
do
    runtime=`echo "$i+1" | bc`
    run_set_of_tests $i 20 $runtime
done


