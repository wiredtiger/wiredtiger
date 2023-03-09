#!/bin/bash

if [ ! -e build_cmake ] ; then
    echo "Build should be run from root directory"
    exit 1
fi

RUN_TIMESTAMP=`date +%y%m%d_%H%M%S`
RESULT_DIR=readahead_results_$RUN_TIMESTAMP
BENCH_RUN_DIR=../bench/wtperf/runners/readahead/

cd build_cmake
mkdir $RESULT_DIR

cp $BENCH_RUN_DIR/readahead*wtperf $RESULT_DIR

echo "Creating the populated database"
./bench/wtperf/wtperf -O $BENCH_RUN_DIR/readahead-pop.wtperf -o verbose=2 > $RESULT_DIR/populate.out

echo "Clearing the file system caches"
echo 3 | sudo tee /proc/sys/vm/drop_caches

echo "Running the test with readahead disabled"
./bench/wtperf/wtperf -O $BENCH_RUN_DIR/readahead-run-off.wtperf -o verbose=2 > $RESULT_DIR/run_read_off.out

echo "Clearing the file system caches"
echo 3 | sudo tee /proc/sys/vm/drop_caches

echo "Running the test with readahead enabled"
./bench/wtperf/wtperf -O $BENCH_RUN_DIR/readahead-run-on.wtperf -o verbose=2 > $RESULT_DIR/run_read_on.out

echo "Analyzing results"
read_off_scans=`grep  "scan operations" $RESULT_DIR/run_read_off.out | cut -d ' ' -f 2`
read_on_scans=`grep  "scan operations" $RESULT_DIR/run_read_on.out | cut -d ' ' -f 2`
read_off_time=`grep  "scan time" $RESULT_DIR/run_read_off.out | cut -d ' ' -f 2`
read_on_time=`grep  "scan time" $RESULT_DIR/run_read_on.out | cut -d ' ' -f 2`

echo "Read ahead on did  $read_on_scans in $read_on_time ms"
echo "Read ahead off did $read_off_scans in $read_off_time ms"
