#!/bin/bash

if [ ! -e build_cmake ] ; then
    echo "Build should be run from root directory"
    exit 1
fi

RUN_TIMESTAMP=`date +%y%m%d_%H%M%S`
RESULT_DIR=`pwd`/readahead_results_$RUN_TIMESTAMP
BENCH_RUN_DIR=`pwd`/bench/wtperf/runners/readahead/
WTPERF_BIN_DIR=build_cmake/bench/wtperf/

mkdir -p $RESULT_DIR
cd $WTPERF_BIN_DIR

cp $BENCH_RUN_DIR/readahead*wtperf $RESULT_DIR

mkdir $RESULT_DIR/readahead_off_run
mkdir $RESULT_DIR/readahead_on_run

echo "Creating the populated database"
./wtperf -O $BENCH_RUN_DIR/readahead-pop.wtperf -o verbose=2 > $RESULT_DIR/populate.out

# Cleanup statistics file - the same database directory is used.
rm WT_TEST/WiredTigerStat*

echo "Clearing the file system caches"
echo 3 | sudo tee /proc/sys/vm/drop_caches

echo "Running the test with readahead disabled"
./wtperf -O $BENCH_RUN_DIR/readahead-run-off.wtperf -o verbose=2 > $RESULT_DIR/run_read_off.out

# Save results from the run
cp WT_TEST/WiredTigerStat* $RESULT_DIR/readahead_off_run/
cp WT_TEST/test.stat $RESULT_DIR/readahead_off_run/
ls -lh WT_TEST > $RESULT_DIR/readahead_off_run/run_file_info.txt

# Cleanup statistics file - the same database directory is used.
rm WT_TEST/WiredTigerStat*

echo "Clearing the file system caches"
echo 3 | sudo tee /proc/sys/vm/drop_caches

echo "Running the test with readahead enabled"
./wtperf -O $BENCH_RUN_DIR/readahead-run-on.wtperf -o verbose=2 > $RESULT_DIR/run_read_on.out

# Save results from the run
cp WT_TEST/WiredTigerStat* $RESULT_DIR/readahead_on_run/
cp WT_TEST/test.stat $RESULT_DIR/readahead_on_run/
ls -lh WT_TEST > $RESULT_DIR/readahead_on_run/run_file_info.txt

echo "Analyzing results"
read_off_scans=`grep  "scan operations" $RESULT_DIR/run_read_off.out | cut -d ' ' -f 2`
read_on_scans=`grep  "scan operations" $RESULT_DIR/run_read_on.out | cut -d ' ' -f 2`
read_off_time=`grep  "scan time" $RESULT_DIR/run_read_off.out | cut -d ' ' -f 2`
read_on_time=`grep  "scan time" $RESULT_DIR/run_read_on.out | cut -d ' ' -f 2`

echo "Read ahead on did  $read_on_scans in $read_on_time ms"
echo "Read ahead off did $read_off_scans in $read_off_time ms"
