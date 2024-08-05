#!/bin/bash

# To make sure we have plenty of flush_tier calls, we set the flush frequency high
# and the time between checkpoints low. We specify only using tables, as that's the
# only kind of URI that participates in tiered storage.
set -o errexit
set -o verbose

times=$1
test_format_extra_args=$2

format_args="tiered_storage.storage_source=dir_store tiered_storage.flush_frequency=60 checkpoint.wait=15 runs.source=table runs.timer=10 runs.in_memory=0"
for i in $(seq $times); do
  echo Iteration $i/$times
  rm -rf RUNDIR
  ./t $format_args $test_format_extra_args
  ./t -R $format_args $test_format_extra_args
done