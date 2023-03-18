#!/bin/bash
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#	All rights reserved.
#
# See the file LICENSE for redistribution information.
#

#
# This tool runs wtperf on the /data partition on Evergreen hosts and captures the traces.
#
# Note that this tool is DANGEROUS: It formats the /data partition according to the specified
# file system. Please make sure that you understand what this tool does before using it.
#

set -e

WORKLOAD="$1"

WT_DIR="`git rev-parse --abbrev-ref HEAD`"
TRACES_DIR=~/Traces

WORKLOAD_DEVICE=/dev/nvme1n1
WORKLOAD_MOUNT=/data
WORKLOAD_FS="$2"

if [ -z $2 ]; then
	echo "Usage: $0 WORKLOAD FS"
	exit 1
fi

OUTPUT_TAG="${WORKLOAD}-$WORKLOAD_FS"
WORKLOAD_DIR=$WORKLOAD_MOUNT/wt
WORKLOAD_FILE=$WT_DIR/bench/wtperf/runners/${WORKLOAD}.wtperf

if [ ! -f $WORKLOAD_FILE ]; then
	echo "File not found: $WORKLOAD_FILE"
	exit 1
fi



# Initialize

mkdir -p $TRACES_DIR
(cd "$WT_DIR" && /bin/rm -f wt-trace-io.* || true)


# Format

sudo umount $WORKLOAD_MOUNT || true
sudo wipefs -a $WORKLOAD_DEVICE
sudo mkfs.$WORKLOAD_FS $WORKLOAD_DEVICE
sudo mount $WORKLOAD_DEVICE $WORKLOAD_MOUNT
sudo chown $USER:$USER $WORKLOAD_MOUNT


# We're done with the setup

set +e


# Start the trace

(cd "$TRACES_DIR" && sudo blktrace -d $WORKLOAD_DEVICE -o $OUTPUT_TAG) &


# Start the workload

(cd "$WT_DIR" && export WIREDTIGER_CONFIG="verbose=[read:2,write:2]" && \
	./build/bench/wtperf/wtperf -O $WORKLOAD_FILE -h $WORKLOAD_DIR \
		> ${TRACES_DIR}/${OUTPUT_TAG}---stdout.txt ; \
	sleep 10 ; sudo killall -INT blktrace) || true


# Wait and parse

sleep 10

(cd "$TRACES_DIR" && blkparse -t -i ${OUTPUT_TAG} > ${OUTPUT_TAG}---device.txt)
(cd "$TRACES_DIR" && iowatcher -t ${OUTPUT_TAG} -o ${OUTPUT_TAG}---summary.svg)

echo
echo '********** DONE *********'
