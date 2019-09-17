#!/bin/bash

# wtperf_xray.sh - run wtperf regression tests with xray profiling and generate
# profiling information
#
# Usage
# ./wtperf_xray.sh <TEST> <TEST_ARGS>
#
# Environment variables
# XRAY_BINARY: The binary to use to inspect the xray log. (default: llvm-xray)
# FLAME_GRAPH_PATH: The path to your copy of Brendan Gregg's FlameGraph scripts.

if test "$#" -lt "1"; then
	echo "$0: must specify wtperf test to run"
	exit 1
fi

# Check symbols to ensure we've compiled with XRay.
objdump_out=$(objdump -h -j xray_instr_map ./wtperf)
if test -z "$objdump_out"; then
	echo "$0: wtperf not compiled with xray, add '-fxray-instrument' to your CFLAGS"
	exit 1
fi

rm xray-log.wtperf.* \
	wtperf_account.txt \
	wtperf_stack.txt \
	wtperf_graph.svg \
	wtperf_flame.svg

export XRAY_OPTIONS="patch_premain=true xray_mode=xray-basic verbosity=1"
wtperf_out=$(./wtperf -O "$@")

xray_log=$(ls xray-log.wtperf.*)
num_logs=$(echo "$xray_log" | wc -w)
if test "$num_logs" -ne "1"; then
	echo "$0: detected more than one xray log"
	exit 1
fi

if test -z "$XRAY_BINARY"; then
	xray_bin="llvm-xray"
	echo "$0: XRAY_BINARY is unset, defaulting to $xray_bin"
else
	xray_bin="$XRAY_BINARY"
fi

$xray_bin account $xray_log \
	-top=10 -sort=sum -sortorder=dsc -instr_map ./wtperf > \
	wtperf_account.txt

# Use the -per-thread-stacks option to get the top 10 stacks for each thread.
# We could use the -aggregate-threads flag here so get the top stacks for all threads (omitting duplicates).
$xray_bin stack -per-thread-stacks $xray_log \
	-instr_map ./wtperf > \
	wtperf_stack.txt

# Generate a DOT graph.
$xray_bin graph $xray_log \
	-m ./wtperf -color-edges=sum -edge-label=sum | \
	unflatten -f -l10 | \
	dot -Tsvg -o wtperf_graph.svg

# This file can be inspected in the Google Chrome Trace Viewer.
# It seems to take a long time to generate this so just disable it for now.
# $xray_bin convert -symbolize -instr_map=./wtperf -output-format=trace_event $xray_log | gzip > wtperf_trace.txt.gz

if test -z "$FLAME_GRAPH_PATH"; then
	echo "$0: FLAME_GRAPH_PATH is unset, skipping flame graph generation"
else
	$xray_bin stack $xray_log \
		-instr_map ./wtperf -stack-format=flame -aggregation-type=time -all-stacks | \
		"$FLAME_GRAPH_PATH" > wtperf_flame.svg
fi
