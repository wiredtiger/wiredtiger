#!/bin/bash

if test "$#" -lt "1"; then
    echo "$0: must specify wtperf test to run"
    exit 1
fi

# Check symbols to ensure we've compiled with XRay.
objdump_out=$(objdump -h -j xray_instr_map ./wtperf)
if test -z "$objdump_out"; then
    echo "$0: wtperf not compiled with xray"
    exit 1
fi

rm xray-log.wtperf.*

export XRAY_OPTIONS="patch_premain=true xray_mode=xray-basic verbosity=1"
wtperf_out=$(./wtperf -O "$@")

xray_log=$(ls xray-log.wtperf.*)
num_logs=$(wc -w "$xray_log")
if test "$num_logs" -ne "1"; then
    echo "$0: detected more than one xray log"
    exit 1
fi

llvm-xray-8 account $xray_log -top=10 -sortorder=dsc -instr_map ./wtperf > wtperf_account.txt

# Use the -per-thread-stacks option to get the top 10 stacks for each thread.
# We could use -aggregate-threads flag here so get the top stacks for all threads (omitting duplicates).
llvm-xray-8 stack $xray_log -instr_map -per-thread-stacks ./wtperf > wtperf_stack.txt

# Generate a DOT graph.
llvm-xray-8 graph $xray_log -m ./wtperf -color-edges=sum -edge-label=sum | unflatten -f -l10 | dot -Tsvg -o wtperf_graph.svg
llvm-xray-8 convert -symbolize -instr_map=./wtperf -output-format=trace_event $xray_log | gzip > wtperf_trace.txt.gz
llvm-xray-8 stack $xray_log -instr_map ./wtperf -stack-format=flame -aggregation-type=time -all-stacks | ~/work/FlameGraph/flamegraph.pl > wtperf_flame.svg
