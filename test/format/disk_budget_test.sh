#!/bin/sh

set -eu
ulimit -c 0

work=$(mktemp -d ./WT_TEST.disk-budget.XXXXXX)
trap 'rm -rf "$work"' EXIT
home="$work/RUNDIR"

./t -c . -q -h "$home" btree.compression=off cache=32 \
    background_compact=0 backup=0 checkpoint=off disagg.mode=off logging=0 ops.alter=0 \
    ops.compaction=0 ops.verify=1 random.data_seed=12345 random.extra_seed=67890 \
    runs.disk_budget_mb=512 runs.in_memory=0 runs.mirror=0 runs.ops=1000 runs.rows=1000 \
    runs.tables=1 runs.threads=1 runs.timer=0 runs.type=row transaction.timestamps=0 \
    >"$work/normal.log" 2>&1 || { cat "$work/normal.log"; exit 1; }
grep -qx 'runs.disk_budget_mb=512' "$home/CONFIG"

# Exceed the budget with a file in a subdirectory of the run home.
mkdir "$home/OPS.TRACE"
dd if=/dev/urandom of="$home/OPS.TRACE/data" bs=1048576 count=8 2>/dev/null

# Verification alone should finish even when there is no budget for more workload.
./t -Rqv -h "$home" runs.disk_budget_mb=4 >"$work/verify.log" 2>&1 || {
    cat "$work/verify.log"
    exit 1
}

status=0
./t -R -q -h "$home" runs.disk_budget_mb=4 >"$work/limited.log" 2>&1 || status=$?
if [ "$status" -ne 0 ] ||
    ! grep -q 'disk budget reached after initial verification' "$work/limited.log" ||
    ! grep -q 'disk budget limited run' "$work/limited.log"; then
    cat "$work/limited.log"
    exit 1
fi
