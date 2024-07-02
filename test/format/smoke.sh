#! /bin/sh

set -e

cache_max=$(cat /proc/meminfo | grep "MemTotal: " | grep -o "[[:digit:]]*")
# Use special command to perform floating point number arithmetic, remove all decimals and
# convert to MB
cache_max=$(echo "0.2 * $cache_max / 1024" | bc | cut -d. -f1)

# Smoke-test format as part of running "make check".
args="-c . "
args="$args btree.compression=off "
args="$args cache.minimum=40 "
args="$args cache.maximum=$cache_max "
args="$args logging_compression=off"
args="$args runs.rows=100000 "
args="$args runs.source=table "
args="$args runs.tables=3 "
args="$args runs.threads=6 "
args="$args runs.timer=1 "
args="$args transaction.timestamps=1 "

$TEST_WRAPPER ./t $args runs.type=fix
$TEST_WRAPPER ./t $args runs.type=row
$TEST_WRAPPER ./t $args runs.type=var

# Temporarily disable LSM.
# $TEST_WRAPPER ./t $args runs.type=row runs.source=lsm
