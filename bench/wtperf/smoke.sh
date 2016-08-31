#! /bin/sh

# Smoke-test wtperf as part of running "make check".
#./wtperf -O `dirname $0`/runners/small-lsm.wtperf -o "run_time=20"

# Run something with a lot of random options so we test for memory leaks
# with more than simple cases.
args=""
args="$args async_threads=4,"
args="$args checkpoint_interval=10,"
args="$args checkpoint_threads=1,"
args="$args checkpoint_stress_rate=100,"
args="$args compact=true,"
args="$args compression=snappy,"
args="$args create=true,"
args="$args database_count=3,"
args="$args drop_tables=true,"
args="$args idle_table_cycle=20,"
args="$args index=true,"
args="$args insert_rmw=true,"
args="$args key_sz=20,"
args="$args log_partial=true,"
args="$args min_throughput=40,"
args="$args min_throughput_fatal=false,"
args="$args max_latency=1000,"
args="$args max_latency_fatal=false,"
args="$args pareto=37,"
args="$args populate_ops_per_txn=5,"
args="$args populate_threads=4,"
args="$args random_range=0,"
args="$args random_value=true,"
args="$args range_partition=true,"
args="$args read_range=10,"
args="$args reopen_connection=true,"
args="$args report_interval=5,"
args="$args sample_interval=10,"
args="$args sess_config=\"isolation=snapshot\","
args="$args session_count_idle=100,"
args="$args table_config=\"leaf_page_max=8kb\","
args="$args table_count=10",
args="$args table_count_idle=10",
args="$args threads=\"(count=8,reads=1,inserts=2,updates=1)\","
args="$args transaction_config=\"name=foo\","
args="$args table_name=test,"
args="$args run_time=20,"
args="$args warmup=1,"

./wtperf \
    -C "checkpoint_sync=true" -C "checkpoint_sync=true" \
    -h "WT_TEST" \
    -m "WT_TEST" \
    -T "block_allocation=first" -T "block_allocation=first" \
    -o "$args"
