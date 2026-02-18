#/usr/bin/env python
# cache-stuck workload via workgen (no explicit txn() wrappers)

import sys
sys.path.append("/home/ubuntu/work/wiredtiger/bench/workgen/runner")

from runner import *
from wiredtiger import *
from workgen import *
import os
import time

context = Context()

# Connection: disagg + small cache
conn_config = (
    "cache_size=1GB,"
    "disaggregated=(drain_threads=2,page_log=palite,role=leader),"
    "extensions=(\"../../ext/page_log/palite/libwiredtiger_palite.so\"=),"
    "cache_stuck_timeout_ms=60000"
)
conn = context.wiredtiger_open("create," + conn_config)
s = conn.open_session("")

# Table: small pages + layered/disagg
wtperf_table_config = (
    "key_format=S,value_format=S,"
    "exclusive=true,allocation_size=4kb,"
    "internal_page_max=4kb,leaf_page_max=4kb,split_pct=100,"
)
compress_table_config = ""
table_config = (
    "key_format=S,value_format=S,leaf_page_max=4k,internal_page_max=4k,"
    "type=layered,block_manager=disagg"
)

tables = []
tname = "table:test"
table = Table(tname)
s.create(tname, wtperf_table_config + compress_table_config + table_config)
table.options.key_size = 20
table.options.value_size = 1024
tables.append(table)

# Checkpoint every 30 seconds
ops_ckpt_1 = Operation(Operation.OP_SLEEP, "30") + Operation(Operation.OP_CHECKPOINT, "")
checkpoint_thread_1 = Thread(ops_ckpt_1)
ckpt_threads = 2

# Populate large dataset (20M rows * 1KB ~ 20GB logical)
populate_threads = 8
icount = 10000000
pop_ops = Operation(Operation.OP_INSERT, table)
nops_per_thread = icount // populate_threads
pop_thread = Thread(pop_ops * nops_per_thread)
pop_workload = Workload(context, populate_threads * pop_thread + checkpoint_thread_1 * ckpt_threads)
print("populate Start:")
start_time = time.time()
ret = pop_workload.run(conn)
print("populate End:")
end_time = time.time()
print("Populate took %d minutes" % ((end_time - start_time) // 60))
assert ret == 0, ret

# --- Run phase: simple, non-txnal operations ---

# Heavy updates
op_update = Operation(Operation.OP_UPDATE, table)
tupdate = Thread(op_update)          # run repeatedly, driven by run_time
tupdate.options.session_config = "isolation=snapshot"

# Inserts growing the working set
op_insert = Operation(Operation.OP_INSERT, table)
tinsert = Thread(op_insert)
tinsert.options.session_config = "isolation=snapshot"

# Readers using search
op_read = Operation(Operation.OP_SEARCH, table)
tread = Thread(op_read)

# Checkpoint every 30 seconds
ops_ckpt = Operation(Operation.OP_SLEEP, "30") + Operation(Operation.OP_CHECKPOINT, "")
checkpoint_thread = Thread(ops_ckpt)

workload = Workload(
    context,
    24 * tupdate +
    8 * tinsert +
    8 * tread +
    checkpoint_thread
)

workload.options.run_time = 900
workload.options.report_interval = 5

# If you still want automatic oldest/stable movement, you can leave this on:
workload.options.oldest_timestamp_lag = 20
workload.options.stable_timestamp_lag = 10
workload.options.timestamp_advance = 1

print("cache-stuck workload:")
start_time = time.time()
ret = workload.run(conn)
print("workload.run returned:", ret)
end_time = time.time()

print("Workload took %d minutes" % ((end_time - start_time) // 60))

latency_filename = os.path.join(context.args.home, "latency.out")
latency.workload_latency(workload, latency_filename)
conn.close()
