#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

from runner import *
from wiredtiger import *
from workgen import *
import os
import time

context = Context()
wt_builddir = os.getenv('WT_BUILDDIR')
# Connection: disagg + small cache
conn_config = (
    "cache_size=1GB,precise_checkpoint=true,"
    "disaggregated=(drain_threads=2,page_log=palite,role=leader),"
    "extensions=(\"{wt_builddir}/ext/page_log/palite/libwiredtiger_palite.so\"=),"
    "cache_stuck_timeout_ms=60000"
)
conn = context.wiredtiger_open("create," + conn_config)
s = conn.open_session("")

# Table: small pages + layered/disagg
table_config = (
    "key_format=S,value_format=S,"
    "exclusive=true,allocation_size=4kb,"
    "internal_page_max=4kb,leaf_page_max=4kb,split_pct=100,"
    "type=layered,block_manager=disagg"
)

tables = []
tname = "table:test"
table = Table(tname)
s.create(tname, table_config)
table.options.key_size = 20
table.options.value_size = 1024
tables.append(table)

# Checkpoint every 30 seconds
ops_ckpt_1 = Operation(Operation.OP_SLEEP, "30") + Operation(Operation.OP_CHECKPOINT, "")
checkpoint_thread_1 = Thread(ops_ckpt_1)
ckpt_threads = 2

# Populate large dataset (10M rows * 1KB ~ 20GB logical)
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

# --- Run phase: transactional operations ---

# Heavy updates in snapshot transactions with commit timestamps
op_update = Operation(Operation.OP_UPDATE, table)
update_txn = txn(op_update, 'isolation=snapshot')
update_txn.transaction.use_commit_timestamp = True
tupdate = Thread(update_txn)
tupdate.options.session_config = "isolation=snapshot"

# Inserts growing the working set, also transactional
op_insert = Operation(Operation.OP_INSERT, table)
insert_txn = txn(op_insert, 'isolation=snapshot')
insert_txn.transaction.use_commit_timestamp = True
tinsert = Thread(insert_txn)
tinsert.options.session_config = "isolation=snapshot"

# Readers using read-timestamp transactions that lag behind
op_read = Operation(Operation.OP_SEARCH, table)
read_txn = txn(op_read, 'read_timestamp')
read_txn.transaction.read_timestamp_lag = 30
tread = Thread(read_txn)
tread.options.session_config = "isolation=snapshot"

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
