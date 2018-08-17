#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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

# Drive a constant high workload through, even if WiredTiger isn't keeping
# up by dividing the workload across a lot of threads. This needs to be
# tuned to the particular machine so the workload is close to capacity in the
# steady state, but not overwhelming.
#
################
# A workload with small cache, small internal and leaf page sizes, faster splits
# and multiple threads inserting keys in random order. It stresses the page
# splits in order to catch split races.
#
from runner import *
from wiredtiger import *
from workgen import *

# Helper functions.
def op_append(ops, op):
    if ops == None:
        ops = op
    else:
        ops += op
    return ops

def make_op(optype, table, key, value = None):
    if value == None:
        return Operation(optype, table, key)
    else:
        return Operation(optype, table, key, value)

def operations(optype, tables, key, value = None, ops_per_txn = 0, logtable = None):
    txn_list = []
    ops = None
    nops = 0
    for table in tables:
        ops = op_append(ops, make_op(optype, table, key, value))
        if logtable != None:
            ops = op_append(ops, make_op(optype, logtable, logkey, value))
        nops += 1
        if ops_per_txn > 0 and nops % ops_per_txn == 0:
            txn_list.append(txn(ops))
            ops = None
    if ops_per_txn > 0:
        if ops != None:
            txn_list.append(txn(ops))
            ops = None
        for t in txn_list:
            ops = op_append(ops, t)
    return ops

# Connection and table configuration.
context = Context()
conn_config="create,cache_size=100MB,log=(enabled=false),statistics=[fast],statistics_log=(wait=5,json=false)"
conn = wiredtiger_open("WT_TEST", conn_config)
s = conn.open_session()

table_config="leaf_page_max=8k,internal_page_max=8k,leaf_item_max=1433,internal_item_max=3100,type=file,memory_page_max=1MB,split_deepen_min_child=100"

tables = []
for i in range(0, 3):
    tname = "file:test_" + str(i)
    s.create(tname, 'key_format=S,value_format=S,' + table_config)
    table = Table(tname)
    table.options.range = 100000000 # 100 million
    tables.append(table)

# Populate phase.
icount=50000
ins_op = operations(Operation.OP_INSERT, tables, Key(Key.KEYGEN_APPEND, 64), Value(200))
thread = Thread(ins_op * icount)
pop_workload = Workload(context, thread)
print('populating...')
pop_workload.run(conn)

# Run phase.
ins_ops = operations(Operation.OP_INSERT, tables, Key(Key.KEYGEN_APPEND, 64), Value(200))
ins_thread = Thread(ins_ops)
ins_thread.options.name = "Insert"
threads = ins_thread * 20
workload = Workload(context, threads)
workload.options.run_time = 10
workload.options.report_interval = 5
print('Split stress workload running...')
workload.run(conn)
