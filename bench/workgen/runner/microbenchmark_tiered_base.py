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

# This is the tiered microbenchmark base class that has the common code needed
# for microbenchmark-tiered-checkpoint.py and microbenchmark-tiered-checkpoint-flush.py
#

import sys
sys.path.append("/data/wiredtiger/bench/workgen/runner")

from runner import *
from wiredtiger import *
from workgen import *

context = Context()

tiered_storage_config = ",tiered_storage=(bucket=./bucket,bucket_prefix=pfx-,local_retention=0,name=dir_store)"
tiered_storage_config += ",extensions=(./ext/storage_sources/dir_store/libwiredtiger_dir_store.so=(early_load=true))"
conn_config = ""
conn_config += ",cache_size=2G,log=(enabled,file_max=10MB)"   # explicitly added
conn_config += tiered_storage_config

conn = context.wiredtiger_open("create," + conn_config)
s = conn.open_session("")

wtperf_table_config = "key_format=S,value_format=S," +\
        "exclusive=true,allocation_size=4kb," +\
        "internal_page_max=64kb,leaf_page_max=4kb,split_pct=100,"
compress_table_config = ""
table_config = "type=file"
tables = []
table_count = 4
for i in range(0, table_count):
    tname = "table:test" + str(i)
    table = Table(tname)
    s.create(tname, wtperf_table_config +\
            compress_table_config + table_config)
    table.options.key_size = 20
    table.options.value_size = 100
    tables.append(table)

print ("Populate tables start")
populate_threads = 5
icount = 50000
# There are multiple tables to be filled during populate,
# the icount is split between them all.
pop_ops = Operation(Operation.OP_INSERT, tables[0])
pop_ops = op_multi_table(pop_ops, tables)
nops_per_thread = icount // (populate_threads * table_count)
pop_ops = op_group_transaction(pop_ops, 100, "")
pop_thread = Thread(pop_ops * nops_per_thread)
pop_workload = Workload(context, populate_threads * pop_thread)
ret = pop_workload.run(conn)
assert ret == 0, ret

print ("Populate tables end")

ops = Operation(Operation.OP_SEARCH, tables[0])
ops = op_multi_table(ops, tables, False)
read_thread = Thread(ops)

ops = Operation(Operation.OP_UPDATE, tables[0])
ops = op_multi_table(ops, tables, False)
update_thread = Thread(ops)

ops = Operation(Operation.OP_INSERT, tables[0])
ops = op_multi_table(ops, tables, False)
insert_thread = Thread(ops)

def get_context():
    return (context)
    
def get_insert_thread():
    return (insert_thread)

def get_update_thread():
    return (update_thread)

def get_reader_thread():
    return (read_thread)

def get_conn():
    return (conn)

def get_bucket_name():
    return ("bucket")
