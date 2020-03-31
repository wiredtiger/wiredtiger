#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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

conn = wiredtiger_open("WT_TEST", "create,cache_size=500MB")
s = conn.open_session()
tname = "table:test"
#s.create(tname, 'key_format=S,value_format=S')
config = "key_format=S,value_format=S,"
s.create(tname, config)
table = Table(tname)
table.options.key_size = 20
table.options.value_size = 10

context = Context()
op = Operation(Operation.OP_INSERT, table)
thread = Thread(op * 5000)
pop_workload = Workload(context, thread)
print('populate:')
pop_workload.run(conn)

opread = Operation(Operation.OP_SEARCH, table)
read_txn = txn(opread * 10, 'read_timestamp')
read_txn._transaction.read_timestamp_lag = 5
treader = Thread(read_txn)

opwrite = Operation(Operation.OP_INSERT, table)
write_txn = txn(opwrite * 10, 'isolation=snapshot')
write_txn._transaction.prepare_time=0.1
twriter = Thread(write_txn)

opupdate = Operation(Operation.OP_INSERT, table)
update_txn = txn(opupdate * 10, 'isolation=snapshot')
update_txn._transaction.commit_with_timestamp = True
tupdate = Thread(update_txn)

workload = Workload(context, 10 * twriter + 10 * tupdate + 10 * treader)
workload.options.run_time = 50
workload.options.report_interval=500
workload.options.oldest_timestamp_lag=30
workload.options.stable_timestamp_lag=10
workload.options.timestamp_advance=1
print('transactional write workload:')
workload.run(conn)
