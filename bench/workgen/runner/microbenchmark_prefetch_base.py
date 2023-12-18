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
from workgen import *

context = Context()
conn_config = "create,cache_size=1G,checkpoint=(wait=60,log_size=2GB),eviction=(threads_min=12,threads_max=12),session_max=600,statistics=(fast),statistics_log=(wait=1,json),prefetch=(available=true,default=false)"
conn = context.wiredtiger_open(conn_config)
session = conn.open_session()

table_config = "type=file"
table_count = 1
for i in range(0, table_count):
    table_name = "table:test_prefetch" + str(i)
    table = Table(table_name)
    session.create(table_name, table_config)
    table.options.key_size = 12
    table.options.value_size = 138

print("Populating database...")
pop_icount = 10000
pop_threads = 1
pop_ops = Operation(Operation.OP_INSERT, table)
pop_thread = Thread(pop_ops * pop_icount)
pop_workload = Workload(context, pop_threads * pop_thread)
ret = pop_workload.run(conn)
assert ret == 0, ret
print("Finished populating database.")
