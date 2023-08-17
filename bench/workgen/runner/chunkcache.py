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

from runner import *
from wiredtiger import *
from workgen import *


# Set up the WiredTiger connection.
context = Context()
conn_config = "create,cache_size=70MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true)"
restart_config = "create,cache_size=70MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),chunk_cache=[enabled=true,chunk_size=10MB,capacity=1GB,type=FILE,storage_path=/home/ubuntu/wiredtiger/w/chunk_file]"
conn = context.wiredtiger_open(conn_config)
s = conn.open_session()
tname = 'table:chunkcache'
s.create(tname, 'key_format=S,value_format=S')
table = Table(tname)
table.options.key_size = 20
table.options.value_size = 100

# Populate phase
insert_ops = Operation(Operation.OP_INSERT, table)
insert_thread = Thread(insert_ops * 20000)
populate_workload = Workload(context, insert_thread * 40)
ret = populate_workload.run(conn)
assert ret == 0, ret

# Reopen the connection and reconfigure
conn.close()
conn = context.wiredtiger_open(restart_config)
s = conn.open_session("")

# Read into the chunkcache 
read_op = Operation(Operation.OP_SEARCH, table)
read_op._config = 'reopen'
read_thread = Thread(read_op * 20000)
read_workload = Workload(context, read_thread * 40)
read_workload.options.run_time = 30
read_workload.options.report_interval = 5
ret = read_workload.run(conn)
assert ret == 0, ret

# Close the connection.
conn.close()