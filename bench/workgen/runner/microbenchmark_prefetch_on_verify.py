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
from microbenchmark_prefetch_base import *

prefetch = microbenchmark_prefetch()
prefetch.populate()

# Re-open the connection to flush the cache and turn pre-fetching on.
prefetch.conn.close()
prefetch.conn = prefetch.context.wiredtiger_open(prefetch.conn_config)
prefetch.session = prefetch.conn.open_session("prefetch=(enabled=true)")

def run_workload():
    print("Start verifying...")
    verify_op = Operation(Operation.OP_VERIFY, prefetch.table, "prefetch=(enabled=true)")
    verify_thread = Thread(verify_op)
    verify_workload = Workload(prefetch.context, verify_thread)
    verify_workload.options.run_time = 300
    ret = verify_workload.run(prefetch.conn)
    assert ret == 0, ret
    print("Finished verifying.")

run_workload()
prefetch.print_prefetch_stats()
prefetch.session.close()
prefetch.conn.close()
