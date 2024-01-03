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

"""
# wtperf options file: evict btree configuration
conn_config="cache_size=1G,checkpoint=(wait=60,log_size=2GB),eviction=(threads_min=12,threads_max=12),log=(enabled=true),session_max=600,statistics=(fast),statistics_log=(wait=1,json),read_ahead=true"
key_sz=12
value_sz=138
table_config="type=file"
create=false
compression="snappy"
icount=10000
scan_icount=120000000
    report_interval=5
populate_threads=1
    run_time=100
scan_table_count=1
scan_type=forward
scan_interval=99
scan_pct=20
table_count=1
# The benchmark needs some non-scan acticity
threads=((count=1,reads=1,throttle=5))
# Add throughput/latency monitoring
max_latency=50000
sample_interval=5
"""

from runner import *
from workgen import *
from microbenchmark_prefetch_base import *

prefetch = microbenchmark_prefetch()
prefetch.populate()

# Turn pre-fetching on.
prefetch.session = prefetch.conn.open_session("prefetch=(enabled=true)")

scan_ops = Operation(Operation.OP_SEARCH, prefetch.table)
scan_thread = Thread(scan_ops * 120000000)
scan_thread.options.name = "Scan"
scan_thread.options.throttle = 5

def run_workload():
    print("Start scanning...")
    workload = Workload(prefetch.context, scan_thread)
    workload.options.run_time=100
    workload.options.report_interval=5
    workload.options.sample_interval_ms=5
    workload.options.max_latency=50000
    ret = workload.run(prefetch.conn)
    assert ret == 0, ret
    print("Finished scanning.")
    latency_filename = prefetch.context.args.home + "/latency.out"
    latency.workload_latency(workload, latency_filename)

run_workload()
