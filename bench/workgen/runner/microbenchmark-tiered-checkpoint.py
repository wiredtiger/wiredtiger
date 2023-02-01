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

from microbenchmark_tiered_base import *

ops = Operation(Operation.OP_SLEEP, "30") + \
    Operation(Operation.OP_CHECKPOINT, "")

checkpoint_thread = Thread(ops)

workload = Workload(get_context(), 4 * get_reader_thread() + 2 * get_update_thread() + 2 * get_insert_thread() + checkpoint_thread)
workload.options.run_time=300
workload.options.report_interval=5
#workload.options.max_latency=5000
ret = workload.run(get_conn())
assert ret == 0, ret
latency_filename = get_context().args.home + "/latency.out"
latency.workload_latency(workload, latency_filename)
get_conn().close()
