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

# test_btree_usage03.py
#   WT-17717: concurrency stress for the btree-usage snapshot. Multiple threads
#   hammer the connection statistics cursor (the statistics/FTDC path, which
#   read-locks the usage lock) while other threads churn handles and drive btree
#   activity, and the sweep server runs the usage-collection pass (which
#   write-locks the usage lock and walks the dhandle list). This reproduces the
#   contention mongod sees at startup. Surviving without a crash is the test.

import threading, time
import wttest

class test_btree_usage03(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(fast),' + \
        'file_manager=(close_scan_interval=1,close_idle_time=1,close_handle_minimum=0)'

    def test_concurrent_stats_and_churn(self):
        stop = threading.Event()
        errors = []

        def monitor():
            # Tight statistics/FTDC-like loop over the connection stats.
            try:
                s = self.conn.open_session()
                while not stop.is_set():
                    c = s.open_cursor('statistics:', None, None)
                    while c.next() == 0:
                        c.get_value()
                    c.close()
                s.close()
            except Exception as e:
                errors.append(repr(e))

        def churn(tid):
            # Create/populate (incl. updates -> history store)/drop handles.
            try:
                s = self.conn.open_session()
                n = 0
                while not stop.is_set():
                    uri = 'table:bu03_%d_%d' % (tid, n)
                    s.create(uri, 'key_format=S,value_format=S')
                    cur = s.open_cursor(uri, None, None)
                    for k in range(30):
                        cur['%06d' % k] = 'v' * 30
                    for k in range(30):
                        cur['%06d' % k] = 'w' * 30
                    cur.close()
                    if n >= 4:
                        old = 'table:bu03_%d_%d' % (tid, n - 4)
                        try:
                            s.drop(old, 'force=true')
                        except Exception:
                            pass
                    n += 1
                s.close()
            except Exception as e:
                errors.append(repr(e))

        threads = [threading.Thread(target=monitor) for _ in range(3)] + \
                  [threading.Thread(target=churn, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        time.sleep(20)
        stop.set()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], 'worker threads hit errors: %s' % errors)
