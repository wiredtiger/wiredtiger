#!/usr/bin/env python3
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

import time
import wttest
from sweep_util import sweep_util
from wiredtiger import stat

# Sweep must not close a table while its file is in use, including when the file is reopened after
# the table was marked idle.
@wttest.skip_for_hook("disagg", "Disagg doesn't sweep layered dhandles")
class test_sweep08(sweep_util):
    idle_time = 2
    conn_config = 'file_manager=(close_handle_minimum=0,' + \
                  'close_idle_time=%d,close_scan_interval=1)' % idle_time
    uri = 'table:test_sweep08'

    def test_sweep_table_file_reopened(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(100):
            cursor[i] = 'a'
        cursor.close()
        self.session.checkpoint()

        # Wait for sweep to close the idle file, and then for the next sweep, which marks the table
        # idle because its file is closed.
        self.wait_for_sweep(baseline=0, statistic=stat.conn.dh_sweep_expired_close,
            poll_interval=0.1)
        self.wait_for_sweep(increment=2, poll_interval=0.1)

        # Use the table again through a cached cursor, for longer than the idle time.
        cursor = self.session.open_cursor(self.uri)
        cursor[0] = 'b'
        cursor.close()
        closes = self.get_stat(stat.conn.dh_sweep_expired_close)
        sweeps = self.get_stat(stat.conn.dh_sweeps)
        deadline = time.time() + 60
        i = 0
        while self.get_stat(stat.conn.dh_sweeps) < sweeps + 3 * self.idle_time:
            self.assertLess(time.time(), deadline, 'sweep did not run within 60 seconds')
            cursor = self.session.open_cursor(self.uri)
            cursor[i] = 'c'
            cursor.close()
            i += 1
            time.sleep(0.1)

        # Sweep closed neither the table nor its file.
        self.assertEqual(self.get_stat(stat.conn.dh_sweep_expired_close), closes)

        # The table is still open, so a cursor in a new session doesn't take the schema lock.
        session2 = self.conn.open_session()
        schema_locks = self.get_stat(stat.conn.lock_schema_count)
        cursor = session2.open_cursor(self.uri)
        self.assertEqual(self.get_stat(stat.conn.lock_schema_count), schema_locks)
        cursor.close()
        session2.close()
