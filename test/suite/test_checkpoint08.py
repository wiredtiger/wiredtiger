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
# test_checkpoint08.py
# Test that the checkpoints doesn't discard data that is older than the checkpoint's oldest timestamp.

import threading, time
import wiredtiger, wttest
from wtthread import checkpoint_thread

def timestamp_str(t):
    return '%x' % t

class test_checkpoint08(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB,timing_stress_for_test=[prepare_checkpoint_delay_after_get_ts]'
    session_config = 'isolation=snapshot'

    def large_updates(self, uri, value, nrows, commit_ts):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[str(i)] = value
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_deletes(self, uri, nrows, commit_ts):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor.set_key(str(i))
            cursor.remove()
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value)
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)

    def test_checkpoint08(self):
        nrows = 1000

        uri = "table:checkpoint08"
        self.session.create(uri, 'key_format=S,value_format=S')

        value1 = "a" * 500
        value2 = "b" * 500

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        # Insert keys at timestamp 20
        self.large_updates(uri, value1, nrows, 20)

        # Update the keys at timestamp 30
        self.large_updates(uri, value2, nrows, 30)

        # Remove the keys at timestamp 40
        self.large_deletes(uri, nrows, 40)

        # Set stable to timestamp 50.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(50))

        # Create a checkpoint thread
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        try:
            ckpt.start()
            time.sleep(1)

            # Concurrently set oldest to timestamp 40.
            self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(40))
        finally:
            done.set()
            ckpt.join()

        # Simulate a server crash and restart.
        self.simulate_crash_restart(".", "RESTART")

        # Check the data is present in the checkpoint after restart
        self.check(value2, uri, nrows, 30)
        self.check(value1, uri, nrows, 20)
