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
# test_timestamp19.py
# Use the oldest timestamp in the metadata as the oldest timestamp on restart.
import wiredtiger, wttest
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

class test_timestamp19(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def updates(self, uri, value, ds, nrows, commit_ts):
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def test_timestamp(self):
        uri = "table:test_timestamp19"
        create_params = 'value_format=S,key_format=i'
        self.session.create(uri, create_params)

        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        nrows = 1000
        value_x = 'x' * 1000
        value_y = 'y' * 1000
        value_z = 'z' * 1000

        # Set the oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
          ', stable_timestamp=' + timestamp_str(10))

        # Insert values with varying timestamps.
        self.updates(uri, value_x, ds, nrows, 20)
        self.updates(uri, value_y, ds, nrows, 30)
        self.updates(uri, value_z, ds, nrows, 40)

        # Perform a checkpoint.
        self.session.checkpoint('use_timestamp=true')

        # Move the oldest and stable timestamps to 40.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(40) +
          ', stable_timestamp=' + timestamp_str(40))

        # Update values.
        self.updates(uri, value_z, ds, nrows, 50)
        self.updates(uri, value_x, ds, nrows, 60)
        self.updates(uri, value_y, ds, nrows, 70)

        # Perform a checkpoint.
        self.session.checkpoint('use_timestamp=true')

        # Close and reopen the connection.
        self.close_conn()
        self.conn = self.setUpConnectionOpen('.')
        self.session = self.setUpSessionOpen(self.conn)

        # The oldest timestamp on recovery is 40. Trying to set it earlier is a no-op.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10))
        self.assertTimestampsEqual(self.conn.query_timestamp('get=oldest'), timestamp_str(40))

        # Trying to set an earlier stable timestamp is an error.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' + timestamp_str(10)),
            '/oldest timestamp \(0, 40\) must not be later than stable timestamp \(0, 10\)/')
        self.assertTimestampsEqual(self.conn.query_timestamp('get=stable'), timestamp_str(40))

        # Move the oldest and stable timestamps to 70.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(70) +
          ', stable_timestamp=' + timestamp_str(70))
        self.assertTimestampsEqual(self.conn.query_timestamp('get=oldest'), timestamp_str(70))
        self.assertTimestampsEqual(self.conn.query_timestamp('get=stable'), timestamp_str(70))

if __name__ == '__main__':
    wttest.run()
