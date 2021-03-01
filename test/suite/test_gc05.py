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

from test_gc01 import test_gc_base
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_gc05.py
# Verify a locked checkpoint is not removed during garbage collection.
class test_gc05(test_gc_base):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_gc(self):
        uri = "table:gc05"
        create_params = 'value_format=S,key_format=i'
        self.session.create(uri, create_params)

        nrows = 10000
        value_x = "xxxxx" * 100
        value_y = "yyyyy" * 100
        value_z = "zzzzz" * 100
        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Set the oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        # Insert values with varying timestamps.
        self.large_updates(uri, value_x, ds, nrows, 20)
        self.large_updates(uri, value_y, ds, nrows, 30)
        self.large_updates(uri, value_z, ds, nrows, 40)

        # Perform a checkpoint.
        self.session.checkpoint("name=checkpoint_one")

        # Check statistics.
        self.check_gc_stats()

        # Open a cursor to the checkpoint just performed.
        ckpt_cursor = self.session.open_cursor(uri, None, "checkpoint=checkpoint_one")

        # Move the oldest and stable timestamps to 40.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(40) +
            ',stable_timestamp=' + timestamp_str(40))

        # Insert values with varying timestamps.
        self.large_updates(uri, value_z, ds, nrows, 50)
        self.large_updates(uri, value_y, ds, nrows, 60)
        self.large_updates(uri, value_x, ds, nrows, 70)

        # Move the oldest and stable timestamps to 70.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(70) +
            ',stable_timestamp=' + timestamp_str(70))

        # Perform a checkpoint.
        self.session.checkpoint()
        self.check_gc_stats()

        # Verify checkpoint_one still exists and contains the expected values.
        for i in range(0, nrows):
            ckpt_cursor.set_key(i)
            ckpt_cursor.search()
            self.assertEqual(value_z, ckpt_cursor.get_value())

        # Close checkpoint cursor.
        ckpt_cursor.close()

if __name__ == '__main__':
    wttest.run()
