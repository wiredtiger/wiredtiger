#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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
from helper import copy_wiredtiger_home
import unittest, wiredtiger, wttest
from wtdataset import SimpleDataSet
from test_gc01 import test_gc_base

def timestamp_str(t):
    return '%x' % t

# test_gc02.py
# Test that checkpoint cleans the obsolete lookaside internal pages.
class test_gc02(test_gc_base):
    conn_config = 'cache_size=1GB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def large_updates(self, uri, value, ds, nrows, commit_ts):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_modifies(self, uri, value, ds, location, nbytes, nrows, commit_ts):
        # Load a slight modification.
        session = self.session
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        for i in range(1, nrows):
            cursor.set_key(i)
            mods = [wiredtiger.Modify(value, location, nbytes)]
            self.assertEqual(cursor.modify(mods), 0)
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
        session.rollback_transaction()
        self.assertEqual(count, nrows)

    def test_gc(self):
        nrows = 100000

        # Create a table without logging.
        uri = "table:gc02"
        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        bigvalue = "aaaaa" * 100
        bigvalue2 = "ddddd" * 100
        self.large_updates(uri, bigvalue, ds, nrows, 10)

        # Check that all updates are seen
        #self.check(bigvalue, uri, nrows, 20)

        self.large_updates(uri, bigvalue2, ds, nrows, 20)

        # Check that the new updates are only seen after the update timestamp
        #self.check(bigvalue2, uri, nrows, 100)

        # Checkpoint to ensure that the history store is checkpointed and no clean
        self.session.checkpoint()
        self.check_gc_stats()

        # Pin oldest and stable to timestamp 100.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(100) +
            ',stable_timestamp=' + timestamp_str(100))

        # Check that the new updates are only seen after the update timestamp
        #self.check(bigvalue2, uri, nrows, 100)

        # Load a slight modification with a later timestamp.
        self.large_modifies(uri, 'A', ds, 10, 1, nrows, 110)
        self.large_modifies(uri, 'B', ds, 20, 1, nrows, 120)
        self.large_modifies(uri, 'C', ds, 30, 1, nrows, 130)

        # set of update operations with increased timestamp
        self.large_updates(uri, bigvalue2, ds, nrows, 150)

        # Check that the new updates are only seen after the update timestamp
        #self.check(bigvalue2, uri, nrows, 150)

        # set of update operations with increased timestamp
        self.large_updates(uri, bigvalue, ds, nrows, 180)

        # Check that the new updates are only seen after the update timestamp
        #self.check(bigvalue, uri, nrows, 180)

        # set of update operations with increased timestamp
        self.large_updates(uri, bigvalue2, ds, nrows, 200)

        # Pin oldest and stable to timestamp 200.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(200) +
            ',stable_timestamp=' + timestamp_str(200))

        # Checkpoint to ensure that the history store is cleaned
        self.session.checkpoint()
        self.check_gc_stats()

        # Check that the new updates are only seen after the update timestamp
        #self.check(bigvalue2, uri, nrows, 200)

        # When this limitation is fixed we'll need to uncomment the calls to self.check
        # and fix self.check_gc_stats.
        self.KNOWN_LIMITATION('values stored by this test are not yet validated')

if __name__ == '__main__':
    wttest.run()
