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

import time
from helper import copy_wiredtiger_home
import unittest, wiredtiger, wttest
from wtdataset import SimpleDataSet
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable01.py
# Shared base class used by gc tests.
class test_rollback_to_stable_base(wttest.WiredTigerTestCase):
    def large_updates(self, uri, value, ds, nrows, commit_ts, start_key=0):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(start_key, nrows+start_key):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_modifies(self, uri, value, ds, location, nbytes, nrows, commit_ts):
        # Load a slight modification.
        session = self.session
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        for i in range(0, nrows):
            cursor.set_key(i)
            mods = [wiredtiger.Modify(value, location, nbytes)]
            self.assertEqual(cursor.modify(mods), 0)
        session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_removes(self, uri, ds, nrows, commit_ts):
        # Remove a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor.set_key(i)
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

    def count(self, check_value, uri, nrows, read_ts):
        session = self.session
        session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            if (v == check_value):
                count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)

# Test that rollback to stable clears the remove operation.
class test_rollback_to_stable01(test_rollback_to_stable_base):
    # Force a small cache.
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_rollback_to_stable(self):
        nrows = 10000

        # Create a table without logging.
        uri = "table:rollback_to_stable01"
        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        valuea = "aaaaa" * 100
        self.large_updates(uri, valuea, ds, nrows, 10)
        # Check that all updates are seen.
        self.check(valuea, uri, nrows, 10)

        # Remove all keys with newer timestamp.
        self.large_removes(uri, ds, nrows, 20)
        # Check that the update is not visible at newer timestamp.
        self.check(valuea, uri, 0, 20)

        # Pin stable to timestamp 10.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(10))
        # Checkpoint to ensure that all the updates are flushed to disk.
        self.session.checkpoint()

        self.conn.rollback_to_stable()
        # Check that the update is visible at the newer timestamp.
        self.check(valuea, uri, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        upd_aborted = (stat_cursor[stat.conn.txn_rts_upd_aborted][2] +
            stat_cursor[stat.conn.txn_rts_hs_removed][2])
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        keys_removed = stat_cursor[stat.conn.txn_rts_keys_removed][2]
        keys_restored = stat_cursor[stat.conn.txn_rts_keys_restored][2]
        pages_visited = stat_cursor[stat.conn.txn_rts_pages_visited][2]
        stat_cursor.close()
        self.assertEqual(calls, 1)
        self.assertTrue(upd_aborted >= nrows)
        self.tty("upd_aborted: " + str(upd_aborted-hs_removed))
        self.tty("hs_removed: " + str(hs_removed))
        self.tty("keys_removed: " + str(keys_removed))
        self.tty("keys_restored: " + str(keys_restored))
        self.tty("pages_visited: " + str(pages_visited))


if __name__ == '__main__':
    wttest.run()
