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

from helper import copy_wiredtiger_home
import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_prepare_hs04.py
# Read prepared updates from on-disk with ignore_prepare.
# Committing or aborting a prepared update when there exists a tombstone for that key already.
#
class test_prepare_hs04(wttest.WiredTigerTestCase):
    # Force a small cache.
    conn_config = 'cache_size=50MB,statistics=(fast)'

    # Create a small table.
    uri = "table:test_prepare_hs04"

    nsessions = 3
    nkeys = 40
    nrows = 100

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def search_key_timestamp_and_ignore(self, ds, timestamp, ignore_prepare_str, after_crash=None):
        cursor = self.session.open_cursor(self.uri)

        txn_config = 'read_timestamp=' + timestamp_str(timestamp) + ',ignore_prepare=' + ignore_prepare_str
        commit_key = "C"
        self.session.begin_transaction(txn_config)
        for i in range(1, self.nsessions * self.nkeys):
            key = commit_key + ds.key(self.nrows + i)
            cursor.set_key(key)
            if timestamp <= 10:
                self.assertEqual(cursor.search(), 0)
            elif timestamp == 20 and ignore_prepare_str == "true":
                if after_crash == True:
                    # Cursor search should not find the key with ignore_prepare=true and after_crash
                    # Because the prepared_updates are committed with timestamp 30
                    self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
                else:
                    # Cursor search should not find the key with ignore_prepare=true
                    self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
            elif timestamp == 20 and ignore_prepare_str == "false":
                if after_crash == True:
                    # Cursor search should not find the key with ignore_prepare=false and after_crash
                    # Because the prepared_updates are committed with timestamp 30
                    self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
                else:
                    preparemsg = '/conflict with a prepared update/'
                    # Make sure we get the expected prepare conflict message.
                    self.assertRaisesException(wiredtiger.WiredTigerError, lambda:cursor.search(), preparemsg)
            elif timestamp == 30:
                # Keys are visible at timestamp 30.
                self.assertEqual(cursor.search(), 0)

        cursor.close()
        self.session.commit_transaction()

    def prepare_updates(self, ds):

        # Commit some updates to get eviction and history store fired up
        # Insert a ket at timestamp 1
        commit_key = "C"
        commit_value = b"bbbbb" * 100
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, self.nsessions * self.nkeys):
            self.session.begin_transaction('isolation=snapshot')
            key = commit_key + ds.key(self.nrows + i)
            cursor.set_key(key)
            cursor.set_value(commit_value)
            self.assertEquals(cursor.insert(), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(1))
        cursor.close()

        # Call checkpoint
        self.session.checkpoint()

        # Remove the committed key at timestamp 10
        commit_value = b"bbbbb" * 100
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, self.nsessions * self.nkeys):
            self.session.begin_transaction('isolation=snapshot')
            key = commit_key + ds.key(self.nrows + i)
            cursor.set_key(key)
            self.assertEquals(cursor.remove(), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))
        cursor.close()

        hs_writes_start = self.get_stat(stat.conn.cache_write_hs)
        # Have prepared updates in multiple sessions. This should ensure writing
        # prepared updates to the history store
        # Insert the same key at timestamp 20, but with prepare updates.
        sessions = [0] * self.nsessions
        cursors = [0] * self.nsessions
        prepare_value = b"ccccc" * 100
        for j in range (0, self.nsessions):
            sessions[j] = self.conn.open_session()
            sessions[j].begin_transaction('isolation=snapshot')
            cursors[j] = sessions[j].open_cursor(self.uri)
            # Each session will update many consecutive keys.
            start = (j * self.nkeys)
            end = start + self.nkeys
            for i in range(start, end):
                cursors[j].set_key(commit_key + ds.key(self.nrows + i))
                cursors[j].set_value(prepare_value)
                self.assertEquals(cursors[j].insert(), 0)
            sessions[j].prepare_transaction('prepare_timestamp=' + timestamp_str(20))

        hs_writes = self.get_stat(stat.conn.cache_write_hs) - hs_writes_start

        # Assert if not writing anything to the history store.
        self.assertGreaterEqual(hs_writes, 0)

        # Search the key with timestamp 5 and ignore_prepare=false
        self.search_key_timestamp_and_ignore(ds, 5, "false")

        # Search the key with timestamp 20 and ignore_prepare=true
        self.search_key_timestamp_and_ignore(ds, 20, "true")

        # Search the key with timestamp 20 and ignore_prepare=false
        self.search_key_timestamp_and_ignore(ds, 20, "false")

        # Commit the prepared_transactions with timestamp 30 to search below the key
        # and expect it not to return prepared conflict message.
        for j in range (0, self.nsessions):
            sessions[j].commit_transaction(
                'commit_timestamp=' + timestamp_str(30) + ',durable_timestamp=' + timestamp_str(30))

        #self.session.commit_transaction()
        self.session.checkpoint()

        # Simulate a crash by copying to a new directory(RESTART).
        copy_wiredtiger_home(".", "RESTART")

        # Open the new directory.
        self.conn = self.setUpConnectionOpen("RESTART")
        self.session = self.setUpSessionOpen(self.conn)

        # After simulating a crash, search for the keys inserted.

        # Search the key with timestamp 5 and ignore_prepare=false
        self.search_key_timestamp_and_ignore(ds, 5, "false")

        # Search the key with timestamp 20 and ignore_prepare=false.
        self.search_key_timestamp_and_ignore(ds, 20, "true", True)

        # Search the key with timestamp 20 and ignore_prepare=false.
        self.search_key_timestamp_and_ignore(ds, 20, "false", True)

        # Search the key with timestamp 30 and ignore_prepare=false.
        self.search_key_timestamp_and_ignore(ds, 30, "false", True)

        # Search the key with timestamp 30 and ignore_prepare=true.
        self.search_key_timestamp_and_ignore(ds, 30, "true", True)

        # Close all cursors and sessions, this will cause prepared updates to be
        # rollback-ed
        for j in range (0, self.nsessions):
            cursors[j].close()
            sessions[j].close()

    def test_prepare_hs(self):

        ds = SimpleDataSet(self, self.uri, self.nrows, key_format="S", value_format='u')
        ds.populate()
        bigvalue = b"aaaaa" * 100

        # Initially load huge data
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 10000):
            cursor.set_key(ds.key(self.nrows + i))
            cursor.set_value(bigvalue)
            self.assertEquals(cursor.insert(), 0)
        cursor.close()
        self.session.checkpoint()

        # We put prepared updates in multiple sessions so that we do not hang
        # because of cache being full with uncommitted updates.
        self.prepare_updates(ds)

if __name__ == '__main__':
    wttest.run()
