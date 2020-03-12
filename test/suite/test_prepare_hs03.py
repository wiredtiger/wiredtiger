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
import unittest, wiredtiger, wttest
from wtdataset import SimpleDataSet
import os, shutil

def timestamp_str(t):
    return '%x' % t

# test_prepare_hs03.py
# test to ensure salvage, verify & simulating crash are working for prepared transactions.
class test_prepare_hs03(wttest.WiredTigerTestCase):
    # Force a small cache.
    conn_config = 'cache_size=50MB'

    # Create a small table.
    uri = "table:test_prepare_hs03"

    def corrupt_file(self):
        filename="test_prepare_hs03.wt"
        self.assertEquals(os.path.exists(filename), True)

        with open(filename, 'r+') as log:
            log.seek(1024)
            log.write('Bad!' * 1024)

    def corrupt_salvage_verify(self):
        self.corrupt_file()
        self.session.salvage(self.uri, "force")
        self.session.verify(self.uri, None)

    def prepare_updates(self, ds, nrows, nsessions, nkeys):
        # Insert some records with commit timestamp, corrupt file and call salvage, verify before checkpoint.

        # Commit some updates to get eviction and history store fired up
        bigvalue1 = b"bbbbb" * 2
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, nkeys):
            self.session.begin_transaction('isolation=snapshot')
            tkey = 'C' + ds.key(nrows + i)
            cursor.set_key(tkey)
            cursor.set_value(bigvalue1)
            self.assertEquals(cursor.insert(), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(1))
        cursor.close()

        # Corrupt the table, Call salvage to recover data from the corrupted table and call verify
        self.corrupt_salvage_verify()

        # Call checkpoint
        self.session.checkpoint()

        # Corrupt the table, Call salvage to recover data from the corrupted table and call verify
        self.corrupt_salvage_verify()

        # Have prepared updates in multiple sessions. This should ensure writing
        # prepared updates to the history store
        sessions = [0] * nsessions
        cursors = [0] * nsessions
        bigvalue2 = b"ccccc" * 2
        for j in range (0, nsessions):
            sessions[j] = self.conn.open_session()
            sessions[j].begin_transaction('isolation=snapshot')
            cursors[j] = sessions[j].open_cursor(self.uri)
            # Each session will update many consecutive keys.
            start = (j * nkeys)
            end = start + nkeys
            for i in range(start, end):
                tkey = 'P' + ds.key(nrows + i)
                cursors[j].set_key(tkey)
                cursors[j].set_value(bigvalue2)
                self.assertEquals(cursors[j].insert(), 0)
            sessions[j].prepare_transaction('prepare_timestamp=' + timestamp_str(4))

        # Testing if we can read prepared updates from the history store.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + timestamp_str(2))
        for i in range(1, nsessions * nkeys):
            tkey = 'P' + ds.key(nrows + i)
            cursor.set_key(tkey)
            # The search should fail i.e, WT_NOTFOUND
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

        # Close all cursors and sessions, this will cause prepared updates to be
        # rollback-ed
        for j in range (0, nsessions):
            cursors[j].close()
            sessions[j].close()

        self.corrupt_salvage_verify()

        self.session.commit_transaction()
        self.session.checkpoint()

        self.corrupt_salvage_verify()

        # Finally, search for the keys inserted with commit timestamp
        cursor = self.session.open_cursor(self.uri)
        self.pr('Read Keys')
        self.session.begin_transaction('read_timestamp=' + timestamp_str(1))
        for i in range(1, nkeys):
            tkey = 'C' + ds.key(nrows + i)
            cursor.set_key(tkey)
            # The search should pass
            self.assertEqual(cursor.search(), 0)
        cursor.close()

        self.session.commit_transaction()
        self.session.checkpoint()

        # Simulate a crash by copying to a new directory(RESTART).
        copy_wiredtiger_home(".", "RESTART")

        # Open the new directory.
        self.conn = self.setUpConnectionOpen("RESTART")
        self.session = self.setUpSessionOpen(self.conn)
        cursor = self.session.open_cursor(self.uri)
        
        self.session.begin_transaction('read_timestamp=' + timestamp_str(1))
        for i in range(1, nkeys):
            tkey = 'C' + ds.key(nrows + i)
            cursor.set_key(tkey)
            # The search should pass
            self.assertEqual(cursor.search(), 0)
        cursor.close()
        self.session.commit_transaction()

        # After simulating a crash, corrupt the table, call salvage to recover data from the corrupted table
        # and call verify
        self.corrupt_salvage_verify()

    def test_prepare_hs(self):
        nrows = 100
        ds = SimpleDataSet(self, self.uri, nrows, key_format="S", value_format='u')
        ds.populate()
        bigvalue = b"aaaaa" * 100

        # Initially load huge data
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 10000):
            cursor.set_key(ds.key(nrows + i))
            cursor.set_value(bigvalue)
            self.assertEquals(cursor.insert(), 0)
        cursor.close()
        self.session.checkpoint()

        # Check if the history store is working properly with prepare transactions.
        # We put prepared updates in multiple sessions so that we do not hang
        # because of cache being full with uncommitted updates.
        nsessions = 3
        nkeys = 4000
        self.prepare_updates(ds, nrows, nsessions, nkeys)

if __name__ == '__main__':
    wttest.run()
