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

import fnmatch, os, shutil, threading, time
from wtthread import checkpoint_thread, op_thread
from helper import copy_wiredtiger_home
import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wiredtiger import stat

# test_checkpoint_snapshot02.py
#   This test is to run checkpoint and eviction in parallel with timing
#   stress for checkpoint and let eviction write more data than checkpoint.
#

class test_checkpoint_snapshot02(wttest.WiredTigerTestCase):

    # Create a table.
    uri = "table:test_checkpoint_snapshot02"
    nrows = 100

    def conn_config(self):
        config = 'cache_size=5MB,statistics=(all),statistics_log=(json,on_close,wait=1),log=(enabled=true),timing_stress_for_test=[checkpoint_slow]'
        return config

    def large_updates(self, uri, value, ds, nrows):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction()
        cursor.close()

    def test_checkpoint_snapshot(self):

        ds = SimpleDataSet(self, self.uri, self.nrows, key_format="S", value_format='u')
        ds.populate()
        value = b"aaaaa" * 200

        # Create a checkpoint thread
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        try:
            self.pr("start checkpoint thread")
            ckpt.start()
            self.large_updates(self.uri, value, ds, self.nrows)

        finally:
            done.set()
            ckpt.join()

        #Simulate a crash by copying to a new directory(RESTART).
        copy_wiredtiger_home(".", "RESTART")

        # Open the new directory.
        self.conn = self.setUpConnectionOpen("RESTART")
        self.session = self.setUpSessionOpen(self.conn)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        inconsistent_ckpt = stat_cursor[stat.conn.txn_rts_inconsistent_ckpt][2]
        stat_cursor.close()

        self.assertGreater(inconsistent_ckpt, 0)

if __name__ == '__main__':
    wttest.run()
