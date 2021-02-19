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
from helper import copy_wiredtiger_home
import wiredtiger, wttest, unittest
from wiredtiger import stat
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable17.py
# Test that rollback to stable handles updates present on history store and data store for variable length column store.
class test_rollback_to_stable17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=20MB,statistics=(all)'
    session_config = 'isolation=snapshot'

    def insert_update_data_at_given_timestamp(self, uri, value, start_row, end_row, timestamp):
        cursor =  self.session.open_cursor(uri)
        for i in range(start_row, end_row):
            self.session.begin_transaction()
            cursor[i] = value
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(timestamp))
        cursor.close()

    def simulate_crash_restart(self, olddir, newdir):
        ''' Simulate a crash from olddir and restart in newdir. '''
        # with the connection still open, copy files to new directory
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        for fname in os.listdir(olddir):
            fullname = os.path.join(olddir, fname)
            # Skip lock file on Windows since it is locked
            if os.path.isfile(fullname) and \
                "WiredTiger.lock" not in fullname and \
                "Tmplog" not in fullname and \
                "Preplog" not in fullname:
                shutil.copy(fullname, newdir)
        #
        # close the original connection and open to new directory
        # NOTE:  This really cannot test the difference between the
        # write-no-sync (off) version of log_flush and the sync
        # version since we're not crashing the system itself.
        #
        self.close_conn()
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        if read_ts == 0:
            session.begin_transaction()
        else:
            session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value)
            count += 1
        session.commit_transaction()
        # self.assertEqual(count, nrows)

    def test_rollback_to_stable(self):
        # Create a table.
        uri = "table:rollback_to_stable17"
        nrows = 200
        create_params = 'key_format=r,value_format=S'
        self.session.create(uri, create_params)

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        value20 = "aaaa"
        value30 = "bbbb"
        value40 = "cccc"
        value50 = "dddd"

        self.insert_update_data_at_given_timestamp(uri, value20, 1, 200, 2)
        self.insert_update_data_at_given_timestamp(uri, value30, 1, 200, 5)
        self.insert_update_data_at_given_timestamp(uri, value40, 1, 200, 7)
        self.insert_update_data_at_given_timestamp(uri, value50, 1, 200, 9)

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(2))
        self.session.checkpoint()
        self.simulate_crash_restart(".", "RESTART")

        self.check(value20, uri, nrows - 1, 2)
        self.check(value20, uri, nrows - 1, 7)
        self.check(value20, uri, nrows - 1, 9)

        self.session.close()
if __name__ == '__main__':
    wttest.run()