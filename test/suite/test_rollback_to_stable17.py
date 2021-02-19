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
from wtscenario import make_scenarios
def timestamp_str(t):
    return '%x' % t
# test_rollback_to_stable16.py
# Test that roll back to stable handles updates present on disk for variable length column store.
# Attempt to evict to test on disk values.
class test_rollback_to_stable17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=2MB,statistics=(all)'
    session_config = 'isolation=snapshot'
    key_format_values = [
        ('column', dict(key_format='r')),
        # ('integer', dict(key_format='i')),
    ]
    value_format_values = [
        # Fixed length
        # ('fixed', dict(value_format='8t')),
        # Variable length
        ('variable', dict(value_format='S')),
    ]
    scenarios = make_scenarios(key_format_values, value_format_values)

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
            self.assertEqual(v, check_value + str(count +1))
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)

    def test_rollback_to_stable(self):
        # Create a table.
        uri = "table:rollback_to_stable17"
        nrows = 200
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(uri, create_params)
        cursor =  self.session.open_cursor(uri)
        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))
        value20 = "aaaa"
        value30 = "bbbb"
        value40 = "cccc"
        value50 = "dddd"

        #Insert value20 at timestamp 2
        for i in range(1, nrows):
            self.session.begin_transaction()
            cursor[i] = value20 + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))
        #First Update to value 30 at timestamp 5
        for i in range(1, nrows):
            self.session.begin_transaction()
            cursor[i] = value30 + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))
        #Second Update to value40 at timestamp 7
        for i in range(1, nrows):
            self.session.begin_transaction()
            cursor[i] = value40 + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(7))
        # Third Update to value50 at timestamp 9
        for i in range(1, nrows):
            self.session.begin_transaction()
            cursor[i] = value50 + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(9))

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(2))

        self.session.checkpoint()

        #Set stable timestamp to 7
        # self.conn.rollback_to_stable()

        self.simulate_crash_restart(".", "RESTART")

        #Check that only value30 is available
    #    self.check(value20, uri, nrows - 1, 5)
        self.check(value20, uri, nrows - 1, 2)
        self.check(value20, uri, nrows - 1, 7)
        self.check(value20, uri, nrows - 1, 9)
        # stat_cursor = self.session.open_cursor('statistics:', None, None)
        # calls = stat_cursor[stat.conn.txn_rts][2]
        # upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        # stat_cursor.close()
        # self.assertEqual(upd_aborted, (nrows*2) - 2)
        # self.assertEqual(calls, 2)
        # self.session.checkpoint()
        self.session.close()
if __name__ == '__main__':
    wttest.run()