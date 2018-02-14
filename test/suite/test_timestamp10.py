#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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
# test_timestamp10.py
#   Timestamps: Saving and querying the checkpoint recovery timestamp
#

import fnmatch, os, shutil
from suite_subprocess import suite_subprocess
import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_timestamp10(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config = 'config_base=false,create,log=(enabled)'
    coll_uri = 'table:collection10'
    oplog_uri = 'table:oplog10'

    def copy_dir(self, olddir, newdir):
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
        # close the original connection.
        self.close_conn()

    def test_timestamp_recovery(self):
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        self.session.create(self.oplog_uri, 'key_format=i,value_format=i')
        self.session.create(self.coll_uri, 'key_format=i,value_format=i,log=(enabled=false)')
        c_op = self.session.open_cursor(self.oplog_uri)
        c_coll = self.session.open_cursor(self.coll_uri)

        # Begin by adding some data.
        max = 10
        ts = max - 3
        for i in range(1,max):
          self.session.begin_transaction()
          c_op[i] = i
          c_coll[i] = i
          self.session.commit_transaction(
            'commit_timestamp=' + timestamp_str(i))

        # Set the oldest and stable timestamp a bit earlier than the data
        # we inserted. Take a checkpoint to the stable timestamp.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(ts) +
            ',stable_timestamp=' + timestamp_str(ts))

        self.session.checkpoint()

        # Copy to a new database and then recover.
        self.copy_dir(".", "RESTART")
        new_conn = self.wiredtiger_open("RESTART", self.conn_config)
        # Query the recovery timestamp and verify the data in the new database.
        new_session = new_conn.open_session()
        self.assertTimestampsEqual(new_conn.query_timestamp('get=recovery'), timestamp_str(ts))
        
        c_op = new_session.open_cursor(self.oplog_uri)
        c_coll = new_session.open_cursor(self.coll_uri)
        for i in range(1,max):
            self.assertEquals(c_op[i], i)
            c_coll.set_key(i)
            if i <= ts:
                self.assertEquals(c_coll[i], i)
            else:
                self.assertEqual(c_coll.search(), wiredtiger.WT_NOTFOUND)

if __name__ == '__main__':
    wttest.run()
