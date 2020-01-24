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
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_insert_remove01.py
class test_insert_remove01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=1MB,log=(enabled),statistics=(all),statistics_log=(wait=0,on_close=true)'
    session_config = 'isolation=snapshot'

    def test_insert_and_delete(self):
        ds = SimpleDataSet(self, "table:insert_and_remove", 100000, key_format="i",
            value_format="S", config='log=(enabled=false)')
        ds.populate()

        ds_extra = SimpleDataSet(self, "table:extra", 100000, key_format="i",value_format="S", config='log=(enabled=false)')
        ds_extra.populate()
        
        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))
        
        # Checkpoint to ensure that the history store is gets populated
        self.session.checkpoint()

        # Now that the database contains as much data as will fit into
        # the configured cache, verify removes succeed.
        cursor = self.session.open_cursor("table:insert_and_remove", None)
        self.session.begin_transaction()
        for i in range(1, 10):
            cursor.set_key(ds.key(i))
            cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))
        # Checkpoint to ensure that the history store is gets populated
        self.session.checkpoint()

        cursor_extra = self.session.open_cursor("table:extra", None)
        self.session.begin_transaction()
        for i in range(1, 10000):
            cursor_extra.set_key(ds.key(i))
            cursor_extra.set_value("update")
            cursor_extra.update()
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(12))
        
        # Pin oldest and stable to timestamp 15.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(15) +
            ',stable_timestamp=' + timestamp_str(15))
        
        self.session.begin_transaction()
        for i in range(11, 20):
            cursor.set_key(ds.key(i))
            cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Pin oldest and stable to timestamp 25.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(25) +
            ',stable_timestamp=' + timestamp_str(25))
        
        # Checkpoint to ensure that the history store is gets populated
        self.session.checkpoint()


if __name__ == '__main__':
    wttest.run()
