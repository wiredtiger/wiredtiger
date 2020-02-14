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
from test_rollback_to_stable01 import test_rollback_to_stable_base

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable03.py
# Test that rollback to stable clears the history store updates from reconciled pages.
class test_rollback_to_stable01(test_rollback_to_stable_base):
    conn_config = 'cache_size=4GB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_rollback_to_stable(self):
        nrows = 1000

        # Create a table without logging.
        uri = "table:rollback_to_stable03"
        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        valuea = "aaaaa" * 100
        valueb = "bbbbb" * 100
        self.large_updates(uri, valuea, ds, nrows, 10)
        # Check that all updates are seen
        self.check(valuea, uri, nrows, 10)

        # Remove all keys with newer timestamp
        self.large_updates(uri, valueb, ds, nrows, 20)
        # Check that all updates are seen
        self.check(valueb, uri, nrows, 20)

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(10))
        # Checkpoint to ensure that all the updates are flushed to disk.
        self.session.checkpoint()

        self.conn.rollback_to_stable()
        # Check that the old updates are only seen even with the update timestamp
        self.check(valuea, uri, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        upd_aborted = (stat_cursor[stat.conn.txn_rts_upd_aborted][2] +
            stat_cursor[stat.conn.txn_rts_hs_removed][2])
        stat_cursor.close()
        self.assertEqual(calls, 1)
        self.assertTrue(upd_aborted == nrows * 2)

if __name__ == '__main__':
    wttest.run()
