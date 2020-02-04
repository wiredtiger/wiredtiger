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

# test_rollback_to_stable02.py
# Test that rollback to stable brings back the history value to replace on-disk value.
class test_rollback_to_stable02(test_rollback_to_stable_base):
    # Force a small cache.
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    @unittest.skip("Temporarily disabled")
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
        valueb = "bbbbb" * 100
        valuec = "ccccc" * 100
        valued = "ddddd" * 100
        self.large_updates(uri, valuea, ds, nrows, 10)

        # Check that all updates are seen
        self.check(valuea, uri, nrows, 10)

        self.large_updates(uri, valueb, ds, nrows, 20)

        # Check that the new updates are only seen after the update timestamp
        self.check(valueb, uri, nrows, 20)

        self.large_updates(uri, valuec, ds, nrows, 30)

        # Check that the new updates are only seen after the update timestamp
        self.check(valuec, uri, nrows, 30)

        self.large_updates(uri, valued, ds, nrows, 40)

        # Check that the new updates are only seen after the update timestamp
        self.check(valued, uri, nrows, 40)

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(10))

        # Checkpoint to ensure that all the data is flushed.
        self.session.checkpoint()

        self.conn.rollback_to_stable()

        # Check that the new updates are only seen after the update timestamp
        self.check(valuea, uri, nrows, 40)

if __name__ == '__main__':
    wttest.run()
