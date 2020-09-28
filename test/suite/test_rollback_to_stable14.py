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
import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wiredtiger import stat
from wtscenario import make_scenarios
from test_rollback_to_stable01 import test_rollback_to_stable_base

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable05.py
# Test that rollback to stable works without stable timestamp.
class test_rollback_to_stable05(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    in_memory_values = [
        ('no_inmem', dict(in_memory=False)),
        ('inmem', dict(in_memory=True))
    ]

    scenarios = make_scenarios(in_memory_values)

    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all)'
        if self.in_memory:
            config += ',in_memory=true'
        else:
            config += ',log=(enabled),in_memory=false'
        return config

    def test_rollback_to_stable(self):
        nrows = 1000

        # Create two tables without logging.
        uri_1 = "table:rollback_to_stable14_1"
        ds_1 = SimpleDataSet(
            self, uri_1, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds_1.populate()

        uri_2 = "table:rollback_to_stable14_2"
        ds_2 = SimpleDataSet(
            self, uri_2, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds_2.populate()

        # Pin oldest to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))

        valuea = "aaaaa" * 100
        valueb = "bbbbb" * 100
        valuec = "ccccc" * 100
        valued = "ddddd" * 100
        self.large_updates(uri_1, valuea, ds_1, nrows, 0)
        self.check(valuea, uri_1, nrows, 0)

        self.large_updates(uri_2, valuea, ds_2, nrows, 0)
        self.check(valuea, uri_2, nrows, 0)

        # Start a long running transaction and keep it open.
        session_2 = self.conn.open_session()
        session_2.begin_transaction('isolation=snapshot')

        self.large_updates(uri_1, valueb, ds_1, nrows, 0)
        self.check(valueb, uri_1, nrows, 0)

        self.large_updates(uri_1, valuec, ds_1, nrows, 0)
        self.check(valuec, uri_1, nrows, 0)

        self.large_updates(uri_1, valued, ds_1, nrows, 0)
        self.check(valued, uri_1, nrows, 0)

        # Add updates to the another table.
        self.large_updates(uri_2, valueb, ds_2, nrows, 0)
        self.check(valueb, uri_2, nrows, 0)

        self.large_updates(uri_2, valuec, ds_2, nrows, 0)
        self.check(valuec, uri_2, nrows, 0)

        self.large_updates(uri_2, valued, ds_2, nrows, 0)
        self.check(valued, uri_2, nrows, 0)

        # Checkpoint to ensure that all the data is flushed.
        if not self.in_memory:
            self.session.checkpoint()

        # Clear all running transactions before rollback to stable.
        session_2.commit_transaction()
        session_2.close()

        self.conn.rollback_to_stable()
        self.check(valued, uri_1, nrows, 0)
        self.check(valued, uri_2, nrows, 0)

if __name__ == '__main__':
    wttest.run()

