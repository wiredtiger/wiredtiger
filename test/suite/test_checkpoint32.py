#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
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

import threading, time
import wttest
import wiredtiger
from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# FIXME - wrong name
# test_checkpoint32.py
#
# Test reading a cursor when the aggregate time window is visible to the snapshot
# but not all deleted keys on-disk version are not visible.
@wttest.skip_for_hook("tiered", "FIXME-WT-9809 - Fails for tiered")
class test_checkpoint(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,statistics=(all)'

    format_values = [
        # ('column', dict(key_format='r', value_format='S', extraconfig='')),
        ('string_row', dict(key_format='S', value_format='S', extraconfig='')),
    ]
    scenarios = make_scenarios(format_values)

    def large_updates(self, uri, ds, nrows, value, ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, nrows):
            # Avoid a conflict with Txn 1 that inserted this key already.
            if (i == 5000):
                continue
            cursor[ds.key(i)] = value
            if i % 101 == 0:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
                self.session.begin_transaction()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def start_large_updates(self, session, uri, ds, start_key, nrows, value, ts):
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        for i in range(start_key, start_key + nrows + 1):
            cursor[ds.key(i)] = value
            cursor.reset()
        # Purposefully insert a key in the middle of the range to ensure we evict all pages.
        cursor[ds.key(5000)] = value
        cursor.reset()
        return cursor

    def end_large_updates(self, session, cursor, ts):
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.reset()
        cursor.close()

    def evict(self, ds, lo, hi, value, ts):
        evict_cursor = self.session.open_cursor(ds.uri, None, "debug=(release_evict)")
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        # Evict every 10th key. FUTURE: when that's possible, evict each page exactly once.
        for k in range(lo, hi, 10):
            v = evict_cursor[ds.key(k)]
            self.assertEqual(evict_cursor.reset(), 0)
        self.session.rollback_transaction()

    def check(self, session_local, ds, nrows, value, ts):
        cursor = session_local.open_cursor(ds.uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, value)
            count += 1
        self.assertEqual(count, nrows)
        cursor.close()

    def test_checkpoint(self):
        uri = 'table:checkpoint32'
        nrows = 10000

        # Create a table.
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format=self.value_format,
            config=self.extraconfig)
        ds.populate()

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100

        # Pin oldest and stable timestamps to 5.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5) +
            ',stable_timestamp=' + self.timestamp_str(5))

        # Write some data at in Txn 1 at time 10 but don't commit yet. 
        # Write this data starting after nrows so we don't conflict with Txn 2.
        txn1_session = self.conn.open_session()
        c1 = self.start_large_updates(txn1_session, uri, ds, nrows, 1, value_a, 10)
        
        # Start and commit Txn 2 at Time 20
        self.large_updates(uri, ds, nrows, value_b, 20)

        # Start the truncate transaction. Here the snapshot should see Txn 2 but not Txn 1.
        truncate_session = self.conn.open_session()
        truncate_session.begin_transaction()

        # Commit Txn 1. 
        self.end_large_updates(txn1_session, c1, 10)

        # Evict everything at t=30.
        self.evict(ds, 1, nrows + 10, None, 30)

        # Truncate everything (this should be fast truncate following our eviction).
        lo_cursor = truncate_session.open_cursor(uri)
        hi_cursor = truncate_session.open_cursor(uri)

        lo_cursor.set_key(ds.key(0))
        hi_cursor.set_key(ds.key(nrows + 100))
        
        truncate_session.truncate(None, lo_cursor, hi_cursor, None)
        truncate_session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        validate_cursor = self.session.open_cursor(ds.uri)

        # Search for our keys inserted by Txn 1. We expect these to be removed with current behaviour.
        validate_cursor.set_key(ds.key(nrows))
        self.assertEqual(validate_cursor.search(), 0)

        validate_cursor.set_key(ds.key(nrows+1))
        self.assertEqual(validate_cursor.search(), 0)

        validate_cursor.set_key(ds.key(5000))
        self.assertEqual(validate_cursor.search(), 0)

if __name__ == '__main__':
    wttest.run()