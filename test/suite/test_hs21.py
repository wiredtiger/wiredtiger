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

import time, wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# test_hs21.py
# Test out of order timestamp and mixed mode handing in history store reconciliation
def timestamp_str(t):
    return '%x' % t

class test_hs21(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=10MB,statistics=(all)'
    session_config = 'isolation=snapshot'
    update_types = [
        ("update", dict(isDelete=False)),
        ("tombstone", dict(isDelete=True)),
    ]
    timestamps = [
        ("out-of-order10", dict(ts=10)),
        ("out-of-order20", dict(ts=20)),
        ("out-of-order30", dict(ts=30)),
        ("mixed-mode", dict(ts=0)),
    ]
    scenarios = make_scenarios(timestamps, update_types)

    def test_oldest_update_fix_out_of_order(self):
        uri = 'table:test_hs21'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        largerValue = 'a' * 1000
        values = ['a', 'b', 'c', 'd', 'e']

        # Update a value at 10
        self.session.begin_transaction()
        cursor[str(0)] = values[0]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Update a value at 20
        self.session.begin_transaction()
        cursor[str(0)] = values[1]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Update a value at 30
        self.session.begin_transaction()
        cursor[str(0)] = values[2]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        # Do a checkpoint to ensure we move value1 and value2 to the history store
        self.session.checkpoint()

        # Large updates to evict the page
        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = largerValue
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        self.session.begin_transaction()
        if self.isDelete:
            cursor.set_key(str(0))
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.remove(), 0)
        else:
            cursor[str(0)] = values[3]
        if self.ts == 0:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(self.ts))

        # Update a value at 40
        self.session.begin_transaction()
        cursor[str(0)] = values[4]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(40))

        # Make the out of order update globally visible
        if self.ts != 0:
            self.conn.set_timestamp(
                'oldest_timestamp=' + timestamp_str(self.ts) + ',stable_timestamp=' + timestamp_str(self.ts))

        # Do a checkpoint to trigger history store reconcilation
        self.session.checkpoint()

        # Check the stats that history store records have been fixed.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        hs_removed = stat_cursor[stat.conn.cache_hs_order_remove][2]
        hs_reinserted = stat_cursor[stat.conn.cache_hs_order_reinsert][2]
        stat_cursor.close()

        removed = (30 - self.ts)/10 - 1 if (30 - self.ts)/10 - 1 > 0 else 0
        self.assertEqual(hs_removed, removed)
        self.assertEqual(hs_reinserted, removed)

    def test_insert_hs_fix_out_of_order(self):
        uri = 'table:test_hs21'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        largerValue = 'a' * 1000
        values = ['a', 'b', 'c', 'd', 'e']

        # Pin a session
        session2 = self.conn.open_session(self.session_config)
        session2.begin_transaction()

        # Update a value at 10
        self.session.begin_transaction()
        cursor[str(0)] = values[0]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Update a value at 20
        self.session.begin_transaction()
        cursor[str(0)] = values[1]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Update a value at 30
        self.session.begin_transaction()
        cursor[str(0)] = values[2]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        # Do a checkpoint to ensure we move value1 and value2 to the history store
        self.session.checkpoint()

        # Large updates to evict the page
        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = largerValue
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        self.session.begin_transaction()
        if self.isDelete:
            cursor.set_key(str(0))
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.remove(), 0)
        else:
            cursor[str(0)] = values[3]
        if self.ts == 0:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(self.ts))

        # Update a value at 40
        self.session.begin_transaction()
        cursor[str(0)] = values[4]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(40))

        # Do a checkpoint to trigger history store reconcilation
        self.session.checkpoint()

        # Check the stats that history store records have been fixed.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        hs_removed = stat_cursor[stat.conn.cache_hs_order_remove][2]
        hs_reinserted = stat_cursor[stat.conn.cache_hs_order_reinsert][2]
        stat_cursor.close()

        removed = removed = (30 - self.ts)/10 - 1 if (30 - self.ts)/10 - 1 > 0 else 0
        self.assertEqual(hs_removed, removed)
        self.assertEqual(hs_reinserted, removed)

        session2.rollback_transaction()
