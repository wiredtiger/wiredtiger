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

import wttest
from wiredtiger import stat

class test_txn_pinned_timestamp_stat(wttest.WiredTigerTestCase):

    conn_config = 'statistics=(all)'

    def read_stat(self, stat_key):
        cursor = self.session.open_cursor('statistics:', None, 'statistics=(all)')
        cursor.set_key(stat_key)
        self.assertEqual(cursor.search(), 0)
        val = cursor.get_value()[2]
        cursor.close()
        return val

    # durable > oldest - stat must equal durable - oldest.
    def test_pinned_oldest_normal(self):
        oldest_ts = 10
        commit_ts = 20

        self.conn.set_timestamp(
            f'oldest_timestamp={oldest_ts:x},stable_timestamp={oldest_ts:x}')

        self.session.create('table:stat_test', 'key_format=i,value_format=i')
        cursor = self.session.open_cursor('table:stat_test', None, None)

        self.session.begin_transaction()
        cursor[1] = 1
        self.session.commit_transaction(f'commit_timestamp={commit_ts:x}')
        cursor.close()

        pinned_oldest = self.read_stat(stat.conn.txn_pinned_timestamp_oldest)
        durable = self.read_stat(stat.conn.txn_global_durable_timestamp)
        oldest = self.read_stat(stat.conn.txn_global_oldest_timestamp)

        self.assertGreaterEqual(pinned_oldest, 0,
            f'txn_pinned_timestamp_oldest must be non-negative, got {pinned_oldest}')
        self.assertEqual(durable, commit_ts,
            f'expected durable={commit_ts}, got {durable}')
        self.assertEqual(oldest, oldest_ts,
            f'expected oldest={oldest_ts}, got {oldest}')
        self.assertEqual(pinned_oldest, durable - oldest,
            f'expected pinned_oldest={durable - oldest}, got {pinned_oldest}')

    # Oldest advances past durable - stat must remain non-negative.
    def test_pinned_oldest_underflow_guard(self):
        oldest_ts = 10
        commit_ts = 20
        advanced_oldest_ts = 30

        self.conn.set_timestamp(
            f'oldest_timestamp={oldest_ts:x},stable_timestamp={oldest_ts:x}')

        self.session.create('table:stat_test', 'key_format=i,value_format=i')
        cursor = self.session.open_cursor('table:stat_test', None, None)

        self.session.begin_transaction()
        cursor[1] = 1
        self.session.commit_transaction(f'commit_timestamp={commit_ts:x}')
        cursor.close()

        self.conn.set_timestamp(
            f'oldest_timestamp={advanced_oldest_ts:x},'
            f'stable_timestamp={advanced_oldest_ts:x}')

        oldest = self.read_stat(stat.conn.txn_global_oldest_timestamp)
        self.assertEqual(oldest, advanced_oldest_ts,
            f'expected oldest={advanced_oldest_ts}, got {oldest}')

        pinned_oldest = self.read_stat(stat.conn.txn_pinned_timestamp_oldest)
        self.assertGreaterEqual(pinned_oldest, 0,
            f'txn_pinned_timestamp_oldest must be non-negative, got {pinned_oldest}')

        # Adjacent stats computed in the same function must also be non-negative.
        for key, label in [
            (stat.conn.txn_pinned_timestamp_lag, 'txn_pinned_timestamp_lag'),
            (stat.conn.txn_pinned_timestamp_checkpoint_lag, 'txn_pinned_timestamp_checkpoint_lag'),
            (stat.conn.txn_pinned_timestamp_reader_lag, 'txn_pinned_timestamp_reader_lag'),
        ]:
            val = self.read_stat(key)
            self.assertGreaterEqual(val, 0,
                f'{label} must be non-negative, got {val}')

