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
#
# test_prepare_cursor03.py
#   Non-layered equivalent of test_layered74 for delete->prepare-conflict paths.

import wiredtiger
import wttest
from wtscenario import make_scenarios


class test_prepare_cursor03(wttest.WiredTigerTestCase):
    tablename = 'test_prepare_cursor03'
    uri = 'table:' + tablename

    resolve_scenarios = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    scenarios = make_scenarios(resolve_scenarios)

    conn_config = 'cache_size=10MB,statistics=(all)'

    def setup_table_with_data(self, keys):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        for key in keys:
            cursor[key] = f'value_{key}'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        cursor.close()

    def delete_key(self, key):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor.set_key(key)
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        cursor.close()

    def prepare_key_in_separate_session(self, key, value, prepare_ts=50):
        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)

        prepare_session.begin_transaction()
        prepare_cursor[key] = value
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(prepare_ts))

        return prepare_session, prepare_cursor

    def resolve_prepare(self, prepare_session):
        if self.commit:
            prepare_session.breakpoint()
            prepare_session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(60) +
                ',durable_timestamp=' + self.timestamp_str(60))
        else:
            prepare_session.rollback_transaction()

    def test_search_near_deleted_then_prepare_conflict(self):
        self.setup_table_with_data([1, 3])
        self.delete_key(1)

        prepare_session, prepare_cursor = self.prepare_key_in_separate_session(2, 'prepared_value')

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))
        cursor.set_key(1)

        # search_near skips deleted key 1 and reaches prepared key 2.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.search_near())
        self.assertEqual(
            cursor.get_key(), 1,
            'Search key should stay set after prepare conflict while skipping tombstone')

        # Retry should not lose the original search key.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.search_near())
        self.assertEqual(cursor.get_key(), 1, 'Search key should stay set across retries')

        self.resolve_prepare(prepare_session)

        # Insert key 1 back and verify search_near now returns that exact key.
        insert_session = self.conn.open_session()
        insert_cursor = insert_session.open_cursor(self.uri)
        insert_session.begin_transaction()
        insert_cursor[1] = 'reinserted_value_1'
        insert_session.commit_transaction('commit_timestamp=' + self.timestamp_str(61))
        insert_cursor.close()
        insert_session.close()

        self.session.rollback_transaction()
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(61))
        cursor.set_key(1)
        self.assertEqual(cursor.search_near(), 0)
        self.assertEqual(cursor.get_key(), 1)
        self.assertEqual(cursor.get_value(), 'reinserted_value_1')

        prepare_cursor.close()
        prepare_session.close()
        cursor.close()
        self.session.rollback_transaction()

    def test_next_deleted_then_prepare_conflict(self):
        self.setup_table_with_data([1, 3, 5])
        self.delete_key(3)

        prepare_session, prepare_cursor = self.prepare_key_in_separate_session(4, 'prepared_value')

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        cursor.set_key(1)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_key(), 1)

        # next skips deleted key 3, then encounters prepared key 4.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.next())
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 1, 'Cursor should remain usable after prepare conflict')

        self.resolve_prepare(prepare_session)

        if self.commit:
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), 4)
            self.assertEqual(cursor.get_value(), 'prepared_value')
        else:
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), 5)
            self.assertEqual(cursor.get_value(), 'value_5')

        prepare_cursor.close()
        prepare_session.close()
        cursor.close()
        self.session.rollback_transaction()

    def test_prev_deleted_then_prepare_conflict(self):
        self.setup_table_with_data([1, 3, 5])
        self.delete_key(3)

        prepare_session, prepare_cursor = self.prepare_key_in_separate_session(2, 'prepared_value')

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        cursor.set_key(5)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_key(), 5)

        # prev skips deleted key 3, then encounters prepared key 2.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.prev())
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 5, 'Cursor should remain usable after prepare conflict')

        self.resolve_prepare(prepare_session)

        if self.commit:
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), 2)
            self.assertEqual(cursor.get_value(), 'prepared_value')
        else:
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), 1)
            self.assertEqual(cursor.get_value(), 'value_1')

        prepare_cursor.close()
        prepare_session.close()
        cursor.close()
        self.session.rollback_transaction()
