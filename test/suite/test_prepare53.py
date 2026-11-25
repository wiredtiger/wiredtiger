#!/usr/bin/env python3
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

import errno, wiredtiger, wttest
from wtscenario import make_scenarios


# Check cursor-bound behavior after prepare conflicts in both directions.
class test_prepare53(wttest.WiredTigerTestCase):
    uri = 'table:prepare_bounds'
    conn_config = 'statistics=(all),log=(enabled=false)'
    scenarios = make_scenarios([
        ('next', dict(forward=True)), ('prev', dict(forward=False))])

    def key(self, rank):
        return f'key{rank if self.forward else 5 - rank}'

    def step(self, cursor):
        return cursor.next() if self.forward else cursor.prev()

    def bound(self, cursor, rank, start=True):
        cursor.set_key(self.key(rank))
        return cursor.bound('bound=' + ('lower' if self.forward == start else 'upper'))

    def write(self, session, ranks, timestamp):
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        for rank in ranks:
            cursor[self.key(rank)] = 'v'
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))
        cursor.close()

    def open_reader(self, ranks):
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write(self.session, ranks, 2)
        self.conn.set_timestamp('stable_timestamp=2')
        self.session.checkpoint()
        connection = self.reader_connection()
        return connection.open_session(), connection.open_session()

    def reader_connection(self):
        return self.conn

    def prepare(self, session, timestamp):
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        cursor[self.key(2)] = 'prepared'
        session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(timestamp))
        cursor.close()

    def assert_conflict(self, cursor):
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.step(cursor), '/WT_PREPARE_CONFLICT/')

    def test_bound_set_rejects_retained_position(self):
        # Set up committed data and a prepared update.
        reader, prepared = self.open_reader([1])
        self.write(reader, [1, 3], 3)
        self.prepare(prepared, 4)

        cursor = reader.open_cursor(self.uri)
        self.assertEqual(self.bound(cursor, 1), 0)
        self.assertEqual(self.bound(cursor, 2, start=False), 0)

        reader.begin_transaction('read_timestamp=5')
        cursor.set_key(self.key(1))
        self.assertEqual(cursor.search(), 0)

        # Reject changing bounds after a prepare conflict.
        self.assert_conflict(cursor)
        with self.expectedStderrPattern('setting bounds on a positioned cursor'):
            self.assertRaises(wiredtiger.WiredTigerError, lambda: self.bound(cursor, 0))
        self.assertEqual(reader.get_last_error()[0], errno.EINVAL)
        self.assert_conflict(cursor)
        prepared.rollback_transaction()

        # Verify the original bounds still exclude the remaining key.
        self.assertEqual(self.step(cursor), wiredtiger.WT_NOTFOUND)
        reader.rollback_transaction()

        cursor.close()
        reader.close()
        prepared.close()

    def test_rejected_bound_preserves_existing_range(self):
        # Set up committed data and a prepared update.
        reader, prepared = self.open_reader([1, 3])
        self.prepare(prepared, 3)

        cursor = reader.open_cursor(self.uri)
        self.assertEqual(self.bound(cursor, 2), 0)
        self.assertEqual(self.bound(cursor, 3, start=False), 0)

        # Reject overlapping bounds after a prepare conflict.
        reader.begin_transaction('read_timestamp=4')
        self.assert_conflict(cursor)
        with self.expectedStderrPattern('provided cursor bounds are overlapping'):
            self.assertRaises(wiredtiger.WiredTigerError, lambda: self.bound(cursor, 4))
        self.assertEqual(reader.get_last_error()[0], errno.EINVAL)

        # Verify a new transaction still reads only the original range.
        reader.commit_transaction()
        reader.begin_transaction('read_timestamp=5')
        prepared.rollback_transaction()
        self.assertEqual(self.step(cursor), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(3), 'v'))
        self.assertEqual(self.step(cursor), wiredtiger.WT_NOTFOUND)
        reader.rollback_transaction()

        cursor.close()
        reader.close()
        prepared.close()

    def test_clear_bounds_on_retained_position(self):
        # Set up committed data and a prepared update.
        reader, prepared = self.open_reader([1])
        self.write(reader, [1, 3], 3)
        self.prepare(prepared, 4)

        cursor = reader.open_cursor(self.uri)
        self.assertEqual(self.bound(cursor, 1), 0)
        self.assertEqual(self.bound(cursor, 2, start=False), 0)

        reader.begin_transaction('read_timestamp=5')
        self.assertEqual(self.step(cursor), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(1), 'v'))

        # Clearing bounds is allowed but does not resolve the prepare conflict.
        self.assert_conflict(cursor)
        self.assertEqual(cursor.bound('action=clear'), 0)
        self.assert_conflict(cursor)

        # After rollback, the scan can return the key beyond the old bounds.
        prepared.rollback_transaction()
        self.assertEqual(self.step(cursor), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(3), 'v'))
        self.assertEqual(self.step(cursor), wiredtiger.WT_NOTFOUND)
        reader.rollback_transaction()

        cursor.close()
        reader.close()
        prepared.close()
