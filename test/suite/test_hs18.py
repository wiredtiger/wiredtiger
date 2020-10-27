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

import time, wiredtiger, wttest, unittest

def timestamp_str(t):
    return '%x' % t

# test_hs18.py
# Test various older reader scenarios
class test_hs18(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB,eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'

    def start_txn(self, sessions, cursors, values, i):
        # Start a transaction that will see update 0.
        sessions[i].begin_transaction()
        cursors[i].set_key(str(0))
        self.assertEqual(cursors[i].search(), 0)
        self.assertEqual(cursors[i].get_value(), values[i])
        cursors[i].reset()

    def test_hs18(self):
        uri = 'table:test_hs18'
        self.session.create(uri, 'key_format=S,value_format=S')
        session2 = self.setUpSessionOpen(self.conn)
        cursor = self.session.open_cursor(uri)
        cursor2 = session2.open_cursor(uri)

        value0 = 'f' * 500
        value1 = 'a' * 500
        value2 = 'b' * 500
        value3 = 'c' * 500
        value4 = 'd' * 500
        value5 = 'e' * 500

        # Insert an update at timestamp 3
        self.session.begin_transaction()
        cursor[str(0)] = value0
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Start a long running transaction which could see update 0.
        session2.begin_transaction()
        cursor2.set_key(str(0))
        # Assert we can see a value
        self.assertEqual(cursor2.search(), 0)
        val1 = cursor2.get_value()
        cursor2.reset()

        # Insert an update at timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Insert another update at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Insert a bunch of contents to fill the cache
        for i in range(2000, 10000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = value4
        self.session.commit_transaction()

        # Commit an update with timestamp 15
        self.session.begin_transaction()
        cursor[str(0)] = value5
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        cursor2.set_key(str(0))
        # Given the bug exists this will return WT_NOTFOUND.
        self.assertEqual(cursor2.search(), 0)
        val2 = cursor2.get_value()
        self.assertEqual(val1, val2)

        # Let go of the page since we want to evict.
        cursor2.reset()

        # Insert a bunch of other contents to trigger eviction
        for i in range(10001, 11000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        cursor2.set_key(str(0))
        # Given the bug exists this will return WT_NOTFOUND.
        self.assertEqual(cursor2.search(), 0)
        val2 = cursor2.get_value()
        self.assertEqual(val1, val2)
        session2.rollback_transaction()

    # Test that forces us to ignore tombstone in order to not remove the first non timestamped updated.
    def test_ignore_tombstone(self):
        uri = 'table:test_ignore_tombstone'
        self.session.create(uri, 'key_format=S,value_format=S')
        session2 = self.setUpSessionOpen(self.conn)
        cursor = self.session.open_cursor(uri)
        cursor2 = session2.open_cursor(uri)
        value0 = 'A' * 500
        value1 = 'a' * 500
        value2 = 'b' * 500
        value3 = 'c' * 500
        value4 = 'd' * 500

        # Insert an update without a timestamp
        self.session.begin_transaction()
        cursor[str(0)] = value0
        self.session.commit_transaction()

        # Start a long running transaction which could see update 0.
        session2.begin_transaction()

        # Assert we can see a value
        cursor2.set_key(str(0))
        self.assertEqual(cursor2.search(), 0)
        self.assertEqual(value0, cursor2.get_value())
        cursor2.reset()

        # Insert an update at timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Insert another update at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Insert a bunch of other contents to trigger eviction
        for i in range(1, 10000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        # Ensure our update exists
        cursor2.set_key(str(0))
        self.assertEqual(cursor2.search(), 0)
        self.assertEqual(value0, cursor2.get_value())
        cursor2.reset()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = value4
        self.session.commit_transaction()

        # Insert a bunch of other contents to trigger eviction
        for i in range(10000, 11000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        cursor2.set_key(str(0))
        # Given the bug exists this will return WT_NOTFOUND.
        self.assertEqual(cursor2.search(), 0)
        self.assertEqual(value0, cursor2.get_value())

    # Test older readers for each of the updates moved to the history store.
    def test_multiple_older_readers(self):
        uri = 'table:test_multiple_older_readers'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)

        # The ID of the session corresponds the value it should see.
        sessions = []
        cursors = []
        values = []
        for i in range(0, 5):
            sessions.append(self.setUpSessionOpen(self.conn))
            cursors.append(sessions[i].open_cursor(uri))
            values.append(str(i) * 10)

        value_junk = 'aaaaa' * 100

        # Insert an update at timestamp 3
        self.session.begin_transaction()
        cursor[str(0)] = values[0]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Start a transaction that will see update 0.
        self.start_txn(sessions, cursors, values, 0)

        # Insert an update at timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = values[1]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Start a transaction that will see update 1.
        self.start_txn(sessions, cursors, values, 1)

        # Insert another update at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = values[2]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Start a transaction that will see update 2.
        self.start_txn(sessions, cursors, values, 2)

        # Insert a bunch of other contents to trigger eviction
        for i in range(1000, 10000):
            self.session.begin_transaction()
            cursor[str(i)] = value_junk
            self.session.commit_transaction()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = values[3]
        self.session.commit_transaction()

        # Start a transaction that will see update 3.
        self.start_txn(sessions, cursors, values, 3)

        # Commit an update with timestamp 15
        self.session.begin_transaction()
        cursor[str(0)] = values[4]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Insert a bunch of other contents to trigger eviction
        for i in range(10001, 20000):
            self.session.begin_transaction()
            cursor[str(i)] = value_junk
            self.session.commit_transaction()

        # Validate all values are visible and correct.
        for i in range(0, 3):
            cursors[i].set_key(str(0))
            self.assertEqual(cursors[i].search(), 0)
            self.assertEqual(cursors[i].get_value(), values[i])
            cursors[i].reset()

    def test_multiple_older_readers_with_multiple_mixed_mode(self):
        uri = 'table:test_multiple_older_readers'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)

        # The ID of the session corresponds the value it should see.
        sessions = []
        cursors = []
        values = []
        for i in range(0, 9):
            sessions.append(self.setUpSessionOpen(self.conn))
            cursors.append(sessions[i].open_cursor(uri))
            values.append(str(i) * 10)

        value_junk = 'aaaaa' * 100

        # Insert an update at timestamp 3
        self.session.begin_transaction()
        cursor[str(0)] = values[0]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Start a transaction that will see update 0.
        self.start_txn(sessions, cursors, values, 0)

        # Insert an update at timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = values[1]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Start a transaction that will see update 1.
        self.start_txn(sessions, cursors, values, 1)

        # Insert another update at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = values[2]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Start a transaction that will see update 2.
        self.start_txn(sessions, cursors, values, 2)

        # Insert a bunch of other contents to trigger eviction
        for i in range(1000, 10000):
            self.session.begin_transaction()
            cursor[str(i)] = value_junk
            self.session.commit_transaction()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = values[3]
        self.session.commit_transaction()

        # Start a transaction that will see update 4.
        self.start_txn(sessions, cursors, values, 3)

        # Commit an update with timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = values[4]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Start a transaction that will see update 5.
        self.start_txn(sessions, cursors, values, 4)

        # Commit an update with timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = values[5]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Start a transaction that will see update 6.
        self.start_txn(sessions, cursors, values, 5)

        # Commit an update with timestamp 15
        self.session.begin_transaction()
        cursor[str(0)] = values[6]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Start a transaction that will see update 7.
        self.start_txn(sessions, cursors, values, 6)

        # Insert a bunch of other contents to trigger eviction
        for i in range(10001, 20000):
            self.session.begin_transaction()
            cursor[str(i)] = value_junk
            self.session.commit_transaction()

        # Validate all values are visible and correct.
        for i in range(0, 6):
            cursors[i].set_key(str(0))
            self.assertEqual(cursors[i].search(), 0)
            self.assertEqual(cursors[i].get_value(), values[i])
            cursors[i].reset()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = values[7]
        self.session.commit_transaction()

        # Start a transaction that will see update 8.
        self.start_txn(sessions, cursors, values, 7)

        # Commit an update with timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = values[8]
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Insert a bunch of other contents to trigger eviction
        for i in range(10001, 20000):
            self.session.begin_transaction()
            cursor[str(i)] = values[3]
            self.session.commit_transaction()

        # Validate all values are visible and correct.
        for i in range(0, 7):
            cursors[i].set_key(str(0))
            self.assertEqual(cursors[i].search(), 0)
            self.assertEqual(cursors[i].get_value(), values[i])
            cursors[i].reset()