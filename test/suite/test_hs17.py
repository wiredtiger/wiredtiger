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

import time, wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

# test_hs17.py
class test_hs17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB'
    session_config = 'isolation=snapshot'

    def test_hs17(self):
        uri = 'table:test_hs17'
        self.session.create(uri, 'key_format=S,value_format=S')
        session2 = self.setUpSessionOpen(self.conn)
        cursor = self.session.open_cursor(uri)
        cursor2 = session2.open_cursor(uri)

        value1 = 'a' * 500
        value2 = 'b' * 500
        value3 = 'c' * 500
        value4 = 'd' * 500
        value5 = 'e' * 500

        # Insert an update at timestamp 5
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Insert another update at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Insert a bunch of other contents to trigger eviction
        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        # Start a long running transaction which could see update 0.
        session2.begin_transaction()

        cursor2.set_key(str(0))

        # Assert we can see a value
        self.assertEqual(cursor2.search(), 0)
        session2.breakpoint()
        val1 = cursor2.get_value()
        cursor2.reset()

        # Commit an update without a timestamp on our original key
        self.session.begin_transaction()
        cursor[str(0)] = value4
        self.session.commit_transaction()

        # Commit an update with timestamp 15
        self.session.begin_transaction()
        cursor[str(0)] = value5
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Insert a bunch of other contents to trigger eviction
        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        cursor2.set_key(str(0))
        # Given the bug exists this will return WT_NOTFOUND.
        session2.breakpoint()
        self.assertEqual(cursor2.search(), 0)
        val2 = cursor2.get_value()
        self.assertEqual(val1, val2)
        session2.breakpoint()

        session2.rollback_transaction()

        # Starting a new transaction will let us a see a value again.
        session2.begin_transaction()
        cursor2.set_key(str(0))
        self.assertEqual(cursor2.search(), 0)