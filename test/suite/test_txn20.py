#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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
# test_txn20.py
#   Transactions: more granular testing of isolation levels
#

import wiredtiger, wttest
from suite_subprocess import suite_subprocess
from wiredtiger import stat

class test_txn20(wttest.WiredTigerTestCase):
    uri = 'table:test_txn'

    def test_read_uncommitted(self):
        # Make an update and don't commit it just yet.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None)
        self.session.begin_transaction()
        cursor['key: aaa'] = 'value: bbb'

        # Now begin a new transaction with the 'read-uncommitted' isolation level.
        # Unlike 'read-committed' and 'snapshot' isolation levels, we expect to
        # see the update above even though the transaction has not been committed.
        s = self.conn.open_session()
        cursor = s.open_cursor(self.uri, None)
        s.begin_transaction("isolation=read-uncommitted")
        cursor.set_key('key: aaa')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'value: bbb')

        # Commit the update now. We should still see it from our second session.
        self.session.commit_transaction()
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'value: bbb')

        s.commit_transaction()
        s.close()

    def test_read_committed(self):
        # Make an update and don't commit it just yet.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None)
        self.session.begin_transaction()
        cursor['key: aaa'] = 'value: bbb'

        # Now begin a new transaction with the 'read-committed' isolation level.
        # We shouldn't see the update above since it hasn't been committed yet.
        s = self.conn.open_session()
        cursor = s.open_cursor(self.uri, None)
        s.begin_transaction('isolation=read-committed')
        cursor.set_key('key: aaa')
        self.assertNotEqual(cursor.search(), 0)

        # Commit the update now.
        # We should now see it from our 'read-committed' transaction.
        self.session.commit_transaction()
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'value: bbb')

        s.commit_transaction()
        s.close()

    def test_read_snapshot(self):
        # Make an update and don't commit it just yet.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None)
        self.session.begin_transaction()
        cursor['key: aaa'] = 'value: bbb'

        # Now begin a new transaction with the 'snapshot' isolation level.
        # We should never see the update above regardless of what happens
        # from here on since it wasn't committed at the time of the snapshot.
        s = self.conn.open_session()
        cursor = s.open_cursor(self.uri, None)
        s.begin_transaction('isolation=snapshot')
        cursor.set_key('key: aaa')
        self.assertNotEqual(cursor.search(), 0)

        # Commit the update now.
        # We still shouldn't see the value since it wasn't part of the initial
        # snapshot.
        self.session.commit_transaction()
        self.assertNotEqual(cursor.search(), 0)

        s.commit_transaction()
        s.close()

if __name__ == '__main__':
    wttest.run()
