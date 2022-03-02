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
# test_timestamp08.py
#   Timestamps: API
#

from suite_subprocess import suite_subprocess
import wiredtiger, wttest

class test_timestamp08(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp08'
    uri = 'table:' + tablename

    def test_timestamp_api(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri)

        # Begin by adding some data.
        self.session.begin_transaction()
        c[1] = 1
        self.session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(1))

        # Check that we can set the commit timestamp through either the
        # string or numeric APIs.
        self.session.begin_transaction()
        self.session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(3))
        c[2] = 2

        # Commit timestamp can be equal to the first...
        self.session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_COMMIT, 3)

        # Or greater.
        self.session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_COMMIT, 4)

        # In a single transaction it is illegal to set a commit timestamp
        # older than the first commit timestamp used for this transaction.
        # Check this with both APIs.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(2)),
                '/older than the first commit timestamp/')

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_COMMIT, 2),
                '/older than the first commit timestamp/')
        self.session.rollback_transaction()

        # Commit timestamp >= Oldest timestamp
        self.session.begin_transaction()
        c[3] = 3
        self.session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(3))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(3))

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_COMMIT, 2),
                '/less than the oldest timestamp/')

        # Commit timestamp <= Stable timestamp.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(6))
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_COMMIT, 5),
                '/less than the stable timestamp/')
        self.session.rollback_transaction()

        # read things back out
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(3))
        self.assertEqual(c[1], 1)
        c.set_key(2)
        self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(c[3], 3)
        self.session.commit_transaction()

if __name__ == '__main__':
    wttest.run()
