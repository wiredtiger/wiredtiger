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
# test_prepare05.py
#   Prepare: Timestamps validation for prepare API's
#

from suite_subprocess import suite_subprocess
import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_prepare05(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_prepare05'
    uri = 'table:' + tablename

    def test_timestamp_api(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri)

        # It is illegal to set a prepare timestamp older than oldest timestamp.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(2))
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.prepare_transaction(
            'prepare_timestamp=' + timestamp_str(1)),
            "/older than the oldest timestamp/")
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Check setting the prepare timestamp same as oldest timestamp is valid.
        self.session.begin_transaction()
        self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(2))
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # In a single transaction it is illegal to set a commit timestamp
        # before invoking prepare for this transaction.
        # Note: Values are not important, setting commit timestamp before
        # prepare itself is illegal.
        self.session.begin_transaction()
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(3))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.prepare_transaction(
            'prepare_timestamp=' + timestamp_str(2)),
            "/should not have been set before/")
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # It is illegal to set a prepare timestamp same as or earlier than an
        # active read timestamp.
        # Start a new reader to have an active read timestamp.
        s_reader = self.conn.open_session()
        s_reader.begin_transaction('read_timestamp=' + timestamp_str(4))
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.prepare_transaction(
            'prepare_timestamp=' + timestamp_str(4)),
            "/must be greater than the latest active read timestamp/")
        self.session.rollback_transaction()

        # Check setting the prepare timestamp as later than active read
        # timestamp is valid.
        self.session.begin_transaction()
        c[1] = 1
        self.session.prepare_transaction(
                'prepare_timestamp=' + timestamp_str(5))
        # Resolve the reader transaction started earlier.
        s_reader.rollback_transaction()
        self.session.rollback_transaction()

        # It is illegal to set a commit timestamp older than prepare
        # timestamp of a transaction.
        self.session.begin_transaction()
        c[1] = 1
        self.session.prepare_transaction(
                'prepare_timestamp=' + timestamp_str(5))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
            'commit_timestamp=' + timestamp_str(4)),
            "/older than the prepare timestamp/")

        # It is legal to set a commit timestamp as same as prepare
        # timestamp.
        self.session.begin_transaction()
        c[1] = 1
        self.session.prepare_transaction(
                'prepare_timestamp=' + timestamp_str(5))
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

if __name__ == '__main__':
    wttest.run()
