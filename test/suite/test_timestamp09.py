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
# test_timestamp09.py
#   Timestamps: API
#

from suite_subprocess import suite_subprocess
import wiredtiger, wttest

class test_timestamp09(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp09'
    uri = 'table:' + tablename

    def test_timestamp_txn_api(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri)

        # Begin by adding some data.
        self.session.begin_transaction()
        c[1] = 1
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))

        # In a single transaction it is illegal to set a commit timestamp
        # older than the first commit timestamp used for this transaction.
        # Check both timestamp_transaction and commit_transaction APIs.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(3))
        c[3] = 3
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(2)),
                '/older than the first commit timestamp/')
        self.session.rollback_transaction()

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(4))
        c[4] = 4
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(3)),
                '/older than the first commit timestamp/')

        # Commit timestamp >= stable timestamp
        # Check both timestamp_transaction and commit_transaction APIs.
        self.session.begin_transaction()
        c[3] = 3
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(3))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(3))

        self.session.begin_transaction()
        c[2] = 2
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(2)),
                '/after the stable timestamp/')
        self.session.rollback_transaction()

        self.session.begin_transaction()
        c[2] = 2
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(2)),
                '/after the stable timestamp/')

        self.session.begin_transaction()
        c[4] = 4
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(4))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(5))

        # Commit timestamp > stable timestamp.
        # Check both timestamp_transaction and commit_transaction APIs.
        # Oldest and stable timestamp are set to 5 at the moment.
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(5)),
                '/must be after the stable timestamp/')
        self.session.rollback_transaction()

        self.session.begin_transaction()
        c[5] = 5
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(5)),
                '/must be after the stable timestamp/')

        # When explicitly set, commit timestamp for a transaction can be earlier
        # than the commit timestamp of an earlier transaction.
        self.session.begin_transaction()
        c[6] = 6
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(6))
        self.session.begin_transaction()
        c[8] = 8
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(8))
        self.session.begin_transaction()
        c[7] = 7
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(7))

        # Read timestamp >= oldest timestamp
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(7) +
            ',stable_timestamp=' + self.timestamp_str(7))
        if wiredtiger.standalone_build():
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                self.session.begin_transaction('read_timestamp=' + self.timestamp_str(6)))
        else:
            # This is a MongoDB message, not written in standalone builds.
            with self.expectedStdoutPattern('less than the oldest timestamp'):
                self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                    self.session.begin_transaction('read_timestamp=' + self.timestamp_str(6)))

        # c[8] is not visible at read_timestamp < 8
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(7))
        self.assertEqual(c[6], 6)
        self.assertEqual(c[7], 7)
        c.set_key(8)
        self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        self.session.commit_transaction()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(8))
        self.assertEqual(c[6], 6)
        self.assertEqual(c[7], 7)
        self.assertEqual(c[8], 8)
        self.assertTimestampsEqual(
            self.conn.query_timestamp('get=oldest_reader'), self.timestamp_str(8))
        self.session.commit_transaction()

        # We can move the oldest timestamp backwards with "force"
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5) + ',force')
        if wiredtiger.standalone_build():
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                self.session.begin_transaction('read_timestamp=' + self.timestamp_str(4)))
        else:
            # This is a MongoDB message, not written in standalone builds.
            with self.expectedStdoutPattern('less than the oldest timestamp'):
                self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                    self.session.begin_transaction('read_timestamp=' + self.timestamp_str(4)))
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(6))
        self.assertTimestampsEqual(
            self.conn.query_timestamp('get=oldest_reader'), self.timestamp_str(6))
        self.session.commit_transaction()

    # Check setting durable timestamp without first setting stable.
    def test_timestamp_global_durable_first(self):
        # Durable can be set if stable hasn't been set.
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(4))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(5))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(6))

        # But once stable is set, durable must be after stable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(8))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('durable_timestamp=' +
                self.timestamp_str(7)),
                '/durable timestamp .* must be after the stable timestamp/')

    # Check setting oldest timestamp without first setting stable.
    def test_timestamp_global_oldest_first(self):
        # Oldest can be set if stable hasn't been set.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(4))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(6))

        # But once stable is set, oldest must be before stable.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' +
                self.timestamp_str(4)),
                '/oldest timestamp .* must not be after the stable timestamp/')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(7))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('oldest_timestamp=' +
                self.timestamp_str(8)),
                '/oldest timestamp .* must not be after the stable timestamp/')

    # Check setting durable timestamp without setting stable first in the call.
    def test_timestamp_global_stable_first_durable_order(self):
        # It doesn't matter what order the set is done if done in a single call.
        self.conn.set_timestamp('durable_timestamp=' +
            self.timestamp_str(4) + ',stable_timestamp=' + self.timestamp_str(2)),

    # Check setting oldest timestamp without setting stable first in the call.
    def test_timestamp_global_stable_first_oldest_order(self):
        # It doesn't matter what order the set is done if done in a single call.
        self.conn.set_timestamp('oldest_timestamp=' +
            self.timestamp_str(1) + ',stable_timestamp=' + self.timestamp_str(2)),

    # Confirm durable vs. stable.
    def test_timestamp_global_durable_vs_stable(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        # Durable timestamp > stable timestamp.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('durable_timestamp=' +
                self.timestamp_str(6)),
                '/durable timestamp .* must be after the stable timestamp/')

        # Confirm durable can move both forward and backward, freely.
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(20))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(21))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(22))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(23))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(22))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(21))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(20))

    # Confirm oldest vs. stable.
    def test_timestamp_global_oldest_vs_stable(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(3))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(3))

        # Oldest timestamp <= stable timestamp.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('oldest_timestamp=' +
                self.timestamp_str(6)),
                '/oldest timestamp .* must not be after the stable timestamp/')

    # Confirm moving oldest/stable backwards has no effect, and moving durable backwards
    # is allowed.
    def test_timestamp_global_backwards(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(3))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(3))
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(4))

        # Oldest timestamp is 3 at the moment, setting it to an earlier timestamp is a no-op.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.assertTimestampsEqual(\
            self.conn.query_timestamp('get=oldest_timestamp'), self.timestamp_str(3))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(5))

        # Stable timestamp is 5 at the moment, setting it to an earlier timestamp is a no-op.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(4))
        self.assertTimestampsEqual(\
            self.conn.query_timestamp('get=stable_timestamp'), self.timestamp_str(5))

        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(10))

        # Durable  timestamp is 10 at the moment, setting it to an earlier timestamp is allowed.
        self.conn.set_timestamp('durable_timestamp=' + self.timestamp_str(8))
        self.assertTimestampsEqual(\
            self.conn.query_timestamp('get=all_durable'), self.timestamp_str(8))

    # Check forcibly moving oldest/stable backwards.
    def test_timestamp_global_backwards_force(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5))

        # Oldest timestamp is 5 at the moment, forcibly set it to an earlier timestamp.
        self.conn.set_timestamp('force=true,oldest_timestamp=' + self.timestamp_str(4))
        self.assertTimestampsEqual(\
            self.conn.query_timestamp('get=oldest_timestamp'), self.timestamp_str(4))

        # But even force won't let you move oldest past stable.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('oldest_timestamp=' +
                self.timestamp_str(15)),
                '/oldest timestamp .* must not be after the stable timestamp/')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('force=true,oldest_timestamp=' +
                self.timestamp_str(15)),
                '/oldest timestamp .* must not be after the stable timestamp/')

        # Stable timestamp is 10 at the moment, forcibly set it to an earlier timestamp.
        self.conn.set_timestamp('force=true,stable_timestamp=' + self.timestamp_str(8))
        self.assertTimestampsEqual(\
            self.conn.query_timestamp('get=stable_timestamp'), self.timestamp_str(8))

        # But even force won't let you move stable before oldest.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('force=true,stable_timestamp=' +
                self.timestamp_str(3)),
                '/oldest timestamp .* must not be after the stable timestamp/')

if __name__ == '__main__':
    wttest.run()
