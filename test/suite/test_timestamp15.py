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
# test_timestamp15.py
#   Test various timestamp scenarios for active txns
#

import random
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_timestamp15(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp15'
    uri = 'table:' + tablename

    def test_commit_timestamp_before_stable(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        cur1 = self.session.open_cursor(self.uri)
        self.conn.set_timestamp('stable_timestamp=1')

        # Scenario 1: A simple case where we start a transaction
        # specify a commit timestamp then the move the stable timestamp
        # past the commit timestamp, then attempt to commit.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=2')
        self.conn.set_timestamp('stable_timestamp=3')
        cur1[1] = 1
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(),
                '/commit timestamp \(0,2\) older ' +
                    'than stable timestamp \(0,3\)/')

        # Scenario 2:
        # Specify multiple commit timestamps some being after
        # the stable timestamp.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=4')
        self.conn.set_timestamp('stable_timestamp=5')
        cur1[2] = 2
        self.session.timestamp_transaction('commit_timestamp=6')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(),
                '/commit timestamp \(0,4\) older ' +
                    'than stable timestamp \(0,5\)/')

        # Scenario 3:
        # Specify a commit timestamp equal to a stable timestamp.
        # This is also not legal.
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction('commit_timestamp=5'),
                '/commit timestamp 5 older than stable timestamp \(0,5\)/')
        self.session.commit_transaction()

        # Scenario 4:
        # Ensure that if the transaction is prepared it is not
        # going to be rejected if a durable timestamp that is
        # older than the stable timestamp is provided.
        self.session.begin_transaction()
        cur1[3] = 3
        self.session.prepare_transaction('prepare_timestamp=3')
        self.session.timestamp_transaction('commit_timestamp=4')
        self.session.timestamp_transaction('durable_timestamp=6')
        self.session.commit_transaction()

        # Scenario 5:
        # Given a prepared transaction ensure that first commit
        # timestamp provided is newer than the stable timestamp
        # as we are not specifying a durable
        # timestamp.
        self.session.begin_transaction()
        cur1[4] = 4
        self.session.prepare_transaction('prepare_timestamp=3')
        self.session.timestamp_transaction('commit_timestamp=4')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(),
                '/commit timestamp \(0,4\) older ' +
                    'than stable timestamp \(0,5\)/')

if __name__ == '__main__':
    wttest.run()