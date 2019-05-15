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
# test_timestamp13.py
#   Timestamps: session query_timestamp
#

import random
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_timestamp13(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp13'
    uri = 'table:' + tablename

    scenarios = make_scenarios([
        ('col', dict(extra_config=',key_format=r')),
        ('lsm', dict(extra_config=',type=lsm')),
        ('row', dict(extra_config='')),
    ])

    conn_config = 'log=(enabled)'
    session_config = 'isolation=snapshot'

    # Confirm that query_timestamp raises an exception
    def assert_query_timestamp_raises_excep(self, resource, ts_query, msg=''):
        # Confirm with both hexadecimal and numeric APIs
        if msg:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: resource.query_timestamp(ts_query), msg)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: resource.query_timestamp_numeric(ts_query), msg)
        else:
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: resource.query_timestamp(ts_query))
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: resource.query_timestamp_numeric(ts_query))

    # Check if query_timestamp and query_timestamp_numeric return the expected timestamp
    def assert_query_timestamp_equals(self, resource, ts_query, expected_val_numeric):
        # Confirm the expected hex timestamp return value
        q = resource.query_timestamp(ts_query)
        self.pr(ts_query + ' in hex:' + q)
        self.assertTimestampsEqual(q, timestamp_str(expected_val_numeric))

        # Confirm the expected numeric timestamp return value
        q = resource.query_timestamp_numeric(ts_query)
        self.pr(ts_query + ' in decimal:' + str(q))
        self.assertEqual(q, expected_val_numeric)

    def test_degenerate_timestamps(self):
        self.session.create(self.uri,
            'key_format=i,value_format=i' + self.extra_config)

        query_choices = ['commit', 'first_commit', 'prepare', 'read']
        # Querying a session's timestamps will error when not in a transaction.
        for query in query_choices:
            self.assert_query_timestamp_raises_excep(self.session, 'get=' + query)

        self.session.begin_transaction()
        # Nothing has been set, all queries will return timestamp 0.
        for query in query_choices:
            self.assert_query_timestamp_equals(self.session, 'get=' + query, 0)

        self.assert_query_timestamp_raises_excep(self.session, 'get=unknown',
            '/not a permitted choice for key/')

        self.session.commit_transaction()
        # Querying a session's timestamps will error when not in a transaction.
        for query in query_choices:
            self.assert_query_timestamp_raises_excep(self.session, 'get=' + query)

    def test_query_read_commit_timestamps(self):
        self.session.create(self.uri,
            'key_format=i,value_format=i' + self.extra_config)

        self.session.begin_transaction('isolation=snapshot')
        self.session.timestamp_transaction_numeric('read_timestamp=' + '10')
        self.assert_query_timestamp_equals(self.session, 'get=read', 10)

        # The first commit_timestamp will set both the commit and first_commit
        # values.
        self.session.timestamp_transaction_numeric('commit_timestamp=' + '20')
        self.assert_query_timestamp_equals(self.session, 'get=commit', 20)
        self.assert_query_timestamp_equals(self.session, 'get=first_commit', 20)

        # The second commit_timestamp will update the commit value, leaving
        # first_commit alone.
        self.session.timestamp_transaction_numeric('commit_timestamp=' + '30')
        self.assert_query_timestamp_equals(self.session, 'get=commit', 30)
        self.assert_query_timestamp_equals(self.session, 'get=first_commit', 20)
        self.session.commit_transaction()

    def test_query_round_read_timestamp(self):
        self.session.create(self.uri,
            'key_format=i,value_format=i' + self.extra_config)

        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10))
        # Rounding to the oldest timestamp will allow the stale read_timestamp
        # to succeed. The follow-up call to get the read timestamp returns the
        # chosen read timestamp.
        self.session.begin_transaction('isolation=snapshot,roundup_timestamps=(read=true)')
        self.session.timestamp_transaction_numeric('read_timestamp=' + '5')
        self.assert_query_timestamp_equals(self.session, 'get=read', 10)

        # Moving the oldest timestamp has no bearing on the read timestamp
        # returned.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(20))
        self.assert_query_timestamp_equals(self.session, 'get=read', 10)
        self.session.commit_transaction()

    def test_query_prepare_timestamp(self):
        self.session.create(self.uri,
            'key_format=i,value_format=i' + self.extra_config)

        self.session.begin_transaction()
        self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(10))
        self.assert_query_timestamp_equals(self.session, 'get=prepare', 10)

        self.session.timestamp_transaction_numeric('commit_timestamp=' + '20')
        self.session.timestamp_transaction_numeric('durable_timestamp=' + '20')
        self.assert_query_timestamp_equals(self.session, 'get=prepare', 10)
        self.assert_query_timestamp_equals(self.session, 'get=commit', 20)
        self.session.commit_transaction()
