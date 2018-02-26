#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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
# test_prepare04.py
#   Prepare: prepared update confilct
#

import random
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_prepare04(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp02'
    uri = 'table:' + tablename

    scenarios = make_scenarios([
        ('col', dict(extra_config=',log=(enabled=false),key_format=r')),
        ('lsm', dict(extra_config=',log=(enabled=false),type=lsm')),
        ('row', dict(extra_config=',log=(enabled=false)')),
    ])

    conn_config = 'log=(enabled)'

    # Check that a cursor (optionally started in a new transaction), sees the
    # expected values.
    def check_one(self, session, txn_config, expected):
        if txn_config:
            session.begin_transaction(txn_config)
        c = session.open_cursor(self.uri, None)
        actual = c[1]
        #print "A: "
        #print actual
        #print "E: "
        #print expected
        self.assertTrue(actual == expected)
        c.close()
        if txn_config:
            session.commit_transaction()

    def test_basic(self):
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        self.session.create(self.uri,
            'key_format=i,value_format=i' + self.extra_config)
        c = self.session.open_cursor(self.uri)

        # Insert keys 1..100 each with timestamp=key, in some order
        orig_keys = range(1, 101)
        keys = orig_keys[:]
        random.shuffle(keys)

        k = 1
        self.session.begin_transaction()
        c[k] = 1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(100))

        # Everything up to and including timestamp 100 has been committed.
        self.assertTimestampsEqual(self.conn.query_timestamp(), timestamp_str(100))

        # Bump the oldest timestamp, we're not going back...
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(100))

        # make prepared updates.
        k = 1
        self.session.begin_transaction('isolation=snapshot')
        c.set_key(1)
        c.set_value(2)
        c.update()
        self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(200))
        conflictmsg = '/conflict between concurrent operations/'

        #'''
        # Verify transaction with a read timestamp earlier than the prepare timestamp
        s_earlier_ts = self.conn.open_session()
        c_earlier_ts = s_earlier_ts.open_cursor(self.uri, None)
        s_earlier_ts.begin_transaction(
            'isolation=snapshot,read_timestamp='+timestamp_str(150))
        c_earlier_ts.set_key(1)
        c_earlier_ts.search()
        self.assertTrue(c_earlier_ts.get_value() == 1)
        c_earlier_ts.set_value(3)
        self.assertRaises(wiredtiger.WiredTigerError, lambda:c_earlier_ts.update())
        s_earlier_ts.commit_transaction()
        #'''

        #'''
        # Verify transaction with a read timestamp later than the prepare timestamp
        s_later_ts = self.conn.open_session()
        c_later_ts = s_later_ts.open_cursor(self.uri, None)
        s_later_ts.begin_transaction(
            'isolation=snapshot,read_timestamp='+timestamp_str(250))
        c_later_ts.set_key(1)
        c_later_ts.search()
        self.assertTrue(c_later_ts.get_value() == 1)
        c_later_ts.set_value(3)
        self.assertRaises(wiredtiger.WiredTigerError, lambda:c_later_ts.update())
        s_later_ts.commit_transaction()
        #'''

        #'''
        # Verify transaction with out a read timestamp
        s_no_ts = self.conn.open_session()
        c_no_ts = s_no_ts.open_cursor(self.uri, None)
        s_no_ts.begin_transaction('isolation=snapshot')
        c_no_ts.set_key(1)
        c_no_ts.search()
        self.assertTrue(c_no_ts.get_value() == 1)
        c_no_ts.set_value(3)
        self.assertRaises(wiredtiger.WiredTigerError, lambda:c_no_ts.update())
        s_no_ts.commit_transaction()
        #'''

        self.session.commit_transaction('commit_timestamp=' + timestamp_str(300))

if __name__ == '__main__':
    wttest.run()
