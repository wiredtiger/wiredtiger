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
# test_timestamp16.py
#   Test to ensure read timestamp is properly cleared at the
#   end of a txn.
#

import random
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_timestamp16(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_timestamp16'
    uri = 'table:' + tablename
    session_config = 'isolation=snapshot'

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

    def test_read_timestamp_cleared(self):
        # Ensure that the read timestamp doesn't move our checkpoint.
        self.session.create(self.uri, 'key_format=i,value_format=i')
        self.session.begin_transaction('read_timestamp=' + timestamp_str(100))
        self.session.rollback_transaction()
        self.session.checkpoint('use_timestamp=true')
        self.assert_query_timestamp_equals(self.conn, 'get=last_checkpoint', 0)

        # Set a stable and make sure that we still checkpoint at
        # the stable.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(1))
        self.session.begin_transaction('read_timestamp=' + timestamp_str(100))
        self.session.rollback_transaction()
        self.session.checkpoint('use_timestamp=true')
        self.assert_query_timestamp_equals(self.conn, 'get=last_checkpoint', 1)

        # Finally make sure that commit also resets the read timestamp.
        self.session.create(self.uri, 'key_format=i,value_format=i')
        self.session.begin_transaction('read_timestamp=' + timestamp_str(150))
        self.session.commit_transaction()
        self.session.checkpoint('use_timestamp=true')
        self.assert_query_timestamp_equals(self.conn, 'get=last_checkpoint', 1)

if __name__ == '__main__':
    wttest.run()
