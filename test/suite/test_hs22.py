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

import time, re
import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_hs22.py
# Test we don't crash when the onpage value is an out of order timestamp update.
class test_hs22(wttest.WiredTigerTestCase):
    # Configure handle sweeping to occur within a specific amount of time.
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'

    def test_onpage_out_of_order_timestamp(self):
        uri = 'table:test_hs22'
        # Set a very small maximum leaf value to trigger writing overflow values
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a'
        value2 = 'b'

        # Insert a value
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Remove the value
        self.session.begin_transaction()
        cursor.set_key(str(0))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Do an out of order update
        self.session.begin_transaction()
        cursor[str(0)] = value2
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Insert another key
        self.session.begin_transaction()
        cursor[str(1)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Update the key
        self.session.begin_transaction()
        cursor[str(1)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        # Do a checkpoint to trigger history store reconciliation
        self.session.checkpoint()
