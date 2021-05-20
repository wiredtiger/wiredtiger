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

import time, wiredtiger, wttest

# test_hs20.py
# Ensure we never reconstruct a reverse modify update in the history store based on the onpage overflow value
def timestamp_str(t):
    return '%x' % t

class test_hs20(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'

    def test_hs20(self):
        uri = 'table:test_hs20'
        # Set a very small maximum leaf value to trigger writing overflow values
        self.session.create(uri, 'key_format=S,value_format=S,leaf_value_max=10B')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a' * 500
        value2 = 'b' * 50

        # Insert a value that is larger than the maximum leaf value.
        for i in range(0, 10):
            self.session.begin_transaction()
            cursor[str(i)] = value1
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Do 2 modifies.
        for i in range(0, 10):
            self.session.begin_transaction()
            cursor.set_key(str(i))
            mods = [wiredtiger.Modify('B', 500, 1)]
            self.assertEqual(cursor.modify(mods), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        for i in range(0, 10):
            self.session.begin_transaction()
            cursor.set_key(str(i))
            mods = [wiredtiger.Modify('C', 501, 1)]
            self.assertEqual(cursor.modify(mods), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(4))

        # Insert more data to trigger eviction.
        for i in range(10, 100000):
            self.session.begin_transaction()
            cursor[str(i)] = value2
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Update the overflow values.
        for i in range(0, 10):
            self.session.begin_transaction()
            cursor[str(i)] = value2
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Do a checkpoint to move the overflow values to the history store but keep the current in memory disk image.
        self.session.checkpoint()

        # Search the first modifies.
        for i in range(0, 10):
            self.session.begin_transaction('read_timestamp=' + timestamp_str(3))
            self.assertEqual(cursor[str(i)], value1 + "B")
            self.session.rollback_transaction()
