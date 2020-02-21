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

import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

# test_hs11.py
# Ensure that when we delete a key due to a tombstone being globally visible, we delete its
# associated history store content.
class test_hs11(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'

    def test_key_deletion_clears_hs(self):
        uri = 'table:test_hs11'
        create_params = 'key_format=S,value_format=S'
        self.session.create(uri, create_params)

        value1 = 'a' * 500
        value2 = 'b' * 500
        value3 = 'c' * 500
        value4 = 'd' * 500

        # Apply a series of updates from timestamps 1-5.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        cursor = self.session.open_cursor(uri)
        for i in range(1, 5):
            self.session.begin_transaction()
            for j in range(1, 2000):
                cursor[str(j)] = str(j)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(i))

        # Reconcile and flush versions 1-4 to the history store.
        self.session.checkpoint()

        # Apply a tombstone at timestamp 10.
        self.session.begin_transaction()
        for i in range(1, 2000):
            cursor.set_key(str(i))
            cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Bring oldest forward.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(11))

        # Reconcile. We should destroy the keys since we have a globally visible tombstone.
        self.session.checkpoint()

        # Now apply an update at timestamp 100.
        self.session.begin_transaction()
        for i in range(1, 2000):
            cursor[str(i)] = str(i)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(100))

        # Now let's recreate the keys.
        self.session.checkpoint()

        # Ensure that we blew away history store content. If we didn't, we'll see the older value at
        # timestamp 4.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(99))
        for i in range(1, 2000):
            cursor.set_key(str(i))
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
