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

import wttest

def timestamp_str(t):
    return '%x' % t

# test_hs23.py
# Test the case that we have update, out of order timestamp
# update, and update again in the same transaction
class test_hs23(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'

    def test(self):
        uri = 'table:test_hs23'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a'
        value2 = 'b'
        value3 = 'c'
        value4 = 'd'
        value5 = 'e'

        # Insert a key.
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Update at 10, update at 20, update at 15 (out of order), and
        # update at 20 in the same transaction
        self.session.begin_transaction()
        cursor.set_key(str(0))
        cursor.set_value(value2)
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(10))
        self.assertEquals(cursor.update(), 0)

        cursor.set_key(str(0))
        cursor.set_value(value3)
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(20))
        self.assertEquals(cursor.update(), 0)

        cursor.set_key(str(0))
        cursor.set_value(value4)
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(15))
        self.assertEquals(cursor.update(), 0)

        cursor.set_key(str(0))
        cursor.set_value(value5)
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(20))
        self.assertEquals(cursor.update(), 0)
        self.session.commit_transaction()

        # Do a checkpoint to trigger
        # history store reconciliation.
        self.session.checkpoint()

        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search the key to evict it.
        self.session.begin_transaction()
        self.assertEqual(evict_cursor[str(0)], value5)
        self.assertEqual(evict_cursor.reset(), 0)
        self.session.rollback_transaction()

        # Search the latest update
        self.session.begin_transaction("read_timestamp=" + timestamp_str(20))
        self.assertEqual(cursor[str(0)], value5)
        self.session.rollback_transaction()

        # Serarch the out of order timestamp update
        self.session.begin_transaction("read_timestamp=" + timestamp_str(15))
        self.assertEqual(cursor[str(0)], value4)
        self.session.rollback_transaction()
