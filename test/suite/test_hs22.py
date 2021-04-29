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

import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

# test_hs22.py
# Test the case that out of order timestamp
# update is followed by a tombstone.
class test_hs22(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'

    def test_onpage_out_of_order_timestamp_update(self):
        uri = 'table:test_hs22'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a'
        value2 = 'b'

        # Insert a key.
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Remove the key.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Do an out of order timestamp
        # update and write it to the data
        # store later.
        self.session.begin_transaction()
        cursor[str(0)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Insert another key.
        self.session.begin_transaction()
        cursor[str(1)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Update the key.
        self.session.begin_transaction()
        cursor[str(1)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        # Do a checkpoint to trigger
        # history store reconciliation.
        self.session.checkpoint()

        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search the key to evict it.
        self.session.begin_transaction("read_timestamp=" + timestamp_str(15))
        self.assertEqual(evict_cursor[str(0)], value2)
        self.assertEqual(evict_cursor.reset(), 0)
        self.session.rollback_transaction()

        # Search the key again to verify the data is still as expected.
        self.session.begin_transaction("read_timestamp=" + timestamp_str(15))
        self.assertEqual(cursor[str(0)], value2)
        self.session.rollback_transaction()

    def test_out_of_order_timestamp_update_newer_than_tombstone(self):
        uri = 'table:test_hs22'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a'
        value2 = 'b'

        # Insert a key.
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        # Remove a key.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Do an out of order timestamp
        # update and write it to the
        # history store later.
        self.session.begin_transaction()
        cursor[str(0)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(15))

        # Add another update.
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Insert another key.
        self.session.begin_transaction()
        cursor[str(1)] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Update the key.
        self.session.begin_transaction()
        cursor[str(1)] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        # Do a checkpoint to trigger
        # history store reconciliation.
        self.session.checkpoint()

        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search the key to evict it.
        self.session.begin_transaction("read_timestamp=" + timestamp_str(15))
        self.assertEqual(evict_cursor[str(0)], value2)
        self.assertEqual(evict_cursor.reset(), 0)
        self.session.rollback_transaction()

        # Search the key again to verify the data is still as expected.
        self.session.begin_transaction("read_timestamp=" + timestamp_str(15))
        self.assertEqual(cursor[str(0)], value2)
        self.session.rollback_transaction()
