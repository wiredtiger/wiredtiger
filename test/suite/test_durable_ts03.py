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

from helper import copy_wiredtiger_home
import wiredtiger, wttest

def timestamp_str(t):
    return '%x' %t

# test_durable_ts03.py
#    Check that the checkpoint honors the durable timestamp of updates.
class test_durable_ts03(wttest.WiredTigerTestCase):
    def conn_config(self):
        return 'cache_size=10MB'

    def test_durable_ts03(self):
        # Create a table.
        uri = 'table:test_durable_ts03'
        nrows = 3000
        self.session.create(uri, 'key_format=i,value_format=u')
        valueA = "aaaaa" * 100
        valueB = "bbbbb" * 100
        valueC = "ccccc" * 100

        # Start with setting a stable and oldest timestamp.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(1) + \
                                ',oldest_timestamp=' + timestamp_str(1))

        # Load the data into the table.
        session = self.conn.open_session()
        cursor = session.open_cursor(uri, None)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[i] = valueA
            session.commit_transaction('commit_timestamp=' + timestamp_str(50))
        cursor.close()

        # Set the stable and the oldest timestamp to checkpoint initial data.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(100) + \
                                ',oldest_timestamp=' + timestamp_str(100))
        self.session.checkpoint()

        # Update all the values within transaction. Commit the transaction with
        # a durable timestamp newer than the stable timestamp.
        cursor = session.open_cursor(uri, None)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[i] = valueB
            session.prepare_transaction('prepare_timestamp=' + timestamp_str(150))
            session.commit_transaction('commit_timestamp=' + timestamp_str(200) + \
                                       ',durable_timestamp=' + timestamp_str(220))

        # Check the checkpoint wrote only the durable updates.
        cursor2 = self.session.open_cursor(
            uri, None, 'checkpoint=WiredTigerCheckpoint')
        for key, value in cursor2:
            self.assertEqual(value, valueA)

        self.assertEquals(cursor.reset(), 0)
        session.begin_transaction('read_timestamp=' + timestamp_str(150))
        for key, value in cursor:
            self.assertEqual(value, valueA)
        session.commit_transaction()

        # Read the updated data to confirm that it is visible.
        self.assertEquals(cursor.reset(), 0)
        session.begin_transaction('read_timestamp=' + timestamp_str(210))
        for key, value in cursor:
            self.assertEqual(value, valueB)
        session.commit_transaction()

        self.session.checkpoint("use_timestamp=true")
        cursor.close()
        session.close()

        self.reopen_conn()
        session = self.conn.open_session()
        cursor = session.open_cursor(uri, None)
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(210) + \
                                ',oldest_timestamp=' + timestamp_str(210))
        for key, value in cursor:
            self.assertEqual(value, valueA)

        self.assertEquals(cursor.reset(), 0)
        for i in range(0, nrows):
            session.begin_transaction()
            cursor[i] = valueC
            session.prepare_transaction('prepare_timestamp=' + timestamp_str(220))
            session.commit_transaction('commit_timestamp=' + timestamp_str(230) + \
                                       ',durable_timestamp=' + timestamp_str(240))

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(250))
        self.session.checkpoint()
        cursor.close()
        session.close()

        self.reopen_conn()
        session = self.conn.open_session()
        cursor = session.open_cursor(uri, None)
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(250) + \
                                ',oldest_timestamp=' + timestamp_str(250))
        for key, value in cursor:
            self.assertEqual(value, valueC)

if __name__ == '__main__':
    wttest.run()
