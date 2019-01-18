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
#    Check that the updates with durable timestamp newer than the stable
#    timestamp fill up the cache and leave it stuck.
class test_durable_ts03(wttest.WiredTigerTestCase):
    # Reduce the cache size to 10MB to get the stuck cache.
    def conn_config(self):
        return 'cache_size=50MB'

    def test_durable_ts03(self):
        # Create a table.
        uri = 'table:test_durable_ts03'
        nrows = 600000
        self.session.create(uri, 'key_format=i,value_format=u')
        value1 = "aaaaa" * 100
        value2 = "bbbbb" * 100

        # Load the data into the table.
        session = self.conn.open_session()
        cursor = session.open_cursor(uri, None)
        for i in range(0, nrows):
            cursor[i] = value1
        cursor.close()

        # Set the stable timestamp to checkpoint initial data set.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(100))
        self.session.checkpoint()

        # Update all the values within transaction. Commit the transaction with
        # a durable timestamp newer than the stable timestamp.
        cursor = session.open_cursor(uri, None)
        self.assertEquals(cursor.next(), 0)
        for i in range(1, nrows):
            session.begin_transaction()
            cursor.set_value(value2)
            self.assertEquals(cursor.update(), 0)
            self.assertEquals(cursor.next(), 0)
            session.prepare_transaction('prepare_timestamp=' + timestamp_str(150))
            session.commit_transaction('commit_timestamp=' + timestamp_str(200) + \
                                       ',durable_timestamp=' + timestamp_str(220))

if __name__ == '__main__':
    wttest.run()
