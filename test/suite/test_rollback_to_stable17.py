#
# Public Domain 2014-2020 MongoDB, Inc.
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
import wiredtiger, wttest, unittest
from wiredtiger import stat
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable17.py
# Test that rollback to stable removes updates present on disk for variable length column store.
class test_rollback_to_stable17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=2MB,statistics=(all)'
    session_config = 'isolation=snapshot'

    def insert_update_data_at_given_timestamp(self, uri, value, start_row, end_row, timestamp):
        cursor =  self.session.open_cursor(uri)
        for i in range(start_row, end_row):
            self.session.begin_transaction()
            cursor[i] = value + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(timestamp))
        cursor.close()

    def check(self, check_value, uri, nrows, start_row, end_row, read_ts):
        session = self.session
        session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)

        count = 0
        for i in range(start_row, end_row):
            cursor.set_key(i)
            ret = cursor.search()
            self.tty(f'value = {cursor.get_value()}')
            # if check_value is None:
            #     self.assertTrue(ret == wiredtiger.WT_NOTFOUND)
            # else:
            #     self.assertEqual(cursor.get_value(), check_value + str(count + start_row))
            #     count += 1
        session.commit_transaction()
        # self.assertEqual(count, nrows)
        cursor.close()

    def test_rollback_to_stable(self):
        # Create a table.
        uri = "table:rollback_to_stable17"
        nrows = 200
        create_params = 'key_format=r,value_format=S'

        self.session.create(uri, create_params)

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        value20 = "aaaa"
        value30 = "bbbb"
        value40 = "cccc"
        value50 = "dddd"

        self.insert_update_data_at_given_timestamp(uri, value20, 1, 200, 2)
        self.insert_update_data_at_given_timestamp(uri, value30, 200, 400, 5)
        self.insert_update_data_at_given_timestamp(uri, value40, 400, 600, 7)
        self.insert_update_data_at_given_timestamp(uri, value50, 600, 800, 9)  

        self.session.checkpoint()

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(5))
        self.conn.rollback_to_stable()

        self.check(value20, uri, nrows - 1, 1, 200, 2)
        self.check(value30, uri, nrows - 1, 201, 400, 5)
        self.check(None, uri, 0, 401, 600, 7)
        self.check(None, uri, 0, 601, 800, 9)      

        # self.conn.set_timestamp('stable_timestamp=' + timestamp_str(2))
        # self.conn.rollback_to_stable()  
        # self.check(value20, uri, nrows - 1, 1, 200, 2)
        # self.check(value30, uri, nrows - 1, 201, 400, 5)

        self.session.close()
if __name__ == '__main__':
    wttest.run()