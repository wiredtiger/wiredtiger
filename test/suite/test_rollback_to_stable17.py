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
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable17.py
# Test that rollback to stable removes updates present on disk for variable length column store.
class test_rollback_to_stable17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB,statistics=(all)'
    session_config = 'isolation=snapshot'

    def insert_update_data_at_given_timestamp(self, uri, value, nrows, timestamp):
        cursor =  self.session.open_cursor(uri)
        for i in range(1, nrows):
            self.session.begin_transaction()
            cursor[i] = value + str(i)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(timestamp))

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        if read_ts == 0:
            session.begin_transaction()
        else:
            session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value + str(count +1))
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)

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

        self.insert_update_data_at_given_timestamp(uri, value20, nrows, 2)
        self.insert_update_data_at_given_timestamp(uri, value30, nrows, 5)
        self.insert_update_data_at_given_timestamp(uri, value40, nrows, 7)
        self.insert_update_data_at_given_timestamp(uri, value50, nrows, 9)

        self.session.checkpoint()

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(4))
        self.conn.rollback_to_stable()

        self.check(value20, uri, 199, 2)

        self.session.close()
if __name__ == '__main__':
    wttest.run()