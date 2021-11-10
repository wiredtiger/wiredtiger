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
#
# test_config10.py
#   Test the debug configuration setting on the session which evicts
#   pages as they are released.
#

import wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet

class test_config10(wttest.WiredTigerTestCase):
    session_config = 'debug=(release_evict_page=true),isolation=snapshot'
    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    scenarios = make_scenarios(key_format_values)

    def test_config10(self):
        uri = 'table:test_config10'
        nrows = 1000

        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))

        value_a = 'a' * 400
        value_b = 'b' * 400

        s = self.conn.open_session()
        cursor = s.open_cursor(uri)

        # Insert some keys at timestamp 10.
        s.begin_transaction()
        cursor[1] = value_a
        cursor[2] = value_a
        cursor[3] = value_a
        cursor[4] = value_b
        s.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Evict the page to force reconciliation.
        s.begin_transaction()
        v = cursor[1]
        self.assertEqual(v, value_a)
        s.rollback_transaction()
        cursor.close()

        cursor = s.open_cursor(uri)
        s.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        self.assertEqual(cursor[1], value_a)
        s.rollback_transaction()

        s.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        self.assertEqual(cursor[1], value_a)
        s.rollback_transaction()

        # Update key 1 at timestamp 30.
        cursor = s.open_cursor(uri)
        s.begin_transaction()
        cursor[1] = value_b
        s.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))
        self.conn.rollback_to_stable()

        # Read at timestamp 40.
        cursor = s.open_cursor(uri)
        s.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        self.assertEqual(cursor[1], value_b)
        self.assertEqual(cursor[2], value_a)
        self.assertEqual(cursor[3], value_a)
        self.assertEqual(cursor[4], value_b)
        s.rollback_transaction()
        cursor.close()

if __name__ == '__main__':
    wttest.run()
