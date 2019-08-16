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
from wiredtiger import stat
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_las06.py
# Verify that triggering lookaside usage does not cause a spike in memory usage
# to form an update chain from the lookaside contents.
#
# The required value should be fetched from lookaside and then passed straight
# back to the user without putting together an update chain.
#
# TODO: Uncomment the checks after the main portion of the relevant history
# project work is complete.
class test_las06(wttest.WiredTigerTestCase):
    # Force a small cache.
    conn_config = 'cache_size=50MB,statistics=(fast)'
    session_config = 'isolation=snapshot'
    key_format_values = [
        ('column_store', dict(key_format='r')),
        ('row_store', dict(key_format='i'))
    ]
    scenarios = make_scenarios(key_format_values)

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def get_non_page_image_memory_usage(self):
        return self.get_stat(stat.conn.cache_bytes_other)

    def test_las_reads_workload(self):
        # Create a small table.
        uri = "table:test_las06"
        create_params = 'key_format={},value_format=S'.format(self.key_format)
        self.session.create(uri, create_params)

        value1 = 'a' * 500
        value2 = 'b' * 500

        # Load 5Mb of data.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 10000):
            cursor[i] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Load another 5Mb of data with a later timestamp.
        self.session.begin_transaction()
        for i in range(1, 10000):
            cursor[i] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Now the latest version will get written to the data file.
        self.session.checkpoint()

        start_usage = self.get_non_page_image_memory_usage()

        # Whenever we request something out of cache of timestamp 2, we should
        # be reading it straight from lookaside without initialising a full
        # update chain of every version of the data.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(2))
        for i in range(1, 10000):
            self.assertEqual(cursor[i], value1)
        self.session.rollback_transaction()

        end_usage = self.get_non_page_image_memory_usage()

        # Non-page related memory usage shouldn't spike significantly.
        #
        # Prior to this change, this type of workload would use a lot of memory
        # to recreate update lists for each page.
        #
        # This check could be more aggressive but to avoid potential flakiness,
        # lets just ensure that it hasn't doubled.
        #
        # TODO: Uncomment this once the project work is done.
        # self.assertLessEqual(end_usage, (start_usage * 2))

    def test_las_modify_reads_workload(self):
        # Create a small table.
        uri = "table:test_las06"
        create_params = 'key_format={},value_format=S'.format(self.key_format)
        self.session.create(uri, create_params)

        # Create initial large values.
        value1 = 'a' * 500
        value2 = 'd' * 500

        # Load 5Mb of data.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 5000):
            cursor[i] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Load a slight modification with a later timestamp.
        self.session.begin_transaction()
        for i in range(1, 5000):
            cursor.set_key(i)
            mods = [wiredtiger.Modify('B', 100, 1)]
            self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # And another.
        self.session.begin_transaction()
        for i in range(1, 5000):
            cursor.set_key(i)
            mods = [wiredtiger.Modify('C', 200, 1)]
            self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(4))

        # Now write something completely different.
        self.session.begin_transaction()
        for i in range(1, 5000):
            cursor[i] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Now the latest version will get written to the data file.
        self.session.checkpoint()

        expected = list(value1)
        expected[100] = 'B'
        expected[200] = 'C'
        expected = str().join(expected)

        # Whenever we request something of timestamp 4, this should be a modify
        # op. We should keep looking backwards in lookaside until we find the
        # newest whole update (timestamp 2).
        #
        # t5: value1 (full update)
        # t4: (delta) <= We're querying for t4 so we begin here.
        # t3: (delta)
        # t2: value2 (full update) <= And finish here, applying all deltas in
        #                             between on value1 to deduce value3.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(4))
        for i in range(1, 5000):
            self.assertEqual(cursor[i], expected)
        self.session.rollback_transaction()

if __name__ == '__main__':
    wttest.run()
