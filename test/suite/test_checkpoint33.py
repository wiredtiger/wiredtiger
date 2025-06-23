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

<<<<<<< HEAD
import threading, time
import wttest
import wiredtiger
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wiredtiger import stat

# test_checkpoint33.py
#
# Test that reconciliation removes obsolete updates on the page.
class test_checkpoint33(wttest.WiredTigerTestCase):

    format_values = [
        ('column', dict(key_format='r', value_format='S', extraconfig='')),
        ('column_fix', dict(key_format='r', value_format='8t',
            extraconfig=',allocation_size=512,leaf_page_max=512')),
        ('string_row', dict(key_format='S', value_format='S', extraconfig='')),
    ]

    scenarios = make_scenarios(format_values)

    def large_updates(self, uri, ds, nrows, value, ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[ds.key(i)] = value
            if i % 101 == 0:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
                self.session.begin_transaction()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def check(self, ds, nrows, value):
        cursor = self.session.open_cursor(ds.uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, value)
            count += 1
        self.assertEqual(count, nrows)
        cursor.close()

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def test_checkpoint(self):
        uri = 'table:checkpoint33'
        nrows = 1000

        # Create a table.
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format=self.value_format,
            config=self.extraconfig)
        ds.populate()

        if self.value_format == '8t':
            value_a = 97
            value_b = 98
            value_c = 99
            value_d = 100
            value_e = 101
        else:
            value_a = "aaaaa" * 100
            value_b = "bbbbb" * 100
            value_c = "ccccc" * 100
            value_d = "ddddd" * 100
            value_e = "eeeee" * 100

        # Write some initial data.
        self.large_updates(ds.uri, ds, nrows, value_a, 5)

        # Pin oldest and stable timestamps to 5.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5) +
            ',stable_timestamp=' + self.timestamp_str(5))

        # Checkpoint and reopen the connection to read from the on-disk version.
        self.session.checkpoint()
        self.reopen_conn()

        # Add updates to each key to check whether they free on reconciliation.
        self.large_updates(ds.uri, ds, nrows, value_b, 10)
        prev_bytes_in_use = self.get_stat(stat.conn.cache_bytes_inuse)
        self.pr('Base bytes in use ' + str(prev_bytes_in_use))

        self.large_updates(ds.uri, ds, nrows, value_c, 20)

        # Pin oldest and stable timestamps to 20.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(20) +
            ',stable_timestamp=' + self.timestamp_str(20))

        # Checkpoint.
        self.session.checkpoint()
        bytes_in_use = self.get_stat(stat.conn.cache_bytes_inuse)
        self.pr('After first checkpoint bytes in use ' + str(bytes_in_use))
        self.assertLess(bytes_in_use, prev_bytes_in_use * 2)

        # Another set of updates.
        self.large_updates(ds.uri, ds, nrows, value_d, 30)

        # Pin oldest and stable timestamps to 30.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(30) +
            ',stable_timestamp=' + self.timestamp_str(30))

        # Checkpoint.
        self.session.checkpoint()
        bytes_in_use = self.get_stat(stat.conn.cache_bytes_inuse)
        self.pr('After second checkpoint bytes in use ' + str(bytes_in_use))
        self.assertLess(bytes_in_use, prev_bytes_in_use * 2)

        # Another set of updates.
        self.large_updates(ds.uri, ds, nrows, value_e, 40)

        # Pin oldest and stable timestamps to 40.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(40) +
            ',stable_timestamp=' + self.timestamp_str(40))

        # Checkpoint.
        self.session.checkpoint()
        bytes_in_use = self.get_stat(stat.conn.cache_bytes_inuse)
        self.pr('After third checkpoint bytes in use ' + str(bytes_in_use))
        self.assertLess(bytes_in_use, prev_bytes_in_use * 2)

if __name__ == '__main__':
    wttest.run()
=======
from test_cc01 import test_cc_base
from suite_subprocess import suite_subprocess
from wiredtiger import stat
import os
import time

# test_checkpoint33.py
#
# Test that checkpoint will not skip tables that have available space at the end that can be
# reclaimed through truncation.
class test_checkpoint33(test_cc_base, suite_subprocess):
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=32KB,'
    # conn_config = 'verbose=[checkpoint:2]'
    uri = 'table:test_checkpoint33'

    table_numkv = 1000000
    value_size = 1024
    value = 'a' * value_size
    min_file_size = 12 * 1024

    def delete(self, timestamp):
        c = self.session.open_cursor(self.uri, None)
        for k in range(self.table_numkv):
            c.set_key(k)
            self.session.begin_transaction()
            c.remove()
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(timestamp))
        c.close()

    def populate(self, timestamp):
        c = self.session.open_cursor(self.uri, None)
        for k in range(self.table_numkv):
            self.session.begin_transaction()
            c[k] = self.value
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(timestamp))
        c.close()

    def get_size(self):
        c = self.session.open_cursor('statistics:' + self.uri, None, None)
        file_size = c[stat.dsrc.block_size][2]
        c.close()
        return file_size

    def evict_all(self):
        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        for k in range(self.table_numkv):
            evict_cursor.set_key(k)
            evict_cursor.search()
            evict_cursor.reset()
        self.session.rollback_transaction()
        evict_cursor.close()

    def test_checkpoint33(self):

        if os.environ.get("TSAN_OPTIONS"):
            self.skipTest("FIXME-WT-14098 This test fails to compress the table when run under TSan")

        # Pin oldest timestamp 1.
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Create and populate a table at timestamp 2.
        self.session.create(self.uri, self.create_params)
        self.session.checkpoint()
        self.populate(timestamp=2)

        # Make everything stable at timestamp 3.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(3)}')

        # Write to disk.
        self.session.checkpoint()

        # Delete everything at timestamp 4.
        self.delete(timestamp=4)

        # Make the deletions stable at timestamp 5.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(5)}')

        # Write to disk.
        self.session.checkpoint()

        # Evict all pages to ensure they all have disk blocks associated with them.
        self.evict_all()

        # Make everything globally visible.
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(5)}')
        self.prout(f'File size: {self.get_size()}')

        # Wait for checkpoint cleanup to clean up all the deleted pages.
        self.wait_for_cc_to_run()

        # Checkpoint should recover the space by truncating the space made available by
        # checkpoint cleanup. Multiple checkpoints are required to move the blocks around and
        # eventually reach the minimum file size of 12KB.
        checkpoints = 0
        max_checkpoints = 10
        file_size = self.get_size()
        while file_size > self.min_file_size and checkpoints < max_checkpoints:
            self.session.checkpoint()
            file_size = self.get_size()
            checkpoints = checkpoints + 1
            self.prout(f'File size: {file_size}')
            self.prout(f'Checkpoints: {checkpoints}')

        self.assertLessEqual(self.get_size(), self.min_file_size)
>>>>>>> develop
