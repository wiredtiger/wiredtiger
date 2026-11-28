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

import time

import wttest
from wiredtiger import stat

# statistics=(fast) never allocates the per-data-source counter array. File size, free space and
# the approximate leaf-page count are read from the block manager and the btree on each statistics
# cursor. Diagnostic counters stay zero. statistics=(all) and an in-memory connection allocate the
# array at open and count from the start. btree_dsrc_stats counts handles that hold the array.
# btree_dsrc_stats_bytes is the size passed to malloc for those arrays.
class stat21_base(wttest.WiredTigerTestCase):
    create_config = 'key_format=S,value_format=S,leaf_page_max=4KB'

    def assert_dsrc_bytes(self, count_before, bytes_before, count_after):
        count = self.get_stat(stat.conn.btree_dsrc_stats)
        nbytes = self.get_stat(stat.conn.btree_dsrc_stats_bytes)
        self.assertEqual(count, count_after)
        delta_count = count - count_before
        delta_bytes = nbytes - bytes_before
        if delta_count == 0:
            self.assertEqual(delta_bytes, 0)
            return
        self.assertEqual(delta_bytes % delta_count, 0)
        # One array is the 23 slot copies of the data-source statistics struct.
        per_array = delta_bytes // delta_count
        self.assertGreater(per_array, 64 * 1024)
        self.assertLess(per_array, 128 * 1024)

    def dsrc(self, uri, key, config):
        cursor = self.session.open_cursor('statistics:' + uri, None, config)
        value = cursor[key][2]
        cursor.close()
        return value

    def populate(self, uri, nrecords):
        self.session.create(uri, self.create_config)
        cursor = self.session.open_cursor(uri)
        for i in range(nrecords):
            cursor[str(i).zfill(10)] = 'v' * 200
        cursor.set_key(str(0).zfill(10))
        self.assertEqual(cursor.search(), 0)
        cursor.close()


@wttest.skip_for_hook("disagg", "layered tables do not share this btree and block manager")
@wttest.skip_for_hook("tiered", "tiered storage does not use the local block-manager size")
class test_stat21(stat21_base):
    # A tight cache forces eviction splits, which is what moves the btree's leaf-page count.
    conn_config = 'statistics=(fast),cache_size=2MB'
    uri = 'table:test_stat21'

    def test_fast_defers_counters(self):
        allocated = self.get_stat(stat.conn.btree_dsrc_stats)
        allocated_bytes = self.get_stat(stat.conn.btree_dsrc_stats_bytes)
        self.populate(self.uri, 10000)
        self.session.checkpoint()
        cursor = self.session.open_cursor(self.uri)
        for i in range(10000):
            cursor.set_key(str(i).zfill(10))
            self.assertEqual(cursor.remove(), 0)
        cursor.close()
        self.session.checkpoint()

        self.assertGreater(self.get_stat(stat.conn.cursor_insert), 0)
        self.assertGreater(self.get_stat(stat.conn.cursor_search), 0)
        self.assertGreater(self.get_stat(stat.conn.cursor_remove), 0)

        # statistics=(size) reads the file size and must not be what starts the counters.
        block_size = self.dsrc(self.uri, stat.dsrc.block_size, 'statistics=(size)')
        self.assertGreater(block_size, 0)

        # A fast cursor does not install the array. Diagnostic counters stay zero across cursors.
        # Size, free space and the leaf-page count come from the block manager and the btree.
        fast = self.session.open_cursor('statistics:' + self.uri, None, 'statistics=(fast)')
        self.assertEqual(fast[stat.dsrc.cursor_insert][2], 0)
        self.assertEqual(fast[stat.dsrc.cursor_remove][2], 0)
        self.assertEqual(fast[stat.dsrc.cursor_search][2], 0)
        self.assertEqual(fast[stat.dsrc.block_size][2], block_size)
        self.assertGreater(fast[stat.dsrc.block_reuse_bytes][2], 0)
        self.assertGreater(fast[stat.dsrc.btree_row_leaf_pages][2], 0)
        fast.close()

        conn_inserts = self.get_stat(stat.conn.cursor_insert)
        cursor = self.session.open_cursor(self.uri)
        cursor['after'] = 'x'
        cursor.close()
        self.assertGreater(self.get_stat(stat.conn.cursor_insert), conn_inserts)
        self.assertEqual(self.dsrc(self.uri, stat.dsrc.cursor_insert, 'statistics=(fast)'), 0)
        self.assertEqual(self.dsrc(self.uri, stat.dsrc.block_size, 'statistics=(fast)'), block_size)
        self.assert_dsrc_bytes(allocated, allocated_bytes, allocated)

        # The size-summary walk is the fast-connection path that does install the array.
        size = self.session.open_cursor(self.uri, None, 'debug=(size_stats)')
        size.close()
        self.assert_dsrc_bytes(allocated, allocated_bytes, allocated + 1)

@wttest.skip_for_hook("disagg", "layered tables do not share this btree and block manager")
@wttest.skip_for_hook("tiered", "tiered storage does not use the local block-manager size")
class test_stat21_all(stat21_base):
    conn_config = 'statistics=(all),file_manager=(close_handle_minimum=0,close_scan_interval=1)'

    def test_all_counts_from_open(self):
        uri = 'table:test_stat21_all'
        allocated = self.get_stat(stat.conn.btree_dsrc_stats)
        allocated_bytes = self.get_stat(stat.conn.btree_dsrc_stats_bytes)
        self.populate(uri, 10)
        self.assert_dsrc_bytes(allocated, allocated_bytes, allocated + 1)
        self.assertGreater(self.get_stat(stat.dsrc.cursor_insert, uri), 0)
        self.assertGreater(self.get_stat(stat.dsrc.cursor_search, uri), 0)
        self.session.checkpoint()
        self.session.drop(uri, None)
        self.session.close()
        self.session = self.conn.open_session()
        # Sweep destroys the handle, which frees the array.
        deadline = time.time() + 10
        while (self.get_stat(stat.conn.btree_dsrc_stats) != allocated or
                self.get_stat(stat.conn.btree_dsrc_stats_bytes) != allocated_bytes):
            self.assertLess(time.time(), deadline, 'dropped btree still has data-source statistics')
            time.sleep(0.1)


@wttest.skip_for_hook("disagg", "in-memory is not a layered table")
@wttest.skip_for_hook("tiered", "in-memory is not tiered storage")
class test_stat21_inmem(stat21_base):
    conn_config = 'statistics=(fast),in_memory=true,log=(enabled=false)'

    def test_in_memory_counts_from_open(self):
        uri = 'table:test_stat21_inmem'
        allocated = self.get_stat(stat.conn.btree_dsrc_stats)
        allocated_bytes = self.get_stat(stat.conn.btree_dsrc_stats_bytes)
        self.populate(uri, 10)
        self.assert_dsrc_bytes(allocated, allocated_bytes, allocated + 1)
        self.assertGreater(self.get_stat(stat.dsrc.cursor_insert, uri), 0)
        self.assertGreater(self.get_stat(stat.dsrc.cursor_insert_bytes, uri), 0)
        self.assertGreater(self.get_stat(stat.dsrc.cursor_search, uri), 0)
