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
from wiredtiger import stat

# Verify cache_read_internal and cache_read_leaf statistics track page reads
# into cache separately by page type.
class test_stat16(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'table:{test_name}'

    # Small page sizes ensure multiple leaf pages and at least one internal page.
    conn_config = 'statistics=(all),cache_size=100MB,mmap=false,prefetch=(available=false)'
    create_config = 'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=4KB'

    def get_conn_stat(self, stat_key):
        return self.get_stat(stat_key)

    def get_read_stats(self):
        cursor = self.session.open_cursor('statistics:' + self.uri, None, 'statistics=(fast)')
        try:
            return tuple(cursor[key][2] for key in (
                stat.dsrc.cache_read, stat.dsrc.cache_read_internal,
                stat.dsrc.cache_read_leaf, stat.dsrc.cache_bytes_read))
        finally:
            cursor.close()

    @wttest.skip_for_hook("disagg", "B-tree preload is disabled for disaggregated connections")
    def test_preload_not_counted_as_cache_read(self):
        # Keep the tree shallow so opening it loads only the root into cache.
        self.session.create(self.uri,
            'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=64KB')
        nrows = 1000
        value = 'v' * 200
        cursor = self.session.open_cursor(self.uri)
        for i in range(nrows):
            cursor[str(i).zfill(8)] = value
        cursor.close()
        self.session.checkpoint()
        self.reopen_conn()

        preload_before = self.get_conn_stat(stat.conn.block_preload)
        cursor = self.session.open_cursor(self.uri)
        self.assertGreater(self.get_conn_stat(stat.conn.block_preload), preload_before,
            'opening the table must preload pages')

        total, internal, leaf, root_bytes = self.get_read_stats()
        self.assertEqual(leaf, 0, 'opening an unpositioned cursor must not load leaf pages into cache')
        self.assertEqual(internal, 1, 'the root must be counted as a cache read')
        self.assertEqual(total, internal)
        self.assertGreater(root_bytes, 0)

        count = 0
        while cursor.next() == 0:
            self.assertEqual(cursor.get_value(), value)
            count += 1
        self.assertEqual(count, nrows)

        reads_after_scan = self.get_read_stats()
        total, internal, leaf, scan_bytes = reads_after_scan
        self.assertGreater(leaf, 0)
        self.assertEqual(internal, 1)
        self.assertEqual(total, internal + leaf)
        self.assertGreater(scan_bytes, root_bytes)

        cursor.reset()
        count = 0
        while cursor.next() == 0:
            count += 1
        self.assertEqual(count, nrows)
        self.assertEqual(self.get_read_stats(), reads_after_scan,
            'reading resident pages must not increment cache read statistics')
        cursor.close()

    def test_cache_read_internal_and_leaf(self):
        self.session.create(self.uri, self.create_config)
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(5000):
            cursor[str(i).zfill(8)] = 'value_' + str(i)
        cursor.close()

        # Checkpoint so all pages are on disk.
        self.session.checkpoint(None)

        # Reopen to clear the cache, forcing subsequent reads to come from disk.
        self.reopen_conn()

        # Read all records to trigger cache misses for both leaf and internal pages.
        cursor = self.session.open_cursor(self.uri, None, None)
        while cursor.next() == 0:
            pass
        cursor.close()

        internal_reads = self.get_conn_stat(stat.conn.cache_read_internal)
        leaf_reads = self.get_conn_stat(stat.conn.cache_read_leaf)
        total_reads = self.get_conn_stat(stat.conn.cache_read)

        self.assertGreater(internal_reads, 0,
            'expected cache_read_internal > 0 after reading a multi-page btree')
        self.assertGreater(leaf_reads, 0,
            'expected cache_read_leaf > 0 after reading pages from disk')
        self.assertEqual(internal_reads + leaf_reads, total_reads,
            'cache_read_internal + cache_read_leaf must equal cache_read')
