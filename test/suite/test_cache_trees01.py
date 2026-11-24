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

# Check the statistics describing how the cache is spread across trees. close_idle_time=0 keeps the
# trees open, and the raised eviction targets keep the eviction server from evicting the pages the
# tests write.
class test_cache_trees01(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=100MB,statistics=(all),'
        'file_manager=(close_scan_interval=1,close_idle_time=0),'
        'eviction_dirty_target=75,eviction_dirty_trigger=90,'
        'eviction_updates_target=70,eviction_updates_trigger=85')

    buckets = [stat.conn.cache_trees_lt_1mb, stat.conn.cache_trees_1mb_16mb,
        stat.conn.cache_trees_16mb_256mb, stat.conn.cache_trees_ge_256mb]
    value = 'v' * 4096
    timeout = 60

    def cache_trees(self):
        cursor = self.session.open_cursor('statistics:', None, None)
        values = {}
        self.descriptions = {}
        for key in [stat.conn.cache_trees_no_leaf, stat.conn.cache_trees_dirty_leaf] + \
                self.buckets:
            self.descriptions[key], _, values[key] = cursor[key]
        cursor.close()
        return values

    # The sweep server publishes these statistics once per close_scan_interval, so a change isn't
    # visible until its next pass. On timeout, report whether sweep ran: dh_sweeps counts a pass as
    # it starts, so an advance of two means a pass that started after the caller's changes finished.
    def wait_for(self, cond, msg):
        sweeps = self.get_stat(stat.conn.dh_sweeps)
        deadline = time.time() + self.timeout
        while True:
            values = self.cache_trees()
            if cond(values):
                return values
            if time.time() > deadline:
                self.assertGreaterEqual(self.get_stat(stat.conn.dh_sweeps), sweeps + 2,
                    '%s: sweep did not run' % msg)
                self.fail('%s: %s' % (msg,
                    {self.descriptions[k]: v for k, v in values.items()}))
            time.sleep(0.1)

    def leaf_trees(self, values):
        return sum(values[t] for t in self.buckets)

    def trees_counted(self, values):
        return values[stat.conn.cache_trees_no_leaf] + self.leaf_trees(values)

    def populate(self, uri, rows):
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(rows):
            cursor['k%08d' % i] = self.value
        cursor.close()

    def test_internal_trees_excluded(self):
        # Without any tables, the only open trees are the metadata and history store, which must
        # not be counted. Wait for a full sweep after this point, since the statistics also read
        # zero before the first one.
        sweeps = self.get_stat(stat.conn.dh_sweeps)
        deadline = time.time() + self.timeout
        while self.get_stat(stat.conn.dh_sweeps) < sweeps + 2:
            self.assertLess(time.time(), deadline, 'sweep did not run')
            time.sleep(0.1)
        self.assertEqual(self.trees_counted(self.cache_trees()), 0)

    def test_no_leaf(self):
        # An empty table has no leaf page until its first insert creates one.
        ntables = 3
        uris = ['table:trees_no_leaf%d' % i for i in range(ntables)]
        for uri in uris:
            self.session.create(uri, 'key_format=S,value_format=S')
            self.session.open_cursor(uri).close()
        self.wait_for(
            lambda v: v[stat.conn.cache_trees_no_leaf] >= ntables and self.leaf_trees(v) == 0,
            'empty trees not counted as having no leaf pages')

        # Keep the cursor positioned on the inserted row so the new leaf page stays resident.
        cursor = self.session.open_cursor(uris[0])
        cursor['k%08d' % 0] = self.value
        self.assertEqual(cursor['k%08d' % 0], self.value)
        self.wait_for(lambda v: self.leaf_trees(v) >= 1,
            'tree with an inserted row still counted as having no leaf pages')
        cursor.close()

    def test_dirty_leaf(self):
        ntables = 5
        for i in range(ntables):
            self.populate('table:trees_dirty%d' % i, 1)
        self.wait_for(lambda v: v[stat.conn.cache_trees_dirty_leaf] >= ntables,
            'dirty trees not counted')

        self.session.checkpoint()
        self.wait_for(lambda v: v[stat.conn.cache_trees_dirty_leaf] < ntables,
            'trees still counted as dirty after a checkpoint')

    def test_size_buckets(self):
        # About 4MB of leaf pages, well inside the 1MB to 16MB bucket.
        self.populate('table:trees_large', 1000)
        nsmall = 5
        for i in range(nsmall):
            self.populate('table:trees_small%d' % i, 1)
        # Checkpoint so the pages are clean and stay resident without cache pressure.
        self.session.checkpoint()

        self.wait_for(lambda v:
            v[stat.conn.cache_trees_1mb_16mb] >= 1 and
            v[stat.conn.cache_trees_lt_1mb] >= nsmall,
            'trees not counted in their size buckets')

if __name__ == '__main__':
    wttest.run()
