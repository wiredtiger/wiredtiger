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

# Check the statistics describing how the cache is spread across trees. They refresh every
# close_scan_interval, so each check polls with a deadline; close_idle_time=0 keeps the trees open.
class test_cache_census01(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=100MB,statistics=(all),'
        'file_manager=(close_scan_interval=1,close_idle_time=0),'
        'eviction_dirty_target=75,eviction_dirty_trigger=90,'
        'eviction_updates_target=70,eviction_updates_trigger=85')

    buckets = [stat.conn.cache_trees_lt_1mb, stat.conn.cache_trees_1mb_16mb,
        stat.conn.cache_trees_ge_16mb]
    value = 'v' * 4096
    timeout = 60

    def census(self):
        cursor = self.session.open_cursor('statistics:', None, None)
        values = {}
        self.descriptions = {}
        for key in [stat.conn.cache_trees_no_leaf, stat.conn.cache_trees_dirty_leaf] + \
                self.buckets:
            self.descriptions[key], _, values[key] = cursor[key]
        cursor.close()
        return values

    def wait_for(self, cond, msg):
        deadline = time.time() + self.timeout
        while True:
            values = self.census()
            if cond(values):
                return values
            self.assertLess(time.time(), deadline, '%s: %s' % (msg,
                {self.descriptions[k]: v for k, v in values.items()}))
            time.sleep(0.1)

    def trees_counted(self, values):
        return values[stat.conn.cache_trees_no_leaf] + sum(values[t] for t in self.buckets)

    def populate(self, uri, rows):
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(rows):
            cursor['k%08d' % i] = self.value
        cursor.close()

    def evict(self, uri, key):
        cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        self.session.begin_transaction()
        cursor.set_key(key)
        cursor.search()
        cursor.reset()
        cursor.close()
        self.session.rollback_transaction()

    def test_internal_trees_excluded(self):
        self.populate('table:census_small', 1)
        values = self.wait_for(lambda v: self.trees_counted(v) >= 1, 'user tree not counted')

        # The metadata and history store trees are open but must not be counted. A hook can back
        # one table with several trees, so only a plain table has an exact count.
        if not self.runningHook('tiered') and not self.runningHook('disagg'):
            self.assertEqual(self.trees_counted(values), 1)

    def test_no_leaf(self):
        ntables = 3
        uris = ['table:census_no_leaf%d' % i for i in range(ntables)]
        for uri in uris:
            self.populate(uri, 1)
        self.session.checkpoint()
        for uri in uris:
            self.evict(uri, 'k%08d' % 0)

        values = self.wait_for(lambda v: v[stat.conn.cache_trees_no_leaf] >= ntables,
            'trees with evicted leaf pages not counted')

        # Reading a row back brings a leaf page into the cache. Keep the cursor positioned so the
        # page stays resident: some configurations evict it as soon as the cursor lets it go.
        no_leaf = values[stat.conn.cache_trees_no_leaf]
        cursor = self.session.open_cursor(uris[0])
        self.assertEqual(cursor['k%08d' % 0], self.value)
        self.wait_for(lambda v: v[stat.conn.cache_trees_no_leaf] < no_leaf,
            'tree with a leaf page read back still counted as having none')
        cursor.close()

    def test_dirty_leaf(self):
        ntables = 5
        for i in range(ntables):
            self.populate('table:census_dirty%d' % i, 1)
        self.wait_for(lambda v: v[stat.conn.cache_trees_dirty_leaf] >= ntables,
            'dirty trees not counted')

        self.session.checkpoint()
        self.wait_for(lambda v: v[stat.conn.cache_trees_dirty_leaf] < ntables,
            'trees still counted as dirty after a checkpoint')

    def test_size_buckets(self):
        # About 4MB of leaf pages, well inside the 1MB to 16MB bucket.
        self.populate('table:census_large', 1000)
        nsmall = 5
        for i in range(nsmall):
            self.populate('table:census_small%d' % i, 1)
        # Checkpoint so the pages are clean and stay resident without cache pressure.
        self.session.checkpoint()

        self.wait_for(lambda v:
            v[stat.conn.cache_trees_1mb_16mb] >= 1 and
            v[stat.conn.cache_trees_lt_1mb] >= nsmall,
            'trees not counted in their size buckets')

if __name__ == '__main__':
    wttest.run()
