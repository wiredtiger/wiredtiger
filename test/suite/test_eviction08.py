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
import wiredtiger
import wttest
from wiredtiger import stat

# test_eviction08.py
# Verify the per-btree dirty-index ring is exercised end-to-end.
#
# The ring is fed by every cursor modify on a leaf page and drained by the
# eviction walker. The ring is allocated at btree open (before the handle is
# published for eviction), so the very first cursor write after create/open
# populates it -- no checkpoint is needed to trigger allocation.
#
#   Produce side  cache_eviction_dirty_index_insert        -- leaf entered the ring
#   Consume side  cache_eviction_dirty_index_drain_scanned -- ring slots examined
#
# The connection starts with a roomy cache and high dirty triggers so the
# produce-side checks see uninterrupted inserts. The drain check tightens
# the cache to force eviction pressure.
#
class test_eviction08(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=200MB,statistics=(all),'
                   'eviction_dirty_index=true,'
                   'eviction_dirty_index_disagg=true,'
                   'eviction_dirty_target=80,eviction_dirty_trigger=95,'
                   'eviction_updates_target=80,eviction_updates_trigger=95')

    nrows = 20000
    value_size = 1500
    batch_size = 200

    def get_stat(self, stat_key, uri=None):
        stat_cursor = self.session.open_cursor('statistics:' if uri is None else 'statistics:' + uri)
        val = stat_cursor[stat_key][2]
        stat_cursor.close()
        return val

    def _write_batch(self, cursor, batch_start, batch_end, value):
        # Retry on rollback in case any background pressure rolls the writer back.
        while True:
            self.session.begin_transaction()
            try:
                for i in range(batch_start, batch_end):
                    cursor[i] = value
                self.session.commit_transaction()
                return
            except wiredtiger.WiredTigerError as e:
                self.session.rollback_transaction()
                if 'WT_ROLLBACK' not in str(e):
                    raise

    def _write_rows(self, uri, start, count, value):
        cursor = self.session.open_cursor(uri)
        for batch_start in range(start, start + count, self.batch_size):
            self._write_batch(cursor,
                              batch_start,
                              min(batch_start + self.batch_size, start + count),
                              value)
        cursor.close()

    def test_dirty_index_insert_and_drain(self):
        # Phase 1: the ring is allocated at create/open, so the first wave of
        # writes populates it directly -- the insert counter is non-zero with
        # no checkpoint needed to trigger allocation.
        uri = 'table:test_eviction08'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')

        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)
        self.assertGreater(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri), 0)

        # Phase 2: drive the drain. Tighten the cache and dirty triggers to
        # force eviction pressure, then keep writing so the ring stays
        # non-empty when the walker next visits this btree.
        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')

        for _ in range(40):
            self._write_rows(uri, self.nrows, 500, 'z' * self.value_size)
            drain_consumed = (
                self.get_stat(stat.dsrc.cache_eviction_dirty_index_drain_queued, uri) +
                self.get_stat(stat.dsrc.cache_eviction_dirty_index_drain_filtered, uri) +
                self.get_stat(stat.dsrc.cache_eviction_dirty_index_drain_stale, uri))
            if drain_consumed > 0:
                break
            time.sleep(0.05)

        self.assertGreater(self.get_stat(stat.dsrc.cache_eviction_dirty_index_drain_scanned, uri), 0)
        self.assertGreater(drain_consumed, 0)

    def test_dirty_index_duplicate_suppression(self):
        uri = 'table:test_eviction08_duplicate'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')

        cursor = self.session.open_cursor(uri)
        baseline_insert = self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri)
        for i in range(100):
            cursor[1] = str(i)
        cursor.close()
        self.assertEqual(
            self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri) - baseline_insert, 1)

        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')
        self._write_rows(uri, 2, 10000, 'z' * self.value_size)

        for _ in range(40):
            scanned = self.get_stat(stat.dsrc.cache_eviction_dirty_index_drain_scanned, uri)
            if scanned > 1:
                break
            time.sleep(0.05)
        self.assertGreater(scanned, 1)

    def test_dirty_index_column_insert(self):
        uri = 'table:test_eviction08_column'
        self.session.create(uri, 'key_format=r,value_format=S,leaf_page_max=4KB')
        self._write_rows(uri, 1, self.nrows, 'x' * self.value_size)
        self.assertGreater(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri), 0)

    def test_dirty_index_default_off_for_hook(self):
        if not self.runningHook('disagg'):
            self.skipTest('requires the disagg hook')

        self.reopen_conn(config='cache_size=200MB,statistics=(all),'
                                'eviction_dirty_index=true,'
                                'eviction_dirty_index_disagg=false')
        uri = 'table:test_eviction08_disagg_off'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)
        self.assertEqual(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri), 0)

    def test_dirty_index_disabled(self):
        # Disabling the feature prevents new insertions while the walker remains available.
        self.reopen_conn(config='cache_size=200MB,statistics=(all),'
                                'eviction_dirty_index=false')
        uri = 'table:test_eviction08_off'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')

        baseline_insert = self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri)
        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)
        self.assertEqual(
            self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri) - baseline_insert, 0)

        self.conn.reconfigure('eviction_dirty_index=true')
        enabled_uri = 'table:test_eviction08_enabled'
        self.session.create(enabled_uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(enabled_uri, 0, self.nrows, 'x' * self.value_size)
        self.assertGreater(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, enabled_uri), 0)

        self.conn.reconfigure('eviction_dirty_index=false')
        disabled_uri = 'table:test_eviction08_disabled_again'
        self.session.create(disabled_uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(disabled_uri, 0, self.nrows, 'x' * self.value_size)
        self.assertEqual(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, disabled_uri), 0)

    def test_dirty_index_disabled_at_runtime(self):
        # Reconfiguring the feature off must stand the producer down too, not just
        # the drain. The ring stays allocated (it is freed only at btree close), so
        # a producer keyed solely on ring existence would keep filling it. Open
        # with the feature on, allocate and exercise a ring, then turn it off: no
        # later write may advance the producer counter.
        uri = 'table:test_eviction08_runtime'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)

        self.assertGreater(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, uri), 0)

        self.conn.reconfigure('eviction_dirty_index=false')

        # Application writes are synchronous, so a completed checkpoint leaves no producer work
        # from this session outstanding when the runtime flag is disabled.
        self.session.checkpoint()

        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')
        disabled_uri = 'table:test_eviction08_runtime_disabled'
        self.session.create(disabled_uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(disabled_uri, 0, self.nrows, 'z' * self.value_size)

        self.assertEqual(self.get_stat(stat.dsrc.cache_eviction_dirty_index_insert, disabled_uri), 0)

if __name__ == '__main__':
    wttest.run()
