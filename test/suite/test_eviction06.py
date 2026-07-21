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

# test_eviction06.py
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
class test_eviction06(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=200MB,statistics=(all),'
                   'eviction_dirty_index=true,'
                   'eviction_dirty_target=80,eviction_dirty_trigger=95,'
                   'eviction_updates_target=80,eviction_updates_trigger=95')

    nrows = 20000
    value_size = 1500
    batch_size = 200

    def get_stat(self, stat_key):
        stat_cursor = self.session.open_cursor('statistics:')
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
        uri = 'table:test_eviction06'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')

        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)
        self.assertGreater(self.get_stat(stat.conn.cache_eviction_dirty_index_insert), 0)

        # Phase 2: drive the drain. Tighten the cache and dirty triggers to
        # force eviction pressure, then keep writing so the ring stays
        # non-empty when the walker next visits this btree.
        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')

        for _ in range(40):
            self._write_rows(uri, self.nrows, 500, 'z' * self.value_size)
            if self.get_stat(stat.conn.cache_eviction_dirty_index_drain_scanned) > 0:
                break
            time.sleep(0.05)

        self.assertGreater(self.get_stat(stat.conn.cache_eviction_dirty_index_drain_scanned), 0)

    # The producer also deduplicates on the page's dirty_index_slot back-pointer
    # (a non-zero value means the page is already in the ring). That property is
    # not asserted here: with the ring allocated at open and the eviction drain
    # always active on an evictable tree, the drain pops and re-fills slots
    # between any two write waves, so the insert counter cannot cleanly isolate
    # the back-pointer fast path. That check is exercised implicitly by the
    # insert/drain test and verified by code review.

    def test_dirty_index_disabled(self):
        # Reopen configured off from the start, rather than reconfiguring a live
        # connection to false. A runtime reconfigure only stands the drain down:
        # it does not free rings already allocated for trees opened while the
        # feature was on, and the producer fast path keys off ring existence, not
        # the live flag. On a disaggregated leader those background trees keep
        # taking writes, so their producers would go on filling the surviving
        # rings and the insert counter would climb. Opening with the feature off
        # means no ring is ever allocated, so neither the producer nor the drain
        # can advance a counter under the same write + eviction pressure.
        self.reopen_conn(config='cache_size=200MB,statistics=(all),'
                                'eviction_dirty_index=false')
        uri = 'table:test_eviction06_off'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')

        baseline_insert = self.get_stat(stat.conn.cache_eviction_dirty_index_insert)
        baseline_drain  = self.get_stat(stat.conn.cache_eviction_dirty_index_drain_scanned)

        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)
        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')
        for _ in range(20):
            self._write_rows(uri, self.nrows, 500, 'z' * self.value_size)
            time.sleep(0.05)

        self.assertEqual(self.get_stat(stat.conn.cache_eviction_dirty_index_insert)       - baseline_insert, 0)
        self.assertEqual(self.get_stat(stat.conn.cache_eviction_dirty_index_drain_scanned) - baseline_drain,  0)

    def test_dirty_index_disabled_at_runtime(self):
        # Reconfiguring the feature off must stand the producer down too, not just
        # the drain. The ring stays allocated (it is freed only at btree close), so
        # a producer keyed solely on ring existence would keep filling it. Open
        # with the feature on, allocate and exercise a ring, then turn it off: no
        # later write may advance the producer counter.
        uri = 'table:test_eviction06_runtime'
        self.session.create(uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        self._write_rows(uri, 0, self.nrows, 'x' * self.value_size)

        self.assertGreater(self.get_stat(stat.conn.cache_eviction_dirty_index_insert), 0)

        self.conn.reconfigure('eviction_dirty_index=false')

        # Let any in-flight insert settle before the baseline: a checkpoint plus a
        # short pause lets an insert outstanding when the flag flipped finish and
        # be counted. Once the disable is visible every producer bails, so the
        # post-baseline delta isolates activity under the disabled feature -- none.
        self.session.checkpoint()
        time.sleep(1)
        baseline_insert = self.get_stat(stat.conn.cache_eviction_dirty_index_insert)

        self.conn.reconfigure('cache_size=20MB,'
                              'eviction_dirty_target=2,eviction_dirty_trigger=5')
        for _ in range(20):
            self._write_rows(uri, self.nrows, 500, 'z' * self.value_size)
            time.sleep(0.05)

        self.assertEqual(self.get_stat(stat.conn.cache_eviction_dirty_index_insert) - baseline_insert, 0)

if __name__ == '__main__':
    wttest.run()
