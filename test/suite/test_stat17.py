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

import re
import wttest
from wiredtiger import stat

# test_stat17.py
#   Tests for btree_row_leaf_avg_entries, btree_row_leaf_pages, and
#   btree_row_leaf_pages_accurate.
#
#   btree_row_leaf_avg_entries:        EWMA of K/V pairs per row-store leaf page.
#   btree_row_leaf_pages:               incremental approximate leaf page count.
#   btree_row_leaf_pages_accurate:      whether btree_row_leaf_pages can be trusted.
#
#   Both leaf-page stats are available without WT_STAT_TYPE_TREE_WALK.
#
#   btree_row_leaf_pages is incremented at each leaf split (in-memory or
#   eviction). The cache is kept small so that eviction splits fire during
#   inserts, giving the counter something to track without a tree walk.
#
#   When WT_STAT_TYPE_TREE_WALK is requested, btree_row_leaf_pages is
#   corrected to the exact count obtained by the walk; the corrected value is
#   reflected in both the stat cursor and the in-memory btree field so a
#   subsequent fast read still sees it.
#
#   All three values are persisted through checkpoint metadata and survive a
#   server restart.
#
#   A table created after this stat was added always has an accurate count
#   (it starts empty, which is exact). A table checkpointed before this
#   tracking existed has no way to tell "empty" apart from "never corrected";
#   btree_row_leaf_pages_accurate lets a caller distinguish the two by
#   reading false until a WT_STAT_TYPE_TREE_WALK stats cursor corrects it.
class test_stat17(wttest.WiredTigerTestCase):
    uri = 'table:test_stat17'

    # Small pages and a tight cache ensure leaf splits (both in-memory and
    # eviction) fire during inserts, so approx_leaf_pages is non-zero after
    # a checkpoint without requiring a tree walk.
    conn_config = 'statistics=(all),cache_size=2MB'
    create_params = 'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=4KB'
    nrows = 10000

    def _insert(self, n):
        c = self.session.open_cursor(self.uri)
        for i in range(n):
            c[str(i).zfill(10)] = 'v' + str(i).zfill(10)
        c.close()

    def _scan(self):
        c = self.session.open_cursor(self.uri)
        while c.next() == 0:
            pass
        c.close()

    def _dsrc_stat(self, stat_key, cfg='fast'):
        sc = self.session.open_cursor('statistics:' + self.uri, None,
                                      'statistics=(' + cfg + ')')
        val = sc[stat_key][2]
        sc.close()
        return val

    # Both stats must be non-zero without a tree walk. The small cache forces
    # eviction splits during inserts, incrementing approx_leaf_pages. The
    # EWMA is updated at reconciliation when pages are written to disk.
    def test_available_without_tree_walk(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        avg   = self._dsrc_stat(stat.dsrc.btree_row_leaf_avg_entries)
        pages = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)

        self.assertGreater(avg, 0,
            'btree_row_leaf_avg_entries must be non-zero after reconciliation')
        self.assertGreater(pages, 0,
            'btree_row_leaf_pages must be non-zero after eviction splits')

    # The EWMA should be within 50% of the exact average (total entries /
    # leaf pages from a tree walk). Tested after a reopen + full scan so that
    # fault-in updates the EWMA from on-disk page sizes.  The 50% bound is
    # intentionally loose; the EWMA converges gradually across many accesses.
    def test_avg_entries_within_tolerance(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        self.reopen_conn()
        self._scan()

        approx_avg = self._dsrc_stat(stat.dsrc.btree_row_leaf_avg_entries)

        exact_entries = self._dsrc_stat(stat.dsrc.btree_entries, 'all')
        exact_pages   = self._dsrc_stat(stat.dsrc.btree_row_leaf, 'all')

        self.assertGreater(exact_pages, 0)
        exact_avg = exact_entries // exact_pages

        self.assertGreater(approx_avg, exact_avg // 2,
            'btree_row_leaf_avg_entries %d too low vs exact avg %d'
            % (approx_avg, exact_avg))
        self.assertLess(approx_avg, exact_avg * 2,
            'btree_row_leaf_avg_entries %d too high vs exact avg %d'
            % (approx_avg, exact_avg))

    # After a tree walk (statistics=(all)), both btree_row_leaf_pages and
    # btree_row_leaf_avg_entries must equal the exact values from the walk.
    def test_corrected_by_tree_walk(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        sc = self.session.open_cursor('statistics:' + self.uri, None,
                                      'statistics=(all)')
        exact_pages   = sc[stat.dsrc.btree_row_leaf][2]
        exact_entries = sc[stat.dsrc.btree_entries][2]
        corrected_pages = sc[stat.dsrc.btree_row_leaf_pages][2]
        corrected_avg   = sc[stat.dsrc.btree_row_leaf_avg_entries][2]
        sc.close()

        self.assertGreater(exact_pages, 0)
        self.assertEqual(corrected_pages, exact_pages,
            'btree_row_leaf_pages (%d) must equal btree_row_leaf (%d) after tree walk'
            % (corrected_pages, exact_pages))
        self.assertEqual(corrected_avg, exact_entries // exact_pages,
            'btree_row_leaf_avg_entries (%d) must equal exact avg (%d) after tree walk'
            % (corrected_avg, exact_entries // exact_pages))

    # After a tree walk corrects both counters in memory, subsequent fast-stat
    # reads (no walk) must return the corrected values.
    def test_correction_persists_in_memory(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        sc = self.session.open_cursor('statistics:' + self.uri, None,
                                      'statistics=(all)')
        exact_pages   = sc[stat.dsrc.btree_row_leaf][2]
        exact_entries = sc[stat.dsrc.btree_entries][2]
        sc.close()

        fast_pages = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)
        fast_avg   = self._dsrc_stat(stat.dsrc.btree_row_leaf_avg_entries)

        self.assertEqual(fast_pages, exact_pages,
            'fast read after tree-walk correction should return %d, got %d'
            % (exact_pages, fast_pages))
        self.assertEqual(fast_avg, exact_entries // exact_pages,
            'fast avg after tree-walk correction should return %d, got %d'
            % (exact_entries // exact_pages, fast_avg))

    # Both stats must survive a server restart. The checkpoint during the
    # insert run saves the values; after reopen they are restored from the
    # checkpoint metadata (meta_ckpt.c parse + bt_handle.c restore path).
    def test_checkpoint_persistence(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        avg_before   = self._dsrc_stat(stat.dsrc.btree_row_leaf_avg_entries)
        pages_before = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)

        self.assertGreater(avg_before, 0)
        self.assertGreater(pages_before, 0)

        self.reopen_conn()

        avg_after   = self._dsrc_stat(stat.dsrc.btree_row_leaf_avg_entries)
        pages_after = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)

        self.assertEqual(avg_after, avg_before,
            'btree_row_leaf_avg_entries must survive checkpoint/restart')

        # approx_leaf_pages is restored from the checkpoint snapshot.
        # Between the checkpoint write and the pages_before read, the
        # deleted_entries cleanup loop may have decremented the in-memory
        # counter, so pages_after (checkpoint value) >= pages_before.
        self.assertGreaterEqual(pages_after, pages_before,
            'btree_row_leaf_pages must be at least as large after restart')

    # A table created after this stat was added starts empty, which is an
    # exact count, so it's accurate from the very first checkpoint even
    # without a tree walk.
    def test_accurate_for_new_table(self):
        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()

        accurate = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages_accurate)
        self.assertEqual(accurate, 1,
            'a table created after this stat exists should always be accurate')

        self.reopen_conn()
        accurate = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages_accurate)
        self.assertEqual(accurate, 1,
            'accuracy must survive checkpoint/restart for a table created with this stat')

    # Simulate a table checkpointed before this stat existed: strip
    # approx_leaf_pages_accurate (and zero approx_leaf_pages) from the
    # underlying file's metadata, the way metadata written by a version of
    # WT that predates this tracking would look.
    def _simulate_legacy_metadata(self, fileuri):
        mc = self.session.open_cursor('metadata:', None, 'readonly=false')
        mc.set_key(fileuri)
        mc.search()
        config = mc.get_value()
        config = re.sub(r'approx_leaf_pages_accurate=\d+', 'approx_leaf_pages_accurate=0', config)
        config = re.sub(r'approx_leaf_pages=\d+', 'approx_leaf_pages=0', config)
        mc.set_key(fileuri)
        mc.set_value(config)
        mc.update()
        mc.close()

    # A plain reopen_conn() checkpoints any dirty tree on close, which would
    # immediately overwrite our simulated legacy metadata with the true
    # in-memory state if background eviction has touched the tree since the
    # last explicit checkpoint() call. Skip that close-time checkpoint so
    # the on-disk metadata we just edited is what gets read back.
    def _reopen_conn_no_checkpoint(self):
        self.close_conn('debug=(skip_checkpoint=true)')
        self.open_conn()

    # A table whose metadata predates this stat (simulated here) must read
    # as inaccurate, even though btree_row_leaf_pages itself reads 0 - the
    # same 0 a genuinely empty table would report.
    def test_inaccurate_for_legacy_table(self):
        fileuri = 'file:' + self.uri.split(':')[1] + '.wt'

        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()
        self._simulate_legacy_metadata(fileuri)
        self._reopen_conn_no_checkpoint()

        accurate = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages_accurate)
        pages = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)
        self.assertEqual(accurate, 0,
            'table with metadata predating this stat should read as inaccurate')
        self.assertEqual(pages, 0,
            'the simulated legacy counter should read 0, same as a genuinely empty table')

    # Once a tree walk corrects a legacy table's counter and a subsequent
    # write dirties the tree so the correction is checkpointed, the accurate
    # flag flips true and stays true across a restart.
    def test_legacy_table_corrected_by_tree_walk(self):
        fileuri = 'file:' + self.uri.split(':')[1] + '.wt'

        self.session.create(self.uri, self.create_params)
        self._insert(self.nrows)
        self.session.checkpoint()
        self._simulate_legacy_metadata(fileuri)
        self._reopen_conn_no_checkpoint()

        self.assertEqual(self._dsrc_stat(stat.dsrc.btree_row_leaf_pages_accurate), 0)

        # The tree-walk correction only updates in-memory state; it needs a
        # subsequent write to dirty the tree so the next checkpoint actually
        # persists the correction (a checkpoint on an unmodified tree is a
        # no-op and would leave the stale metadata in place).
        exact_pages = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages, 'all')
        self.assertGreater(exact_pages, 0)
        c = self.session.open_cursor(self.uri)
        c['extra_key'] = 'v'
        c.close()
        self.session.checkpoint()

        self._reopen_conn_no_checkpoint()
        accurate = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages_accurate)
        pages = self._dsrc_stat(stat.dsrc.btree_row_leaf_pages)
        self.assertEqual(accurate, 1,
            'accuracy must flip true once a tree walk has corrected the counter and it is checkpointed')
        self.assertEqual(pages, exact_pages,
            'corrected count must survive the restart')
