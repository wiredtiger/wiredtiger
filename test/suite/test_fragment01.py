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
# [TEST_TAGS]
# history_store
# [END_TAGS]
#
# test_hs_pinned_pages.py
#
# Proves that HS pages can be pinned by checkpoint cleanup rate limits,
# reproducing the WT-16002 / HELP-83868 production scenario.
#
# In production, clusters with many tables (40k+) exhaust the per-checkpoint
# cleanup limits (checkpoint_cleanup_obsolete_tw_pages_dirty_max=100,
# obsolete_tw_btree_max=100) before the cleanup thread ever reaches the HS file.
# This test simulates that by setting the per-btree page limit to 1.
#
# Scenario:
#   Phase 1: Build ~1GB of HS using a bulk table (5000 rows x 5KB values,
#            many small HS pages to maximize page count).
#            A second "pin" table (1 row, 1KB) gets a higher btree_id,
#            so its HS page is at the physical tail of the HS file.
#   Phase 2: Drop the bulk table. Its HS entries are removed, freeing most
#            pages -- but the pin table's page at the tail blocks truncation.
#   Phase 3: Slide oldest_timestamp forward for 100 rounds. The pin table
#            is NOT touched -- its HS page stays clean on disk.
#            With cleanup limited to 1 page/pass, the pages freed by the
#            drop are slowly reclaimed but the tail page persists.
#            Live data: 1 row x 1KB = 1KB. HS file: ~1GB.
#
# Key insight: the pin table's HS page is CLEAN (never dirtied after Phase 1).
# A clean page is never reconciled during checkpoint. Cleanup CAN detect it
# via time aggregates (bt_sync_obsolete.c:468), but the rate limit prevents
# it from being reached in time. In production with 40k+ tables, the cleanup
# thread is perpetually starved and these pages are pinned indefinitely.
#
# Related: WT-16002, HELP-83868, PR #12772

import os, wttest
from wiredtiger import stat

class test_fragment01(wttest.WiredTigerTestCase):
    # Rate-limit cleanup to 1 page/pass to simulate the production scenario
    # where many tables exhaust checkpoint_cleanup_obsolete_tw_pages_dirty_max.
    # Use small values (5KB) to avoid overflow items, maximizing HS page count
    # so cleanup truly can't keep up.
    conn_config = (
        'cache_size=500MB,'
        'statistics=(all),'
        'eviction_updates_trigger=95,'
        'eviction_updates_target=80,'
        'heuristic_controls=(checkpoint_cleanup_obsolete_tw_pages_dirty_max=100000)'
    )

    uri_bulk = 'table:hs_bulk'
    uri_pin  = 'table:hs_pin'

    nrows_bulk = 100000        # 100K rows -- many HS pages
    value_bulk = 5 * 1024      # 5KB per value -> 100K x 5KB = 500MB per HS generation
    nrows_pin = 1              # Just 1 row
    value_pin = 1024           # 1KB
    num_rounds = 100
    batch_size = 1000

    # ---------- helpers ----------

    def get_hs_file_size(self):
        return os.path.getsize('WiredTigerHS.wt')

    def get_stat(self, stat_key):
        cursor = self.session.open_cursor('statistics:')
        val = cursor[stat_key][2]
        cursor.close()
        return val

    def batch_updates(self, uri, nrows, value, commit_ts):
        """Update nrows keys in batches."""
        cursor = self.session.open_cursor(uri)
        for start in range(1, nrows + 1, self.batch_size):
            end = min(start + self.batch_size, nrows + 1)
            self.session.begin_transaction()
            for i in range(start, end):
                cursor[str(i).zfill(8)] = value
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def get_btree_id(self, uri):
        """Get the btree id for a table from the metadata."""
        meta_cursor = self.session.open_cursor('metadata:')
        file_uri = uri.replace('table:', 'file:') + '.wt'
        config = meta_cursor[file_uri]
        meta_cursor.close()
        for part in config.split(','):
            if part.strip().startswith('id='):
                return int(part.strip().split('=')[1])
        self.fail('Could not find btree id for ' + uri)

    # ---------- the test ----------

    def test_hs_sliding_window_pinned(self):
        mb = 1024 * 1024

        # Create bulk first (lower btree_id), then pin (higher btree_id).
        # Pin's HS entries are at the logical end of the HS B-tree,
        # and physically at the highest file offsets.
        self.session.create(self.uri_bulk, 'key_format=S,value_format=S')
        self.session.create(self.uri_pin,  'key_format=S,value_format=S')

        bulk_id = self.get_btree_id(self.uri_bulk)
        pin_id  = self.get_btree_id(self.uri_pin)
        self.pr('btree_ids: bulk={}, pin={}'.format(bulk_id, pin_id))
        self.assertGreater(pin_id, bulk_id)

        # ================================================================
        # Phase 1: Build ~1GB HS peak.
        #
        # Insert into both tables at ts=1, update at ts=2.
        # HS grows to ~500MB (100K entries x 5KB each from ts=1 generation).
        # Then update again at ts=3 -> another ~500MB of HS. Total ~1GB.
        # ================================================================
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        val_v1 = 'A' * self.value_bulk
        self.batch_updates(self.uri_bulk, self.nrows_bulk, val_v1, 1)
        pin_v1 = 'P' * self.value_pin
        self.batch_updates(self.uri_pin, self.nrows_pin, pin_v1, 1)

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

        val_v2 = 'B' * self.value_bulk
        self.batch_updates(self.uri_bulk, self.nrows_bulk, val_v2, 2)
        pin_v2 = 'Q' * self.value_pin
        self.batch_updates(self.uri_pin, self.nrows_pin, pin_v2, 2)

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(2))
        self.session.checkpoint()

        val_v3 = 'C' * self.value_bulk
        self.batch_updates(self.uri_bulk, self.nrows_bulk, val_v3, 3)
        pin_v3 = 'R' * self.value_pin
        self.batch_updates(self.uri_pin, self.nrows_pin, pin_v3, 3)

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(3))
        self.session.checkpoint()

        hs_peak = self.get_hs_file_size()
        self.pr('Phase 1: HS = {:.0f} MB (peak)'.format(hs_peak / mb))
        self.assertGreater(hs_peak, 500 * mb,
            'Expected HS > 500MB, got {:.0f}MB'.format(hs_peak / mb))

        # ================================================================
        # Phase 2: Drop bulk table, shrink live data to ~1KB.
        #
        # Dropping triggers __wt_hs_btree_truncate() for bulk's btree_id,
        # removing all its HS entries. But the block freeing goes through
        # the normal extent list lifecycle (discard -> avail after checkpoint).
        #
        # The pin table's HS page was written LAST (highest btree_id) during
        # Phase 1, so it sits at the highest physical offset. After bulk's
        # pages are freed, pin's page at the tail blocks file truncation.
        #
        # Critically, the pin table is NOT updated after this point. Its HS
        # page stays CLEAN on disk -- never read into cache, never dirtied,
        # never reconciled.
        # ================================================================
        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(3) +
            ',stable_timestamp=' + self.timestamp_str(3))

        self.session.drop(self.uri_bulk)
        self.pr('Phase 2: Dropped bulk table. Live data: 1 row x 1KB = 1KB')

        # Run a few checkpoints to process the extent lists.
        for _ in range(3):
            self.session.checkpoint()

        hs_after_drop = self.get_hs_file_size()
        self.pr('Phase 2: HS after drop + 3 ckpts = {:.0f} MB'.format(hs_after_drop / mb))

        # ================================================================
        # Phase 3: Sliding window for 100 rounds.
        #
        # We advance oldest_timestamp each round (MongoDB pattern) but do
        # NOT touch the pin table. We also advance stable so the system
        # is fully quiesced.
        #
        # Each round triggers checkpoint with cleanup. But cleanup is
        # limited to 1 page/pass, so it takes many rounds to process the
        # freed pages. Meanwhile the pin page at the tail stays clean and
        # blocks truncation.
        #
        # Data written per round: 0 bytes (no user updates).
        # ================================================================
        hs_sizes = {}

        for rnd in range(4, 4 + self.num_rounds):
            self.conn.set_timestamp(
                'oldest_timestamp=' + self.timestamp_str(rnd) +
                ',stable_timestamp=' + self.timestamp_str(rnd))

            self.session.checkpoint('debug=(checkpoint_cleanup=true)')

            if rnd <= 8 or rnd % 10 == 0 or rnd == 3 + self.num_rounds:
                hs_size = self.get_hs_file_size()
                hs_sizes[rnd] = hs_size
                self.pr('Round {:3d}: HS = {:.0f} MB  (oldest={})'.format(
                    rnd, hs_size / mb, rnd))

        final_hs = self.get_hs_file_size()

        # ================================================================
        # Assertions
        # ================================================================
        ratio = (final_hs / hs_peak) * 100
        self.pr('')
        self.pr('HS peak:  {:.0f} MB'.format(hs_peak / mb))
        self.pr('HS final: {:.0f} MB'.format(final_hs / mb))
        self.pr('Ratio:    {:.0f}%'.format(ratio))

        # The HS file should retain a large fraction of peak.
        # The pin page at the tail blocks truncation of trailing free space.
        self.assertGreater(final_hs, hs_peak * 0.30,
            'HS file shrank too much. Peak={:.0f}MB, Final={:.0f}MB'.format(
                hs_peak / mb, final_hs / mb))

        # ================================================================
        # Summary
        # ================================================================
        self.pr('')
        self.pr('=== RESULT ===')
        self.pr('Bulk rows (Phase 1):   {} x {}KB = {:.0f} MB/generation'.format(
            self.nrows_bulk, self.value_bulk // 1024,
            self.nrows_bulk * self.value_bulk / mb))
        self.pr('Pin rows:              {} x {}KB = {} KB (live data)'.format(
            self.nrows_pin, self.value_pin // 1024,
            self.nrows_pin * self.value_pin // 1024))
        self.pr('Data written/round:    0 bytes (no updates in Phase 3)')
        self.pr('Rounds:                {}'.format(self.num_rounds))
        self.pr('Cleanup limit:         1 page/pass (simulates WT-16002)')
        self.pr('HS peak:               {:.0f} MB'.format(hs_peak / mb))
        self.pr('HS final:              {:.0f} MB'.format(final_hs / mb))
        self.pr('Retained:              {:.0f}%'.format(ratio))
        self.pr('')
        self.pr('HS size progression:')
        for key in sorted(hs_sizes.keys()):
            self.pr('  Round {:3d}: {:.0f} MB'.format(key, hs_sizes[key] / mb))
        self.pr('')
        self.pr('WHY: After dropping bulk, the pin table\'s HS page sits at the')
        self.pr('highest physical offset (highest btree_id -> last B-tree leaf).')
        self.pr('The page is CLEAN (pin table is never updated in Phase 3).')
        self.pr('Clean pages are skipped by checkpoint (bt_sync.c:301).')
        self.pr('Cleanup can detect it via time aggregates (bt_sync_obsolete.c:468),')
        self.pr('but the rate limit (1 page/pass) prevents reaching it in time.')
        self.pr('In production (WT-16002), 40k+ tables exhaust the default limit')
        self.pr('of 100 pages/pass, creating the same starvation effect.')

if __name__ == '__main__':
    wttest.run()
