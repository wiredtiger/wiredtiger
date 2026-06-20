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

import re, wttest
from wiredtiger import stat
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_disagg_checkpoint_size10.py
#   Regression test for the multiblock old-state fix in __rec_write_wrapup
#   (rec_write.c).
#
#   Bug: when a disagg page has mod->rec_result == WT_PM_REC_REPLACE (set by a
#   prior save_update_restore eviction that produced exactly one block) and
#   __wt_split_rewrite subsequently fails, the page stays in the multiblock state.
#   If the next reconciliation writes a full-image reusing the same page_id
#   (disagg_page_free_required=false, free_blocks=false in __rec_split_discard),
#   __wt_block_disagg_obsolete_delta_chain was never called for the old
#   cumulative_size, permanently inflating block_disagg->size on every such cycle.
#
#   The fix adds the same obsolete_delta_chain call to the multiblock case in
#   __rec_write_wrapup that was already present for case 0 and WT_PM_REC_REPLACE,
#   mirroring the same accounting guard:
#
#     if (disagg_page_is_valid && !disagg_page_free_required &&
#       r->multi_next == 1 && !F_ISSET(r->multi, WT_MULTI_SKIP_WRITE) &&
#       r->multi->block_meta->delta_count == 0)
#         __wt_block_disagg_obsolete_delta_chain(
#           session, page->disagg_info->block_meta.cumulative_size);
#
#   Exact triggering path:
#     1. Eviction falls back to save_update_restore for a disagg page with uncommitted
#        updates: wrapup sets the multiblock reconciliation result with one entry.
#        The entry's cumulative_size S1 is counted in block_disagg->size.
#     2. __wt_split_rewrite fails (e.g., allocation error).  The page stays in
#        the multiblock state; S1 remains counted.
#     3. Uncommitted updates are rolled back.
#     4. A subsequent checkpoint or eviction reconciles the page cleanly (no
#        uncommitted updates), writing a full-image that reuses the same page_id.
#        wrapup enters the multiblock case as the old state and must subtract S1
#        via obsolete_delta_chain; if it doesn't, S1 leaks indefinitely.
#
#   This test verifies the broader accounting invariant: after repeated
#   save_update_restore eviction cycles followed by full-image checkpoint writes,
#   checkpoint size remains stable and does not grow with each cycle.
#   cache_scrub_restore > 0 confirms __split_multi_inmem was reached (step 1).

@disagg_test_class
class test_disagg_checkpoint_size10(wttest.WiredTigerTestCase):

    uri_base = 'test_disagg_ckpt_size10'
    conn_config = (
        'disaggregated=(role="leader",lose_all_my_data=true),'
        'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=10)'
    )
    uri = 'layered:' + uri_base
    stable_uri = 'file:' + uri_base + '.wt_stable'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def get_checkpoint_size(self):
        mc = self.session.open_cursor('metadata:')
        mc.set_key(self.stable_uri)
        self.assertEqual(mc.search(), 0)
        sizes = re.findall(r',size=(\d+),', mc.get_value())
        mc.close()
        self.assertGreater(len(sizes), 0, 'No size= found in checkpoint metadata')
        return int(sizes[-1])

    def insert_rows(self, cursor, start, count, value_char):
        for i in range(start, start + count):
            cursor[f'key{i:06d}'] = value_char * 200

    def evict_page(self, key):
        evict = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        self.session.begin_transaction()
        evict.set_key(key)
        evict.search()
        evict.reset()
        evict.close()
        self.session.rollback_transaction()

    def get_stat(self, stat_key):
        s = self.session.open_cursor('statistics:' + self.stable_uri)
        val = s[stat_key][2]
        s.close()
        return val

    # -----------------------------------------------------------------------
    # test_multiblock_to_replace_size_stable
    # -----------------------------------------------------------------------
    # Regression for the multiblock case in rec_write.c:__rec_write_wrapup
    # obsolete_delta_chain fix.
    #
    # Flow:
    #   1. Write initial rows and checkpoint (full-image baseline).
    #   2. Partial update + checkpoint to build a delta chain so
    #      cumulative_size > 0 on disk.
    #   3. Loop until cache_scrub_restore > 0:
    #      a. Open session_a and write uncommitted updates to the page.
    #         Evict the page.  Uncommitted updates cannot go to the history
    #         store, so eviction falls back to save_update_restore and calls
    #         __split_multi_inmem.  cache_scrub_restore is incremented.
    #      b. Rollback session_a.
    #      c. Switch to delta_pct=1 to force a full-image write, then
    #         run a checkpoint.  The wrapup enters case 0 (new page from
    #         __wt_split_rewrite) and calls obsolete_delta_chain for the
    #         old cumulative S1.
    #   4. After enough cycles, verify checkpoint size is stable (not growing
    #      by S1 each cycle).
    def test_multiblock_to_replace_size_stable(self):
        nrows = 20
        scrub_stat = stat.dsrc.cache_scrub_restore

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: initial full-image write + checkpoint.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.session.checkpoint()
        size_baseline = self.get_checkpoint_size()
        self.assertGreater(size_baseline, 0)

        # Step 2: partial update + checkpoint to build a delta chain.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_baseline,
            'Expected checkpoint size to grow after a delta write')

        # Step 3: loop until save_update_restore is triggered at least once.
        max_iters = 200
        for i in range(max_iters):
            # (a) Uncommitted write forces save_update_restore on eviction.
            session_a = self.conn.open_session()
            session_a.begin_transaction()
            ca = session_a.open_cursor(self.uri)
            for j in range(nrows // 2):
                ca[f'key{j:06d}'] = chr(ord('C') + (i % 20)) * 200
            ca.close()
            self.evict_page('key000000')

            # (b) Rollback: uncommitted updates become aborted.
            session_a.rollback_transaction()
            session_a.close()

            # (c) Force a full-image write (delta_pct=1) and checkpoint.
            #     Writes committed data to dirty the page, then checkpoints.
            self.conn.reconfigure('page_delta=(delta_pct=1)')
            c = self.session.open_cursor(self.uri)
            self.insert_rows(c, 0, nrows, chr(ord('D') + (i % 20)))
            c.close()
            self.session.checkpoint()
            self.conn.reconfigure(
                'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=10)'
            )

            if self.get_stat(scrub_stat) > 0:
                break
        else:
            self.fail(
                f'Failed to trigger save_update_restore (__split_multi_inmem) '
                f'after {max_iters} iterations'
            )

        # Step 4: run a few more full-image checkpoints to confirm size stability.
        # If the multiblock old-state path failed to subtract the old cumulative S1,
        # block_disagg->size would grow on every cycle, making size_final >> size_with_delta.
        for _ in range(5):
            self.conn.reconfigure('page_delta=(delta_pct=1)')
            c = self.session.open_cursor(self.uri)
            self.insert_rows(c, 0, nrows, 'Z')
            c.close()
            self.session.checkpoint()
            self.conn.reconfigure(
                'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=10)'
            )

        size_final = self.get_checkpoint_size()

        # cache_scrub_restore > 0: __split_multi_inmem was reached.
        self.assertGreater(self.get_stat(scrub_stat), 0,
            'cache_scrub_restore should be > 0: save_update_restore was not triggered')

        # Checkpoint size should not be inflated beyond a reasonable bound.
        # Each full-image write replaces the previous delta chain; if obsolete_delta_chain
        # is called correctly, the size reflects only the current page content.
        # Allow 2x size_with_delta as headroom for implementation overhead.
        self.assertLess(size_final, 2 * size_with_delta,
            f'Checkpoint size {size_final} after repeated SUPD_RESTORE + full-image cycles '
            f'is inflated beyond 2x the delta baseline {size_with_delta}. '
            f'Old delta chain cumulative_size may have been double-counted.')
