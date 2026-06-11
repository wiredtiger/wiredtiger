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

# test_disagg_checkpoint_size09.py
#   Exercises the WT-16864 fix in bt_split.c:__split_multi_inmem for the
#   cumulative_size_aggregated invariant.
#
#   When eviction encounters a page with uncommitted updates that cannot be
#   moved to the history store, it falls back to save_update_restore: the page is
#   kept in memory via __wt_split_rewrite  __split_multi_inmem, which
#   instantiates a new in-memory page from the existing disk image.
#
#   Before the fix, __split_multi_inmem did not restore
#   cumulative_size_aggregated after copying multi->block_meta into
#   page->disagg_info->block_meta.  The copy-site reset left the field false
#   even though the on-disk delta chain is already counted in block_disagg->size.
#
#   The fix adds:
#     page->disagg_info->block_meta.cumulative_size_aggregated =
#         multi->block_meta->cumulative_size > 0;
#   immediately after the struct copy, matching the pattern already applied to
#   the two __rec_write_wrapup call sites fixed by the companion rec_write.c
#   changes (covered by test_disagg_checkpoint_size08.py).
#
#   Crash path (before fix):
#     1. Build a delta chain so cumulative_size > 0 on disk.
#     2. An uncommitted write on the page forces save_update_restore on eviction.
#        __split_multi_inmem creates page N with aggregated=false (bug).
#     3. Rolling back the uncommitted write leaves the page dirty with only
#        aborted updates; the committed value has WT_UPDATE_DURABLE.
#     4. Checkpoint: newer_updates_than_last_rec_used stays false so skip_write
#        fires.  The rec_write.c line 3205 wrapup sets mod->rec_result=REPLACE.
#        Without the companion rec_write.c fix it also propagated aggregated=false;
#        with the bt_split.c fix correct, the rec_write.c fix restores it to true.
#     5. Enable Failpoint_REC_BEFORE_wrapup, dirty the page, and force eviction.
#        The failpoint fires after the full-image write but before wrapup, calling
#        __rec_write_err.  Without the fixes: REPLACE + cumulative_size>0 +
#        aggregated=false triggers the assert at rec_write.c:3356.
#
#   Verification:
#     - cache_scrub_restore > 0 proves __split_multi_inmem was reached.
#     - rec_free_page_id_due_to_failed_replacement_reconciliation > 0 proves
#       the __rec_write_err error path ran without crashing.
#     - Checkpoint size after recovery is not inflated by leaked cumulative_size.

@disagg_test_class
class test_disagg_checkpoint_size09(wttest.WiredTigerTestCase):

    uri_base = 'test_disagg_ckpt_size09'
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
    # test_split_multi_inmem_aggregated_flag
    # -----------------------------------------------------------------------
    # Regression for bt_split.c:__split_multi_inmem fix (WT-16864).
    #
    # __split_multi_inmem is called from __wt_split_rewrite when eviction
    # produces a multiblock reconciliation result with exactly one entry that has
    # WT_MULTI_SAVE_UPDATE_RESTORE set.  This happens when the page has uncommitted
    # updates that cannot be written to the history store.
    #
    # Flow:
    #   1. Write initial rows and checkpoint (full-image baseline).
    #   2. Partially update rows and checkpoint (builds delta chain,
    #      cumulative_size > 0).
    #   3. In a loop until both signal stats fire:
    #      a. Open session_a and write uncommitted updates to the page.
    #         Evict the page.  Because uncommitted updates from session_a cannot
    #         go to the history store, eviction falls back to save_update_restore and
    #         calls __split_multi_inmem.  cache_scrub_restore is incremented.
    #      b. Rollback session_a.  The page retains only aborted updates plus
    #         the previously committed 'B' value with WT_UPDATE_DURABLE.
    #      c. Checkpoint.  __rec_selected_key_changed returns false for the
    #         WT_UPDATE_DURABLE 'B' value, so newer_updates_than_last_rec_used
    #         stays false and skip_write fires.  The wrapup (rec_write.c
    #         line ~3205, now fixed) sets mod->rec_result=REPLACE and restores
    #         cumulative_size_aggregated=(cumulative_size>0).
    #      d. Enable Failpoint_REC_BEFORE_wrapup + delta_pct=1.  Write new
    #         committed data to dirty the page and force a full-image eviction.
    #         The failpoint fires ~1% of the time, calling __rec_write_err with
    #         delta_count=0, rec_result=REPLACE, cumulative_size>0.
    #         Before the fixes: aggregated=false  process abort.
    #         After the fixes: aggregated=true  stat incremented, no crash.
    #   4. Run a final checkpoint and verify size is not inflated.
    def test_split_multi_inmem_aggregated_flag(self):
        nrows = 20
        scrub_stat = stat.dsrc.cache_scrub_restore
        err_stat = stat.dsrc.rec_free_page_id_due_to_failed_replacement_reconciliation

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: initial full-image write + checkpoint.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.session.checkpoint()
        size_baseline = self.get_checkpoint_size()
        self.assertGreater(size_baseline, 0)

        # Step 2: partial update + checkpoint to build a delta chain so
        # cumulative_size > 0 on disk.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_baseline,
            'Expected checkpoint size to grow after a delta write')

        # Step 3: loop until __split_multi_inmem has been called at least once
        # AND the __rec_write_err error path has been reached at least once.
        max_iters = 500
        for i in range(max_iters):
            # (a) Uncommitted write forces save_update_restore on eviction.
            #     Uncommitted updates from session_a cannot be written to the history
            #     store (they may still be rolled back), so eviction keeps the page
            #     in memory via __split_multi_inmem and increments cache_scrub_restore.
            session_a = self.conn.open_session()
            session_a.begin_transaction()
            ca = session_a.open_cursor(self.uri)
            for j in range(nrows // 2):
                ca[f'key{j:06d}'] = chr(ord('C') + (i % 20)) * 200
            ca.close()
            self.evict_page('key000000')

            # (b) Rollback: uncommitted updates become aborted.  The page now has
            #     only the committed 'B' value (WT_UPDATE_DURABLE) plus aborted updates.
            session_a.rollback_transaction()
            session_a.close()

            # (c) Checkpoint: skip_write fires because newer_updates_than_last_rec_used
            #     stays false (the only visible update is already WT_UPDATE_DURABLE).
            #     The wrapup sets mod->rec_result=REPLACE and (after the fix)
            #     cumulative_size_aggregated=true.
            self.session.checkpoint()

            # (d) Enable the failpoint and force a full-image eviction.
            #     The committed write makes the page dirty; delta_pct=1 forces a
            #     full-image write; the failpoint fires ~1% of the time.
            self.conn.reconfigure(
                'page_delta=(delta_pct=1),'
                'timing_stress_for_test=[failpoint_rec_before_wrapup]'
            )
            c = self.session.open_cursor(self.uri)
            self.insert_rows(c, 0, nrows, chr(ord('D') + (i % 20)))
            c.close()
            self.evict_page('key000000')
            self.conn.reconfigure('timing_stress_for_test=[]')

            if self.get_stat(scrub_stat) > 0 and self.get_stat(err_stat) > 0:
                break
        else:
            self.fail(
                f'Failed to trigger both save_update_restore (__split_multi_inmem) and '
                f'the __rec_write_err error path after {max_iters} iterations'
            )

        # Step 4: final checkpoint after the error path has run.
        self.session.checkpoint()
        size_after_recovery = self.get_checkpoint_size()

        # __split_multi_inmem was called (save_update_restore ran).
        self.assertGreater(self.get_stat(scrub_stat), 0,
            'cache_scrub_restore should be > 0: __split_multi_inmem was not reached')

        # The __rec_write_err error path ran without crashing.
        self.assertGreater(self.get_stat(err_stat), 0,
            'rec_free_page_id_due_to_failed_replacement_reconciliation should be > 0 '
            'after running with failpoint_rec_before_wrapup')

        # Checkpoint size is not inflated.  Without the fixes, __rec_write_err
        # skipped the obsolete_delta_chain cleanup, leaking cumulative_size into
        # block_disagg->size on every error-path hit.
        self.assertLess(size_after_recovery, size_with_delta + size_baseline,
            f'Checkpoint size {size_after_recovery} after recovery is inflated: '
            f'baseline={size_baseline}, after_delta={size_with_delta}. '
            f'Old delta chain cumulative_size may have been double-counted.')
