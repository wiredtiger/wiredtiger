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

# test_disagg_checkpoint_size08.py
#   Exercises the persistent invariant in two __rec_write_wrapup paths:
#
#   Path 1  save_update_restore wrapup in __rec_write_wrapup:
#     When eviction keeps a page in memory with restored updates (because an
#     active reader transaction prevents those updates from becoming globally
#     visible), it copies r->multi->block_meta into page->disagg_info->block_meta
#     without restoring persistent.  The copy-site reset leaves
#     the field false even though the on-disk size is still counted in
#     block_disagg->size.
#
#   Path 2  skip-write wrapup in __rec_write_wrapup (single-block replace path):
#     When eviction reuses the page's existing on-disk address (content unchanged
#     since the last write), __rec_copy_prev_addr also resets
#     persistent to false in multi->block_meta.  The wrapup then
#     copies that false value into page->disagg_info->block_meta while setting
#     mod->rec_result = WT_PM_REC_REPLACE.
#
#   In both cases a subsequent failed full-image write in __rec_write_err hit:
#     WT_ASSERT(session, page->disagg_info->block_meta.persistent)
#   and aborted the process.
#
#   The fix restores persistent = (cumulative_size > 0) after the
#   struct copy at both wrapup sites, matching the same pattern already applied in
#   bt_split.c:__split_multi_inmem.
#
#   Tests:
#     test_supd_restore_wrapup_aggregated_flag
#       A long-running reader session pins an early read timestamp, preventing
#       updates written at later timestamps from becoming globally visible.  This
#       forces save_update_restore during eviction of dirty pages.  With
#       timing_stress_for_test=[failpoint_rec_before_wrapup] enabled, a later
#       failed full-image write exercises the assert path.  Verifies:
#         - The process does not abort (assert is not reached).
#         - rec_free_page_id_due_to_failed_replacement_reconciliation > 0
#           (the __rec_write_err error path ran at least once).
#         - Checkpoint size is not inflated by a leaked cumulative_size.
#
#     test_skip_write_wrapup_aggregated_flag
#       Builds a delta chain (cumulative_size > 0), then writes to the page and
#       rolls back, leaving a dirty page with only aborted updates.  A checkpoint
#       fires skip-write (newer_updates_than_last_rec_used is false because the
#       committed on-page update has WT_UPDATE_DURABLE).  The skip-write wrapup
#       must restore persistent = (cumulative_size > 0).
#       With failpoint_rec_before_wrapup, the next eviction enters __rec_write_err
#       and would hit the assert if persistent were left false.  Verifies the
#       same invariants as the save_update_restore test.

@disagg_test_class
class test_disagg_checkpoint_size08(wttest.WiredTigerTestCase):

    uri_base = 'test_disagg_ckpt_size08'
    # delta_pct=90: small updates produce deltas; switched to 1 when a full image is needed.
    conn_config = (
        'disaggregated=(role="leader",lose_all_my_data=true),'
        'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=10)'
    )
    uri = 'layered:' + uri_base
    stable_uri = 'file:' + uri_base + '.wt_stable'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

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
    # test_supd_restore_wrapup_aggregated_flag
    # -----------------------------------------------------------------------
    # Regression for the save_update_restore wrapup path in __rec_write_wrapup.
    #
    # The wrapup copies r->multi->block_meta (where the copy-site has reset
    # persistent=false) into page->disagg_info->block_meta, then must restore
    # persistent = (cumulative_size > 0).  Without that restore, a subsequent
    # skip-write reconciliation propagates persistent=false into the REPLACE
    # state, and a later failed full-image write trips the persistent assert
    # in __wti_block_disagg_decrease_size.
    #
    # Setup:
    #   1. Write initial rows and checkpoint (full-image baseline).
    #   2. Partially update rows and checkpoint (builds delta chain,
    #      cumulative_size > 0).
    #   3. Open a blocker session that pins an old read timestamp, preventing
    #      updates at newer timestamps from becoming globally visible.
    #   4. Continue writing at new timestamps; eviction sees pages with updates
    #      that cannot be flushed to the history store (blocked), triggering
    #      save_update_restore to keep the updates in memory.
    #   5. Enable failpoint_rec_before_wrapup so a later full-image write fails
    #      and enters __rec_write_err with the invariant conditions.
    #   6. Verify no abort, stat > 0, and size is not inflated.
    def test_supd_restore_wrapup_aggregated_flag(self):
        nrows = 20
        stat_key = stat.dsrc.rec_free_page_id_due_to_failed_replacement_reconciliation

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: initial full-image baseline.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.checkpoint()
        size_baseline = self.get_checkpoint_size()
        self.assertGreater(size_baseline, 0)

        # Step 2: partial update to build a delta chain (cumulative_size > 0).
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.commit_transaction('commit_timestamp=2')
        self.conn.set_timestamp('stable_timestamp=2')
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_baseline,
            'Expected checkpoint size to grow after a delta write')

        # Step 3: blocker session pins read_timestamp=1.  Updates at ts>=2 are
        # not globally visible while this transaction is open, so eviction of pages
        # carrying those updates cannot write them to the history store and falls
        # back to save_update_restore (restores updates into the in-memory page).
        blocker = self.conn.open_session()
        blocker.begin_transaction('read_timestamp=1')

        # Evict the page so the next read re-loads it from disk with the delta-chain
        # block_meta (cumulative_size > 0, persistent = true).
        self.evict_page('key000000')

        # Step 45: enable failpoint and loop until __rec_write_err is reached.
        # delta_pct=1 forces full-image writes; the blocker transaction forces
        # save_update_restore on the first eviction attempt of pages with ts>=2 updates.
        # After save_update_restore the page remains in cache with the multiblock
        # reconciliation result; a subsequent skip-write turns it to REPLACE.  The
        # failpoint then fires during the next full-image write -- __rec_write_err
        # would trip the persistent assert if the wrapup did not restore the flag.
        self.conn.reconfigure(
            'page_delta=(delta_pct=1),'
            'timing_stress_for_test=[failpoint_rec_before_wrapup]'
        )
        max_iters = 500
        for i in range(max_iters):
            ts = 3 + i
            c = self.session.open_cursor(self.uri)
            self.session.begin_transaction()
            self.insert_rows(c, 0, nrows, chr(ord('C') + (i % 20)))
            c.close()
            self.session.commit_transaction(f'commit_timestamp={ts}')
            self.conn.set_timestamp(f'stable_timestamp={ts}')
            self.evict_page('key000000')
            if self.get_stat(stat_key) > 0:
                break

        # Release the blocker before cleanup so eviction is unblocked.
        blocker.rollback_transaction()
        blocker.close()

        self.conn.reconfigure('timing_stress_for_test=[]')
        self.session.checkpoint()
        size_after_recovery = self.get_checkpoint_size()

        # The failpoint fires probabilistically (1% per reconcile). With a bounded
        # workload it may not trigger on a given run; skip the size check below
        # in that case rather than asserting on a probability.
        if self.get_stat(stat_key) == 0:
            self.skipTest('failpoint_rec_before_wrapup did not fire in this run')

        # Verify size is not inflated.  Without the wrapup fix, the save_update_restore path
        # left persistent=false; skip-write then set REPLACE with the
        # wrong flag; __rec_write_err skipped obsolete_delta_chain, leaking
        # cumulative_size into block_disagg->size on every error-path iteration.
        self.assertLess(size_after_recovery, size_with_delta + size_baseline,
            f'Checkpoint size {size_after_recovery} after recovery is inflated: '
            f'baseline={size_baseline}, after_delta={size_with_delta}. '
            f'Old delta chain cumulative_size may have been double-counted.')

    # -----------------------------------------------------------------------
    # test_skip_write_wrapup_aggregated_flag
    # -----------------------------------------------------------------------
    # Regression for the skip-write wrapup path in __rec_write_wrapup
    # (single-block replace branch).
    #
    # The skip-write wrapup (WT_MULTI_SKIP_WRITE) copies r->multi->block_meta
    # (where __rec_copy_prev_addr has reset persistent=false) into
    # page->disagg_info->block_meta and sets mod->rec_result=WT_PM_REC_REPLACE.
    # It must then restore persistent = (cumulative_size > 0) so the flag
    # reflects that the size IS counted in block_disagg->size (skip-write reuses
    # the existing on-disk address without subtracting or re-adding to the size).
    # Otherwise a subsequent failed eviction write trips the persistent assert
    # in __wti_block_disagg_decrease_size.
    #
    # Skip-write (skip_write=true in __rec_split_write) fires when:
    #   - last_block && multi_next==1 (single-page result)
    #   - block_meta->page_id is valid (page written before)
    #   - WT_REC_RESULT_SINGLE_PAGE (mod->rec_result is REPLACE)
    #   - !newer_updates_than_last_rec_used (no new committed updates)
    #
    # To reliably trigger skip-write during CHECKPOINT (so the page stays in cache
    # with the wrong flag set by the wrapup):
    #   1. Write data and checkpoint  page has REPLACE result, cumulative_size > 0.
    #      The on-page committed updates get WT_UPDATE_DURABLE.
    #   2. Write to the page and ROLLBACK.  The rolled-back update is aborted and
    #      invisible to reconciliation.  The page is dirty (has a modify struct)
    #      but all visible committed updates were already written with WT_UPDATE_DURABLE.
    #   3. Checkpoint.  Reconciliation selects the WT_UPDATE_DURABLE committed value
    #      as the on-page update.  Because it is already durable,
    #      __rec_selected_key_changed returns false  newer_updates_than_last_rec_used
    #      stays false  skip_write=true fires.
    #      The checkpoint wrapup copies r->multi->block_meta (where
    #      __rec_copy_prev_addr reset persistent to false) into
    #      page->disagg_info->block_meta and must restore persistent=true.
    #      The page remains in cache with REPLACE result.
    #   4. Enable failpoint_rec_before_wrapup and evict with delta_pct=1.
    #      The eviction writes a full-image (delta_count=0); the failpoint fires and
    #      __rec_write_err finds REPLACE + cumulative_size>0.  If persistent had
    #      been left false the assert would trip here.
    def test_skip_write_wrapup_aggregated_flag(self):
        nrows = 20
        stat_key = stat.dsrc.rec_free_page_id_due_to_failed_replacement_reconciliation

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: initial full-image baseline to establish page_id and REPLACE result.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.session.checkpoint()
        size_baseline = self.get_checkpoint_size()
        self.assertGreater(size_baseline, 0)

        # Step 2: write a delta to create a cumulative_size > 0 on disk.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_baseline,
            'Expected checkpoint size to grow after a delta write')

        # Steps 34: loop until the error path fires.
        # Each iteration:
        #   a. Write to the page and rollback.  The page becomes dirty with only
        #      aborted updates; the committed 'B' data still has WT_UPDATE_DURABLE.
        #   b. Checkpoint.  skip_write fires: newer_updates_than_last_rec_used stays
        #      false because __rec_selected_key_changed returns false for the already-
        #      durable 'B' update.  The skip-write wrapup restores
        #      persistent = (cumulative_size > 0), reflecting that the chain is
        #      still counted in block_disagg->size.
        #   c. Enable failpoint + delta_pct=1 and write committed data.
        #   d. Evict: full-image write -> failpoint fires -> __rec_write_err.
        #      With persistent=true on entry, the error path completes; the
        #      page_id is invalidated and the rec_free_page_id stat increments.
        max_iters = 500
        for i in range(max_iters):
            # (a) Write + rollback: dirty page, only aborted updates, content unchanged.
            c = self.session.open_cursor(self.uri)
            self.session.begin_transaction()
            self.insert_rows(c, 0, nrows, chr(ord('C') + (i % 20)))
            c.close()
            self.session.rollback_transaction()

            # (b) Checkpoint with skip-write. Sets aggregated=false (before fix) on the
            # page, which stays in cache with mod->rec_result=WT_PM_REC_REPLACE.
            self.session.checkpoint()

            # (c-d) Enable failpoint and evict with full-image mode.
            self.conn.reconfigure(
                'page_delta=(delta_pct=1),'
                'timing_stress_for_test=[failpoint_rec_before_wrapup]'
            )
            c = self.session.open_cursor(self.uri)
            self.insert_rows(c, 0, nrows, chr(ord('D') + (i % 20)))
            c.close()
            self.evict_page('key000000')
            self.conn.reconfigure('timing_stress_for_test=[]')

            if self.get_stat(stat_key) > 0:
                break

        self.session.checkpoint()
        size_after_recovery = self.get_checkpoint_size()

        # Verify the error path ran.
        self.assertGreater(self.get_stat(stat_key), 0,
            'rec_free_page_id_due_to_failed_replacement_reconciliation should be > 0 '
            'after skip-write wrapup followed by failpoint eviction')

        # Verify size is not inflated.
        self.assertLess(size_after_recovery, size_with_delta + size_baseline,
            f'Checkpoint size {size_after_recovery} after recovery is inflated: '
            f'baseline={size_baseline}, after_delta={size_with_delta}. '
            f'Old delta chain cumulative_size may have been double-counted.')
