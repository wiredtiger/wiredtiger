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

# test_disagg_checkpoint_size11.py
#   Exercises the post-page-log write failure path for delta writes using
#   failpoint_rec_before_wrapup.
#
# Background:
#   failpoint_rec_before_wrapup fires 1% of the time during eviction
#   reconciliation, after the block write is committed to the page log and
#   before __rec_write_wrapup is called.
#
#   For a delta write whose plh_put succeeds:
#     - block_meta->persistent == true   (write reached the page log)
#     - block_meta->delta_count > 0      (delta write)
#     - multi->addr.block_cookie != NULL (address was packed)
#     - block_disagg->size was incremented by delta_size
#
#   In __rec_write_err, when persistent==true and delta_count>0:
#     - block_free (via page_discard) subtracts cookie.size ==
#       (previous cumulative_size + delta_size), removing both the old chain
#       and the new delta from block_disagg->size.
#     - page->disagg_info->block_meta.persistent is set to false so the
#       next reconciliation does NOT pass old_block_meta to
#       __wti_block_disagg_write.  If persistent were left true, the next
#       reconciliation would call decrease_size(old_cumulative) on a chain
#       already removed by page_discard, under flowing block_disagg->size.
#     - page_id is invalidated; the next reconciliation allocates a fresh
#       page_id and writes a full image.
#
#   This test verifies that after a post-page-log delta write failure:
#     1. rec_free_page_id_due_to_failed_replacement_reconciliation is
#        incremented (the delta branch of __rec_write_err was exercised).
#     2. The recovery checkpoint size is not corrupted -- a bug that fails to
#        clear page->disagg_info->block_meta.persistent would pass old_block_meta
#        into the recovery checkpoint, triggering a second decrease_size on a
#        chain already removed by page_discard and asserting in
#        __wti_block_disagg_decrease_size.
#     3. All written rows remain readable after recovery.

@disagg_test_class
class test_disagg_checkpoint_size11(wttest.WiredTigerTestCase):

    uri_base = 'test_disagg_ckpt_size11'
    conn_config = (
        'disaggregated=(role="leader",lose_all_my_data=true),'
        'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=32)'
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
    # test_rec_write_err_delta_post_plh_put
    # -----------------------------------------------------------------------
    # Exercises the delta branch of __rec_write_err:
    #   - r->multi->block_meta->delta_count > 0    (delta write reached page log)
    #   - mod->rec_result == WT_PM_REC_REPLACE      (prior successful reconciliation)
    #   - page->disagg_info->block_meta.persistent  (old chain is live)
    #
    # This is the counterpart to test_disagg_checkpoint_size07, which exercises
    # the full-image branch (delta_count == 0) of the same __rec_write_err block.
    #
    # Flow:
    #   1. Write initial rows + checkpoint -> size_initial.
    #   2. Write partial update + checkpoint -> size_with_delta > size_initial.
    #   3. Evict the leaf page so it is re-read from the page service on next
    #      access with persistent=true.
    #   4. Enable failpoint_rec_before_wrapup (fires 1% of the time during
    #      eviction reconciliation after the block write, before wrapup).
    #      Keep delta_pct=90 so writes remain deltas.  Only nrows//2 rows
    #      are updated each iteration so the delta is ~50% of the full-image
    #      size, well below the delta_pct=90 threshold.
    #   5. Loop dirty+evict until rec_free_page_id_due_to_failed_replacement_
    #      reconciliation > 0.  Early iterations succeed, establishing
    #      mod->rec_result == WT_PM_REC_REPLACE and a growing delta chain.
    #      When the failpoint fires after a delta plh_put, __rec_write_err
    #      enters the delta branch: page_discard subtracts cookie.size ==
    #      (old_cumulative + delta_size), clears persistent, and invalidates
    #      page_id.
    #   6. Set delta_pct=1 BEFORE disabling the failpoint to prevent background
    #      eviction from writing a delta after the failpoint is lifted.
    #   7. Disable failpoint and run a recovery checkpoint.
    #   8. Assert stat > 0 (delta error path was reached).
    #   9. Assert size_after_recovery < 2 * size_initial (no double-decrement
    #      underflow from a second decrease_size on the old chain).
    #  10. Verify all rows are readable.
    def test_rec_write_err_delta_post_plh_put(self):
        nrows = 20

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: Initial full-image checkpoint.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.session.checkpoint()
        size_initial = self.get_checkpoint_size()
        self.assertGreater(size_initial, 0)


        # Step 2: Append a delta to build a chain with cumulative_size > 0.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_initial,
            'Size should grow after appending a delta to the chain')


        # Step 3: Evict the leaf page so it is re-loaded from the page service
        # on next access with persistent=true and the existing chain intact.
        self.evict_page('key000000')


        # Step 4: Enable the failpoint.  Keep delta_pct=90 (inherited from
        # conn_config) so evictions produce delta writes.  max_consecutive_delta=32
        # is the engine maximum; a full-image rollover occurs every 33 iterations
        # but the 1% failpoint is expected to fire well within that window.
        self.conn.reconfigure(
            'timing_stress_for_test=[failpoint_rec_before_wrapup]'
        )

        # Step 5: Dirty the page and force eviction in a loop.  Early iterations
        # succeed and build a longer delta chain while establishing
        # mod->rec_result == WT_PM_REC_REPLACE.  When the 1% failpoint eventually
        # fires after a delta write reaches the page log, __rec_write_err enters
        # the delta branch (delta_count > 0, WT_PM_REC_REPLACE, persistent=true):
        # page_discard subtracts the full chain (old_cumulative + delta_size),
        # persistent is cleared, and page_id is invalidated.
        stat_key = stat.dsrc.rec_free_page_id_due_to_failed_replacement_reconciliation
        max_iters = 500
        for i in range(max_iters):
            c = self.session.open_cursor(self.uri)
            # Update only half the rows so the delta is ~50% of the full image,
            # well below the delta_pct=90 threshold, keeping writes as deltas.
            self.insert_rows(c, 0, nrows // 2, chr(ord('C') + (i % 20)))
            c.close()
            self.evict_page('key000000')
            if self.get_stat(stat_key) > 0:

                break
        else:
            self.fail(
                f'failpoint_rec_before_wrapup never triggered the delta __rec_write_err '
                f'error path after {max_iters} evictions'
            )

        # Switch to full-image writes BEFORE disabling the failpoint.  This
        # prevents background eviction from writing a delta between the failpoint
        # disable and the recovery checkpoint, which could inflate block_disagg->size
        # and produce a false assertion failure on the correct path.
        self.conn.reconfigure('page_delta=(delta_pct=1)')
        self.conn.reconfigure('timing_stress_for_test=[]')

        # Step 6: Recovery checkpoint.  The page (page_id invalidated by
        # __rec_write_err) gets a fresh page_id and is written as a full image,
        # so size_after_recovery should be close to size_initial.
        self.session.checkpoint()
        size_after_recovery = self.get_checkpoint_size()


        self.conn.reconfigure(
            'page_delta=(delta_pct=90,leaf_page_delta=true,max_consecutive_delta=32)'
        )

        # Step 7: Confirm the delta post-plh_put error path was exercised.
        self.assertGreater(self.get_stat(stat_key), 0,
            'rec_free_page_id_due_to_failed_replacement_reconciliation should be > 0')

        # Step 8: Guard against double-decrement corruption.  __rec_write_err must
        # clear persistent after a delta error; otherwise the recovery checkpoint
        # would pass old_block_meta with persistent=true to
        # __wti_block_disagg_write, which calls decrease_size(old_cumulative) on a
        # chain already removed by page_discard -- asserting in
        # __wti_block_disagg_decrease_size or, with assertions disabled, wrapping
        # block_disagg->size to a huge value.  Use size_with_delta + size_initial
        # as the upper bound: the correct path gives approximately size_initial.
        self.assertLess(size_after_recovery, size_with_delta + size_initial,
            f'Checkpoint size {size_after_recovery} after delta error-path recovery '
            f'is too large (size_initial={size_initial}, '
            f'size_with_delta={size_with_delta}). '
            f'block_disagg->size may have been corrupted by a double decrease_size '
            f'of the old delta chain.')

        # Step 9: Data integrity -- all rows must be readable after recovery.
        c = self.session.open_cursor(self.uri)
        row_count = 0
        while c.next() == 0:
            row_count += 1
        c.close()
        self.assertEqual(row_count, nrows,
            f'Expected {nrows} rows after recovery, found {row_count}')
