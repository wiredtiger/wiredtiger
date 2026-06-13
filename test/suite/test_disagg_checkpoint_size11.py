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
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_disagg_checkpoint_size11.py
#   Verifies that a plh_put failure leaves block_disagg->size unchanged (no
#   premature subtraction of the old delta chain) and that the recovery checkpoint
#   correctly cleans up the old chain via old_block_meta.
#
# When failpoint_page_log_handle_put fires, __wti_block_disagg_write_internal
# returns EBUSY before plh_put, so the new block is never written to the page
# log.  __wti_block_disagg_write sets block_meta->in_persistent_store = false on
# the new-block copy to signal this.  __rec_write_err detects the false flag and
# skips page_id invalidation, leaving page->disagg_info->block_meta.page_id intact
# so the next reconciliation can reference the old chain via old_block_meta and
# call decrease_size cleanly on success.
#
# Without the WT-16864 fix __rec_write_err would set page_id = INVALID regardless
# of whether the write reached the page log.  This forced old_block_meta = NULL on
# the retry, permanently leaking the old chain's cumulative_size from
# block_disagg->size.
#
# Test flow:
#   1. Write rows + full-image checkpoint (baseline).
#   2. Partial update + delta checkpoint (cumulative_size > baseline).
#   3. Evict the page so it reloads from the page service with in_persistent_store=true.
#   4. Enable failpoint_page_log_handle_put (100% probability) + delta_pct=1.
#   5. Dirty the page and force one eviction  write fails, page_id stays intact.
#   6. Disable failpoint and run a recovery checkpoint.  The checkpoint reconciles
#      the dirty page using old_block_meta (same page_id), calls decrease_size for
#      the old chain, and adds the new block, keeping block_disagg->size correct.
#   7. Assert checkpoint size is not inflated.

@disagg_test_class
class test_disagg_checkpoint_size11(wttest.WiredTigerTestCase):

    uri_base = 'test_disagg_ckpt_size11'
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

    # -----------------------------------------------------------------------
    # test_write_failure_obsolete_delta_chain
    # -----------------------------------------------------------------------
    # Verifies that a plh_put failure leaves block_disagg->size unchanged.
    #
    # When plh_put fails, __wti_block_disagg_write returns EBUSY before calling
    # decrease_size or increase_size, so block_disagg->size is never modified.
    # __rec_write_err detects in_persistent_store==false on multi->block_meta
    # and skips page_id invalidation, leaving the old chain intact for the next
    # reconciliation.
    def test_write_failure_obsolete_delta_chain(self):
        nrows = 20

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Step 1: Initial full-image checkpoint.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'A')
        c.close()
        self.session.checkpoint()
        size_initial = self.get_checkpoint_size()
        self.assertGreater(size_initial, 0)

        # Step 2: Append a delta to build a chain with cumulative_size > size_initial.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows // 2, 'B')
        c.close()
        self.session.checkpoint()
        size_with_delta = self.get_checkpoint_size()
        self.assertGreater(size_with_delta, size_initial,
            'Size should grow after appending a delta to the chain')

        # Step 3: Evict the page so the disk-load path sets in_persistent_store=true
        # on page->disagg_info->block_meta, reflecting the live delta chain.
        self.evict_page('key000000')

        # Step 4: Enable the failpoint (100% probability) and switch to full-image writes.
        # failpoint_page_log_handle_put fires inside __wti_block_disagg_write_internal
        # before plh_put, returning EBUSY so the write never reaches the page log.
        # __rec_write_err detects block_meta->in_persistent_store==false on the new-block
        # copy and skips page_id invalidation, leaving the old chain intact.
        self.conn.reconfigure(
            'page_delta=(delta_pct=1),'
            'timing_stress_for_test=[failpoint_page_log_handle_put]'
        )

        # Step 5: Dirty the page and force one eviction.  With 100% failpoint probability
        # the write always fails; page_id stays valid and block_disagg->size is unchanged.
        c = self.session.open_cursor(self.uri)
        self.insert_rows(c, 0, nrows, 'C')
        c.close()
        self.evict_page('key000000')

        # Step 6: Disable the failpoint and run the recovery checkpoint.
        # The checkpoint reconciles the still-dirty page using the same page_id
        # (old_block_meta != NULL), so decrease_size(old_chain) is called followed by
        # increase_size(new_block), keeping block_disagg->size accurate.
        self.conn.reconfigure('timing_stress_for_test=[]')
        self.session.checkpoint()
        size_after_recovery = self.get_checkpoint_size()

        # Step 7: Size check  without the WT-16864 fix __rec_write_err set page_id =
        # INVALID regardless of write success/failure, forcing old_block_meta = NULL on
        # the retry and permanently leaking old_cumulative into block_disagg->size.
        # With the fix the recovery checkpoint uses old_block_meta and correctly subtracts
        # the old chain, so size_after_recovery is close to size_initial rather than
        # size_with_delta + size_initial.
        self.assertLess(size_after_recovery, size_with_delta + size_initial,
            f'Checkpoint size {size_after_recovery} after write-failure recovery is too large '
            f'(size_initial={size_initial}, size_with_delta={size_with_delta}). '
            f'The old delta chain cumulative_size may have leaked into block_disagg->size.')
