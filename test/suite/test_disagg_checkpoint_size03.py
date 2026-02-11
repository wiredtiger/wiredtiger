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
import unittest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_disagg_checkpoint_size03.py

@disagg_test_class
class test_disagg_checkpoint_size03(wttest.WiredTigerTestCase):

    uri_base = "test_disagg_ckpt_size03"
    conn_config = 'disaggregated=(role="leader",lose_all_my_data=true), page_delta=(delta_pct=90,internal_page_delta=true,leaf_page_delta=true,max_consecutive_delta=5)'
    uri = "layered:" + uri_base

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def get_checkpoint_size(self):
        stable_uri = f'file:{self.uri_base}.wt_stable'
        mc = self.session.open_cursor('metadata:')
        mc.set_key(stable_uri)
        mc.search()
        size = int(re.findall(r',size=(\d+),', mc.get_value())[-1])
        mc.close()
        return size

    @unittest.skip("Skipping test_bytes_total_leak")   
    def test_bytes_total_leak(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        nrows = 1
        val_size = 10

        # Insert data and take the baseline checkpoint.
        c = self.session.open_cursor(self.uri)
        for i in range(nrows):
            c[f'key{i:06d}'] = 'a' * val_size
        c.close()
        self.session.checkpoint()
        baseline = self.get_checkpoint_size()

        # Track delta vs full page statistics
        delta_count = 0
        full_page_count = 0

        # Rewrite every row three times, checkpointing each time.
        # Each cycle rewrites all leaf, internal, and root pages.
        # for cycle, ch in enumerate(['b', 'c', 'd', 'e', 'f'], start=1):
        for cycle, ch in enumerate(['b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'], start=1):
            c = self.session.open_cursor(self.uri)
            for i in range(nrows):
                c[f'key{i:06d}'] = ch * val_size
            c.close()
            self.session.checkpoint()

            # Check if this rewrite created a delta or full page
            stat_cursor = self.session.open_cursor('statistics:' + self.uri)
            cycle_deltas = stat_cursor[stat.dsrc.rec_page_delta_leaf][2]
            cycle_full_pages = stat_cursor[stat.dsrc.rec_page_full_image_leaf][2]
            stat_cursor.close()

            # Track cumulative counts
            delta_count = cycle_deltas
            full_page_count = cycle_full_pages

            self.pr(f"Cycle {cycle}: deltas={cycle_deltas}, full_pages={cycle_full_pages}")

        final = self.get_checkpoint_size()

        # Report what type of writes occurred
        self.pr(f"Total: {delta_count} deltas, {full_page_count} full pages written")

        # The data volume hasn't changed — same nrows, same val_size.
        # The checkpoint size should stay near the baseline, not grow to ~3x.
        self.pr(f"Final: {final}, Baseline: {baseline}, multiple of baseline: {final/baseline:.1f}x")
        self.assertLess(final, baseline * 2,
            f"bytes_total is leaking: baseline={baseline}, after 3 rewrite cycles={final} "
            f"({final/baseline:.1f}x). Old disagg page blocks are not being freed during "
            f"single-page reconciliation — see disagg_page_free_required in rec_write.c "
            f"and disagg_free_block in __wt_ref_block_free().")


    def test_bytes_total_leak_delta(self):  
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Write initial page with multiple keys
        c = self.session.open_cursor(self.uri)
        for i in range(10):
            c[f'key{i:02d}'] = f'value{i}'
        c.close()
        self.session.checkpoint()

        # The size of the first page we wrote + its root page.
        baseline = self.get_checkpoint_size()

        # Track statistics across cycles
        total_deltas = 0
        total_full_pages = 0
        sizes = [baseline]
        expected_table_size = 0
        prev_full_pages = 0 
        cur_deltas = 0

        # Multiple iterations: first create deltas, then force full page writes
        for cycle, ch in enumerate(['b', 'c', 'd', 'e', 'f', 'g', 'h'], start=1):#, 'h', 'i', 'j', 'k', 'l', 'm'], start=1):
            c = self.session.open_cursor(self.uri)
            # Update existing keys to create deltas
            for i in range(0, 10, 2):
                c[f'key{i:02d}'] = f'newvalue{cycle}{i}'
            c.close()
            self.session.checkpoint()

            # Check statistics
            stat_cursor = self.session.open_cursor('statistics:' + self.uri)
            cycle_deltas = stat_cursor[stat.dsrc.rec_page_delta_leaf][2]
            cycle_full_pages = stat_cursor[stat.dsrc.rec_page_full_image_leaf][2]
            stat_cursor.close()

            # Track cumulative counts
            total_deltas = cycle_deltas
            if (cycle_full_pages != prev_full_pages):
                self.pr(f"Cycle {cycle}: full pages changed from {prev_full_pages} to {cycle_full_pages}")
                cur_deltas = 0
            else:
                cur_deltas += 1

            
            expected_table_size = baseline * (cur_deltas + 1)
            total_full_pages = cycle_full_pages
            self.pr(f"Expected delta size: {expected_table_size} & cycle_deltas: {cycle_deltas}")


            current_size = self.get_checkpoint_size()
            sizes.append(current_size)
            
            self.pr(f"Cycle {cycle}: deltas={cycle_deltas}, full_pages={cycle_full_pages}, size={current_size}")
            prev_full_pages = cycle_full_pages

        final = self.get_checkpoint_size()

        # Report results
        self.pr(f"Final: {final}, Baseline: {baseline}, multiple: {final/baseline:.1f}x")
        self.pr(f"Total: {total_deltas} deltas, {total_full_pages} full pages written")
        self.pr(f"Expected delta size: {expected_table_size}")

        # Size should not grow excessively even with delta->full page transitions
        # The final size should be less than the baseline + the expected delta size.
        self.pr(f"Expected final size: {expected_table_size}")
        self.assertLess(final, expected_table_size * 1.10,
            f"Size leak detected: baseline={baseline}, final={final} ({final/baseline:.1f}x). "
            f"Check delta chain termination handling in rec_write.c")

        # Verify we actually created deltas during the test
        self.assertGreater(total_deltas, 0, "No deltas were created during test")


    @unittest.skip("Skipping test_bytes_total_leak_delta_normal_ops")   
    def test_bytes_total_leak_delta_normal_ops(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Write initial page with multiple keys
        c = self.session.open_cursor(self.uri)
        for i in range(10):
            c[f'key{i:02d}'] = f'value{i}'
        c.close()
        self.session.checkpoint()
        baseline = self.get_checkpoint_size()

        # Multiple iterations updating existing keys to create deltas
        for cycle, ch in enumerate(['b', 'c', 'd', 'e', 'f'], start=1):
            c = self.session.open_cursor(self.uri)
            # Update existing keys instead of inserting new ones
            for i in range(0, 10, 2):  # Update every other key
                c[f'key{i:02d}'] = f'newvalue{cycle}{i}'
            c.close()
            self.session.checkpoint()

            # Verify deltas were created
            stat_cursor = self.session.open_cursor('statistics:' + self.uri)
            delta_count = stat_cursor[stat.dsrc.rec_page_delta_leaf][2]
            stat_cursor.close()
            self.assertGreater(delta_count, 0,
                f"Cycle {cycle}: Expected leaf page deltas but got {delta_count}")
