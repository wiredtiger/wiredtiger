#!/usr/bin/env python3
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

import wiredtiger, wttest

# test_checkpoint_failpoint01.py
# Test that the failpoint_checkpoint_error_between_trees timing stress option exercises the
# checkpoint reset path and that the system remains consistent after a failed checkpoint
# followed by a successful one.
class test_checkpoint_failpoint01(wttest.WiredTigerTestCase):
    uri1 = 'table:test_ckpt_fp_1'
    uri2 = 'table:test_ckpt_fp_2'
    table_config = 'key_format=i,value_format=S'

    def test_failpoint_checkpoint_error_between_trees(self):
        # Create multiple tables so the checkpoint iterates over more than one tree,
        # giving the failpoint a chance to fire between them.
        self.session.create(self.uri1, self.table_config)
        self.session.create(self.uri2, self.table_config)

        # Populate both tables and take a clean baseline checkpoint.
        c1 = self.session.open_cursor(self.uri1)
        c2 = self.session.open_cursor(self.uri2)
        for i in range(500):
            c1[i] = 'a' * 100
            c2[i] = 'b' * 100
        c1.close()
        c2.close()
        self.session.checkpoint()

        # Now enable the failpoint.
        self.conn.reconfigure(
            'timing_stress_for_test=[failpoint_checkpoint_error_between_trees]')

        # Run checkpoints in a loop, inserting fresh data each iteration so the trees are
        # always dirty and the checkpoint must walk all handles.
        failed = 0
        succeeded = 0
        key = 500
        for _ in range(80):
            c1 = self.session.open_cursor(self.uri1)
            c2 = self.session.open_cursor(self.uri2)
            c1[key] = 'x' * 100
            c2[key] = 'y' * 100
            c1.close()
            c2.close()
            key += 1

            try:
                self.session.checkpoint()
                succeeded += 1
            except wiredtiger.WiredTigerError:
                failed += 1

        self.pr(f'Checkpoint attempts: {failed} failed, {succeeded} succeeded')

        # With 80 attempts, each touching multiple dirty trees at 5% per tree, the
        # probability of zero failures is negligible.
        self.assertGreater(failed, 0,
            'Expected at least one failpoint-triggered checkpoint error')
        self.assertGreater(succeeded, 0,
            'Expected at least one successful checkpoint')

        # Disable the failpoint so the final checkpoint and verify are clean.
        self.conn.reconfigure('timing_stress_for_test=()')
        self.session.checkpoint()

        # Verify both tables are consistent after the failed+successful checkpoint sequence.
        self.session.verify(self.uri1)
        self.session.verify(self.uri2)
