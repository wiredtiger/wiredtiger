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

import wiredtiger, wttest

# test_checkpoint_failpoint01.py
# Test that the failpoint_checkpoint_error_between_trees timing stress option exercises the
# checkpoint reset path and that the system remains consistent after a failed checkpoint
# followed by a successful one.
#
# We use a single user table intentionally. With multiple tables the failpoint can fire after
# earlier trees have already passed the block-level point of no return (WT_CKPT_PANIC_ON_FAILURE),
# which correctly causes a panic during checkpoint resolve unroll. That panic path is a known
# limitation of the current checkpoint error recovery and is tested separately via test/format.
# This test focuses on the non-panic reset path where the error occurs before any tree has been
# irrevocably committed.
class test_checkpoint_failpoint01(wttest.WiredTigerTestCase):
    uri = 'table:test_ckpt_fp'
    table_config = 'key_format=i,value_format=S'

    def test_failpoint_checkpoint_error_between_trees(self):
        self.session.create(self.uri, self.table_config)

        # Populate the table and take a clean baseline checkpoint.
        c = self.session.open_cursor(self.uri)
        for k in range(500):
            c[k] = 'a' * 100
        c.close()
        self.session.checkpoint()

        # Enable the failpoint. With one user table plus internal handles (history store, etc.)
        # the checkpoint walks a few handles. At 5% per handle, each checkpoint attempt has a
        # small but non-trivial chance of failure. Over 500 attempts the probability of zero
        # failures is negligible (~0.98^500 < 0.005%).
        self.conn.reconfigure(
            'timing_stress_for_test=[failpoint_checkpoint_error_between_trees]')

        failed = 0
        succeeded = 0
        key = 500
        for _ in range(500):
            c = self.session.open_cursor(self.uri)
            c[key] = 'x' * 100
            c.close()
            key += 1

            try:
                self.session.checkpoint()
                succeeded += 1
            except wiredtiger.WiredTigerError:
                failed += 1

        self.pr(f'Checkpoint attempts: {failed} failed, {succeeded} succeeded')
        self.assertGreater(failed, 0,
            'Expected at least one failpoint-triggered checkpoint error')
        self.assertGreater(succeeded, 0,
            'Expected at least one successful checkpoint')

        # Disable the failpoint before cleanup so the final checkpoint and verify are clean.
        self.conn.reconfigure('timing_stress_for_test=()')
        self.session.checkpoint()

        # Verify the table is consistent after the failed+successful checkpoint sequence.
        self.session.verify(self.uri)

        # The failpoint produces expected error messages on stderr. Suppress them so the
        # test framework does not flag them as unexpected output during teardown.
        self.ignoreStderrPatternIfExists('failpoint: simulated checkpoint error between trees')
