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
# test_checkpoint_scrub_evict.py
#
# Tests for WT-18005: with precise_checkpoint=true, checkpoint reconciliation
# of row-leaf pages sets WT_REC_SCRUB, saving mod_disk_image so the eviction
# server can priority-evict those pages without I/O.
#
# Observable behaviors tested:
#   1. cache_write_restore_scrub stat increases after a checkpoint cycle when
#      precise_checkpoint=true — evidence that scrub reconciliation fired.
#   2. cache_scrub_restore stat increases — pages re-instantiated from disk
#      image after checkpoint scrub.
#   3. cache_eviction_blocked_precise_checkpoint stat is present and does not
#      go negative (smoke-test the counter is wired up correctly).
#   4. Basic read-write workload produces consistent data after checkpoint.
#   5. Fuzzy (non-precise) checkpoint does NOT increment cache_write_restore_scrub
#      — confirming the stat is precise_checkpoint-specific.

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

class test_checkpoint_scrub_evict(wttest.WiredTigerTestCase):
    """
    Verify scrub-eviction behaviour introduced by WT-18005.

    Two scenario axes:
      - precise_checkpoint on/off
      - small / larger value size (to ensure multiple pages are written)
    """

    uri = 'table:scrub_evict'

    ckpt_precision = [
        ('precise', dict(precise=True,  ckpt_cfg='precise_checkpoint=true')),
        ('fuzzy',   dict(precise=False, ckpt_cfg='precise_checkpoint=false')),
    ]
    value_sz = [
        ('small',  dict(vsize=100)),
        ('medium', dict(vsize=1000)),
    ]
    scenarios = make_scenarios(ckpt_precision, value_sz)

    # Keep cache intentionally small to encourage eviction pressure.
    def conn_config(self):
        return (
            'cache_size=50MB,'
            'statistics=(all),'
            + self.ckpt_cfg
        )

    # ------------------------------------------------------------------ helpers

    def _populate(self, nrows, value_size):
        """Insert nrows key/value pairs using a simple pattern."""
        cursor = self.session.open_cursor(self.uri)
        val = 'x' * value_size
        for i in range(nrows):
            cursor[i] = val
        cursor.close()

    def _verify_reads(self, nrows, value_size):
        """Read back all rows and verify values are intact."""
        cursor = self.session.open_cursor(self.uri)
        expected = 'x' * value_size
        for i in range(nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), expected)
        cursor.close()

    # ------------------------------------------------------------------ tests

    def test_scrub_stat_after_checkpoint(self):
        """
        After a checkpoint with precise_checkpoint=true, the
        cache_write_restore_scrub counter should be non-zero (scrub
        reconciliation must have fired on at least one page).

        With precise_checkpoint=false the stat should remain at zero
        because WT_REC_SCRUB is never set on that path.
        """
        nrows = 5000

        self.session.create(self.uri, 'key_format=i,value_format=S')

        # precise_checkpoint requires a stable timestamp to be set.
        if self.precise:
            self.conn.set_timestamp('stable_timestamp=1')

        self._populate(nrows, self.vsize)

        # Snapshot the counter before the checkpoint.
        before = self.get_stat(stat.conn.cache_write_restore_scrub)

        self.session.checkpoint()

        after = self.get_stat(stat.conn.cache_write_restore_scrub)

        if self.precise:
            # At least some pages must have been scrub-reconciled.
            self.assertGreater(
                after, before,
                msg=(
                    'cache_write_restore_scrub should increase after a '
                    'precise checkpoint: before={}, after={}'.format(before, after)
                )
            )
        else:
            # Fuzzy checkpoint must not trigger scrub reconciliation.
            self.assertEqual(
                after, before,
                msg=(
                    'cache_write_restore_scrub should NOT increase for a '
                    'fuzzy checkpoint: before={}, after={}'.format(before, after)
                )
            )

    def test_scrub_restore_stat_after_checkpoint(self):
        """
        cache_scrub_restore counts pages re-instantiated from a saved disk
        image (mod_disk_image).  After a precise checkpoint + subsequent
        eviction pressure, the counter should be > 0.

        With fuzzy checkpoint this path is never taken.
        """
        nrows = 5000

        self.session.create(self.uri, 'key_format=i,value_format=S')

        if self.precise:
            self.conn.set_timestamp('stable_timestamp=1')

        self._populate(nrows, self.vsize)
        self.session.checkpoint()

        # Generate more write pressure so the eviction server runs and
        # encounters the scrubbed pages.
        self._populate(nrows, self.vsize)

        if self.precise:
            # Allow the eviction server a moment to process scrubbed pages.
            self.assertStatGreaterSoon(
                stat.conn.cache_scrub_restore, 0,
                timeout=5,
                msg='cache_scrub_restore should be > 0 after precise checkpoint + eviction pressure'
            )
        # For fuzzy we only verify the stat doesn't go negative.
        val = self.get_stat(stat.conn.cache_scrub_restore)
        self.assertGreaterEqual(val, 0, 'cache_scrub_restore must not be negative')

    def test_blocked_precise_checkpoint_stat_wired(self):
        """
        Smoke-test that cache_eviction_blocked_precise_checkpoint is a valid,
        non-negative counter — confirming it is wired up in the build under test.
        """
        self.session.create(self.uri, 'key_format=i,value_format=S')

        if self.precise:
            self.conn.set_timestamp('stable_timestamp=1')

        self._populate(1000, self.vsize)
        self.session.checkpoint()

        val = self.get_stat(stat.conn.cache_eviction_blocked_precise_checkpoint)
        self.assertGreaterEqual(
            val, 0,
            'cache_eviction_blocked_precise_checkpoint must be >= 0'
        )

    def test_data_integrity_after_checkpoint(self):
        """
        Basic correctness: data written before a precise checkpoint must be
        fully readable afterwards.  This catches any regression where scrub
        reconciliation corrupts the on-disk image.
        """
        nrows = 2000

        self.session.create(self.uri, 'key_format=i,value_format=S')

        if self.precise:
            self.conn.set_timestamp('stable_timestamp=1')

        self._populate(nrows, self.vsize)
        self.session.checkpoint()

        # Verify all rows are readable and correct.
        self._verify_reads(nrows, self.vsize)

        # Update half the rows, checkpoint again, verify everything.
        cursor = self.session.open_cursor(self.uri)
        val2 = 'y' * self.vsize
        for i in range(0, nrows, 2):
            cursor[i] = val2
        cursor.close()

        self.session.checkpoint()

        cursor = self.session.open_cursor(self.uri)
        for i in range(nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            expected = ('y' if i % 2 == 0 else 'x') * self.vsize
            self.assertEqual(
                cursor.get_value(), expected,
                msg=f'Row {i} has wrong value after second checkpoint'
            )
        cursor.close()

    def test_multiple_checkpoint_cycles(self):
        """
        Run several checkpoint cycles with concurrent writes to verify the
        scrub path is stable over time and does not accumulate errors.
        """
        nrows = 1000

        self.session.create(self.uri, 'key_format=i,value_format=S')

        if self.precise:
            self.conn.set_timestamp('stable_timestamp=1')

        for cycle in range(5):
            val = chr(ord('a') + cycle) * self.vsize
            cursor = self.session.open_cursor(self.uri)
            for i in range(nrows):
                cursor[i] = val
            cursor.close()
            self.session.checkpoint()

        # After 5 cycles the last value written should be readable.
        cursor = self.session.open_cursor(self.uri)
        expected = 'e' * self.vsize
        for i in range(nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), expected,
                msg=f'Row {i} has stale value after 5 checkpoint cycles')
        cursor.close()

        # Scrub stat must be non-negative.
        scrub = self.get_stat(stat.conn.cache_write_restore_scrub)
        self.assertGreaterEqual(scrub, 0)

        if self.precise:
            # After 5 cycles there must be at least one scrubbed page.
            self.assertGreater(
                scrub, 0,
                msg='cache_write_restore_scrub should accumulate over multiple precise checkpoints'
            )
