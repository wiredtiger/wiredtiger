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

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# test_eviction06.py
#
# Functional correctness tests for the two_phase_eviction mode. The same suite
# of operations (clean eviction, dirty eviction, update visibility after
# eviction, runtime toggle) is run under both the new two-phase model
# (two_phase_eviction=true, the default) and the legacy single-phase model
# (two_phase_eviction=false) to confirm identical observable behavior.
#
# In the two-phase model reconciliation happens in phase 1 while the ref stays
# at WT_REF_MEM (readable under a hazard pointer); the exclusive WT_REF_LOCKED
# state is acquired only for the fast swap-out in phase 2. In the single-phase
# model the exclusive lock is held for the entire eviction.
@wttest.skip_for_hook("disagg", "Fails due to evict a page.")
class test_eviction06(wttest.WiredTigerTestCase):
    """Verify data correctness under two-phase and single-phase eviction."""

    nrows = 200

    scenarios = make_scenarios([
        ('two_phase_enabled',  dict(two_phase='true')),
        ('two_phase_disabled', dict(two_phase='false')),
    ])

    def conn_config(self):
        return (
            'cache_size=50MB,statistics=(all),'
            f'eviction=[two_phase_eviction={self.two_phase}]'
        )

    def get_stat(self, stat_key):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat_key][2]
        stat_cursor.close()
        return val

    def _force_evict(self, uri, key):
        """Force-evict the page containing key via the release_evict debug cursor."""
        evict_cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        evict_cursor.set_key(key)
        self.assertEqual(evict_cursor.search(), 0)
        evict_cursor.reset()
        evict_cursor.close()

    def test_clean_page_eviction(self):
        """Checkpoint makes pages clean; evicting them must leave all data readable."""
        uri = 'table:test_eviction06_clean'
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'value_' + str(i)
        cursor.close()

        # Checkpoint flushes all dirty data; pages are now clean.
        self.session.checkpoint()

        # Force-evict the first page in the table.
        self._force_evict(uri, 0)

        # All rows must still be readable with their original values.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'value_' + str(i))
        cursor.close()

        # At least one clean page was evicted.
        self.assertGreater(self.get_stat(stat.conn.cache_eviction_clean), 0)

    def test_dirty_page_eviction(self):
        """Evicting a dirty (un-checkpointed) page must write it to disk and keep
        all committed data visible afterward."""
        uri = 'table:test_eviction06_dirty'
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'value_' + str(i)
        cursor.close()
        # Intentionally skip checkpoint so the page remains dirty.

        self._force_evict(uri, 0)

        # Evicted dirty page must be recoverable from disk.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'value_' + str(i))
        cursor.close()

        # At least one dirty page was written out during eviction.
        self.assertGreater(self.get_stat(stat.conn.cache_eviction_dirty), 0)

    def test_updates_visible_after_eviction(self):
        """Insert initial values, overwrite them, evict the page, then confirm
        that only the updated values (not the original ones) are visible."""
        uri = 'table:test_eviction06_updates'
        self.session.create(uri, 'key_format=i,value_format=S')

        # Initial population followed by a checkpoint.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'initial_' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Overwrite every row; page is dirty again.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'updated_' + str(i)
        cursor.close()

        self._force_evict(uri, 0)

        # Only the updated values must be returned.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'updated_' + str(i))
        cursor.close()

    def test_reader_survives_concurrent_eviction(self):
        """Open a read cursor positioned on a page, trigger eviction of that page
        from a second session, then verify the original cursor can still return the
        correct value.

        This exercises the key two-phase safety property: a reader holding a hazard
        pointer on the page either completes its read before the phase-2 CAS or
        causes the eviction to back off (EBUSY), but either way the data is never
        lost or corrupted.
        """
        uri = 'table:test_eviction06_concurrent'
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'value_' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Open a reader in a separate session and position it on the page.
        reader_session = self.conn.open_session()
        reader_cursor = reader_session.open_cursor(uri)
        reader_cursor.set_key(0)
        self.assertEqual(reader_cursor.search(), 0)

        # Attempt eviction from the main session while the reader cursor is active.
        evict_session = self.conn.open_session()
        evict_cursor = evict_session.open_cursor(uri, None, 'debug=(release_evict)')
        evict_cursor.set_key(0)
        # Eviction may succeed or return EBUSY (hazard pointer held by reader);
        # both outcomes are valid — we are only testing that data is not corrupted.
        evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        evict_session.close()

        # The reader must still see the correct value regardless of eviction outcome.
        self.assertEqual(reader_cursor.get_value(), 'value_0')
        reader_cursor.close()
        reader_session.close()

    def test_runtime_toggle_during_eviction(self):
        """Toggle two_phase_eviction at runtime while actively inserting and
        evicting data, verifying no data is lost across the mode switch."""
        uri = 'table:test_eviction06_toggle'
        self.session.create(uri, 'key_format=i,value_format=S')

        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = 'before_' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Evict under the initial mode.
        self._force_evict(uri, 0)

        # Toggle the mode.
        opposite = 'false' if self.two_phase == 'true' else 'true'
        self.conn.reconfigure(f'eviction=[two_phase_eviction={opposite}]')

        # Insert new rows under the toggled mode and evict again.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows, self.nrows * 2):
            cursor[i] = 'after_' + str(i)
        cursor.close()
        self._force_evict(uri, self.nrows)

        # All data (both batches) must be intact.
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'before_' + str(i))
        for i in range(self.nrows, self.nrows * 2):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'after_' + str(i))
        cursor.close()


if __name__ == '__main__':
    wttest.run()
