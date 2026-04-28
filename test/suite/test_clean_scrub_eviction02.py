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

from wiredtiger import stat
from helper_clean_scrub import CleanScrubBase, clean_scrub_scenarios
import wiredtiger, wttest

# test_clean_scrub_eviction02.py
#       Memory accounting and runtime configuration: saved-image bytes are visible in the
#       cache counters, system btrees are excluded, the inventory gauge returns to zero, and
#       the feature can be turned on or off at runtime.
@wttest.skip_for_hook("disagg",
    "disaggregated storage auto-enables clean-scrub eviction; runtime on/off toggling tests don't apply there")
class test_clean_scrub_eviction02(CleanScrubBase, wttest.WiredTigerTestCase):
    scenarios = clean_scrub_scenarios
    uri = "table:test_clean_scrub_eviction02"

    # The connection-wide gauge tracks saves and falls when images are scrubbed/discarded.
    def test_image_bytes_tracked(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        stat_cursor = self.session.open_cursor('statistics:')
        self.assertEqual(stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2], 0)
        stat_cursor.close()

        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_bytes = stat_cursor[stat.conn.cache_clean_scrub_image_saved_bytes][2]
        outstanding = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        # The gauge applies the cache overhead factor (see __wt_cache_bytes_plus_overhead),
        # so it can slightly exceed the raw cumulative saves; only the order of magnitude matters.
        self.assertGreater(outstanding, 0)
        self.assertLess(outstanding, saved_bytes * 2)

        # Pressure to drive scrubs/discards.
        self.populate(self.nrows, self.nrows * 12)

        stat_cursor = self.session.open_cursor('statistics:')
        after = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertLess(after, outstanding)

    # The metadata and history-store btrees must never hold clean-scrub images.
    def test_system_btrees_not_saved(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        user = self.session.open_cursor('statistics:' + self.uri)
        user_saved = user[stat.dsrc.cache_clean_scrub_image_saved][2]
        user.close()
        self.assertGreater(user_saved, 0)

        for system_uri in ('statistics:file:WiredTigerHS.wt',
                           'statistics:file:WiredTiger.wt'):
            try:
                c = self.session.open_cursor(system_uri)
            except wiredtiger.WiredTigerError:
                continue
            saved = c[stat.dsrc.cache_clean_scrub_image_saved][2]
            c.close()
            self.assertEqual(saved, 0,
                "system btree {} has clean-scrub images: {}".format(system_uri, saved))

    # Catches drift: after the table is dropped, the gauge must return to zero.
    def test_inventory_returns_to_zero(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        # Verify that saves happened via the monotonic counter; the gauge can race with eviction
        # scrubbing saved images back out under cache pressure on faster builds.
        stat_cursor = self.session.open_cursor('statistics:')
        saves = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(saves, 0)

        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.populate(self.nrows, self.nrows * 12)

        self.session.close()
        self.session = self.conn.open_session()
        self.dropUntilSuccess(self.session, self.uri)

        stat_cursor = self.session.open_cursor('statistics:')
        remaining = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertEqual(remaining, 0,
            "inventory gauge did not return to zero after table drop: {} bytes".format(remaining))

    # Disabling the feature at runtime stops scrub evictions.
    def test_clean_scrub_off(self):
        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 2)

        stat_cursor = self.session.open_cursor('statistics:')
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()
        self.assertEqual(evictions, 0)

    # Cycle the feature on -> off (no new saves) -> on (saves resume).
    def test_reconfigure_cycle(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_before_off = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(saved_before_off, 0)

        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.populate(self.nrows, self.nrows * 2)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_after_off = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertEqual(saved_after_off, saved_before_off,
            "saves grew while the feature was off")

        self.conn.reconfigure('eviction=(clean_scrub_eviction=true)')
        self.populate(self.nrows * 2, self.nrows * 3)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_after_on = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(saved_after_on, saved_after_off,
            "saves did not resume after re-enable")
