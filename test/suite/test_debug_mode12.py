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
import wttest

# test_debug_mode12.py
# Test the debug_mode.clean_scrub config knob.
#
# With clean_scrub enabled, every reconciliation of a leaf page saves a disk
# image regardless of the normal update-volume and read-generation thresholds.
# The eviction walk then picks up clean pages with saved disk images and
# re-instantiates them in memory from the disk image (clean-scrub eviction),
# which reclaims the memory held by old in-memory update chains without a
# disk read.
class test_debug_mode12(wttest.WiredTigerTestCase):
    # Use a cache large enough that pages stay dirty in memory until checkpoint,
    # but small enough that a second wave of inserts triggers eviction and
    # gives the eviction walk a chance to find clean-scrub candidates.
    conn_config = 'cache_size=50MB,statistics=(all),eviction=(clean_scrub_eviction=true),debug_mode=(clean_scrub=true,evict_walk_full=true),checkpoint=(wait=0)'
    uri = "table:test_debug_mode12"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    # Verify that a checkpoint saves disk images for reconciled leaf pages
    # when debug_mode=(clean_scrub=true) bypasses the normal thresholds.
    def test_images_saved_on_checkpoint(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(images_saved, 0)

    # Verify that clean pages with saved disk images are re-instantiated via
    # the clean-scrub eviction path. A checkpoint produces clean pages with
    # saved disk images; a second wave of inserts then pressures the cache
    # and drives the eviction server to find and scrub those pages.
    def test_clean_scrub_eviction(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        # Insert enough data to exceed the cache size (50MB) so that the
        # eviction server runs and walks over the clean pages that have saved
        # disk images from the checkpoint.
        self.populate(self.nrows, self.nrows * 12)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()

        self.assertGreater(images_saved, 0)
        self.assertGreater(evictions, 0)

    # Verify that data is still readable and correct after clean-scrub
    # re-instantiation replaces the in-memory page content.
    def test_clean_scrub_data_correct(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 2, value_char='b')

        cursor = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'a' * self.value_size)
        for i in range(self.nrows, self.nrows * 2):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'b' * self.value_size)
        cursor.close()

    # Verify that saved-image bytes are reflected in the connection cache bytes counters so
    # the memory used by clean-scrub inventory is visible to operators.
    def test_image_bytes_tracked(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        # Baseline: nothing saved yet.
        stat_cursor = self.session.open_cursor('statistics:')
        self.assertEqual(stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2], 0)
        stat_cursor.close()

        self.populate(0, self.nrows)
        self.session.checkpoint()

        # After checkpoint: the outstanding gauge should be positive and at most the cumulative
        # saved bytes. The gauge (cache_clean_scrub_image_bytes) is an inventory-style counter;
        # the cumulative counter (cache_clean_scrub_image_saved_bytes) monotonically increases.
        stat_cursor = self.session.open_cursor('statistics:')
        saved_bytes = stat_cursor[stat.conn.cache_clean_scrub_image_saved_bytes][2]
        outstanding = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        # The gauge includes a small overhead fudge factor (see __wt_cache_bytes_plus_overhead),
        # so it can slightly exceed the raw cumulative saved_bytes. We only care that it tracks
        # saves: positive, and within a sensible range of the cumulative figure.
        self.assertGreater(outstanding, 0)
        self.assertLess(outstanding, saved_bytes * 2)

        # Force cache pressure so the images are eventually scrubbed/discarded.
        self.populate(self.nrows, self.nrows * 12)

        # After eviction has run the outstanding gauge drops: images are either scrubbed (the
        # bytes move into the new child pages' disk image counters) or the owning pages are
        # discarded. Either way our inventory counter should decrease.
        stat_cursor = self.session.open_cursor('statistics:')
        after = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertLess(after, outstanding)

    # Verify that disabling the flag at runtime stops clean-scrub evictions.
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
