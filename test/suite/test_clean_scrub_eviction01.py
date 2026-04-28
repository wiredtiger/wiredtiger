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
import wttest

# test_clean_scrub_eviction01.py
#       Core clean-scrub eviction behavior: reconciliation saves disk images, the eviction
#       walk picks them up and re-instantiates pages from them, and the data stays correct.
@wttest.skip_for_hook("disagg",
    "disaggregated storage auto-enables clean-scrub eviction; coverage there is in test_clean_scrub_eviction_disagg01")
class test_clean_scrub_eviction01(CleanScrubBase, wttest.WiredTigerTestCase):
    scenarios = clean_scrub_scenarios
    uri = "table:test_clean_scrub_eviction01"

    # Reconciliation must retain a disk image on each leaf page.
    def test_images_saved_on_checkpoint(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(images_saved, 0)

    # Cache pressure on top of saved images drives the eviction walk to scrub them.
    def test_clean_scrub_eviction(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        # Insert past the cache size so eviction runs and walks over the saved-image pages.
        self.populate(self.nrows, self.nrows * 12)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()

        self.assertGreater(images_saved, 0)
        self.assertGreater(evictions, 0)

    # Data remains readable after re-instantiation replaces the in-memory page content.
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
