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

# test_clean_scrub_eviction_in_memory01.py
#       In-memory btrees never reach the on-disk reconciliation paths, so clean-scrub must not
#       save any disk images for them.
class test_clean_scrub_eviction_in_memory01(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,statistics=(all),in_memory=true,'
                   'eviction=(clean_scrub_eviction=true),'
                   'debug_mode=(clean_scrub=true)')
    uri = "table:test_clean_scrub_eviction_in_memory01"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    def test_in_memory_not_saved(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertEqual(images_saved, 0,
            "in-memory btree saved clean-scrub images: {}".format(images_saved))
