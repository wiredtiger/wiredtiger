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

import time
import wttest
from wiredtiger import stat


class test_eviction11(wttest.WiredTigerTestCase):
    """Pages evicted from the urgent queue report how long they waited there."""

    conn_config = "cache_size=10MB,statistics=(all)"
    uri = f"file:{__qualname__}"
    value = "a" * 1000

    def conn_stat(self, key):
        with wttest.open_cursor(self.session, "statistics:") as stat_cursor:
            return stat_cursor[key][2]

    def test_urgent_wait_time(self):
        self.session.create(self.uri, "key_format=i,value_format=S,memory_page_max=100KB")

        # Dirty several times the cache size so large dirty pages are queued for urgent eviction.
        deadline = time.time() + 120
        with wttest.open_cursor(self.session, self.uri) as cursor:
            for i in range(200000):
                cursor[i] = self.value
                if i % 10000 == 0 and self.conn_stat(stat.conn.eviction_urgent_pages_taken) > 0:
                    break
                self.assertLess(time.time(), deadline, "no page was taken from the urgent queue")

        queued = self.conn_stat(stat.conn.eviction_pages_queued_urgent)
        taken = self.conn_stat(stat.conn.eviction_urgent_pages_taken)
        total = self.conn_stat(stat.conn.eviction_urgent_wait_time)
        longest = self.conn_stat(stat.conn.eviction_urgent_wait_time_max)

        self.assertGreater(taken, 0)
        self.assertLessEqual(taken, queued)
        self.assertGreater(longest, 0)
        self.assertLessEqual(longest, total)


if __name__ == "__main__":
    wttest.run()
