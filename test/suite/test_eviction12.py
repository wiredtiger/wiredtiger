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


class test_eviction12(wttest.WiredTigerTestCase):
    """Check the statistics that time the eviction server."""

    # A table four times the cache size can only be written if the server keeps refilling its
    # queues.
    conn_config = "cache_size=50MB,eviction_dirty_target=5,eviction_dirty_trigger=10," \
        "statistics=(all)"
    uri = "file:test_eviction12"

    def test_eviction_server_time(self):
        self.session.create(self.uri, "key_format=i,value_format=S")
        with wttest.open_cursor(self.session, self.uri) as cursor:
            for i in range(200 * 1024):
                cursor[i] = "a" * 1000

        # With the cache far larger than the data the server has no reason to refill, and verify
        # waits for the server to finish its current pass. Refill statistics are then final.
        self.conn.reconfigure("cache_size=10GB")
        self.verifyUntilSuccess()

        refills = self.get_stat(stat.conn.eviction_server_refills)
        refill = self.get_stat(stat.conn.eviction_server_refill_time)
        tree_walk = self.get_stat(stat.conn.eviction_server_refill_tree_walk_time)
        post_walk = self.get_stat(stat.conn.eviction_server_refill_post_walk_time)
        slept = self.get_stat(stat.conn.eviction_server_slept_time)
        # Idle passes keep adding pass time, so read it last.
        pass_time = self.get_stat(stat.conn.eviction_server_pass_time)

        self.assertGreater(refills, 0)
        self.assertGreater(tree_walk, 0)
        self.assertLessEqual(tree_walk + post_walk, refill)
        self.assertLessEqual(refill + slept, pass_time)


if __name__ == "__main__":
    wttest.run()
