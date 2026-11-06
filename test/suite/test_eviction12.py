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

# Before the eviction server sleeps it releases the walk points it holds in the trees it has
# walked. With many trees that cost must follow the walk points, not the number of open handles:
# the release holds locks that table opens and the sweep server wait for.
@wttest.skip_for_hook("disagg", "Layout and eviction behavior differ under disaggregated storage.")
class test_eviction12(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=20MB,statistics=(all),'
                   'eviction=(threads_min=1,threads_max=1,evict_use_softptr=false)')
    ntables = 300

    # With nothing to evict there is no walk point to release, so releasing them must not visit
    # the handles at all.
    def test_eviction_clear_walks_without_walk_points(self):
        for i in range(self.ntables):
            uri = f'table:test_eviction12_{i}'
            self.session.create(uri, 'key_format=i,value_format=S')
            cursor = self.session.open_cursor(uri)
            cursor[1] = 'abc'
            cursor.close()

        calls = self.get_stat(stat.conn.eviction_server_clear_all_walks)
        visited = self.get_stat(stat.conn.eviction_server_clear_all_walks_handles)
        self.assertStatGreaterSoon(stat.conn.eviction_server_clear_all_walks, calls + 4,
            timeout=60, msg='eviction server never released its walks')

        new_calls = self.get_stat(stat.conn.eviction_server_clear_all_walks) - calls
        new_visited = self.get_stat(stat.conn.eviction_server_clear_all_walks_handles) - visited
        # Visiting every handle would be at least one handle per table on every call.
        self.assertLess(new_visited, new_calls * self.ntables // 10)
