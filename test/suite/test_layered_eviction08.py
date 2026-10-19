#!/usr/bin/env python3
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
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Verify an eviction pass on a leader can look up its cached shared history store cursor without
# keeping that table's handle.
@disagg_test_class
class test_layered_eviction08(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = (
        'cache_size=10MB,statistics=(all),'
        'eviction_dirty_target=1,eviction_dirty_trigger=5,'
        'eviction_updates_target=1,eviction_updates_trigger=5,'
        'disaggregated=(role="leader")'
    )
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)
    uri = f'layered:{test_name}'
    nrows = 10000

    def test_eviction_passes_with_cached_shared_history_store(self):
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.create(self.uri, 'key_format=i,value_format=S')
        passes = self.get_stat(wiredtiger.stat.conn.cache_eviction_trigger_dirty_reached)

        # Keep the cache above the dirty trigger so every eviction pass reports it.
        cursor = self.session.open_cursor(self.uri)
        value = 'x' * 512
        for ts, start in enumerate(range(0, self.nrows, 500), start=2):
            self.session.begin_transaction()
            for key in range(start, start + 500):
                cursor[key] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

        # A pass reports the dirty trigger only after it has looked up the cached cursors.
        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.cache_eviction_trigger_dirty_reached, passes, timeout=60)
        self.assertEqual(
            self.get_stat(wiredtiger.stat.conn.cache_eviction_hs_shared_cursor_not_cached), 0)
