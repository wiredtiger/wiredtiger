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

import wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from sweep_util import sweep_util
from wtscenario import make_scenarios


# Verify eviction tolerates shared history store replacement across role transitions.
@disagg_test_class
class test_layered_eviction08(sweep_util):
    test_name = __qualname__
    conn_base_config = (
        'cache_size=10MB,statistics=(all),'
        'file_manager=(close_handle_minimum=0,close_idle_time=1,close_scan_interval=1),'
    )
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)
    uri = f'layered:{test_name}'
    nrows = 10000

    def write_generation(self, cursor, generation, timestamp):
        value = f'g{generation}' + 'x' * 510
        for start in range(0, self.nrows, 500):
            self.session.begin_transaction()
            for key in range(start, start + 500):
                cursor[key] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(timestamp - 1))
        self.session.checkpoint()

    def test_shared_history_store_reopen_during_eviction(self):
        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(1) + ',stable_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S,block_manager=disagg')

        cursor = self.session.open_cursor(self.uri)
        self.write_generation(cursor, 0, 5)
        self.write_generation(cursor, 1, 10)
        self.conn.reconfigure(
            'eviction_dirty_target=1,eviction_dirty_trigger=5,'
            'eviction_updates_target=1,eviction_updates_trigger=5'
        )
        self.write_generation(cursor, 2, 15)
        cursor.close()

        self.assertStatGreaterSoon(wiredtiger.stat.conn.cache_hs_insert, 0, timeout=60)
        self.assertStatGreaterSoon(wiredtiger.stat.conn.cache_eviction_dirty, 0, timeout=60)
        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.cache_eviction_trigger_dirty_reached, 0, timeout=60)
        self.assertEqual(
            self.get_stat(wiredtiger.stat.conn.cache_eviction_hs_shared_cursor_not_cached), 0)

        self.session.close()
        self.session = self.conn.open_session('')

        sweep_baseline = self.get_stat(wiredtiger.stat.conn.dh_sweeps)
        expired_close_baseline = self.get_stat(wiredtiger.stat.conn.dh_sweep_expired_close)
        remove_baseline = self.get_stat(wiredtiger.stat.conn.dh_sweep_remove)
        for _ in range(20):
            self.conn.reconfigure('disaggregated=(role="follower")')
            self.conn.reconfigure('disaggregated=(role="leader")')
            hs_cursor = self.session.open_cursor('file:WiredTigerSharedHS.wt_stable')
            hs_cursor.close()

        # The sweep server waits five seconds before closing an outdated shared history store handle.
        self.wait_for_sweep(baseline=sweep_baseline, increment=7, timeout=30, poll_interval=1)
        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.dh_sweep_expired_close, expired_close_baseline, timeout=10)
        self.assertStatGreaterSoon(wiredtiger.stat.conn.dh_sweep_remove, remove_baseline, timeout=10)

        dirty_evictions = self.get_stat(wiredtiger.stat.conn.cache_eviction_dirty)
        dirty_trigger_reaches = self.get_stat(
            wiredtiger.stat.conn.cache_eviction_trigger_dirty_reached)
        cursor = self.session.open_cursor(self.uri)
        self.write_generation(cursor, 3, 20)
        cursor.close()

        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.cache_eviction_dirty, dirty_evictions, timeout=60)
        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.cache_eviction_trigger_dirty_reached,
            dirty_trigger_reaches,
            timeout=60,
        )
