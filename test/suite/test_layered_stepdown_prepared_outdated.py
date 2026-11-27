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

import time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# A prepared transaction left open across step-down keeps the outdated stable tree's dirty pages in
# cache under eviction and sweep pressure, and the pages are discarded once the transaction ends.
@disagg_test_class
class test_layered_stepdown_prepared_outdated(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_stepdown_prepared_outdated'
    nitems = 5000
    timeout = 60
    eviction_relaxed = 'eviction_dirty_target=200MB,eviction_updates_target=200MB'
    eviction_pressure = 'eviction_dirty_target=1MB,eviction_updates_target=1MB'

    # Eviction only targets the prepared content while the test applies pressure.
    def conn_config(self):
        return 'statistics=(all),cache_size=1GB,' + self.eviction_relaxed + ',' \
            'eviction_dirty_trigger=500MB,eviction_updates_trigger=500MB,' \
            'debug_mode=(disagg_stepdown_prepare=true),' \
            'file_manager=(close_scan_interval=1,close_idle_time=1),' \
            'disaggregated=(role="leader")'

    def dirty_stable(self):
        return self.get_stat(wiredtiger.stat.conn.cache_bytes_dirty_stable)

    def test_prepared_pages_survive_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {f'key{i:06d}': 'v' for i in range(self.nitems)}, 10)
        self.set_global_ts(10, 10)
        self.session.checkpoint()

        # Close the cursor before preparing: from here only the transaction holds the stable tree.
        prepare_session = self.conn.open_session()
        cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        for i in range(self.nitems):
            cursor[f'key{i:06d}'] = 'p' * 1024
        cursor.close()
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.prepared_id_str(1))

        self.conn.reconfigure('disaggregated=(role="follower")')
        baseline = self.dirty_stable()
        self.assertGreater(baseline, 0)

        # The stat covers every stable tree, so compare against half the baseline rather than all
        # of it: small changes elsewhere stay well within the margin, while discarding the outdated
        # tree's pages takes almost all of the baseline with it.
        threshold = baseline // 2

        # Nothing can write an outdated tree, so a large drop in its dirty content means its pages
        # were discarded. Hold eviction pressure for a full sweep pass: a sweep pass counts itself
        # as it starts, so two new passes mean one has finished.
        sweeps = self.get_stat(wiredtiger.stat.conn.dh_sweeps)
        self.conn.reconfigure(self.eviction_pressure)
        deadline = time.time() + self.timeout
        while self.get_stat(wiredtiger.stat.conn.dh_sweeps) < sweeps + 2:
            self.assertGreater(self.dirty_stable(), threshold,
                'outdated tree discarded while a prepared transaction holds it')
            self.assertLess(time.time(), deadline, 'sweep did not run')
            time.sleep(0.1)
        self.assertGreater(self.dirty_stable(), threshold,
            'outdated tree discarded while a prepared transaction holds it')
        self.conn.reconfigure(self.eviction_relaxed)

        # FIXME-WT-19187: a follower cannot yet resolve a prepared update on the outdated stable
        # tree. Once it can, require the rollback to succeed.
        try:
            prepare_session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(30))
        except wiredtiger.WiredTigerError:
            self.assertEqual(prepare_session.get_last_error()[1], wiredtiger.WT_CONFLICT_DISAGG)
        prepare_session.close()

        # With the transaction gone, sweep closes the outdated tree and discards its pages.
        deadline = time.time() + self.timeout
        while self.dirty_stable() > threshold:
            self.assertLess(time.time(), deadline, 'outdated tree was not discarded')
            time.sleep(0.5)
