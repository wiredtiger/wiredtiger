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

# Verify the checkpoint cleanup thread starts on step-up and stops on step-down,
# and that repeated role transitions leave the thread in the expected state.
@disagg_test_class
class test_layered_checkpoint_cleanup_role(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="follower"),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def get_conn_stat(self, stat_key):
        c = self.session.open_cursor('statistics:')
        val = c[stat_key][2]
        c.close()
        return val

    def start_count(self):
        return self.get_conn_stat(wiredtiger.stat.conn.checkpoint_cleanup_thread_start)

    def stop_count(self):
        return self.get_conn_stat(wiredtiger.stat.conn.checkpoint_cleanup_thread_stop)

    def test_role_transitions(self):
        # The connection opens as a follower but conn open still starts the checkpoint cleanup
        # thread once, so record the baseline rather than assuming zero.
        base_start = self.start_count()
        base_stop = self.stop_count()

        # Step down while already a follower: no state change, no counter movement.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.start_count(), base_start)
        self.assertEqual(self.stop_count(), base_stop)

        # Step up. Thread starts iff it wasn't already running; either way after this call the
        # net (start - stop) must be exactly one and start must have advanced from baseline by
        # at most one so we haven't double-created.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.start_count() - self.stop_count(), base_start - base_stop)
        self.assertLessEqual(self.start_count() - base_start, 1)

        # Cycle: step down, step up, several times. Each pair should advance start and stop by
        # exactly one, and the thread must remain joinable each cycle.
        prev_start = self.start_count()
        prev_stop = self.stop_count()
        for _ in range(5):
            self.conn.reconfigure('disaggregated=(role="follower")')
            self.assertEqual(self.stop_count(), prev_stop + 1)
            self.assertEqual(self.start_count(), prev_start)

            self.conn.reconfigure('disaggregated=(role="leader")')
            self.assertEqual(self.start_count(), prev_start + 1)
            self.assertEqual(self.stop_count(), prev_stop + 1)

            prev_start = self.start_count()
            prev_stop = self.stop_count()

        # Idempotent: staying in the same role twice must not advance the counters.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.start_count(), prev_start)
        self.assertEqual(self.stop_count(), prev_stop)

        self.conn.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.stop_count(), prev_stop + 1)
        self.assertEqual(self.start_count(), prev_start)
