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

import threading, time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered_stepdown_open_race.py
#    A layered cursor operation that starts on the leader and first opens its stable constituent
#    while a step-down is in flight completes under the follower role, unless its transaction
#    holds a snapshot from the leader role, which the follower cannot serve.
@disagg_test_class
class test_layered_stepdown_open_race(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    txn_modes = [
        ('autocommit', dict(read_ts=None, leader_snapshot=False)),
        ('read_timestamp', dict(read_ts=15, leader_snapshot=False)),
        ('leader_snapshot', dict(read_ts=None, leader_snapshot=True)),
    ]
    scenarios = make_scenarios(disagg_storages, txn_modes)

    uri = 'layered:test_layered_stepdown_open_race'
    uri_other = 'layered:test_layered_stepdown_open_race_other'

    def conn_stat(self, key):
        c = self.session.open_cursor('statistics:')
        val = c[key][2]
        c.close()
        return val

    def write(self, uri, ts):
        self.session.begin_transaction()
        c = self.session.open_cursor(uri)
        c['k'] = 'v'
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))

    def test_open_races_step_down(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))
        for uri in (self.uri, self.uri_other):
            self.session.create(uri, 'key_format=S,value_format=S')
            self.write(uri, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

        # Restart so the stable constituent's handle is closed and the next open is a fresh one.
        self.reopen_conn()

        cursor = self.session.open_cursor(self.uri)
        if self.read_ts is not None:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(self.read_ts))
        elif self.leader_snapshot:
            self.session.begin_transaction()
            other = self.session.open_cursor(self.uri_other)
            other.set_key('k')
            self.assertEqual(other.search(), 0)
            other.close()

        # The step-down sleeps holding the schema lock before it publishes the follower role.
        self.conn.reconfigure('timing_stress_for_test=[disagg_role_transition]')
        step_down = threading.Thread(
            target=lambda: self.conn.reconfigure('disaggregated=(role="follower")'))
        step_down.start()

        deadline = time.time() + 60
        while self.conn_stat(stat.conn.disagg_step_down_in_progress) == 0:
            self.assertLess(time.time(), deadline, 'the step-down never started')
            time.sleep(0.01)

        # The search reads the leader role, then its stable open waits for the step-down.
        cursor.set_key('k')
        if self.leader_snapshot:
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.search(),
                wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
            err, sub_level_err, _ = self.session.get_last_error()
            self.assertEqual(err, wiredtiger.WT_ROLLBACK)
            self.assertEqual(sub_level_err, wiredtiger.WT_NONE)
        else:
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'v')
        if self.read_ts is not None or self.leader_snapshot:
            self.session.rollback_transaction()

        step_down.join()
        self.conn.reconfigure('timing_stress_for_test=[]')
        self.assertGreater(self.conn_stat(stat.conn.layered_stable_live_open_refused), 0)
        self.assertEqual(self.conn_stat(stat.conn.layered_curs_open_stable_stepdown_race), 1)
        cursor.close()
