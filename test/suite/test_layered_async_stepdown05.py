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
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown05.py
#    Arm validation and timestamp guards for a planned step-down.
@disagg_test_class
class test_layered_async_stepdown05(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:armcheck'

    # The cutoff cannot be re-armed while one is set.
    def test_double_arm_rejected(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(20)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.arm(30), '/step down timestamp is already set/')

    # Arming is only valid on a leader.
    def test_arm_on_follower_rejected(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.arm(20), '/can only be set on a disaggregated leader/')

    # The cutoff must sit at or ahead of all_durable; arming exactly at it is allowed.
    def test_arm_below_all_durable_rejected(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.arm(9), '/must not be older than all_durable timestamp/')
        self.arm(10)

    # While armed, stable may reach the cutoff exactly but never pass it.
    def test_stable_cannot_pass_armed_cutoff(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(20)

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(21)),
            '/must not advance past the armed step down timestamp/')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

    # Arming and advancing stable to the cutoff in one set_timestamp call takes full effect.
    def test_arm_and_stable_in_one_call(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20) +
                                ',step_down_timestamp=' + self.timestamp_str(20))

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.arm(30), '/step down timestamp is already set/')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25)),
            '/must not advance past the armed step down timestamp/')

    # A single call that arms and moves stable past the cutoff must be rejected.
    def test_arm_with_stable_overshoot_rejected(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25) +
                                            ',step_down_timestamp=' + self.timestamp_str(20)))

    # Arming below the current stable must be rejected for the same reason.
    def test_arm_below_stable_rejected(self):
        self.set_global_ts(1, 10)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.arm(5))

    # Arming does not change all_durable behavior: an in-flight txn still clamps it, and it
    # moves normally once that txn resolves and post-arm commits land.
    def test_arm_does_not_change_all_durable(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        # An in-flight transaction reserves commit timestamp 12; a later commit at 15 leaves a
        # hole, so all_durable is clamped just below the reservation.
        straddler = self.conn.open_session()
        scur = straddler.open_cursor(self.uri, None, None)
        straddler.begin_transaction()
        scur['held'] = 'v'
        straddler.timestamp_transaction('commit_timestamp=' + self.timestamp_str(12))
        self.write_at(self.uri, {'k2': 'v'}, 15)
        self.assertEqual(self.all_durable(), 11)

        self.arm(20)
        self.assertEqual(self.all_durable(), 11, 'arming must not move all_durable')

        # The straddler resolves and the hole closes.
        straddler.rollback_transaction()
        scur.close()
        straddler.close()
        self.assertEqual(self.all_durable(), 15)

        # A post-arm commit above the cutoff carries all_durable past it: drained.
        self.write_at(self.uri, {'k3': 'v'}, 25)
        self.assertEqual(self.all_durable(), 25)
