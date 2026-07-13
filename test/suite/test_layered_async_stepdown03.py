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

# test_layered_async_stepdown03.py
#    Rollback guards: straddlers roll back; arm validation and timestamp guards.
@disagg_test_class
class test_layered_async_stepdown03(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:guards'

    # Straddler rolls back on write; retry after the arm commits to ingest.
    def test_straddler_rollback_on_write(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Begin a transaction and write before the arm (this lands in stable).
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddle'] = 'before'

        self.arm(20)

        # The next write by the straddling transaction must roll back.       
        def straddle_write():
            cursor['straddle2'] = 'after'
        self.assert_step_down_rollback(straddle_write)
        self.session.rollback_transaction()
        cursor.close()

        # The server retries the transaction after the arm: it now routes cleanly to ingest.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddle'] = 'after'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'straddle'})
        self.assertEqual(self.read_keys_at(self.uri, 40), {'straddle'})

    # Straddler rollback applies to any write; remove rolls back like insert.
    def test_straddler_rollback_non_insert(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k1')

        # The server arms the step-down while this transaction is in flight.
        self.arm(20)

        self.assert_step_down_rollback(lambda: cursor.remove())
        self.session.rollback_transaction()
        cursor.close()

    # Txn writes pre-arm, arm fires, commit rolls back; retry lands in ingest.
    def test_arm_just_before_commit_rolls_back(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'                      

        self.arm(20)                             

        self.assert_step_down_rollback(
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30)))
        cursor.close()

        # The rolled-back write left nothing behind, in either constituent.
        self.assertEqual(self.read_kvs_at(self.uri, 40), {})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 40), set())

        # The retry runs after the arm and commits cleanly to ingest.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1'})

    # A straddler rolls back even when committing at or below the cutoff.
    def test_straddler_commit_below_cutoff_also_rolls_back(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'

        self.arm(20)

        self.assert_step_down_rollback(
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(15)))
        cursor.close()

    # A straddler that wrote only a plain table still rolls back at commit.
    def test_straddler_plain_table_commit_rolls_back(self):
        plain_uri = 'table:guards_plain'
        self.set_global_ts(1, 1)
        self.session.create(plain_uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(plain_uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'

        self.arm(20)

        self.assert_step_down_rollback(
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30)))
        cursor.close()

    # Transactions that begin after the arm and commit above the cutoff are in ingest.
    def test_post_arm_commits_above_cutoff_succeed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        for i, commit_ts in enumerate((30, 40, 50)):
            self.session.begin_transaction()
            cursor[f'post{i}'] = f'v{commit_ts}'
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 60), {'post0': 'v30', 'post1': 'v40', 'post2': 'v50'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 60), {'post0', 'post1', 'post2'})

    # Post-arm commits at or below the cutoff are rejected.
    def test_post_arm_commit_at_or_below_cutoff_rejected(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        for commit_ts in (15, 20):
            self.session.begin_transaction()
            cursor[f'k{commit_ts}'] = 'v'
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(
                    'commit_timestamp=' + self.timestamp_str(commit_ts)),
                '/must be after the step down timestamp/')
        cursor.close()

        # The rejected commits left nothing behind in either constituent.
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 25), set())
        self.assertEqual(self.read_kvs_at(self.uri, 25), {})

    # A read-only straddler commits normally; the guard only fires on writes.
    def test_readonly_straddler_commits_fine(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        # A read-only transaction begins before the arm and commits after it.
        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(rcur['k1'], 'v')
        self.arm(20)
        self.assertEqual(rcur['k1'], 'v')
        # Committing a read-only transaction must succeed (no WT_ROLLBACK).
        self.session.commit_transaction()
        rcur.close()

    # Shared prefix: begin a stable write, arm, advance stable to the cutoff, checkpoint.
    def stable_writer_through_checkpoint(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'
        self.arm(20)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        return cursor

    # A write after the step-down checkpoint, still armed, rolls back.
    def test_stable_writer_write_after_checkpoint_rolls_back(self):
        cursor = self.stable_writer_through_checkpoint()

        def straddle_write():
            cursor['k2'] = 'v'
        self.assert_step_down_rollback(straddle_write)
        self.session.rollback_transaction()
        cursor.close()

    # A commit after the step-down checkpoint, still armed, rolls back.
    def test_stable_writer_commit_after_checkpoint_rolls_back(self):
        cursor = self.stable_writer_through_checkpoint()

        self.assert_step_down_rollback(
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30)))
        cursor.close()

    # An ingest writer begun after the arm commits successfully across the demotion.
    def test_ingest_writer_survives_demotion(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'

        # The server completes the step-down under the in-flight ingest writer: stable advances
        # to the cutoff, the step-down checkpoint is taken (on another session, as this one has
        # the transaction open), the node reconfigures to follower.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.conn.reconfigure('disaggregated=(role="follower")')

        # The ingest writer commits as a follower; its content is in ingest and readable.
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1'})
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v'})

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
            lambda: self.arm(5), '/must not be older than all_durable timestamp/')
        self.arm(10)

    # While armed, stable may reach the cutoff exactly but never pass it.
    def test_stable_cannot_pass_armed_cutoff(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(20)

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25)),
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

    # FIXME-WT-17895: one call that arms and overshoots stable is accepted; pins current behavior.
    def test_arm_with_stable_overshoot_in_one_call(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25) +
                                ',step_down_timestamp=' + self.timestamp_str(20))

    # FIXME-WT-17895: arming below the current stable is accepted; pins current behavior.
    def test_arm_below_stable_accepted(self):
        self.set_global_ts(1, 10)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(5)

    # all_durable is held below the cutoff by an in-flight pre-arm txn, passes it once resolved.
    def test_all_durable_drain_signal_while_armed(self):
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

        # The straddler resolves (the server rolls it back and retries): the hole closes.
        straddler.rollback_transaction()
        scur.close()
        straddler.close()
        self.assertEqual(self.all_durable(), 15)

        # A post-arm commit above the cutoff carries all_durable past it: drained.
        self.write_at(self.uri, {'k3': 'v'}, 25)
        self.assertEqual(self.all_durable(), 25)

    # An untimestamped commit while armed succeeds and lands in ingest; pins current behavior.
    def test_untimestamped_commit_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'
        self.session.commit_transaction()
        cursor.close()

        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 25), {'k1'})
