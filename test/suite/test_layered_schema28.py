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
#
# test_layered_schema28.py
#    A database that stops using schema epochs falls back to the legacy behaviour, where every
#    schema operation is covered by the checkpoint that follows it. The application turns the
#    feature off by no longer setting the stable schema epoch, which only takes effect on a node
#    that restarts, because the epoch cannot move backwards in a live connection. These tests cover
#    running without the feature on a database that used to have it, and returning to it afterwards.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema28(
  LayeredStepdownMixin, wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    table_config = 'key_format=S,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # The schema epoch the database uses before the feature is turned off.
    epoch_before_off = 10

    def uri(self, name):
        """Return a distinct layered table URI within this test."""
        return f'layered:{self.test_name}_{name}'

    def last_checkpoint_epoch(self, conn=None):
        """Return the schema epoch recorded in the most recent checkpoint."""
        conn = conn or self.conn
        return int(conn.query_timestamp('get=last_disaggregated_schema_epoch'), 16)

    def stable_epoch(self, conn=None):
        """Return the live stable schema epoch."""
        conn = conn or self.conn
        return int(conn.query_timestamp('get=stable_disaggregated_schema_epoch'), 16)

    def seed_epoch_world(self, uri=None, rows=None):
        """Publish a table and leave a checkpoint at the pre-off epoch in shared storage."""
        uri = uri or self.uri('seeded')
        self.set_stable_epoch(1)
        self.session.create(uri, self.table_config)
        if rows:
            self.write_at(uri, rows, 5)
        self.publish(uri, self.epoch_before_off)
        self.set_stable_epoch(self.epoch_before_off)
        self.leader_checkpoint(self.epoch_before_off)
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)
        return uri

    def assert_follower_reads(self, uri, expected):
        """A fresh follower picking up the latest checkpoint reads the expected contents."""
        conn_follow, session_follow = self.open_follower()
        cursor = session_follow.open_cursor(uri)
        self.assertEqual({k: v for k, v in cursor}, expected)
        cursor.close()
        self.close_follower(conn_follow, session_follow)

    def assert_follower_absent(self, uri):
        """A fresh follower picking up the latest checkpoint must not see the table."""
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, uri))
        self.close_follower(conn_follow, session_follow)

    def restart_leader(self, epoch=None):
        """
        Restart the leader, optionally setting a stable schema epoch afterwards. Without one the
        node comes up with the feature off, as it would when the application stops setting it.
        """
        self.restart_without_local_files(config=self.conn_config_follower, step_up=True)
        if epoch is not None:
            self.set_stable_epoch(epoch)

    def test_off_period_legacy_operation(self):
        """
        A node that restarts without the feature records no schema epoch, which is how a legacy
        node behaves, and its schema operations are covered by the checkpoint that follows them
        with no publish call involved.
        """
        self.seed_epoch_world()
        self.restart_leader()

        # The restarted node still reports the epoch it picked up, but holds no live epoch.
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)
        self.assertEqual(self.stable_epoch(), 0)

        # Its checkpoints keep advertising the epoch the database reached, so a node that is
        # still using the feature can read them, but the node itself behaves as a legacy node.
        self.leader_checkpoint(20)
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)

        # A table created without the feature reaches shared metadata through the checkpoint alone.
        uri = self.uri('during_off')
        rows = {'k1': 'off', 'k2': 'off'}
        self.session.create(uri, self.table_config)
        self.write_at(uri, rows, 25)
        self.leader_checkpoint(30)
        self.assert_table_state(self.conn, uri, True, True, True)
        self.assert_follower_reads(uri, rows)

        # Dropping it without the feature removes it from shared metadata just as directly.
        self.session.drop(uri)
        self.leader_checkpoint(40)
        self.assert_follower_absent(uri)

    def test_off_period_role_transitions(self):
        """
        Two nodes running without the feature on a database that used to have it can swap roles,
        and a table created by one leader era survives into the next.
        """
        self.seed_epoch_world()
        self.restart_leader()
        self.leader_checkpoint(20)

        uri = self.uri('across_roles')
        rows = {'k1': 'across', 'k2': 'across'}
        self.session.create(uri, self.table_config)
        self.write_at(uri, rows, 25)
        self.leader_checkpoint(30)

        # The other node comes up without the feature too, then takes over.
        conn_follow, session_follow = self.open_follower()
        self.step_down()
        self.step_up(conn_follow)
        self.assertEqual(self.stable_epoch(conn_follow), 0)

        # The new leader checkpoints and the table it inherited is still published.
        self.leader_checkpoint(40, conn_follow, session_follow)
        self.assert_table_state(conn_follow, uri, True, True, True)

        # Fail back, and the table is still readable and still published.
        self.step_down(conn_follow)
        self.step_up()
        self.leader_checkpoint(50)
        self.assert_table_state(self.conn, uri, True, True, True)
        self.assertEqual(self.read_kvs_at(uri, 50), rows)

        self.close_follower(conn_follow, session_follow)

    def test_role_transitions_before_first_checkpoint(self):
        """
        Between an off restart and the node's first checkpoint, the last checkpoint the node picked
        up still carries an epoch while the node holds none. Role transitions in that window must
        behave like the legacy transitions they are.
        """
        self.seed_epoch_world()
        self.restart_leader()

        # No checkpoint yet, so the node still reports the epoch of the checkpoint it picked up.
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)

        # A create with no publish leaves an operation queued for the next checkpoint.
        uri = self.uri('in_window')
        self.session.create(uri, self.table_config)

        # Step down and back up with that operation still queued, checking the window is still
        # open at each transition rather than closed by a checkpoint taken along the way.
        self.step_down()
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)
        self.step_up()
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)

        # The table survives the transition and the next checkpoint publishes it with its rows.
        rows = {'k1': 'window'}
        self.write_at(uri, rows, 25)
        self.leader_checkpoint(30)
        self.assert_table_state(self.conn, uri, True, True, True)
        self.assert_follower_reads(uri, rows)

    def test_return_to_epoch_world(self):
        """
        A database that ran without the feature can take it up again at a higher epoch. Tables from
        the off period stay readable and the epoch machinery gates new schema operations again.
        """
        seeded = self.seed_epoch_world(rows={'k0': 'seeded'})
        self.restart_leader()
        self.leader_checkpoint(20)

        # A table created while the feature was off.
        off_uri = self.uri('from_off_period')
        off_rows = {'k1': 'off'}
        self.session.create(off_uri, self.table_config)
        self.write_at(off_uri, off_rows, 25)
        self.leader_checkpoint(30)

        # The feature comes back at an epoch above the one the database used before.
        self.restart_leader(epoch=20)
        self.assertEqual(self.stable_epoch(), 20)

        # Both older tables are still there and readable.
        self.assert_table_state(self.conn, seeded, True, True, True)
        self.assert_table_state(self.conn, off_uri, True, True, True)
        self.assertEqual(self.read_kvs_at(off_uri, 30), off_rows)

        # A new table is gated again: it stays out of shared metadata until a checkpoint covers
        # the epoch it was published at. The checkpoint stays below the write so the unpublished
        # table holds no stable data.
        new_uri = self.uri('after_return')
        new_rows = {'k1': 'returned'}
        self.session.create(new_uri, self.table_config)
        self.write_at(new_uri, new_rows, 45)
        self.publish(new_uri, 30)
        self.leader_checkpoint(40)
        self.assert_table_state(self.conn, new_uri, True, False, False)

        # A checkpoint at the covering epoch publishes it, and a follower reads it.
        self.set_stable_epoch(30)
        self.leader_checkpoint(50)
        self.assert_table_state(self.conn, new_uri, True, True, True)
        self.assert_follower_reads(new_uri, new_rows)

    def test_epoch_below_last_checkpoint_rejected(self):
        """
        A leader cannot start an era below an epoch the database already recorded, which would hand
        out epochs a previous era used. The checkpoint keeps carrying the epoch through the off
        period, so the floor survives the gap.
        """
        t1 = self.uri('before_off')
        self.set_stable_epoch(1)
        self.session.create(t1, self.table_config)
        self.write_at(t1, {'a': '1'}, 5)
        self.publish(t1, 40)
        self.set_stable_epoch(40)
        self.leader_checkpoint(10)
        self.assertEqual(self.last_checkpoint_epoch(), 40)

        # The off era holds the epoch rather than clearing it.
        self.restart_leader()
        self.leader_checkpoint(20)
        self.assertEqual(self.last_checkpoint_epoch(), 40)

        # Coming back below that epoch is refused.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.set_stable_epoch(30),
            '/must not be older than the schema epoch/')

        # Resuming at the recorded epoch or above is accepted, and gating works again.
        self.set_stable_epoch(40)
        t2 = self.uri('after_off')
        self.session.create(t2, self.table_config)
        self.publish(t2, 50)
        self.leader_checkpoint(30)
        self.assert_table_state(self.conn, t2, True, False, False)
        self.set_stable_epoch(50)
        self.leader_checkpoint(40)
        self.assert_table_state(self.conn, t2, True, True, True)

    def test_off_leader_checkpoint_read_by_epoch_follower(self):
        """
        A node still using the feature can pick up a checkpoint from a node that has turned it off,
        because the checkpoint still carries an epoch. Its own unpublished schema operations are
        left alone rather than discarded as they would be by a checkpoint carrying no epoch.
        """
        self.seed_epoch_world()

        conn_follow, session_follow = self.open_follower_epoch(self.epoch_before_off)

        # Create a table on the follower without publishing it. Its create stays in the queue.
        uri = self.uri('unpublished')
        session_follow.create(uri, self.table_config)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, uri))

        # Only the leader is turned off, and it checkpoints.
        self.restart_leader()
        self.leader_checkpoint(20)
        self.assertEqual(self.last_checkpoint_epoch(), self.epoch_before_off)

        self.disagg_advance_checkpoint(conn_follow)
        self.disagg_wait_for_adoption(conn_follow)

        # The follower kept its queued create: publishing it still builds the stable constituent.
        self.step_up(conn_follow)
        self.assertTrue(self.uri_stable_exists(conn_follow, uri))

        self.close_follower(conn_follow, session_follow)
