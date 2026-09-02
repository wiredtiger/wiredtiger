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

# A checkpoint pickup must not attach an old incarnation's stable constituent to a
# recreated layered table. A follower that applies a mirrored drop and recreate of the
# same URI holds a layered entry with no stable file, exactly like a fresh follower-side
# create. If it then picks up a checkpoint cut before the drop's REMOVE drained - the
# shared metadata still carries the previous incarnation - the pickup would adopt the old
# incarnation's stable file, marrying the dropped table's btree to the recreated table.
# Every later read of the stable constituent would return the old table's data, and the
# next pickup, carrying the new incarnation, halts on the btree ID change.
#
# The pickup must instead leave the stable constituent absent: the table's latest queued
# create sits above the checkpoint's schema epoch, so this checkpoint predates the local
# incarnation, and a later checkpoint that covers the create supplies the right constituent.

import os
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema28(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin, suite_subprocess):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_recreate_skips_stale_stable_pickup(self):
        """
        A pickup older than a local drop+recreate leaves the stable constituent absent
        instead of adopting the dropped incarnation's file.
        """
        # First incarnation: created and published on both nodes, checkpointed by the leader.
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 10)
        self.set_stable_epoch(10)
        self.leader_checkpoint(10)
        first_id = self.stable_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        self.set_stable_epoch(10, conn_follow)
        self.assertEqual(self.stable_id(conn_follow, self.uri), first_id)

        # Both nodes drop the table; the publishes sit above the stable epoch. The leader
        # cannot recreate yet: a checkpoint below its published drop with the recreate queued
        # behind it panics.
        self.session.drop(self.uri)
        self.publish(self.uri, 20)

        session_follow.drop(self.uri)
        self.publish(self.uri, 20, session_follow)
        session_follow.create(self.uri, self.table_config)
        self.publish(self.uri, 30, session_follow)
        self.assertFalse(self.uri_stable_exists(conn_follow, self.uri))

        # A checkpoint cut below the REMOVE's epoch: the shared metadata still carries the
        # first incarnation, while the follower's local state is already the second.
        self.set_stable_epoch(15)
        self.leader_checkpoint(20)
        self.disagg_advance_checkpoint(conn_follow)

        # The stale pickup must not resurrect the first incarnation's stable constituent.
        self.assertFalse(self.uri_stable_exists(conn_follow, self.uri))

        # The leader recreates only now: the next checkpoint covers the drop, so no checkpoint
        # below the drop's epoch ever meets the recreate queued behind it.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 30)

        # A row the recreated table carries into the next era.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'second'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(25))
        cursor.close()

        # A checkpoint covering the recreate supplies the right constituent.
        self.set_stable_epoch(30)
        self.leader_checkpoint(25)
        second_id = self.stable_id(self.conn, self.uri)
        self.assertNotEqual(second_id, first_id)

        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.stable_id(conn_follow, self.uri), second_id)

        # The follower reads the second incarnation's data through the adopted constituent.
        cursor = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor[1], 'second')
        cursor.close()

        self.close_follower(conn_follow, session_follow)

    def subprocess_stable_data_above_pre_drop_schema_epoch(self):
        """
        Build a checkpoint whose stable schema epoch predates a drop while its stable
        timestamp covers data written to the table recreated after that drop. The epoch
        selects the first incarnation while the timestamp makes the second incarnation's
        rows durable, so the checkpoint must not be written.
        """
        # First incarnation, published and checkpointed.
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 10)
        self.set_stable_epoch(10)
        self.leader_checkpoint(10)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'first'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(15))
        cursor.close()
        self.leader_checkpoint(15)

        # Drop and recreate at later epochs.
        self.dropUntilSuccess(self.session, self.uri)
        self.publish(self.uri, 20)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 30)

        # Stable data written only to the second incarnation, whose create is still unstable.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[2] = 'second'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(25))
        cursor.close()

        # A schema epoch below the drop paired with a stable timestamp above the second
        # incarnation's writes.
        self.set_stable_epoch(15)
        self.leader_checkpoint(25)

    def test_stable_data_above_pre_drop_schema_epoch(self):
        """
        A checkpoint may not pair a schema epoch from before a drop with a stable timestamp
        that covers the recreated table's data.
        """
        # The checkpoint cannot be rolled back once it has refused the unpublished stable
        # table, so WiredTiger panics rather than leaving the epoch and the timestamp
        # disagreeing on which incarnation is durable. A panic takes the connection down, so run
        # it apart.
        #
        # The subprocess works in its own home directory, so this connection stays open; it
        # only needs a stable timestamp for the closing checkpoint.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))

        subdir = 'SUBPROCESS_stable_data_above_pre_drop_schema_epoch'
        func = (f'{self.test_name}.{self.test_name}.'
                'subprocess_stable_data_above_pre_drop_schema_epoch')
        returncode, home = self.run_subprocess_function(subdir, func, silent=True,
            scenario=getattr(self, 'scenario_number', None))

        # A panic drops core only in a diagnostic build, so require an abnormal exit rather than a
        # signal.
        self.assertNotEqual(returncode, 0)

        with open(os.path.join(home, 'stderr.txt'), 'r') as f:
            err = f.read()
        self.assertIn('stable data checkpointed for unpublished table', err)
