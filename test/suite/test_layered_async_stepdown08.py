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

# test_layered_async_stepdown08.py
#    Layered table state across the step-down transition: whether a table has a stable constituent,
#    whether that constituent was checkpointed, and whether the shared metadata advertises it. A
#    table created above the final checkpoint has a stable constituent the checkpoint never reached,
#    and the demoted node serves its rows from the frozen tree. Every test runs in both the
#    schema-epoch and the epoch-less world.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_async_stepdown08(
  LayeredStepdownMixin, wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__

    table_config = 'key_format=S,value_format=S'

    # Both worlds run with precise checkpoints, which disaggregated storage expects even from
    # clients that never publish. Only the schema epochs differ between the two worlds.
    base = 'statistics=(all),precise_checkpoint=true,'
    conn_config = base + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = base + 'disaggregated=(role="follower",lose_all_my_data=true)'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    worlds = [
      ('epoch', dict(use_epochs=True)),
      ('legacy', dict(use_epochs=False)),
    ]
    scenarios = make_scenarios(disagg_storages, worlds)

    def uri(self, name):
        return f'layered:{self.test_name}_{name}'

    # The stable timestamp of the final checkpoint every test splits its work on.
    final_ts = 5

    def setup_world(self):
        """Configure the stable schema epoch only in the epoch world."""
        if self.use_epochs:
            self.set_stable_epoch(10)
        self.set_global_ts(1, 1)

    def publish_if_epochs(self, uri, epoch):
        """Publish a create, which the epoch-less world has no notion of."""
        if self.use_epochs:
            self.publish(uri, epoch)

    def publish_and_make_stable(self, uri, epoch):
        """Publish a create and advance the stable schema epoch to it, so a checkpoint covers it."""
        if self.use_epochs:
            self.publish(uri, epoch)
            self.set_stable_epoch(epoch)

    def checkpoint_covering_epoch(self, epoch, stable_ts):
        """Take a checkpoint that covers everything published at or below the given epoch."""
        if self.use_epochs:
            self.set_stable_epoch(epoch)
        self.leader_checkpoint(stable_ts)

    def create_with_rows(self, name, commit_ts):
        """Create a table and write two rows, returning its URI and contents."""
        uri = self.uri(name)
        self.session.create(uri, self.table_config)
        rows = {'k1': name, 'k2': name}
        self.write_at(uri, rows, commit_ts)
        return uri, rows

    def assert_follower_reads(self, uri, expected):
        """A fresh follower picking up the latest checkpoint reads the expected contents."""
        conn_follow, session_follow = self.open_follower()
        cursor = session_follow.open_cursor(uri)
        self.assertEqual({k: v for k, v in cursor}, expected)
        cursor.close()
        self.close_follower(conn_follow, session_follow)

    def test_constituents_on_both_sides_of_the_checkpoint(self):
        """
        A table created below the final checkpoint and one created above it both have a stable
        constituent; only the first is checkpointed. The demotion changes neither, and both keep
        serving their rows.
        """
        self.setup_world()
        before, before_rows = self.create_with_rows('before', 2)
        self.publish_and_make_stable(before, 20)
        self.checkpoint_at(self.final_ts)

        after, after_rows = self.create_with_rows('after', 6)
        self.publish_if_epochs(after, 40)

        def assert_both_sides():
            self.assertTrue(self.stable_constituent_exists(self.conn, before))
            self.assertTrue(self.stable_is_checkpointed(self.conn, before))
            self.assertTrue(self.stable_constituent_exists(self.conn, after))
            self.assertFalse(self.stable_is_checkpointed(self.conn, after))
            self.assertEqual(self.read_kvs_at(before, 7), before_rows)
            self.assertEqual(self.read_kvs_at(after, 7), after_rows)

        assert_both_sides()
        self.demote()
        assert_both_sides()
        self.assertEqual(self.read_kvs(after), after_rows)

    def test_existing_table_window_writes(self):
        """Writes to an existing table above the final checkpoint, in both epoch modes."""
        self.setup_world()
        uri, rows = self.create_with_rows('existing', 2)
        self.publish_and_make_stable(uri, 20)
        self.checkpoint_at(self.final_ts)
        self.write_at(uri, {'window': 'window'}, 6)

        expected = {**rows, 'window': 'window'}
        self.assertEqual(self.read_kvs_at(uri, 7), expected)
        self.assertEqual(self.read_kvs_at(self.stable_uri(uri), 7), expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(uri), 7), {})

        self.demote()
        self.assertEqual(self.read_kvs_at(uri, 7), expected)
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(uri), 7), rows)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(uri), 7), {})

    def test_follower_serves_tables_after_step_down(self):
        """
        After the transition the node reads in its follower role: a covered table serves its rows,
        a table created above the final checkpoint serves them from the frozen tree, and a
        constituent the checkpoint reached empty reads empty.
        """
        self.setup_world()
        covered, covered_rows = self.create_with_rows('covered', 2)
        self.publish_and_make_stable(covered, 20)

        # A table the final checkpoint leaves empty, never published. The epoch world cannot hold
        # one across a step-down with data below the checkpoint, so it only exists without epochs.
        if not self.use_epochs:
            uncovered = self.uri('uncovered')
            self.session.create(uncovered, self.table_config)

        self.checkpoint_at(self.final_ts)
        window, window_rows = self.create_with_rows('window', 6)
        self.publish_if_epochs(window, 40)
        self.demote()

        self.assertEqual(self.read_kvs_at(covered, 7), covered_rows)
        self.assertEqual(self.read_kvs_at(window, 7), window_rows)
        if not self.use_epochs:
            self.assertEqual(self.read_kvs_at(uncovered, 7), {})

    def test_timestampless_step_down_keeps_constituents(self):
        """
        A step-down with no writes above the final checkpoint must keep constituents too: one
        covered by a checkpoint and one the checkpoint never reached.
        """
        self.setup_world()
        covered, rows = self.create_with_rows('covered', 2)
        self.publish_if_epochs(covered, 20)
        self.checkpoint_covering_epoch(20, 3)

        uncovered = self.uri('uncovered')
        self.session.create(uncovered, self.table_config)

        self.demote()

        self.assertTrue(self.stable_constituent_exists(self.conn, covered))
        self.assertTrue(self.stable_constituent_exists(self.conn, uncovered))
        self.assertEqual(self.read_kvs_at(covered, 4), rows)

    def test_window_create_publishes_after_step_up(self):
        """
        A table created above the final checkpoint stays unpublished across the demotion. Stepping
        back up on the same node with no pickup keeps its constituent, and a covering checkpoint
        publishes it with its rows.
        """
        self.setup_world()
        before, before_rows = self.create_with_rows('before', 2)
        self.publish_and_make_stable(before, 20)
        self.checkpoint_at(self.final_ts)

        after, after_rows = self.create_with_rows('after', 6)
        self.publish_if_epochs(after, 40)
        self.assert_table_state(self.conn, before, True, True, True)
        self.assert_table_state(self.conn, after, True, False, False)

        self.demote()
        self.assert_table_state(self.conn, before, True, True, True)
        self.assert_table_state(self.conn, after, True, False, False)

        self.promote()
        self.checkpoint_covering_epoch(40, 7)
        self.assert_table_state(self.conn, before, True, True, True)
        self.assert_table_state(self.conn, after, True, True, True)
        self.assert_follower_reads(before, before_rows)
        self.assert_follower_reads(after, after_rows)

    def test_pre_checkpoint_reader_sees_window_create_empty(self):
        """
        A read transaction that began before the final checkpoint opens a table created above it
        and reads it as empty; a later reader sees the rows, before and after the demotion.
        """
        self.setup_world()
        reader = self.conn.open_session('')
        reader.begin_transaction()

        self.checkpoint_at(self.final_ts)
        uri, rows = self.create_with_rows('window_reader', 6)

        cursor = reader.open_cursor(uri)
        cursor.set_key('k1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()
        reader.rollback_transaction()
        reader.close()

        self.assertEqual(self.read_kvs_at(uri, 7), rows)
        self.demote()
        self.assertEqual(self.read_kvs_at(uri, 7), rows)
