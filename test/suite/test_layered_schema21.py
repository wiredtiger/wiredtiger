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

# Dropping a published but uncheckpointed layered table would discard the only copy of its
# committed data, since the published CREATE obligates a covering checkpoint to include that
# data. The drop must be refused until a checkpoint publishes the table. The same holds for
# ingest content a checkpoint has not covered, on a follower or in a leader's step-down window.

import errno
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema21(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,cache_cursors=false,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'
    nrows = 100

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    #
    # Helper methods
    #

    def create_and_publish_table(self):
        """Create and publish a table under a stable schema epoch so it awaits publication."""
        # Precise checkpoint requires a stable timestamp at connection close.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 10)

    def write_rows(self, commit_ts=None, session=None, keys=None):
        """Write nrows to the table, then commit at commit_ts or roll back if it is None."""
        if session is None:
            session = self.session
        if keys is None:
            keys = range(1, self.nrows + 1)
        session.begin_transaction()
        cursor = session.open_cursor(self.uri)
        for i in keys:
            cursor[i] = 'value'
        cursor.close()
        if commit_ts is None:
            session.rollback_transaction()
        else:
            session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))

    def assert_all_rows(self, session):
        """Assert that every written row is readable through the given session."""
        cursor = session.open_cursor(self.uri)
        count = 0
        while cursor.next() == 0:
            self.assertEqual(cursor.get_value(), 'value')
            count += 1
        cursor.close()
        self.assertEqual(count, self.nrows)

    def assert_drop_refused(self, expect='unpublished data', sub_err=None, session=None,
            config=None):
        """Assert that dropping the table is refused with EBUSY and the given sub-error."""
        if sub_err is None:
            sub_err = wiredtiger.WT_DIRTY_DATA
        if session is None:
            session = self.session
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: session.drop(self.uri, config))
        err, sub, msg = session.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, sub_err)
        self.assertTrue(expect in msg)

    def assert_drop_refused_uncovered(self, session, config=None):
        """Assert the drop is refused because no checkpoint covers the ingest content."""
        self.assert_drop_refused('no checkpoint covers', None, session, config)

    def truncate_all_rows(self, commit_ts):
        """Truncate the whole key range at commit_ts."""
        c_start = self.session.open_cursor(self.uri)
        c_start.set_key(1)
        c_stop = self.session.open_cursor(self.uri)
        c_stop.set_key(self.nrows)
        self.session.begin_transaction()
        self.session.truncate(None, c_start, c_stop, None)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        c_start.close()
        c_stop.close()

    def assert_no_rows(self, session):
        """Assert that the table reads back empty through the given session."""
        cursor = session.open_cursor(self.uri)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def assert_drop_succeeds(self):
        """Drop the table and confirm it is gone from local metadata."""
        self.session.drop(self.uri)
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))

    def count_rows(self, session):
        """Return the number of rows readable through the given session."""
        cursor = session.open_cursor(self.uri)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        return count

    def publish_and_checkpoint_table(self):
        """Create, publish and checkpoint the table on the leader."""
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)
        self.set_stable_epoch(10)
        self.leader_checkpoint(1)

    def publish_checkpoint_and_open_follower(self):
        """Publish and checkpoint the table on the leader, then open a follower holding it."""
        self.publish_and_checkpoint_table()
        return self.open_follower()

    def leader_write_and_checkpoint(self, commit_ts, keys=None):
        """Write the same rows on the leader and checkpoint them, covering the follower's copy."""
        self.write_rows(commit_ts=commit_ts, keys=keys)
        self.leader_checkpoint(commit_ts)

    def set_follower_stable(self, conn, stable_ts):
        """Advance a follower's stable timestamp without delivering a checkpoint to it."""
        conn.set_timestamp('stable_timestamp=' + self.timestamp_str(stable_ts))

    def assert_follower_drop_succeeds(self, conn, session):
        """Drop the table on a follower and confirm its ingest constituent is gone."""
        session.drop(self.uri)
        self.assertFalse(self.uri_in_local_metadata(conn, self.uri))

    #
    # Drop lifecycle tests
    #

    def test_drop_with_committed_data_is_refused(self):
        # A table that awaits publication and holds committed data keeps its only copy in memory,
        # so the drop is refused until a checkpoint publishes it.
        self.create_and_publish_table()
        self.write_rows(commit_ts=3)
        self.assert_drop_refused()

        # The refused drop left no partial state, so the committed rows remain readable.
        self.assert_all_rows(self.session)

        # A checkpoint publishes and persists the table with no data loss: the rows reach a
        # follower, and the published table then drops normally.
        self.set_stable_epoch(10)
        self.leader_checkpoint(3)
        conn_follower, session_follower = self.open_follower()
        self.assert_all_rows(session_follower)
        session_follower.close()
        conn_follower.close()
        self.assert_drop_succeeds()

    def test_checkpoint_between_create_and_drop_epochs_keeps_data(self):
        # Create and publish the table at schema epoch 5, then write committed data at timestamp 10.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)
        self.write_rows(commit_ts=10)

        # A later checkpoint at schema epoch 6 covers the create but not a drop published at epoch
        # 7, so it must include the table with the timestamp-10 writes. Dropping the table now would
        # discard those writes when the handle closes, so the drop is refused first.
        self.assert_drop_refused()

        # The checkpoint at schema epoch 6 and timestamp 11 includes the table and its data.
        self.set_stable_epoch(6)
        self.leader_checkpoint(11)
        conn_follower, session_follower = self.open_follower()
        self.assert_all_rows(session_follower)
        session_follower.close()
        conn_follower.close()

    def test_drop_published_checkpointed_with_dirty_data(self):
        # Create and publish at epoch 5.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)

        # Write and checkpoint at ts 10, epoch 6. Epoch 6 > publish epoch 5, so
        # the checkpoint covers the create and clears AWAITS_PUBLISH.
        self.write_rows(commit_ts=10)
        self.set_stable_epoch(6)
        self.leader_checkpoint(10)

        # Write new data at ts 11 -- uncheckpointed. AWAITS_PUBLISH is now clear.
        self.write_rows(commit_ts=11)

        # Drop at epoch 7. The published table no longer awaits publication, so the
        # unpublished-data guard does not apply. The drop is still refused with EBUSY
        # because closing the dirty ingest file fails, protecting the ts-11 data
        # through the same path a regular table uses.
        self.set_stable_epoch(7)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.drop(self.uri))
        err, sub, msg = self.session.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, wiredtiger.WT_DIRTY_DATA)
        self.assertTrue('dirty data' in msg)

        # Checkpoint to quiesce dirty state so teardown can close cleanly.
        self.set_stable_epoch(8)
        self.leader_checkpoint(11)

    def test_drop_published_with_uncheckpointed_stable_data(self):
        # Create and publish at epoch 5.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)

        # Write and checkpoint at ts 10, epoch 6. Epoch 6 > publish epoch 5, so
        # the checkpoint covers the create and clears AWAITS_PUBLISH.
        self.write_rows(commit_ts=10)
        self.set_stable_epoch(6)
        self.leader_checkpoint(10)

        # Write new data at ts 11 and set stable ts to 11 -- stable, but uncheckpointed data.
        self.write_rows(commit_ts=11)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(11))

        # The drop must be refused: Deferring a drop to a later checkpoint is only safe when the
        # table is fully checkpointed. A checkpoint taken before the drop's epoch becomes stable
        # would still owe the writes at ts 11, which no existing checkpoint holds and a dropped
        # table can no longer produce.
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.drop(self.uri))
        err, sub, msg = self.session.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, wiredtiger.WT_DIRTY_DATA)
        self.assertTrue('dirty data' in msg)

        # Checkpoint to quiesce dirty state so teardown can close cleanly.
        self.set_stable_epoch(8)
        self.leader_checkpoint(12)

    def test_drop_with_drained_data_is_refused(self):
        # The step-up drain moves follower-era ingest rows into a stable constituent created
        # awaiting publication, so until a checkpoint publishes the table those rows exist only in
        # its cache and the drop must be refused, exactly as when the rows were committed directly.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.step_down()

        # A follower-era create and insert: the rows live only in the ingest constituent.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)
        self.write_rows(commit_ts=3)

        # Step up: the missing stable constituent is created awaiting publication and the drain
        # moves the rows into it.
        self.step_up()
        self.assert_drop_refused()

        # The refused drop left no partial state, so the committed rows remain readable.
        self.assert_all_rows(self.session)

        # A checkpoint publishes and persists the table with no data loss: the rows reach a
        # follower, and the published table then drops normally.
        self.set_stable_epoch(10)
        self.leader_checkpoint(3)
        conn_follower, session_follower = self.open_follower()
        self.assert_all_rows(session_follower)
        session_follower.close()
        conn_follower.close()
        self.assert_drop_succeeds()

    def test_drop_empty_is_allowed(self):
        # An awaiting-publication table with no data is transient: nothing obligates a checkpoint,
        # so the drop is allowed.
        self.create_and_publish_table()
        self.assert_drop_succeeds()

    def test_drop_rolled_back_data_is_allowed(self):
        # Rolled-back writes leave no committed durable data, so the drop is allowed.
        self.create_and_publish_table()
        self.write_rows()
        self.assert_drop_succeeds()

    def test_drop_after_checkpoint_is_allowed(self):
        # Once a checkpoint publishes the table, its data is durable and the drop is a normal drop.
        self.create_and_publish_table()
        self.write_rows(commit_ts=3)
        self.set_stable_epoch(10)
        self.leader_checkpoint(3)
        self.assert_drop_succeeds()

    def test_drop_with_replayed_truncate_is_refused(self):
        # A follower-era truncate reaches the stable constituent through the step-up drain rather
        # than the commit path. The deletion it carries lives only in the awaiting-publication
        # table until a checkpoint publishes it, so the drop must be refused meanwhile and the
        # truncate must survive the publish.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)
        self.step_down()

        # A follower-era create, insert and truncate: everything lives in the ingest constituent
        # and the truncate is held in the follower truncate list.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 5)
        self.write_rows(commit_ts=3)
        self.truncate_all_rows(commit_ts=5)

        # Step up: the missing stable constituent is created awaiting publication and the drain
        # moves the truncated rows into it, interleaved with the recorded truncate.
        self.step_up()
        self.assert_drop_refused()

        # The refused drop left no partial state, so the truncate is still in effect.
        self.assert_no_rows(self.session)

        # A checkpoint publishes and persists the table: a follower sees the truncated table
        # rather than the pre-truncate rows, and the published table then drops normally.
        self.set_stable_epoch(10)
        self.leader_checkpoint(6)
        conn_follower, session_follower = self.open_follower()
        self.assert_no_rows(session_follower)
        session_follower.close()
        conn_follower.close()
        self.assert_drop_succeeds()

    #
    # Follower drop tests
    #
    # A follower's writes land in its in-memory ingest constituent, so the drop is bounded by the
    # checkpoint the follower has picked up.
    #

    def test_drop_on_follower_with_uncheckpointed_data_is_refused(self):
        # The follower's rows exist only in its ingest constituent until a checkpoint covers them.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        self.assert_drop_refused_uncovered(session_follower)

        self.assert_all_rows(session_follower)
        self.assertTrue(self.uri_in_local_metadata(conn_follower, self.uri))

        # A picked-up checkpoint covering the same rows makes it a normal drop.
        self.leader_write_and_checkpoint(3)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_follower_before_pickup_is_refused(self):
        # The bound is the checkpoint the follower picked up, not the newest the leader took.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        self.leader_write_and_checkpoint(3)
        self.assert_drop_refused_uncovered(session_follower)

        # The pickup is what makes the difference.
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_follower_with_partially_covered_data_is_refused(self):
        # Partial coverage leaves the rest as the only copy, so it must not unblock the drop.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        early_keys = range(1, self.nrows // 2 + 1)
        late_keys = range(self.nrows // 2 + 1, self.nrows + 1)
        self.write_rows(commit_ts=3, session=session_follower, keys=early_keys)
        self.write_rows(commit_ts=7, session=session_follower, keys=late_keys)

        # The picked-up checkpoint covers the timestamp-3 rows but not the timestamp-7 rows.
        self.leader_write_and_checkpoint(3, keys=early_keys)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_drop_refused_uncovered(session_follower)
        self.assertEqual(self.count_rows(session_follower), self.nrows)

        # Covering the timestamp-7 rows as well releases the drop.
        self.leader_write_and_checkpoint(7, keys=late_keys)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_empty_on_follower_is_allowed(self):
        # A follower that never wrote holds nothing to protect.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_rolled_back_on_follower_is_allowed(self):
        # Rolled-back writes never reach a durable timestamp, so they leave nothing to protect.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=None, session=session_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_force_on_follower_with_uncheckpointed_data_is_refused(self):
        # force only turns a missing table into a success; it does not weaken the guard.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        self.assert_drop_refused_uncovered(session_follower, 'force=true')
        self.assert_all_rows(session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_leader_in_step_down_window_is_refused(self):
        # Past the step-down boundary a leader's commits land in the ingest constituent, which the
        # final step-down checkpoint cannot cover, so the same guard refuses the drop.
        self.publish_and_checkpoint_table()

        self.conn.set_timestamp('step_down_timestamp=' + self.timestamp_str(20) +
            ',step_down_disaggregated_schema_epoch=' + self.timestamp_str(10))
        self.write_rows(commit_ts=30)
        self.assert_drop_refused_uncovered(self.session)

        # The window writes stay readable. Layered reads need an explicit snapshot while the
        # step-down boundary is set.
        self.session.begin_transaction()
        self.assert_all_rows(self.session)
        self.session.rollback_transaction()
        # The stable constituent is dropped before the ingest one, so its metadata removal must
        # have unrolled: both constituents are back.
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri, leader=True))

    def test_drop_on_follower_of_leader_only_data_is_allowed(self):
        # Rows the follower only reads live in the picked-up checkpoint, not in its ingest tree.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.leader_write_and_checkpoint(3)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_all_rows(session_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_ingest_constituent_directly_is_refused(self):
        # The guard lives in the close path, so dropping the ingest constituent URI directly hits
        # it too, not only a layered drop.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        ingest_uri = 'file:' + self.test_name + '.wt_ingest'
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: session_follower.drop(ingest_uri))
        err, sub, msg = session_follower.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, wiredtiger.WT_DIRTY_DATA)
        self.assertTrue('no checkpoint covers' in msg)
        self.close_follower(conn_follower, session_follower)

    def test_drop_waits_for_commit_then_requires_coverage(self):
        # The two guards apply in order: an uncommitted write is refused as uncommitted data, and
        # once committed it is refused as uncovered until a checkpoint accounts for it.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        session_a = conn_follower.open_session('')
        session_a.begin_transaction()
        cursor = session_a.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            cursor[i] = 'value'
        cursor.close()

        # The uncommitted transaction blocks the drop even though its cursor is closed.
        self.assert_drop_refused('uncommitted data', wiredtiger.WT_UNCOMMITTED_DATA,
            session_follower)

        session_a.commit_transaction('commit_timestamp=' + self.timestamp_str(3))
        session_a.close()
        self.assert_drop_refused_uncovered(session_follower)
        self.assert_all_rows(session_follower)

        # A covering picked-up checkpoint releases the drop.
        self.leader_write_and_checkpoint(3)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    #
    # Follower drop tests where only the stable timestamp advances
    #
    # The stable timestamp moves independently of checkpoint pickup, and must not shift the bound
    # in either direction.
    #

    def test_drop_on_follower_after_stable_timestamp_only_is_refused(self):
        # A stable timestamp past the writes says no checkpoint holds them, so it cannot unblock.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        self.set_follower_stable(conn_follower, 100)
        self.assert_drop_refused_uncovered(session_follower)

        self.assert_all_rows(session_follower)
        self.assertTrue(self.uri_in_local_metadata(conn_follower, self.uri))

        # The pickup still releases the drop, though the stable timestamp is well past the writes.
        self.leader_write_and_checkpoint(3)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_follower_after_stable_timestamp_past_leader_checkpoint_is_refused(self):
        # A covering checkpoint plus a caught-up stable timestamp must not substitute for pickup.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.write_rows(commit_ts=3, session=session_follower)
        self.leader_write_and_checkpoint(3)
        self.set_follower_stable(conn_follower, 3)
        self.assert_drop_refused_uncovered(session_follower)

        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_follower_after_stable_timestamp_with_no_data_is_allowed(self):
        # The reverse direction: a stable timestamp on an empty follower must not refuse the drop.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.set_follower_stable(conn_follower, 100)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)

    def test_drop_on_follower_after_stable_timestamp_with_leader_only_data_is_allowed(self):
        # Rows from a picked-up checkpoint stay covered when the stable timestamp passes them.
        conn_follower, session_follower = self.publish_checkpoint_and_open_follower()
        self.leader_write_and_checkpoint(3)
        self.disagg_advance_checkpoint_and_wait(conn_follower)
        self.set_follower_stable(conn_follower, 100)
        self.assert_all_rows(session_follower)
        self.assert_follower_drop_succeeds(conn_follower, session_follower)
        self.close_follower(conn_follower, session_follower)
