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

# Test disaggregated table publication across create, drop, and recreate sequences.
#
# A table's publish status is decided from its latest create/remove entry in the metadata queue.

import wiredtiger, wttest
from contextlib import closing
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema16(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def write_unstable_row(self):
        """Write a row at timestamp 10, above the stable timestamp used by these tests."""
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

    def test_recreate_above_stable_epoch_not_resurrected(self):
        """
        A table dropped at or below the stable schema epoch and recreated above it must not survive
        recovery. The recreate stays unpublished at that epoch and is absent after recovery.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)

        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 2)
        self.session.drop(self.uri)
        self.publish(self.uri, 3)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 9)
        self.write_unstable_row()

        # Checkpoint with the epoch below the recreate; the recreate stays unpublished.
        self.set_stable_epoch(5)
        self.leader_checkpoint(5)
        self.restart_without_local_files(step_up=True)

        # A recovered leader re-establishes its stable timestamp before it can checkpoint again.
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(5) +
            ',oldest_timestamp=' + self.timestamp_str(1))

        # After recovery, the drop is the latest visible operation; the table must be absent.
        # Assert neither constituent survived, so a partial resurrection cannot slip through.
        self.assertFalse(self.uri_stable_exists(self.conn, self.uri))
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))

    def test_checkpoint_skips_pending_recreate_update(self):
        """
        A checkpoint must apply only metadata-queue entries at or below its
        schema epoch. A drop/recreate queued above the epoch must leave the
        original table's checkpoint intact.
        """
        self.conn.set_timestamp(
            "stable_timestamp=" + self.timestamp_str(1)
            + ",oldest_timestamp=" + self.timestamp_str(1)
        )

        # Seed a row that fingerprints the original table; its survival after
        # recovery confirms we restored the right checkpoint.
        self.session.create(self.uri, self.table_config)
        with closing(self.session.open_cursor(self.uri)) as cursor:
            with self.transaction(commit_timestamp=2):
                cursor[1] = "durable"

        # Put the original table's checkpoint on shared storage; this is the
        # state recovery must return to.
        self.leader_checkpoint(3)

        # Queue a drop and recreate above the checkpoint epoch. The queue's
        # latest entry now describes the new table, which the checkpoint must
        # not adopt.
        self.session.drop(self.uri)
        self.session.create(self.uri, self.table_config)
        self.set_stable_epoch(3)
        self.publish(self.uri, 4)

        # This canary row exists only on the newly created table; recovery must
        # not find it. If it did, then the CREATE leaked into the checkpoint.
        with closing(self.session.open_cursor(self.uri)) as cursor:
            with self.transaction(commit_timestamp=5):
                cursor[2] = "future"

        # The checkpoint under test: the CREATE queued at epoch 4 is still
        # above this epoch and must be skipped.
        self.leader_checkpoint(3)

        # Recover from shared storage alone, since local files could mask a
        # wrong checkpoint.
        self.restart_without_local_files(step_up=True)
        self.conn.set_timestamp(
            "stable_timestamp="
            + self.timestamp_str(3)
            + ",oldest_timestamp="
            + self.timestamp_str(1)
        )

        # Key 1 must survive (original checkpoint restored).
        # Key 2 must be absent (the pending CREATE did not leak).
        with closing(self.session.open_cursor(self.uri)) as cursor:
            self.assertEqual(cursor[1], "durable")
            cursor.set_key(2)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

    def test_checkpoint_update_not_skipped_for_other_pending_create(self):
        """
        A pending CREATE gates only its own table's checkpoint UPDATE. A CREATE
        queued above the checkpoint epoch for a different table must not block
        the UPDATE of a published table.
        """
        other_uri = self.uri + "_other"

        self.conn.set_timestamp(
            "stable_timestamp=" + self.timestamp_str(1)
            + ",oldest_timestamp=" + self.timestamp_str(1)
        )

        # Seed a row that fingerprints the original checkpoint; its survival
        # after recovery confirms we restored the right checkpoint.
        self.session.create(self.uri, self.table_config)
        with closing(self.session.open_cursor(self.uri)) as cursor:
            with self.transaction(commit_timestamp=2):
                cursor[1] = "durable"

        # Put the original table's checkpoint on shared storage.
        self.leader_checkpoint(3)

        # Queue a CREATE for a different table above the checkpoint epoch. It
        # is the decoy: the UPDATE skip must match on table name, not merely
        # on the presence of any pending CREATE.
        self.session.create(other_uri, self.table_config)
        self.set_stable_epoch(3)
        self.publish(other_uri, 4)

        # A second stable row on the primary table; recovery must find it. If
        # it did not, the decoy CREATE blocked this table's UPDATE.
        with closing(self.session.open_cursor(self.uri)) as cursor:
            with self.transaction(commit_timestamp=5):
                cursor[2] = "durable"

        # The checkpoint under test: the decoy CREATE queued at epoch 4 is
        # above the schema epoch (3) and must not affect the primary table's
        # UPDATE.
        self.leader_checkpoint(5)

        # Recover from shared storage alone, since local files could mask a
        # skipped UPDATE.
        self.restart_without_local_files(step_up=True)
        self.conn.set_timestamp(
            "stable_timestamp="
            + self.timestamp_str(5)
            + ",oldest_timestamp="
            + self.timestamp_str(1)
        )

        # Both keys must survive: the decoy CREATE did not block the UPDATE.
        with closing(self.session.open_cursor(self.uri)) as cursor:
            self.assertEqual(cursor[1], "durable")
            self.assertEqual(cursor[2], "durable")

    def test_unpublished_table_holds_unstable_data(self):
        """
        An unpublished table may legitimately hold unstable data. The checkpoint skips it without
        raising a violation.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)

        # Epoch 9 is above the stable schema epoch, so the table stays unpublished.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 9)
        self.write_unstable_row()

        self.leader_checkpoint(5)

        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

    def test_create_then_drop_not_published(self):
        """
        A table created and dropped at or below the stable schema epoch is absent from shared
        metadata.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)

        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 2)
        self.session.drop(self.uri)
        self.publish(self.uri, 3)

        self.set_stable_epoch(5)
        self.leader_checkpoint(5)

        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

    def test_recreate_publishes_latest_create(self):
        """
        A table created, dropped, and recreated publishes on the latest create once the stable
        schema epoch reaches it. The stale earlier create does not interfere.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)

        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 2)
        self.session.drop(self.uri)
        self.publish(self.uri, 3)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 9)
        self.write_unstable_row()

        # The epoch and stable timestamp now cover the recreate, so it publishes.
        self.set_stable_epoch(9)
        self.leader_checkpoint(10)

        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))
