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
# test_prepare_discover09.py
#   Test that prepared updates on existing keys are resolved during standby step-up drain.
#   Unlike test_prepare_discover08 (prepared inserts on new keys), this test:
#   - Commits values at keys 1-3 at timestamp 60
#   - Prepares an update to those keys at prepare timestamp 100 (stable), with unstable
#     commit/durable timestamps
#   - After follower step-up and drain, verifies reads at different timestamps:
#     - At the original commit timestamp: original values visible
#     - At the prepared update's commit timestamp: updated values (commit) or original (rollback)
#   This exercises the history store restore path during drain resolution, since the keys
#   have prior committed values that must be preserved.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_prepare_discover09(wttest.WiredTigerTestCase):
    tablename = 'test_prepare_discover09'
    uri = 'layered:' + tablename

    resolve_scenarios = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    disagg_storages = gen_disagg_storages('test_prepare_discover09', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, resolve_scenarios)

    conn_base_config = 'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def test_prepare_update_stepup(self):

        # Phase 1: Insert committed data on the leader. These are the existing values that the
        # prepared transaction will update.
        self.pr("=== Phase 1: Insert committed data on leader ===")

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = "original_value_1"
        cursor[2] = "original_value_2"
        cursor[3] = "original_value_3"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))

        # Verify committed data.
        self.session.begin_transaction("read_timestamp=" + self.timestamp_str(60))
        self.assertEqual(cursor[1], "original_value_1")
        self.assertEqual(cursor[2], "original_value_2")
        self.assertEqual(cursor[3], "original_value_3")
        self.session.rollback_transaction()

        # Phase 2: Prepare an update to the same keys and checkpoint.
        # The prepared update will be written to the stable table because the prepare timestamp
        # is at or before the stable timestamp. The commit/durable timestamp has not happened yet,
        # so the checkpoint contains the unresolved prepared update.
        self.pr("=== Phase 2: Prepare update and checkpoint ===")

        self.session.begin_transaction()
        cursor[1] = "updated_value_1"
        cursor[2] = "updated_value_2"
        cursor[3] = "updated_value_3"

        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(100) +
                                       ',prepared_id=' + self.prepared_id_str(123))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))

        checkpoint_session = self.conn.open_session()
        checkpoint_session.checkpoint()
        checkpoint_session.close()

        # Phase 3: Open a follower and pick up the checkpoint.
        # The follower's stable table now contains the unresolved prepared update on top of the
        # original committed values. The original values should be in the history store.
        self.pr("=== Phase 3: Open follower and pick up checkpoint ===")

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')

        # Verify committed data is visible on the follower at the original commit timestamp.
        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        self.assertEqual(cursor_follow[1], "original_value_1")
        self.assertEqual(cursor_follow[2], "original_value_2")
        self.assertEqual(cursor_follow[3], "original_value_3")
        cursor_follow.close()

        # Phase 4: Discover and resolve the prepared transaction on the follower.
        # The follower finds the prepared transaction via the prepared_discover cursor, claims it,
        # and commits or rolls back. The resolution goes to the ingest table, while the
        # unresolved prepared cell remains on the stable table.
        self.pr("=== Phase 4: Discover and resolve prepared transaction ===")

        discover_cursor = session_follow.open_cursor("prepared_discover:")
        discover_session = conn_follow.open_session()
        count = 0
        found_prepared_id = None

        while discover_cursor.next() == 0:
            count += 1
            prepared_id = discover_cursor.get_key()
            self.assertEqual(prepared_id, 123)
            found_prepared_id = prepared_id
            discover_session.begin_transaction(
                "claim_prepared_id=" + self.prepared_id_str(prepared_id))
            break

        self.assertEqual(count, 1)
        self.assertEqual(found_prepared_id, 123)
        discover_cursor.close()

        if self.commit:
            discover_session.commit_transaction(
                "commit_timestamp=" + self.timestamp_str(200) +
                ",durable_timestamp=" + self.timestamp_str(210))
        else:
            discover_session.rollback_transaction(
                "rollback_timestamp=" + self.timestamp_str(210))
        discover_session.close()

        # Phase 4b: Write a newer update on the follower for the same keys. This goes to the
        # ingest table, creating a chain where the newer update is at the head and the resolved
        # prepare is at the tail. This exercises the case where the ingest chain has multiple
        # updates and the resolved prepare is not the head of the chain.
        self.pr("=== Phase 4b: Write newer update on follower ===")

        write_session = conn_follow.open_session()
        write_cursor = write_session.open_cursor(self.uri)
        write_session.begin_transaction()
        write_cursor[1] = "newer_value_1"
        write_cursor[2] = "newer_value_2"
        write_cursor[3] = "newer_value_3"
        write_session.commit_transaction("commit_timestamp=" + self.timestamp_str(220))
        write_cursor.close()
        write_session.close()

        # Clean up the leader's prepared transaction so it doesn't block shutdown.
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(200) +
                                       ",durable_timestamp=" + self.timestamp_str(210))
        cursor.close()
        session_follow.close()

        # Close the old leader connection.
        self.conn.close("debug=(skip_checkpoint=true)")

        # Phase 5: Step up to leader. This triggers drain (ingest -> stable).
        # The drain moves ingest updates to the stable table via __layered_move_updates.
        # The stable table still has the unresolved prepared cell from Phase 2's checkpoint;
        # the ingest table has the resolution from Phase 4. The drain must resolve the prepared
        # cell on stable and restore the history store entry for the original committed value.
        self.pr("=== Phase 5: Step up to leader (triggers drain) ===")
        conn_follow.reconfigure('disaggregated=(role="leader")')
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        ckpt_session = conn_follow.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

        # Phase 6: Verify data correctness after step-up + drain + checkpoint.
        # Read at the original commit timestamp to confirm the old values are preserved.
        # Read at the prepared update's commit timestamp to confirm commit/rollback outcome.
        self.pr("=== Phase 6: Verify data after step-up ===")

        read_session = conn_follow.open_session()
        read_cursor = read_session.open_cursor(self.uri)

        # At the original commit timestamp (60), the original values must be visible regardless
        # of whether the prepared update was committed or rolled back.
        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(60))
        self.assertEqual(read_cursor[1], "original_value_1")
        self.assertEqual(read_cursor[2], "original_value_2")
        self.assertEqual(read_cursor[3], "original_value_3")
        read_session.rollback_transaction()

        # At the prepared update's commit timestamp (200), the outcome depends on resolution.
        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(200))
        if self.commit:
            # The updated values should be visible.
            self.assertEqual(read_cursor[1], "updated_value_1")
            self.assertEqual(read_cursor[2], "updated_value_2")
            self.assertEqual(read_cursor[3], "updated_value_3")
        else:
            # Rollback: the original committed values should be restored.
            self.assertEqual(read_cursor[1], "original_value_1")
            self.assertEqual(read_cursor[2], "original_value_2")
            self.assertEqual(read_cursor[3], "original_value_3")
        read_session.rollback_transaction()

        # At the newer update's commit timestamp (220), the newer values must be visible
        # regardless of whether the prepared update was committed or rolled back.
        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(220))
        self.assertEqual(read_cursor[1], "newer_value_1")
        self.assertEqual(read_cursor[2], "newer_value_2")
        self.assertEqual(read_cursor[3], "newer_value_3")
        read_session.rollback_transaction()

        read_cursor.close()
        read_session.close()
