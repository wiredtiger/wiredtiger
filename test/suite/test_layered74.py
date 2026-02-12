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
# test_layered74.py
#   Test WT-15795: Layered cursor key preservation bug with prepare conflicts
#   and state changes in disaggregated follower mode.
#
#   This test reproduces the bug where cursor key flags are corrupted when:
#   1. A prepare conflict occurs during search_near
#   2. A checkpoint or state change happens before retry
#   3. __clayered_adjust_state reopens cursors and corrupts parent cursor flags
#   4. Retry fails with "requires key be set" error

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered74(wttest.WiredTigerTestCase):
    tablename = 'test_layered74'
    uri = 'layered:' + tablename

    resolve_scenarios = [
        ('commit', dict(commit = True)),
        ('rollback', dict(commit = False)),
    ]

    disagg_storages = gen_disagg_storages('test_layered74', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, resolve_scenarios)

    conn_base_config = 'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="follower")'

    def setup_table_with_data(self, keys):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        for key in keys:
            cursor[key] = f"value_{key}"
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        cursor.close()

    def prepare_key_in_separate_session(self, key, value, prepare_ts=50):
        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)

        prepare_session.begin_transaction()
        prepare_cursor[key] = value
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(1))

        return prepare_session, prepare_cursor

    def force_checkpoint_and_state_change(self, stable_ts=None):
        """
        Force a checkpoint to trigger state change in follower mode.
        This simulates a new checkpoint arriving from the leader, which causes
        __clayered_adjust_state to reopen constituent cursors.
        """
        # Create a checkpoint with a new timestamp
        # Use provided stable_ts or default to 100
        if stable_ts is None:
            stable_ts = 100
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(stable_ts))
        self.session.checkpoint()

    def test_search_near_with_checkpoint_between_retries(self):
        """
        WT-15795: Test that reproduces the key preservation bug.
        
        This test triggers the bug by:
        1. Setting up a prepare conflict scenario
        2. Calling search_near which returns WT_PREPARE_CONFLICT
        3. Forcing a checkpoint (state change) before retry
        4. Retrying search_near - this SHOULD work but currently FAILS
           with "requires key be set" error
        """
        # Setup: keys 1, 3, 5 committed
        self.setup_table_with_data([1, 3, 5])

        # Prepare a transaction on key 2 (between 1 and 3)
        prepare_session, prepare_cursor = self.prepare_key_in_separate_session(2, "prepared_value")

        # Open cursor and set key to search for prepared key
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        cursor.set_key(2)
        
        # First call: search_near returns WT_PREPARE_CONFLICT
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.search_near())
        
        # Verify key is still accessible after prepare conflict
        retrieved_key = cursor.get_key()
        self.assertEqual(retrieved_key, 2, "Key should be preserved after WT_PREPARE_CONFLICT")

        # Force a checkpoint to trigger state change and reopen cursors
        # Use stable_ts=30 which is before the prepare timestamp (50)
        # This simulates a new checkpoint arriving while the prepare is still active
        self.force_checkpoint_and_state_change(stable_ts=30)

        # BUG: After checkpoint, cursor key should still be preserved
        # but currently fails with "requires key be set"
        retrieved_key = cursor.get_key()
        self.assertEqual(retrieved_key, 2, "Key should be preserved after checkpoint")

        # Resolve the prepared transaction
        if self.commit:
            prepare_session.breakpoint()
            # Advance stable to allow commit
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(60))
            prepare_session.commit_transaction('commit_timestamp=' + self.timestamp_str(60)+',durable_timestamp='+self.timestamp_str(60))
            # After commit, retry should find the committed value
            self.assertEqual(cursor.search_near(), 0)
            self.assertEqual(cursor.get_key(), 2)
            self.assertEqual(cursor.get_value(), "prepared_value")
        else:
            prepare_session.rollback_transaction()
            # After rollback, retry should find the next key
            self.assertEqual(cursor.search_near(), 1)
            self.assertEqual(cursor.get_key(), 3)
            self.assertEqual(cursor.get_value(), "value_3")

        prepare_cursor.close()

    # def test_search_near_with_snapshot_change_between_retries(self):
    #     """
    #     WT-15795: Alternative test that triggers the bug via snapshot generation change.

    #     This test triggers the bug by:
    #     1. Setting up a prepare conflict scenario
    #     2. Calling search_near which returns WT_PREPARE_CONFLICT
    #     3. Committing the transaction to change snapshot generation
    #     4. Starting a new transaction (new snapshot)
    #     5. Retrying search_near - this SHOULD work but currently FAILS
    #     """
    #     # Setup: keys 1, 3, 5 committed
    #     self.setup_table_with_data([1, 3, 5])

    #     # Prepare a transaction on key 2
    #     prepare_session, prepare_cursor = self.prepare_key_in_separate_session(2, "prepared_value")

    #     # Open cursor and set key
    #     cursor = self.session.open_cursor(self.uri)
    #     self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

    #     cursor.set_key(2)

    #     # First call: returns WT_PREPARE_CONFLICT
    #     self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.search_near())

    #     # Verify key is preserved
    #     retrieved_key = cursor.get_key()
    #     self.assertEqual(retrieved_key, 2)

    #     # End transaction and start a new one - this changes snapshot generation
    #     self.session.commit_transaction()

    #     # Force a checkpoint to ensure state change
    #     self.force_checkpoint_and_state_change()

    #     # Start new transaction
    #     self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

    #     # Set key again for retry
    #     cursor.set_key(2)

    #     # Resolve the prepare conflict
    #     if self.commit:
    #         prepare_session.breakpoint()
    #         prepare_session.commit_transaction('commit_timestamp=' + self.timestamp_str(60)+',durable_timestamp='+self.timestamp_str(60))
    #         # Retry should work
    #         self.assertEqual(cursor.search_near(), 0)
    #         self.assertEqual(cursor.get_key(), 2)
    #         self.assertEqual(cursor.get_value(), "prepared_value")
    #     else:
    #         prepare_session.rollback_transaction()
    #         self.assertEqual(cursor.search_near(), 1)
    #         self.assertEqual(cursor.get_key(), 3)
    #         self.assertEqual(cursor.get_value(), "value_3")

    #     prepare_cursor.close()

    # def test_next_with_checkpoint_between_retries(self):
    #     """
    #     WT-15795: Test next() operation with checkpoint between prepare conflict and retry.
    #     """
    #     # Setup: keys 1, 3, 5 committed
    #     self.setup_table_with_data([1, 3, 5])

    #     # Prepare a transaction on key 2 (between 1 and 3)
    #     prepare_session, prepare_cursor = self.prepare_key_in_separate_session(2, "prepared_value")

    #     # Open cursor and position at key 1
    #     cursor = self.session.open_cursor(self.uri)
    #     self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

    #     cursor.set_key(1)
    #     self.assertEqual(cursor.search(), 0)
    #     self.assertEqual(cursor.get_key(), 1)

    #     # next() should encounter prepared key 2 and return prepare conflict
    #     self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.next())

    #     # Force checkpoint to trigger state change
    #     self.force_checkpoint_and_state_change()

    #     # Resolve the prepare conflict
    #     if self.commit:
    #         prepare_session.breakpoint()
    #         prepare_session.commit_transaction('commit_timestamp=' + self.timestamp_str(60)+',durable_timestamp='+self.timestamp_str(60))
    #         # Retry next() should work and return key 2
    #         self.assertEqual(cursor.next(), 0)
    #         self.assertEqual(cursor.get_key(), 2)
    #         self.assertEqual(cursor.get_value(), "prepared_value")
    #     else:
    #         prepare_session.rollback_transaction()
    #         # Retry next() should skip rolled back key and return key 3
    #         self.assertEqual(cursor.next(), 0)
    #         self.assertEqual(cursor.get_key(), 3)
    #         self.assertEqual(cursor.get_value(), "value_3")

    #     prepare_cursor.close()


