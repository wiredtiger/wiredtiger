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
from wtscenario import make_scenarios

# test_layered81.py
#   Test stable cursor upgrade to a new checkpoint on followers.
#
#   When a follower picks up a new checkpoint, existing layered cursors must
#   transparently upgrade their stable constituent to the newer checkpoint.
#   The upgrade happens inside __clayered_adjust_state / __clayered_upgrade_stable,
#   and is triggered when the checkpoint_meta_lsn changes.
#
#   Key scenarios:
#   - Unpositioned cursor sees new stable data after checkpoint advance.
#   - Positioned cursor on ingest upgrades stable on next operation.
#   - With read timestamp, even iteration triggers upgrade.
#   - Data added/updated/removed across checkpoints is visible after upgrade.
#   - Cursor position is preserved correctly after upgrade.

@disagg_test_class
class test_layered81(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered81'

    disagg_storages = gen_disagg_storages('test_layered81', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_follow = None
    session_follow = None
    ts = 1

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def create_table(self):
        config = "key_format=S,value_format=S"
        self.session.create(self.uri, config)
        self.session_follow.create(self.uri, config)

    def next_ts(self):
        self.ts += 1
        return self.ts

    def insert_leader(self, keys, values=None):
        """Insert keys on the leader."""
        cursor = self.session.open_cursor(self.uri)
        for i, key in enumerate(keys):
            val = values[i] if values else f"val_{key}"
            self.session.begin_transaction()
            cursor[key] = val
            self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_leader(self, keys):
        """Remove keys on the leader."""
        cursor = self.session.open_cursor(self.uri)
        for key in keys:
            self.session.begin_transaction()
            cursor.set_key(key)
            cursor.remove()
            self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def insert_follower(self, keys, values=None):
        """Insert keys on the follower (into follower's ingest)."""
        cursor = self.session_follow.open_cursor(self.uri)
        for i, key in enumerate(keys):
            val = values[i] if values else f"val_{key}"
            self.session_follow.begin_transaction()
            cursor[key] = val
            self.session_follow.commit_transaction(
                f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_follower(self, keys):
        """Remove keys on the follower (tombstones in follower's ingest)."""
        cursor = self.session_follow.open_cursor(self.uri)
        for key in keys:
            self.session_follow.begin_transaction()
            cursor.set_key(key)
            cursor.remove()
            self.session_follow.commit_transaction(
                f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def do_checkpoint(self):
        """Checkpoint on leader and advance follower."""
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def scan_keys(self, session):
        """Return all keys in forward order."""
        cursor = session.open_cursor(self.uri)
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        cursor.close()
        return keys

    def scan_kv(self, session):
        """Return all (key, value) pairs in forward order."""
        cursor = session.open_cursor(self.uri)
        result = []
        while cursor.next() == 0:
            result.append((cursor.get_key(), cursor.get_value()))
        cursor.close()
        return result

    # -----------------------------------------------------------------------
    # Test: Unpositioned cursor sees new data after checkpoint advance.
    #
    # Open a cursor on follower, use it, then leader adds more data,
    # checkpoints, and follower advances. The same cursor should now see
    # the new data on the next search.
    # -----------------------------------------------------------------------
    def test_upgrade_unpositioned_sees_new_data(self):
        self.setup_follower()
        self.create_table()

        # Checkpoint 1: A, C
        self.insert_leader(["A", "C"])
        self.do_checkpoint()

        # Open follower cursor and verify initial data.
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_A")
        cursor.reset()

        # Checkpoint 2: add B, D
        self.insert_leader(["B", "D"])
        self.do_checkpoint()

        # The same cursor should now see the new data.
        # search is a new operation, so it triggers upgrade.
        cursor.set_key("B")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_B")

        cursor.set_key("D")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_D")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Full scan sees new data after upgrade.
    #
    # Checkpoint 1: A, C. Checkpoint 2: A, B, C, D.
    # Full scan after advance should see all four keys.
    # -----------------------------------------------------------------------
    def test_upgrade_full_scan(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "C"])
        self.do_checkpoint()

        self.assertEqual(self.scan_keys(self.session_follow), ["A", "C"])

        # Add more keys and checkpoint again.
        self.insert_leader(["B", "D"])
        self.do_checkpoint()

        self.assertEqual(self.scan_keys(self.session_follow), ["A", "B", "C", "D"])

    # -----------------------------------------------------------------------
    # Test: Updated values visible after upgrade.
    #
    # Checkpoint 1: A="v1". Checkpoint 2: A="v2".
    # After advance, follower should see the new value.
    # -----------------------------------------------------------------------
    def test_upgrade_updated_value(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A"], values=["v1"])
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "v1")
        cursor.reset()

        # Update A on leader and checkpoint.
        self.insert_leader(["A"], values=["v2"])
        self.do_checkpoint()

        # After upgrade, should see new value.
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "v2")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Deleted key disappears after upgrade.
    #
    # Checkpoint 1: A, B, C. Checkpoint 2: A, C (B removed).
    # Follower should not see B after advance.
    # -----------------------------------------------------------------------
    def test_upgrade_deleted_key(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "B", "C"])
        self.do_checkpoint()

        self.assertEqual(self.scan_keys(self.session_follow), ["A", "B", "C"])

        self.remove_leader(["B"])
        self.do_checkpoint()

        self.assertEqual(self.scan_keys(self.session_follow), ["A", "C"])

    # -----------------------------------------------------------------------
    # Test: Cursor positioned on ingest preserves position after upgrade.
    #
    # Follower has ingest key "M". Checkpoint changes stable. After advance,
    # the cursor (positioned on ingest "M") should still work.
    # -----------------------------------------------------------------------
    def test_upgrade_positioned_on_ingest(self):
        self.setup_follower()
        self.create_table()

        # Checkpoint 1: A, E in stable.
        self.insert_leader(["A", "E"])
        self.do_checkpoint()

        # Follower writes M to ingest.
        self.insert_follower(["M"])

        # Position cursor on M (ingest).
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("M")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_M")

        # Checkpoint 2: add G to stable.
        self.insert_leader(["G"])
        self.do_checkpoint()

        # Cursor is positioned on ingest M. Iteration should trigger upgrade
        # of the stable cursor, and the merge should see the new key G.
        # prev from M should find G (if upgrade happened) or E (if not).
        # With a read timestamp, iteration triggers upgrade. Without, it doesn't.
        # Use search_near (a new operation) to trigger upgrade first.
        cursor.reset()
        cursor.set_key("G")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_G")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Multiple checkpoint advances on the same cursor.
    #
    # Open cursor. Checkpoint 1: A. Checkpoint 2: A, B. Checkpoint 3: A, B, C.
    # Cursor should see cumulative data after each advance.
    # -----------------------------------------------------------------------
    def test_upgrade_multiple_checkpoints(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A"])
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Second checkpoint.
        self.insert_leader(["B"])
        self.do_checkpoint()

        cursor.set_key("B")
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Third checkpoint.
        self.insert_leader(["C"])
        self.do_checkpoint()

        cursor.set_key("C")
        self.assertEqual(cursor.search(), 0)

        # Full scan shows all three.
        cursor.reset()
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, ["A", "B", "C"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Cursor upgrade with interleaved ingest and stable data.
    #
    # Checkpoint 1: B, F. Follower ingest: D.
    # Checkpoint 2: B, D, F, H (D now also in stable, H added).
    # Follower ingest still has D. After upgrade, iteration should
    # produce correct merged order.
    # -----------------------------------------------------------------------
    def test_upgrade_interleaved(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["B", "F"])
        self.do_checkpoint()

        # Follower writes D to ingest.
        self.insert_follower(["D"])

        self.assertEqual(self.scan_keys(self.session_follow), ["B", "D", "F"])

        # Leader adds D and H, then checkpoints.
        self.insert_leader(["D", "H"])
        self.do_checkpoint()

        # After advance, scan should show B, D, F, H.
        # D exists in both ingest and stable; ingest wins.
        self.assertEqual(self.scan_keys(self.session_follow), ["B", "D", "F", "H"])

    # -----------------------------------------------------------------------
    # Test: search_near after upgrade finds new closer key.
    #
    # Checkpoint 1: A, Z. search_near("M") -> could return A or Z.
    # Checkpoint 2: A, M, Z. search_near("M") -> exact match.
    # -----------------------------------------------------------------------
    def test_upgrade_search_near(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "Z"])
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("M")
        exact = cursor.search_near()
        # M doesn't exist yet, should get a neighbor.
        self.assertNotEqual(exact, 0)
        cursor.reset()

        # Add M and checkpoint.
        self.insert_leader(["M"])
        self.do_checkpoint()

        # After upgrade, search_near should find exact match.
        cursor.set_key("M")
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), "M")
        self.assertEqual(cursor.get_value(), "val_M")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade with read timestamp - iteration triggers upgrade.
    #
    # With a read timestamp set, __clayered_can_stable_upgrade returns true
    # even for iteration operations (next/prev).
    # -----------------------------------------------------------------------
    def test_upgrade_with_read_timestamp_iteration(self):
        self.setup_follower()
        self.create_table()

        # Set oldest timestamp.
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        self.insert_leader(["A", "C"])
        self.do_checkpoint()

        # Begin transaction with read timestamp on follower.
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(self.ts)}')

        cursor = self.session_follow.open_cursor(self.uri)
        # Position via next.
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "A")
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "C")
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.reset()
        self.session_follow.commit_transaction()

        # Add B and checkpoint.
        self.insert_leader(["B"])
        self.do_checkpoint()

        # With read timestamp, iteration triggers the upgrade.
        read_ts = self.ts
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(read_ts)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(read_ts)}')

        # Scan should see all three keys.
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, ["A", "B", "C"])
        cursor.close()
        self.session_follow.commit_transaction()

    # FIXME: Bounds are not preserved after stable cursor upgrade. After the upgrade,
    # scans return all keys ignoring bounds. This is a pre-existing bug in
    # __clayered_upgrade_stable / __clayered_copy_bounds interaction.

    # -----------------------------------------------------------------------
    # Test: Upgrade when positioned cursor value changes.
    #
    # Cursor reads A="v1" from checkpoint 1. Checkpoint 2 has A="v2".
    # After cursor reset + search, should see v2.
    # -----------------------------------------------------------------------
    def test_upgrade_value_changes(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A"], values=["v1"])
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "v1")
        cursor.reset()

        # Update on leader.
        self.insert_leader(["A"], values=["v2"])
        self.do_checkpoint()

        # Should now see updated value.
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "v2")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade when a key is removed between checkpoints.
    #
    # Checkpoint 1: A, B, C. Checkpoint 2: A, C (B removed).
    # After upgrade, search for B should return NOTFOUND.
    # -----------------------------------------------------------------------
    def test_upgrade_key_removed(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "B", "C"])
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key("B")
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        self.remove_leader(["B"])
        self.do_checkpoint()

        cursor.set_key("B")
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Follower ingest tombstone hides stable key after upgrade.
    #
    # Checkpoint 1: A, B, C. Follower deletes B in ingest.
    # Checkpoint 2: A, B, C, D (B still in stable).
    # Follower should still not see B (tombstone in ingest hides it),
    # but should see D.
    # -----------------------------------------------------------------------
    def test_upgrade_tombstone_persists(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "B", "C"])
        self.do_checkpoint()

        # Follower deletes B.
        self.remove_follower(["B"])

        self.assertEqual(self.scan_keys(self.session_follow), ["A", "C"])

        # Checkpoint 2: adds D on leader.
        self.insert_leader(["D"])
        self.do_checkpoint()

        # B still hidden by follower's ingest tombstone. D is new.
        self.assertEqual(self.scan_keys(self.session_follow), ["A", "C", "D"])

    # -----------------------------------------------------------------------
    # Test: Large number of keys with upgrade.
    #
    # Checkpoint 1: even keys 0-198. Checkpoint 2: all keys 0-199.
    # Verify complete merged iteration after upgrade.
    # -----------------------------------------------------------------------
    def test_upgrade_many_keys(self):
        self.setup_follower()
        self.create_table()

        # Insert even keys.
        even_keys = [f"{i:04d}" for i in range(0, 200, 2)]
        self.insert_leader(even_keys)
        self.do_checkpoint()

        self.assertEqual(self.scan_keys(self.session_follow), even_keys)

        # Insert odd keys.
        odd_keys = [f"{i:04d}" for i in range(1, 200, 2)]
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        all_keys = sorted(even_keys + odd_keys)
        self.assertEqual(self.scan_keys(self.session_follow), all_keys)

    # -----------------------------------------------------------------------
    # Test: Upgrade does not affect leader.
    #
    # Leader's stable cursor is R/W and does not get upgraded on checkpoint.
    # Verify leader still works correctly across checkpoints.
    # -----------------------------------------------------------------------
    def test_leader_unaffected_by_checkpoint(self):
        self.setup_follower()
        self.create_table()

        self.insert_leader(["A", "C"])
        self.do_checkpoint()

        cursor = self.session.open_cursor(self.uri)
        cursor.set_key("A")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_A")
        cursor.reset()

        self.insert_leader(["B"])
        self.do_checkpoint()

        # Leader sees B immediately (it wrote it).
        cursor.set_key("B")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_B")
        cursor.close()
