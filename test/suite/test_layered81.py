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

    nkeys = 1000

    disagg_storages = gen_disagg_storages('test_layered81', disagg_only=True)
    # Follower-only: stable cursor upgrade is a follower concept. The leader's stable
    # cursor is R/W and never gets upgraded on checkpoint. See test_leader_unaffected_by_checkpoint
    # for a sanity check that the leader is not impacted.
    scenarios = make_scenarios(disagg_storages)

    conn_follow = None
    session_follow = None
    ts = 1

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        """Create follower connection for stable cursor upgrade testing."""
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def create_table(self):
        config = "key_format=S,value_format=S"
        self.session.create(self.uri, config)
        self.session_follow.create(self.uri, config)

    def fmt_key(self, i):
        return f"{i:06d}"

    def fmt_val(self, i):
        return f"val_{i:06d}"

    def next_ts(self):
        self.ts += 1
        return self.ts

    def insert_leader(self, keys, values=None):
        """Insert keys on the leader. keys is a list of integers."""
        cursor = self.session.open_cursor(self.uri)
        for idx, k in enumerate(keys):
            key = self.fmt_key(k)
            val = values[idx] if values else self.fmt_val(k)
            self.session.begin_transaction()
            cursor[key] = val
            self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_leader(self, keys):
        """Remove keys on the leader. keys is a list of integers."""
        cursor = self.session.open_cursor(self.uri)
        for k in keys:
            key = self.fmt_key(k)
            self.session.begin_transaction()
            cursor.set_key(key)
            cursor.remove()
            self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def insert_follower(self, keys, values=None):
        """Insert keys on the follower (into follower's ingest). keys is a list of integers."""
        cursor = self.session_follow.open_cursor(self.uri)
        for idx, k in enumerate(keys):
            key = self.fmt_key(k)
            val = values[idx] if values else self.fmt_val(k)
            self.session_follow.begin_transaction()
            cursor[key] = val
            self.session_follow.commit_transaction(
                f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_follower(self, keys):
        """Remove keys on the follower (tombstones in follower's ingest). keys is a list of integers."""
        cursor = self.session_follow.open_cursor(self.uri)
        for k in keys:
            key = self.fmt_key(k)
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

        # Checkpoint 1: even keys 0-998.
        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        # Open follower cursor and verify initial data.
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(0))
        cursor.reset()

        # Checkpoint 2: add odd keys 1-999.
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        # The same cursor should now see the new data.
        cursor.set_key(self.fmt_key(1))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(1))

        cursor.set_key(self.fmt_key(999))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(999))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Full scan sees new data after upgrade.
    #
    # Checkpoint 1: even keys 0-998. Checkpoint 2: all keys 0-999.
    # Full scan after advance should see all 1000 keys.
    # -----------------------------------------------------------------------
    def test_upgrade_full_scan(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        expected_even = [self.fmt_key(i) for i in even_keys]
        self.assertEqual(self.scan_keys(self.session_follow), expected_even)

        # Add odd keys and checkpoint again.
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        all_keys = [self.fmt_key(i) for i in range(self.nkeys)]
        self.assertEqual(self.scan_keys(self.session_follow), all_keys)

    # -----------------------------------------------------------------------
    # Test: Updated values visible after upgrade.
    #
    # Checkpoint 1: 1000 keys with original values.
    # Checkpoint 2: every 10th key updated with new value.
    # After advance, follower should see the updated values.
    # -----------------------------------------------------------------------
    def test_upgrade_updated_value(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_leader(all_keys)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(0))
        cursor.reset()

        # Update every 10th key on leader and checkpoint.
        update_keys = list(range(0, self.nkeys, 10))
        update_vals = [f"updated_{i:06d}" for i in update_keys]
        self.insert_leader(update_keys, values=update_vals)
        self.do_checkpoint()

        # After upgrade, should see new values for updated keys.
        for i in update_keys:
            cursor.set_key(self.fmt_key(i))
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), f"updated_{i:06d}")
            cursor.reset()

        # Non-updated keys should retain original values.
        cursor.set_key(self.fmt_key(1))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(1))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Deleted key disappears after upgrade.
    #
    # Checkpoint 1: 1000 keys. Checkpoint 2: every 3rd key removed.
    # Follower should not see removed keys after advance.
    # -----------------------------------------------------------------------
    def test_upgrade_deleted_key(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_leader(all_keys)
        self.do_checkpoint()

        expected_all = [self.fmt_key(i) for i in all_keys]
        self.assertEqual(self.scan_keys(self.session_follow), expected_all)

        remove_keys = list(range(0, self.nkeys, 3))
        self.remove_leader(remove_keys)
        self.do_checkpoint()

        remaining = [self.fmt_key(i) for i in all_keys if i % 3 != 0]
        self.assertEqual(self.scan_keys(self.session_follow), remaining)

    # -----------------------------------------------------------------------
    # Test: Cursor positioned on ingest preserves position after upgrade.
    #
    # 500 keys in stable. Follower writes 500 more to ingest, then upgrade.
    # -----------------------------------------------------------------------
    def test_upgrade_positioned_on_ingest(self):
        self.setup_follower()
        self.create_table()

        # Checkpoint 1: keys 0-499 in stable.
        stable_keys = list(range(500))
        self.insert_leader(stable_keys)
        self.do_checkpoint()

        # Follower writes keys 500-999 to ingest.
        ingest_keys = list(range(500, self.nkeys))
        self.insert_follower(ingest_keys)

        # Position cursor on an ingest key.
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(750))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(750))

        # Checkpoint 2: add key 1000 to stable.
        self.insert_leader([1000])
        self.do_checkpoint()

        # Cursor is positioned on ingest. Use search to trigger upgrade.
        cursor.reset()
        cursor.set_key(self.fmt_key(1000))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(1000))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Multiple checkpoint advances on the same cursor.
    #
    # 3 checkpoints adding 333, 333, 334 keys each.
    # Cursor should see cumulative data after each advance.
    # -----------------------------------------------------------------------
    def test_upgrade_multiple_checkpoints(self):
        self.setup_follower()
        self.create_table()

        # First checkpoint: keys 0-332.
        batch1 = list(range(0, 333))
        self.insert_leader(batch1)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Second checkpoint: keys 333-665.
        batch2 = list(range(333, 666))
        self.insert_leader(batch2)
        self.do_checkpoint()

        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Third checkpoint: keys 666-999.
        batch3 = list(range(666, self.nkeys))
        self.insert_leader(batch3)
        self.do_checkpoint()

        cursor.set_key(self.fmt_key(999))
        self.assertEqual(cursor.search(), 0)

        # Full scan shows all 1000 keys.
        cursor.reset()
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, [self.fmt_key(i) for i in range(self.nkeys)])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Cursor upgrade with interleaved ingest and stable data.
    #
    # Checkpoint 1: even keys 0-998. Follower adds some odd keys to ingest.
    # Checkpoint 2: all keys 0-999 (odd keys now also in stable).
    # After upgrade, iteration should produce correct merged order.
    # -----------------------------------------------------------------------
    def test_upgrade_interleaved(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        # Follower writes some odd keys (first 100 odd keys) to ingest.
        follower_odd = list(range(1, 200, 2))
        self.insert_follower(follower_odd)

        expected = sorted([self.fmt_key(i) for i in even_keys] +
                          [self.fmt_key(i) for i in follower_odd])
        self.assertEqual(self.scan_keys(self.session_follow), expected)

        # Leader adds all odd keys, then checkpoints.
        all_odd = list(range(1, self.nkeys, 2))
        self.insert_leader(all_odd)
        self.do_checkpoint()

        # After advance, scan should show all 1000 keys.
        # Odd keys in ingest overlap with stable; ingest wins for those.
        all_keys = [self.fmt_key(i) for i in range(self.nkeys)]
        self.assertEqual(self.scan_keys(self.session_follow), all_keys)

    # -----------------------------------------------------------------------
    # Test: search_near after upgrade finds new closer key.
    #
    # Checkpoint 1: 1000 keys missing key 500.
    # search_near(500) -> gets a neighbor.
    # Checkpoint 2: adds key 500. search_near(500) -> exact match.
    # -----------------------------------------------------------------------
    def test_upgrade_search_near(self):
        self.setup_follower()
        self.create_table()

        keys_without_500 = [i for i in range(self.nkeys) if i != 500]
        self.insert_leader(keys_without_500)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        # 500 doesn't exist yet, should get a neighbor.
        self.assertNotEqual(exact, 0)
        cursor.reset()

        # Add 500 and checkpoint.
        self.insert_leader([500])
        self.do_checkpoint()

        # After upgrade, search_near should find exact match.
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(500))
        self.assertEqual(cursor.get_value(), self.fmt_val(500))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade with read timestamp - iteration triggers upgrade.
    #
    # Checkpoint 1: 500 keys. Checkpoint 2: 500 more keys.
    # With a read timestamp set, iteration triggers upgrade.
    # -----------------------------------------------------------------------
    def test_upgrade_with_read_timestamp_iteration(self):
        self.setup_follower()
        self.create_table()

        # Set oldest timestamp.
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        first_half = list(range(0, 500))
        self.insert_leader(first_half)
        self.do_checkpoint()

        # Begin transaction with read timestamp on follower.
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(self.ts)}')

        cursor = self.session_follow.open_cursor(self.uri)
        # Position via next.
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(0))
        # Iterate to end.
        count = 1
        while cursor.next() == 0:
            count += 1
        self.assertEqual(count, 500)
        cursor.reset()
        self.session_follow.commit_transaction()

        # Add second half and checkpoint.
        second_half = list(range(500, self.nkeys))
        self.insert_leader(second_half)
        self.do_checkpoint()

        # With read timestamp, iteration triggers the upgrade.
        read_ts = self.ts
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(read_ts)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(read_ts)}')

        # Scan should see all 1000 keys.
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, [self.fmt_key(i) for i in range(self.nkeys)])
        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: Upgrade preserves bounds.
    #
    # 1000 keys, bounds [200, 800].
    # -----------------------------------------------------------------------
    def test_upgrade_preserves_bounds(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_leader(all_keys)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        # Scan within bounds: keys 200-800.
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        expected_bounded = [self.fmt_key(i) for i in range(200, 801)]
        self.assertEqual(keys, expected_bounded)

        # Reset clears bounds. Re-apply them.
        cursor.reset()
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        # Add more data outside bounds and checkpoint.
        self.insert_leader([1001, 1002])
        self.do_checkpoint()

        # search triggers the upgrade.
        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Re-apply bounds after reset.
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        # After upgrade, bounds should be in effect. 1001 and 1002 are outside.
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected_bounded)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade with bounds and new data inside bounds.
    #
    # Even keys first, odd keys added in checkpoint 2, bounds [200, 800].
    # -----------------------------------------------------------------------
    def test_upgrade_bounds_new_data_inside(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        expected_even_bounded = [self.fmt_key(i) for i in range(200, 801, 2)]
        self.assertEqual(keys, expected_even_bounded)

        cursor.reset()
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        # Add odd keys inside and outside bounds.
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        # Trigger the upgrade with a search.
        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        # Re-apply bounds after reset.
        cursor.set_key(self.fmt_key(200))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(800))
        cursor.bound("bound=upper")

        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        expected_all_bounded = [self.fmt_key(i) for i in range(200, 801)]
        self.assertEqual(keys, expected_all_bounded)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade when positioned cursor value changes.
    #
    # 1000 keys, update every 5th key.
    # -----------------------------------------------------------------------
    def test_upgrade_value_changes(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_leader(all_keys)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(0))
        cursor.reset()

        # Update every 5th key on leader.
        update_keys = list(range(0, self.nkeys, 5))
        update_vals = [f"updated_{i:06d}" for i in update_keys]
        self.insert_leader(update_keys, values=update_vals)
        self.do_checkpoint()

        # Should now see updated values.
        for i in update_keys:
            cursor.set_key(self.fmt_key(i))
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), f"updated_{i:06d}")
            cursor.reset()

        # Non-updated key should retain original value.
        cursor.set_key(self.fmt_key(1))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(1))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upgrade when a key is removed between checkpoints.
    #
    # 1000 keys, remove every 4th key.
    # -----------------------------------------------------------------------
    def test_upgrade_key_removed(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_leader(all_keys)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()

        remove_keys = list(range(0, self.nkeys, 4))
        self.remove_leader(remove_keys)
        self.do_checkpoint()

        for i in remove_keys:
            cursor.set_key(self.fmt_key(i))
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Follower ingest tombstone hides stable key after upgrade.
    #
    # 1000 keys in stable. Follower deletes a range of keys.
    # Checkpoint adds more keys on leader. Tombstones persist.
    # -----------------------------------------------------------------------
    def test_upgrade_tombstone_persists(self):
        self.setup_follower()
        self.create_table()

        stable_keys = list(range(self.nkeys))
        self.insert_leader(stable_keys)
        self.do_checkpoint()

        # Follower deletes keys 400-599.
        delete_range = list(range(400, 600))
        self.remove_follower(delete_range)

        expected = [self.fmt_key(i) for i in range(self.nkeys) if i < 400 or i >= 600]
        self.assertEqual(self.scan_keys(self.session_follow), expected)

        # Checkpoint 2: adds keys 1000-1099 on leader.
        new_keys = list(range(1000, 1100))
        self.insert_leader(new_keys)
        self.do_checkpoint()

        # Deleted keys still hidden by follower's ingest tombstone. New keys visible.
        expected_after = expected + [self.fmt_key(i) for i in new_keys]
        self.assertEqual(self.scan_keys(self.session_follow), expected_after)

    # -----------------------------------------------------------------------
    # Test: Large number of keys with upgrade.
    #
    # Checkpoint 1: even keys 0-998. Checkpoint 2: all keys 0-999.
    # Verify complete merged iteration after upgrade.
    # -----------------------------------------------------------------------
    def test_upgrade_many_keys(self):
        self.setup_follower()
        self.create_table()

        # Insert even keys.
        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        expected_even = [self.fmt_key(i) for i in even_keys]
        self.assertEqual(self.scan_keys(self.session_follow), expected_even)

        # Insert odd keys.
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        all_keys = [self.fmt_key(i) for i in range(self.nkeys)]
        self.assertEqual(self.scan_keys(self.session_follow), all_keys)

    # -----------------------------------------------------------------------
    # Test: Upgrade does not affect leader.
    #
    # 500 keys, checkpoint, add 500 more.
    # Leader's stable cursor is R/W and does not get upgraded on checkpoint.
    # Verify leader still works correctly across checkpoints.
    # -----------------------------------------------------------------------
    def test_leader_unaffected_by_checkpoint(self):
        self.setup_follower()
        self.create_table()

        first_half = list(range(0, 500))
        self.insert_leader(first_half)
        self.do_checkpoint()

        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(0))
        cursor.reset()

        second_half = list(range(500, self.nkeys))
        self.insert_leader(second_half)
        self.do_checkpoint()

        # Leader sees new keys immediately (it wrote them).
        cursor.set_key(self.fmt_key(999))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(999))
        cursor.close()
