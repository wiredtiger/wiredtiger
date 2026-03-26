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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered81.py
#   Test that follower cursors see updated data after a new checkpoint is applied.
#
#   Key scenarios:
#   - Unpositioned cursor sees new data after checkpoint advance.
#   - Cursor preserves position correctly when checkpoint advances.
#   - With read timestamp, iteration triggers the upgrade.
#   - Data added/updated/removed across checkpoints is visible after advance.

@disagg_test_class
class test_layered81(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered81'

    nkeys = 1000

    disagg_storages = gen_disagg_storages('test_layered81', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        self.ts = 1
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        config = "key_format=S,value_format=S"
        self.session.create(self.uri, config)
        self.session_follow.create(self.uri, config)

    @staticmethod
    def fmt_key(i):
        return f"{i:06d}"

    @staticmethod
    def fmt_val(i):
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
        """Insert keys on the follower (local writes). keys is a list of integers."""
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
        """Remove keys on the follower (local deletes). keys is a list of integers."""
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
    # Test: An existing cursor sees new data after checkpoint advance.
    #
    # Checkpoint 1: even keys 0-998. Open a cursor, scan, reset (unpositioned).
    # Checkpoint 2: all keys 0-999. The same cursor must see all 1000 keys.
    # -----------------------------------------------------------------------
    def test_upgrade_full_scan(self):

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        # Open one cursor and verify it sees only even keys.
        cursor = self.session_follow.open_cursor(self.uri)
        expected_even = [self.fmt_key(i) for i in even_keys]
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected_even)
        cursor.reset()

        # Advance to a new checkpoint that adds odd keys.
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_leader(odd_keys)
        self.do_checkpoint()

        # Trigger the upgrade with a search, then verify full scan sees all 1000 keys.
        all_keys = [self.fmt_key(i) for i in range(self.nkeys)]
        cursor.set_key(self.fmt_key(0))
        self.assertEqual(cursor.search(), 0)
        cursor.reset()
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, all_keys)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Updated values visible after upgrade.
    #
    # Checkpoint 1: 1000 keys with original values.
    # Checkpoint 2: every 10th key updated with new value.
    # After advance, follower should see the updated values.
    # -----------------------------------------------------------------------
    def test_upgrade_updated_value(self):

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

        # Verify each removed key returns WT_NOTFOUND via search.
        cursor = self.session_follow.open_cursor(self.uri)
        for i in remove_keys:
            cursor.set_key(self.fmt_key(i))
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Cursor positioned on a locally-written key can iterate forward
    # and see new data after a checkpoint advances.
    #
    # Checkpoint 1: keys 0-499. Follower writes 500-999 locally.
    # Cursor positioned on key 750. Checkpoint 2: adds key 1000.
    # Forward iteration from 750 must reach key 1000.
    # -----------------------------------------------------------------------
    def test_upgrade_positioned_on_local_key(self):

        stable_keys = list(range(500))
        self.insert_leader(stable_keys)
        self.do_checkpoint()

        follower_keys = list(range(500, self.nkeys))
        self.insert_follower(follower_keys)

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(750))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(750))

        # Advance checkpoint: adds key 1000.
        self.insert_leader([1000])
        self.do_checkpoint()

        # Without resetting, verify the cursor can find new stable data.
        cursor.set_key(self.fmt_key(1000))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(1000))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Checkpoint 1: even keys 0-998. Follower adds some odd keys locally.
    # Checkpoint 2: all keys 0-999. After advance, iteration shows all keys in order.
    # -----------------------------------------------------------------------
    def test_upgrade_interleaved(self):

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        follower_odd = list(range(1, 200, 2))
        self.insert_follower(follower_odd)

        expected = sorted([self.fmt_key(i) for i in even_keys] +
                          [self.fmt_key(i) for i in follower_odd])
        self.assertEqual(self.scan_keys(self.session_follow), expected)

        all_odd = list(range(1, self.nkeys, 2))
        self.insert_leader(all_odd)
        self.do_checkpoint()

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

        keys_without_500 = [i for i in range(self.nkeys) if i != 500]
        self.insert_leader(keys_without_500)
        self.do_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        # 500 doesn't exist yet; nearest neighbor is either 499 or 501.
        self.assertIn(cursor.get_key(), [self.fmt_key(499), self.fmt_key(501)])
        self.assertNotEqual(exact, 0)
        if cursor.get_key() == self.fmt_key(499):
            self.assertEqual(exact, -1)
        else:
            self.assertEqual(exact, 1)
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
    # Test: Read timestamp controls which checkpoint's data is visible.
    #
    # Checkpoint 1: keys 0-499. Checkpoint 2: keys 500-999.
    # A transaction at checkpoint 1's timestamp sees only keys 0-499.
    # A transaction at checkpoint 2's timestamp sees all 1000 keys.
    # -----------------------------------------------------------------------
    def test_upgrade_with_read_timestamp_iteration(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        first_half = list(range(0, 500))
        self.insert_leader(first_half)
        self.do_checkpoint()
        ts_after_ckpt1 = self.ts

        # Add second half and checkpoint.
        second_half = list(range(500, self.nkeys))
        self.insert_leader(second_half)
        self.do_checkpoint()
        ts_after_ckpt2 = self.ts

        cursor = self.session_follow.open_cursor(self.uri)

        # Read at checkpoint 1's timestamp: only keys 0-499 visible.
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(ts_after_ckpt1)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(ts_after_ckpt1)}')
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, [self.fmt_key(i) for i in first_half])
        cursor.reset()
        self.session_follow.commit_transaction()

        # Read at checkpoint 2's timestamp: all 1000 keys visible.
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(ts_after_ckpt2)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(ts_after_ckpt2)}')
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
    # Test: 1000 keys checkpointed. Follower deletes keys 400-599 locally.
    # Checkpoint 2 adds more keys. Locally deleted keys stay hidden; new keys appear.
    # -----------------------------------------------------------------------
    def test_upgrade_tombstone_persists(self):

        stable_keys = list(range(self.nkeys))
        self.insert_leader(stable_keys)
        self.do_checkpoint()

        delete_range = list(range(400, 600))
        self.remove_follower(delete_range)

        expected = [self.fmt_key(i) for i in range(self.nkeys) if i < 400 or i >= 600]
        self.assertEqual(self.scan_keys(self.session_follow), expected)

        # Advance the checkpoint with new leader keys; locally deleted keys must stay hidden.
        new_keys = list(range(self.nkeys, self.nkeys + 100))
        self.insert_leader(new_keys)
        self.do_checkpoint()

        expected_after = sorted(expected + [self.fmt_key(i) for i in new_keys])
        self.assertEqual(self.scan_keys(self.session_follow), expected_after)

    # -----------------------------------------------------------------------
    # Test: Leader sees its own writes immediately across checkpoints.
    # 500 keys, checkpoint, then add 500 more. Leader cursor sees all 1000.
    # -----------------------------------------------------------------------
    def test_leader_unaffected_by_checkpoint(self):

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

        cursor.set_key(self.fmt_key(999))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(999))
        cursor.close()

    # -----------------------------------------------------------------------
    # Tests for stable cursor upgrade during iteration (collection scan).
    #
    # To trigger a mid-iteration upgrade, ALL of these conditions must hold:
    #   1. Cursor is positioned and actively iterating.
    #   2. A read timestamp is set (with a read timestamp, the stable cursor
    #      can be safely upgraded even during iteration because the view at
    #      a given timestamp is always consistent).
    #   3. The layered cursor is currently returning data from ingest (not
    #      stable), so the stable cursor is the alternate and can be replaced.
    #   4. A new checkpoint was advanced AFTER the cursor started iterating.
    #
    # Each test uses get_stat() to verify that layered_curs_advance_stable
    # was actually incremented, confirming the upgrade really triggered.
    # -----------------------------------------------------------------------

    def get_stat(self, stat_key):
        """Read a connection-level statistic from the follower."""
        stat_cursor = self.session_follow.open_cursor('statistics:')
        stat_cursor.set_key(stat_key)
        stat_cursor.search()
        val = stat_cursor.get_value()
        stat_cursor.close()
        # val is (description, type_string, value)
        return val[2]

    def begin_read_ts_txn(self):
        """Begin a transaction with a read timestamp on the follower."""
        read_ts = self.ts
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(read_ts)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(read_ts)}')

    # -----------------------------------------------------------------------
    # Test: Forward scan with a read timestamp stays monotonically ordered
    # across a mid-scan checkpoint advance.
    #
    # Checkpoint 1: even keys 0-998. Follower writes odd keys 1-999.
    # Begin transaction with read timestamp. Iterate 100 keys. Advance
    # checkpoint. Continue scanning. All keys must be in increasing order.
    # -----------------------------------------------------------------------
    def test_upgrade_during_forward_scan_positioned_on_ingest(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Checkpoint 1: even keys 0-998.
        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()

        # Follower writes odd keys to ingest.
        self.insert_follower(list(range(1, self.nkeys, 2)))

        # Begin transaction with read timestamp.
        self.begin_read_ts_txn()

        upgrades_before = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)

        cursor = self.session_follow.open_cursor(self.uri)

        # Iterate forward past several keys.
        keys_before = []
        for _ in range(100):
            self.assertEqual(cursor.next(), 0)
            keys_before.append(cursor.get_key())

        # Advance checkpoint mid-scan.
        self.insert_leader(list(range(1000, 1100)))
        self.do_checkpoint()

        # Continue scanning after the checkpoint advance.
        keys_after = []
        while cursor.next() == 0:
            keys_after.append(cursor.get_key())

        upgrades_after = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)
        self.assertGreater(upgrades_after, upgrades_before,
            "Stable cursor upgrade did not trigger during iteration")

        all_keys = keys_before + keys_after
        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: Same as forward scan but with prev(). Monotonically decreasing
    # order must be maintained across a mid-scan checkpoint advance.
    # -----------------------------------------------------------------------
    def test_upgrade_during_backward_scan_positioned_on_ingest(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Stable: even keys 0-998.
        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()

        # Follower: odd keys 1-999. Key 999 is the largest; prev() begins there.
        self.insert_follower(list(range(1, self.nkeys, 2)))

        self.begin_read_ts_txn()

        cursor = self.session_follow.open_cursor(self.uri)
        keys_before = []
        for _ in range(100):
            self.assertEqual(cursor.prev(), 0)
            keys_before.append(cursor.get_key())

        self.insert_leader(list(range(1000, 1100)))
        self.do_checkpoint()

        keys_after = []
        while cursor.prev() == 0:
            keys_after.append(cursor.get_key())

        all_keys = keys_before + keys_after
        for i in range(len(all_keys) - 1):
            self.assertGreater(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} <= {all_keys[i + 1]}")

        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: Bounded forward scan [200, 800] stays ordered and within bounds
    # across a mid-scan checkpoint advance.
    # -----------------------------------------------------------------------
    def test_upgrade_during_bounded_scan_positioned_on_ingest(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()

        self.insert_follower(list(range(1, self.nkeys, 2)))

        self.begin_read_ts_txn()

        upgrades_before = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)

        lo, hi = 200, 800
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(lo))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(hi))
        cursor.bound("bound=upper")

        keys_before = []
        for _ in range(50):
            self.assertEqual(cursor.next(), 0)
            keys_before.append(cursor.get_key())

        self.insert_leader(list(range(1000, 1100)))
        self.do_checkpoint()

        keys_after = []
        while cursor.next() == 0:
            keys_after.append(cursor.get_key())

        upgrades_after = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)
        self.assertGreater(upgrades_after, upgrades_before,
            "Stable cursor upgrade did not trigger during bounded iteration")

        all_keys = keys_before + keys_after
        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        for k in all_keys:
            self.assertGreaterEqual(k, self.fmt_key(lo))
            self.assertLessEqual(k, self.fmt_key(hi))

        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: Forward scan with deleted keys and a mid-scan checkpoint advance.
    #
    # Checkpoint 1: even keys 0-998. Follower deletes even keys 400-600 and
    # adds odd keys 1-999. Checkpoint 2 adds keys 1000-1099.
    # Scan must be monotonically ordered and must exclude deleted keys.
    # -----------------------------------------------------------------------
    def test_upgrade_during_scan_with_tombstones_on_ingest(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Checkpoint 1: even keys 0-998 in stable.
        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()

        # Follower ingest: odd keys + tombstones for even keys 400-600.
        self.insert_follower(list(range(1, self.nkeys, 2)))
        self.remove_follower(list(range(400, 601, 2)))

        self.begin_read_ts_txn()

        cursor = self.session_follow.open_cursor(self.uri)
        keys_before = []
        for _ in range(250):
            self.assertEqual(cursor.next(), 0)
            keys_before.append(cursor.get_key())

        # Checkpoint 2: leader adds more keys.
        self.insert_leader(list(range(1000, 1100)))
        self.do_checkpoint()

        keys_after = []
        while cursor.next() == 0:
            keys_after.append(cursor.get_key())

        all_keys = keys_before + keys_after
        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        # Deleted even keys 400-600 must not appear.
        tombstoned = set(self.fmt_key(k) for k in range(400, 601, 2))
        for k in all_keys:
            self.assertNotIn(k, tombstoned, f"Tombstoned key appeared: {k}")

        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: Forward scan across multiple mid-scan checkpoint advances.
    # Order must be monotonic throughout and at least one upgrade must occur.
    # -----------------------------------------------------------------------
    def test_multiple_upgrades_during_scan_on_ingest(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Checkpoint 1: keys 0-299 in stable.
        self.insert_leader(list(range(300)))
        self.do_checkpoint()

        # Follower ingest: odd keys 301-999.
        self.insert_follower(list(range(301, self.nkeys, 2)))

        self.begin_read_ts_txn()

        upgrades_before = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)

        cursor = self.session_follow.open_cursor(self.uri)
        all_keys = []

        for _ in range(150):
            self.assertEqual(cursor.next(), 0)
            all_keys.append(cursor.get_key())

        # Checkpoint 2: add keys 300-599 to stable.
        self.insert_leader(list(range(300, 600)))
        self.do_checkpoint()

        for _ in range(200):
            self.assertEqual(cursor.next(), 0)
            all_keys.append(cursor.get_key())

        # Checkpoint 3: add keys 600-999 to stable.
        self.insert_leader(list(range(600, self.nkeys)))
        self.do_checkpoint()

        while cursor.next() == 0:
            all_keys.append(cursor.get_key())

        upgrades_after = self.get_stat(wiredtiger.stat.conn.layered_curs_advance_stable)
        self.assertGreater(upgrades_after, upgrades_before,
            "Stable cursor upgrade did not trigger during multi-checkpoint scan")

        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        cursor.close()
        self.session_follow.commit_transaction()

    # -----------------------------------------------------------------------
    # Test: A positioned update mid-scan must not disrupt iteration order.
    # The scan must continue forward from the same position after the write.
    # -----------------------------------------------------------------------
    def test_iterate_update_iterate_follower(self):

        # Even keys in stable, odd keys in follower ingest.
        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()
        self.insert_follower(list(range(1, self.nkeys, 2)))

        cursor = self.session_follow.open_cursor(self.uri)

        # Iterate to around key 500.
        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)

        # Advance to a mid-scan position.
        for _ in range(5):
            self.assertEqual(cursor.next(), 0)

        pos_before_update = cursor.get_key()

        # Perform a positioned update at the current cursor position.
        self.session_follow.begin_transaction()
        cursor.set_value("updated")
        cursor.update()
        self.session_follow.commit_transaction(
            f"commit_timestamp={self.timestamp_str(self.next_ts())}")

        # Continue iterating after the update.
        keys = [cursor.get_key()]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order after update at {i}: {keys[i]} >= {keys[i + 1]}")

        # First key after update must be >= the position before update.
        self.assertGreaterEqual(keys[0], pos_before_update,
            f"First key after update {keys[0]} went backward from {pos_before_update}")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounded scan [200, 800], positioned update mid-scan, continue.
    # Iteration must remain monotonic and within bounds after the write.
    # -----------------------------------------------------------------------
    def test_iterate_update_iterate_bounded_follower(self):

        self.insert_leader(list(range(0, self.nkeys, 2)))
        self.do_checkpoint()
        self.insert_follower(list(range(1, self.nkeys, 2)))

        lo, hi = 200, 800
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(lo))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(hi))
        cursor.bound("bound=upper")

        keys_before = []
        for _ in range(100):
            self.assertEqual(cursor.next(), 0)
            keys_before.append(cursor.get_key())

        # Positioned update mid-bounded-scan.
        self.session_follow.begin_transaction()
        cursor.set_value("updated")
        cursor.update()
        self.session_follow.commit_transaction(
            f"commit_timestamp={self.timestamp_str(self.next_ts())}")

        keys_after = []
        while cursor.next() == 0:
            keys_after.append(cursor.get_key())

        all_keys = keys_before + keys_after
        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        for k in all_keys:
            self.assertGreaterEqual(k, self.fmt_key(lo))
            self.assertLessEqual(k, self.fmt_key(hi))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near on a deleted key in a contiguous deleted range returns
    # the next live key. next() from that position must continue in order
    # and must not return any deleted keys.
    #
    # Stable: keys 0-999. Follower deletes 400-600.
    # search_near(500) -> key > 600. next() scans 601-999 in order.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_walk_then_next(self):

        # Stable: all 1000 keys. Follower deletes a contiguous range (400-600).
        self.insert_leader(list(range(self.nkeys)))
        self.do_checkpoint()

        # Delete a contiguous range of keys on the follower.
        self.remove_follower(list(range(400, 601)))

        cursor = self.session_follow.open_cursor(self.uri)

        # search_near(500): key is deleted; nearest live key is > 600.
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()

        # The returned key must be outside the deleted range.
        self.assertGreater(first_key, self.fmt_key(600),
            f"Expected key > 000600, got {first_key}")

        # Iterate forward from the search_near result.
        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        # Verify strict monotonic order.
        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at {i}: {keys[i]} >= {keys[i + 1]}")

        # Deleted keys must not appear.
        deleted = set(self.fmt_key(k) for k in range(400, 601))
        for k in keys:
            self.assertNotIn(k, deleted, f"Deleted key appeared: {k}")

        # All keys from 601 to 999 must be present.
        expected_remaining = [self.fmt_key(k) for k in range(601, self.nkeys)]
        self.assertEqual(keys, expected_remaining)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near on a deleted key where all forward keys are also deleted.
    # The nearest live key is below the search key.
    # prev() from that position must scan in reverse order without deleted keys.
    #
    # Stable: keys 0-999. Follower deletes 500-999.
    # search_near(700) -> key < 500. prev() scans downward in order.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_walk_then_prev(self):

        # Stable: keys 0-999. Ingest: tombstone everything from 500 onward.
        self.insert_leader(list(range(self.nkeys)))
        self.do_checkpoint()
        self.remove_follower(list(range(500, self.nkeys)))

        cursor = self.session_follow.open_cursor(self.uri)

        # search_near(700): key and all keys above 500 are deleted; nearest live key is 499 (below).
        cursor.set_key(self.fmt_key(700))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()
        self.assertLess(first_key, self.fmt_key(500),
            f"Expected key < 000500, got {first_key}")

        # Iterate backward from the result.
        keys = [first_key]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertGreater(keys[i], keys[i + 1],
                f"Out of order at {i}: {keys[i]} <= {keys[i + 1]}")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounded search_near on a deleted key followed by next().
    # Bounds [200, 800]. Follower deletes 300-600.
    # search_near(450) -> nearest live key within bounds.
    # next() must remain ordered and within bounds, skipping deleted keys.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_walk_then_next_with_bounds(self):

        self.insert_leader(list(range(self.nkeys)))
        self.do_checkpoint()

        # Tombstone a range that overlaps the search key.
        self.remove_follower(list(range(300, 601)))

        lo, hi = 200, 800
        cursor = self.session_follow.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(lo))
        cursor.bound("bound=lower")
        cursor.set_key(self.fmt_key(hi))
        cursor.bound("bound=upper")

        # search_near(450): key is deleted; nearest live key within bounds is at 601.
        cursor.set_key(self.fmt_key(450))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at {i}: {keys[i]} >= {keys[i + 1]}")

        # All within bounds.
        for k in keys:
            self.assertGreaterEqual(k, self.fmt_key(lo))
            self.assertLessEqual(k, self.fmt_key(hi))

        # No tombstoned keys.
        tombstoned = set(self.fmt_key(k) for k in range(300, 601))
        for k in keys:
            self.assertNotIn(k, tombstoned)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: A forward scan remains complete when a key visible at scan start
    # is removed and a new checkpoint is applied mid-scan.
    #
    # Stable: even keys 0-998. Follower: odd keys 1-999.
    # Scan past key 500. Leader removes an even key and checkpoints.
    # Restart scan at a new read timestamp. All remaining even keys must appear.
    # -----------------------------------------------------------------------

    def test_upgrade_dup_position_fails(self):

        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Checkpoint 1: even keys 0-998 in stable.
        even_keys = list(range(0, self.nkeys, 2))
        self.insert_leader(even_keys)
        self.do_checkpoint()

        # Follower ingest: odd keys 1-999.
        self.insert_follower(list(range(1, self.nkeys, 2)))

        # Read timestamp AFTER the ingest writes so they're visible.
        read_ts = self.ts

        # Begin transaction with read timestamp. This is required so that
        # the stable cursor upgrade is allowed during iteration.
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(read_ts)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(read_ts)}')

        cursor = self.session_follow.open_cursor(self.uri)

        # Iterate forward past key 500.
        keys_before = []
        for _ in range(502):
            self.assertEqual(cursor.next(), 0)
            keys_before.append(cursor.get_key())

        last_key = keys_before[-1]
        last_key_int = int(last_key)
        key_to_remove = last_key_int + 1 if last_key_int % 2 == 1 else last_key_int

        # Commit the current transaction so we can start a new one later
        # with a higher read timestamp that sees the delete.
        self.session_follow.commit_transaction()

        # Remove the key on the leader and checkpoint.
        self.remove_leader([key_to_remove])
        self.do_checkpoint()

        # Start a new transaction with a read timestamp after the remove,
        # so the deleted key is not visible in the new scan.
        new_read_ts = self.ts
        self.conn_follow.set_timestamp(f'oldest_timestamp={self.timestamp_str(new_read_ts)}')
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(new_read_ts)}')

        # Continue scanning from the repositioned cursor.
        keys_after = []
        while cursor.next() == 0:
            keys_after.append(cursor.get_key())

        all_keys = keys_before + keys_after
        # Check monotonic order.
        for i in range(len(all_keys) - 1):
            self.assertLess(all_keys[i], all_keys[i + 1],
                f"Out of order at {i}: {all_keys[i]} >= {all_keys[i + 1]}")

        # Even keys after the removed key must still appear.
        even_keys_after = [k for k in keys_after if int(k) % 2 == 0]
        self.assertGreater(len(even_keys_after), 0,
            f"No even keys found after upgrade  stable cursor was not repositioned. "
            f"Removed key: {key_to_remove}")

        # Specifically, even keys like 700, 702, ... 998 should be present
        # (they were not removed).
        expected_even_after = [self.fmt_key(k) for k in range(key_to_remove + 2, self.nkeys, 2)]
        found_even_after = set(even_keys_after)
        for k in expected_even_after:
            self.assertIn(k, found_even_after,
                f"Even key {k} missing after upgrade  stable cursor not repositioned")

        cursor.close()
        self.session_follow.commit_transaction()
