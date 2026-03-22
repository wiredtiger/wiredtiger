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

# test_layered80.py
#   Test layered cursor search_near correctness.
#
#   Exercises edge cases in the merge logic that chooses between ingest and
#   stable constituent cursors, including:
#   - Keys on opposite sides of the search key in the two constituents.
#   - Tombstones in the ingest table that logically delete stable keys.
#   - search_near landing on a deleted key and walking forward/backward.
#   - Exact matches in one constituent vs nearby matches in the other.
#   - Correct iteration (next/prev) after search_near.

@disagg_test_class
class test_layered80(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered80'

    disagg_storages = gen_disagg_storages('test_layered80', disagg_only=True)

    role_scenarios = [
        ('leader', dict(role='leader')),
        ('follower', dict(role='follower')),
    ]
    scenarios = make_scenarios(disagg_storages, role_scenarios)

    conn_follow = None
    session_follow = None
    ts = 1

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def get_session(self):
        """Return the session under test."""
        if self.role == 'leader':
            return self.session
        return self.session_follow

    def get_conn(self):
        """Return the connection under test."""
        if self.role == 'leader':
            return self.conn
        return self.conn_follow

    def create_table(self):
        config = "key_format=S,value_format=S"
        self.session.create(self.uri, config)
        self.session_follow.create(self.uri, config)

    def next_ts(self):
        self.ts += 1
        return self.ts

    def insert_keys_on(self, session, keys, values=None):
        """Insert key/value pairs on a specific session."""
        cursor = session.open_cursor(self.uri)
        for i, key in enumerate(keys):
            val = values[i] if values else f"val_{key}"
            session.begin_transaction()
            cursor[key] = val
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_keys_on(self, session, keys):
        """Remove keys on a specific session (creates tombstones in ingest)."""
        cursor = session.open_cursor(self.uri)
        for key in keys:
            session.begin_transaction()
            cursor.set_key(key)
            cursor.remove()
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def insert_stable(self, keys, values=None):
        """
        Insert keys into stable: write on leader, checkpoint, advance follower.
        After this, the keys are in the stable table of both leader and follower.
        """
        self.insert_keys_on(self.session, keys, values)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def insert_ingest(self, keys, values=None):
        """
        Insert keys into ingest on the session under test.
        On leader: writes to leader's ingest.
        On follower: writes to follower's ingest.
        """
        self.insert_keys_on(self.get_session(), keys, values)

    def remove_ingest(self, keys):
        """
        Remove keys on the session under test (tombstones in ingest).
        """
        self.remove_keys_on(self.get_session(), keys)

    def search_near_check(self, search_key, expected_key, expected_exact, expected_value=None):
        """
        Open a cursor, call search_near for search_key, and verify the result.
        """
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key(search_key)
        exact = cursor.search_near()
        self.assertEqual(cursor.get_key(), expected_key,
            f"search_near({search_key}): expected key {expected_key}, got {cursor.get_key()}")
        self.assertEqual(exact, expected_exact,
            f"search_near({search_key}): expected exact={expected_exact}, got {exact}")
        if expected_value is not None:
            self.assertEqual(cursor.get_value(), expected_value,
                f"search_near({search_key}): expected value {expected_value}, got {cursor.get_value()}")
        cursor.close()

    def search_near_notfound(self, search_key):
        """Verify search_near returns WT_NOTFOUND."""
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key(search_key)
        ret = cursor.search_near()
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
            f"search_near({search_key}): expected WT_NOTFOUND, got {ret}")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Empty table returns WT_NOTFOUND.
    # -----------------------------------------------------------------------
    def test_search_near_empty(self):
        self.setup_follower()
        self.create_table()

        self.search_near_notfound("anything")

        # Also test after an empty checkpoint.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        self.search_near_notfound("anything")

    # -----------------------------------------------------------------------
    # Test: All data in ingest only (no checkpoint yet).
    # -----------------------------------------------------------------------
    def test_search_near_ingest_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_ingest(["B", "D", "F"])

        # Exact match
        self.search_near_check("D", "D", 0, "val_D")

        # Key smaller than all: should return "B" (exact=1, larger)
        self.search_near_check("A", "B", 1)

        # Key larger than all: should return "F" (exact=-1, smaller)
        self.search_near_check("G", "F", -1)

        # Key between "B" and "D": should return "D" (prefer larger, exact=1)
        self.search_near_check("C", "D", 1)

        # Key between "D" and "F": should return "F" (prefer larger, exact=1)
        self.search_near_check("E", "F", 1)

    # -----------------------------------------------------------------------
    # Test: All data in stable only (everything checkpointed, no new ingest).
    # With only one constituent, the btree search_near may return either
    # neighbor for a non-exact search. Verify exact matches and that non-exact
    # searches return a valid adjacent key.
    # -----------------------------------------------------------------------
    def test_search_near_stable_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["B", "D", "F"])

        # Exact matches always work.
        self.search_near_check("D", "D", 0, "val_D")
        self.search_near_check("B", "B", 0, "val_B")
        self.search_near_check("F", "F", 0, "val_F")

        # Beyond boundaries: only one possible answer.
        self.search_near_check("A", "B", 1)
        self.search_near_check("G", "F", -1)

        # Between keys: btree search_near may land on either neighbor.
        # Verify the returned key is a valid neighbor.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key("C")
        exact = cursor.search_near()
        key = cursor.get_key()
        self.assertIn(key, ["B", "D"],
            f"search_near(C): expected B or D, got {key}")
        if key == "B":
            self.assertEqual(exact, -1)
        else:
            self.assertEqual(exact, 1)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Data split across ingest and stable.
    # Keys in stable: B, F. Keys in ingest: D.
    # search_near("C") should prefer "D" (larger, closer) over "B" (smaller).
    # search_near("E") should prefer "F" (larger, closer) over "D" (smaller).
    # -----------------------------------------------------------------------
    def test_search_near_split_data(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["B", "F"])
        self.insert_ingest(["D"])

        self.search_near_check("D", "D", 0, "val_D")
        self.search_near_check("C", "D", 1)
        self.search_near_check("A", "B", 1)
        self.search_near_check("G", "F", -1)

        # Between D (ingest) and F (stable): the result depends on where the stable
        # btree search_near lands. If it lands on F (larger), we get F. If it lands
        # on B (smaller, can happen with checkpoint cursors), both constituents are
        # smaller than E, and the biggest (D) is returned.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key("E")
        exact = cursor.search_near()
        key = cursor.get_key()
        self.assertIn(key, ["D", "F"],
            f"search_near(E): expected D or F, got {key}")
        if key == "D":
            self.assertEqual(exact, -1)
        else:
            self.assertEqual(exact, 1)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Cursors on opposite sides of search key (the XOR normalization).
    #
    # Stable has "B" (smaller than search), ingest has "F" (larger than search).
    # search_near("D") should prefer "F" (larger key preferred per semantics).
    # -----------------------------------------------------------------------
    def test_search_near_opposite_sides(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["B"])
        self.insert_ingest(["F"])

        # "D" is between B and F. Prefer larger -> "F".
        self.search_near_check("D", "F", 1)

    # -----------------------------------------------------------------------
    # Test: Opposite sides, but larger is farther away.
    # Stable has "C" (smaller), ingest has "Z" (larger).
    # search_near("D") should return "Z" (larger preferred even if farther).
    # -----------------------------------------------------------------------
    def test_search_near_opposite_sides_farther(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["C"])
        self.insert_ingest(["Z"])

        self.search_near_check("D", "Z", 1)

    # -----------------------------------------------------------------------
    # Test: Both constituents on same side (both larger).
    # Stable: "H", Ingest: "F". Search "D".
    # Both are larger, should return closer one: "F".
    # -----------------------------------------------------------------------
    def test_search_near_both_larger(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["H"])
        self.insert_ingest(["F"])

        self.search_near_check("D", "F", 1)

    # -----------------------------------------------------------------------
    # Test: Both constituents on same side (both smaller).
    # Stable: "A", Ingest: "C". Search "D".
    # No larger key exists. Should return bigger of the two: "C".
    # -----------------------------------------------------------------------
    def test_search_near_both_smaller(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A"])
        self.insert_ingest(["C"])

        self.search_near_check("D", "C", -1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable has the same key.
    #
    # Stable: K2, K5, K7. Ingest: K5(tombstone).
    # search_near("K5"):
    #   - ingest_cmp == 0, but value is a tombstone (deleted == true).
    #   - The 'deleted' flag forces stable to also be searched (line 1495).
    #   - ingest_cmp == 0 still wins at line 1512, picking the tombstone.
    #   - Tombstone walk iterates forward: merge sees K7 in stable. Return K7.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K2", "K5", "K7"])
        self.remove_ingest(["K5"])

        # K5 is an exact match in ingest but deleted. Next larger is K7.
        self.search_near_check("K5", "K7", 1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable has key but nothing
    # larger. Should walk backward.
    #
    # Stable: K2, K5. Ingest: K5(tombstone).
    # search_near("K5"):
    #   - Exact ingest match is tombstone. Walk forward: nothing. Walk backward: K2.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_walk_backward(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K2", "K5"])
        self.remove_ingest(["K5"])

        self.search_near_check("K5", "K2", -1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable does NOT have that
    # key but has neighbors.
    #
    # Stable: K3, K7. Ingest: K5(inserted then deleted).
    # search_near("K5"):
    #   - ingest_cmp == 0 (exact on K5 tombstone), deleted == true.
    #   - Stable is searched: search_near("K5") on {K3, K7}.
    #   - ingest_cmp == 0 wins, picks tombstone. Walk forward: merge finds K7.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_stable_no_match(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K3", "K7"])
        self.insert_ingest(["K5"])
        # Now checkpoint so K5 is in stable too, then delete in ingest.
        # Actually, insert K5 in ingest and delete it in ingest (no checkpoint).
        # The insert+remove in ingest without checkpoint keeps K5 only in ingest.
        self.remove_ingest(["K5"])

        # K5 is tombstoned in ingest, not in stable. Walk forward → K7.
        self.search_near_check("K5", "K7", 1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, only ingest has data.
    # All other ingest keys are also tombstoned.
    #
    # Stable: K3, K7. Ingest: K3(tombstone), K5(tombstone), K7(tombstone).
    # Everything is logically deleted → WT_NOTFOUND.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_all_tombstoned(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K3", "K5", "K7"])
        self.remove_ingest(["K3", "K5", "K7"])

        self.search_near_notfound("K5")

    # -----------------------------------------------------------------------
    # Test: Tombstone in ingest hides a stable key (exact match).
    #
    # Stable: K2, K5, K7. Ingest: tombstone(K5).
    # search_near("K5") should NOT return K5. Should return K7 (next larger).
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_exact(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K2", "K5", "K7"])
        self.remove_ingest(["K5"])

        self.search_near_check("K5", "K7", 1)

    # -----------------------------------------------------------------------
    # Test: search_near where the closest key is a tombstone and we must walk
    # backward because nothing larger exists.
    #
    # Stable: K2, K5. Ingest: tombstone(K5).
    # search_near("K5") -> K5 is deleted, nothing after it -> return K2 (cmp=-1).
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_walk_backward(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K2", "K5"])
        self.remove_ingest(["K5"])

        self.search_near_check("K5", "K2", -1)

    # -----------------------------------------------------------------------
    # Test: All keys are tombstoned -> WT_NOTFOUND.
    # -----------------------------------------------------------------------
    def test_search_near_all_deleted(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C"])
        self.remove_ingest(["A", "B", "C"])

        self.search_near_notfound("B")

    # -----------------------------------------------------------------------
    # Test: Tombstone in ingest hides stable key that is NOT the search key.
    #
    # This is the scenario from the code comment in search_near:
    # Stable: K2, K7. Ingest: K2(real), K7(tombstone).
    # search_near("K5"):
    #   - ingest may land on K2 (cmp<0). Not a tombstone.
    #   - stable lands on K7 (cmp>0, larger, preferred).
    #   - But K7 is logically deleted by the tombstone in ingest!
    # The XOR normalization should move ingest forward past K5 to K7,
    # where it sees the tombstone, and correctly skips it.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_cross_table(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["K2", "K7"])

        # Overwrite K2 in ingest (so it exists in both), and tombstone K7
        self.insert_ingest(["K2"], values=["updated_K2"])
        self.remove_ingest(["K7"])

        # K7 is deleted. K2 is the only live key. search_near("K5") -> K2 (cmp=-1).
        self.search_near_check("K5", "K2", -1)

    # -----------------------------------------------------------------------
    # Test: search_near followed by next/prev iteration produces correct order.
    # This verifies that after search_near positions both constituents, the
    # merge iteration works correctly.
    # -----------------------------------------------------------------------
    def test_search_near_then_iterate(self):
        self.setup_follower()
        self.create_table()

        # Stable: A, C, E, G
        self.insert_stable(["A", "C", "E", "G"])
        # Ingest: B, D, F
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near("D") -> exact match on "D"
        cursor.set_key("D")
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), "D")

        # Iterate forward: E, F, G
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "E")
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "F")
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "G")
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)

        cursor.close()

        # Now test backward iteration from search_near
        cursor = session.open_cursor(self.uri)
        cursor.set_key("D")
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        # Iterate backward: C, B, A
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), "C")
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), "B")
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), "A")
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near on a non-exact key, then iterate.
    # Verifies correct merge when the cursor is positioned between keys.
    # -----------------------------------------------------------------------
    def test_search_near_nonexact_then_iterate(self):
        self.setup_follower()
        self.create_table()

        # Stable: B, F
        self.insert_stable(["B", "F"])
        # Ingest: D, H
        self.insert_ingest(["D", "H"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near("C") -> should land on "D" (larger, closer)
        cursor.set_key("C")
        exact = cursor.search_near()
        self.assertEqual(exact, 1)
        self.assertEqual(cursor.get_key(), "D")

        # Forward from D: F, H
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "F")
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "H")
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)

        cursor.close()

        # Backward from search_near("C") -> D, then prev: B
        cursor = session.open_cursor(self.uri)
        cursor.set_key("C")
        exact = cursor.search_near()
        self.assertEqual(exact, 1)
        self.assertEqual(cursor.get_key(), "D")

        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), "B")
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near with tombstone, then iterate past it.
    # Stable: A, C, E. Ingest: tombstone(C).
    # search_near("C") -> skip tombstone, land on E (cmp=1).
    # prev from E -> A. No "C" should appear.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_then_iterate(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.remove_ingest(["C"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near("C") -> C is deleted, next is E
        cursor.set_key("C")
        exact = cursor.search_near()
        self.assertEqual(exact, 1)
        self.assertEqual(cursor.get_key(), "E")

        # prev should go to A (C is deleted)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), "A")
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Multiple tombstones in a row.
    # Stable: A, B, C, D, E. Ingest: tombstone(B), tombstone(C), tombstone(D).
    # search_near("C") -> skip B,C,D -> land on E (cmp=1).
    # -----------------------------------------------------------------------
    def test_search_near_consecutive_tombstones(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E"])
        self.remove_ingest(["B", "C", "D"])

        # search_near("C") -> C,D deleted, E is next non-deleted larger key
        self.search_near_check("C", "E", 1)

        # search_near("B") -> B,C,D deleted, E is next non-deleted larger key
        self.search_near_check("B", "E", 1)

        # search_near("D") -> D deleted, E is next
        self.search_near_check("D", "E", 1)

    # -----------------------------------------------------------------------
    # Test: Verify full forward and backward scan with interleaved data.
    # Stable: A, C, E. Ingest: B, D, F.
    # Full scan should see: A, B, C, D, E, F (forward) and reverse.
    # -----------------------------------------------------------------------
    def test_search_near_full_scan_interleaved(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        expected = ["A", "B", "C", "D", "E", "F"]

        # Position at the start via search_near
        cursor = session.open_cursor(self.uri)
        cursor.set_key("A")
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), "A")

        # Walk forward
        keys = ["A"]
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

        # Walk backward from end
        cursor = session.open_cursor(self.uri)
        cursor.set_key("F")
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        keys = ["F"]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, list(reversed(expected)))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near with duplicate key in both ingest and stable.
    # Stable has old value, ingest has new value. Ingest should win.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_overrides_stable(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["M"], values=["old_value"])
        self.insert_ingest(["M"], values=["new_value"])

        self.search_near_check("M", "M", 0, "new_value")

    # -----------------------------------------------------------------------
    # Test: search_near on a key larger than everything, data in both tables.
    # Stable: B. Ingest: D.
    # search_near("Z") -> should return "D" (closest smaller, from ingest).
    # -----------------------------------------------------------------------
    def test_search_near_beyond_max(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["B"])
        self.insert_ingest(["D"])

        self.search_near_check("Z", "D", -1)

    # -----------------------------------------------------------------------
    # Test: search_near on a key smaller than everything, data in both tables.
    # Stable: M. Ingest: P.
    # search_near("A") -> should return "M" (closest larger, from stable).
    # -----------------------------------------------------------------------
    def test_search_near_before_min(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["M"])
        self.insert_ingest(["P"])

        self.search_near_check("A", "M", 1)
