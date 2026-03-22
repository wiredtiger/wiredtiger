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

# test_layered82.py
#   Test cursor bounds on layered cursors.
#
#   Bounds are propagated from the layered cursor to both constituent cursors
#   (ingest and stable) via __clayered_copy_bounds. The constituent btree
#   cursors enforce bounds during their own next/prev operations.
#
#   Key scenarios:
#   - Bounds with data split across ingest and stable tables.
#   - Bounds with interleaved keys from both tables.
#   - Bounds with tombstones inside and outside bounds.
#   - Lower-only, upper-only, and both bounds.
#   - Inclusive vs exclusive bound configurations.
#   - Bounds with search_near.
#   - Bounds with reverse iteration (prev).
#   - Clearing bounds.
#   - Bounds set before constituent cursors are opened.

@disagg_test_class
class test_layered82(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered82'

    disagg_storages = gen_disagg_storages('test_layered82', disagg_only=True)

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
        if self.role == 'leader':
            return self.session
        return self.session_follow

    def create_table(self):
        config = "key_format=S,value_format=S"
        self.session.create(self.uri, config)
        self.session_follow.create(self.uri, config)

    def next_ts(self):
        self.ts += 1
        return self.ts

    def insert_stable(self, keys, values=None):
        """Insert keys into stable via leader checkpoint."""
        cursor = self.session.open_cursor(self.uri)
        for i, key in enumerate(keys):
            val = values[i] if values else f"val_{key}"
            self.session.begin_transaction()
            cursor[key] = val
            self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def insert_ingest(self, keys, values=None):
        """Insert keys into ingest on the session under test."""
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        for i, key in enumerate(keys):
            val = values[i] if values else f"val_{key}"
            session.begin_transaction()
            cursor[key] = val
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_ingest(self, keys):
        """Remove keys on the session under test."""
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        for key in keys:
            session.begin_transaction()
            cursor.set_key(key)
            cursor.remove()
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def set_bounds(self, cursor, lower=None, upper=None,
                   lower_inclusive=True, upper_inclusive=True):
        """Set bounds on a cursor. Pass None to skip a bound."""
        if lower is not None:
            cursor.set_key(lower)
            incl = "true" if lower_inclusive else "false"
            cursor.bound(f"bound=lower,inclusive={incl}")
        if upper is not None:
            cursor.set_key(upper)
            incl = "true" if upper_inclusive else "false"
            cursor.bound(f"bound=upper,inclusive={incl}")

    def scan_forward(self, cursor):
        """Scan forward and return list of keys."""
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        return keys

    def scan_backward(self, cursor):
        """Scan backward and return list of keys."""
        keys = []
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        return keys

    # -----------------------------------------------------------------------
    # Test: Basic bounds with all data in ingest.
    # -----------------------------------------------------------------------
    def test_bounds_ingest_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_ingest(["A", "B", "C", "D", "E", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="E")

        self.assertEqual(self.scan_forward(cursor), ["B", "C", "D", "E"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Basic bounds with all data in stable.
    # -----------------------------------------------------------------------
    def test_bounds_stable_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="E")

        self.assertEqual(self.scan_forward(cursor), ["B", "C", "D", "E"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with data split across ingest and stable.
    #
    # Stable: A, C, E. Ingest: B, D, F.
    # Bounds [B, E]. Expected: B, C, D, E.
    # -----------------------------------------------------------------------
    def test_bounds_split_data(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="E")

        self.assertEqual(self.scan_forward(cursor), ["B", "C", "D", "E"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with interleaved data and reverse iteration.
    #
    # Stable: A, C, E. Ingest: B, D, F.
    # Bounds [B, E]. prev should return E, D, C, B.
    # -----------------------------------------------------------------------
    def test_bounds_split_data_prev(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="E")

        self.assertEqual(self.scan_backward(cursor), ["E", "D", "C", "B"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Lower bound only.
    #
    # Data: A-F. Lower bound D. Expected next: D, E, F.
    # -----------------------------------------------------------------------
    def test_bounds_lower_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="D")

        self.assertEqual(self.scan_forward(cursor), ["D", "E", "F"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Upper bound only.
    #
    # Data: A-F. Upper bound C. Expected next: A, B, C.
    # -----------------------------------------------------------------------
    def test_bounds_upper_only(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, upper="C")

        self.assertEqual(self.scan_forward(cursor), ["A", "B", "C"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Exclusive lower bound.
    #
    # Data: A-F. Lower bound B (exclusive). Expected: C, D, E, F.
    # -----------------------------------------------------------------------
    def test_bounds_exclusive_lower(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", lower_inclusive=False)

        self.assertEqual(self.scan_forward(cursor), ["C", "D", "E", "F"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Exclusive upper bound.
    #
    # Data: A-F. Upper bound E (exclusive). Expected: A, B, C, D.
    # -----------------------------------------------------------------------
    def test_bounds_exclusive_upper(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, upper="E", upper_inclusive=False)

        self.assertEqual(self.scan_forward(cursor), ["A", "B", "C", "D"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Both bounds exclusive.
    #
    # Data: A-F. Bounds (B, E) exclusive. Expected: C, D.
    # -----------------------------------------------------------------------
    def test_bounds_both_exclusive(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="E",
                        lower_inclusive=False, upper_inclusive=False)

        self.assertEqual(self.scan_forward(cursor), ["C", "D"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds where bound keys don't exist in the table.
    #
    # Data: B, D, F. Bounds [C, E]. Expected: D.
    # -----------------------------------------------------------------------
    def test_bounds_nonexistent_keys(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["B", "F"])
        self.insert_ingest(["D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="E")

        self.assertEqual(self.scan_forward(cursor), ["D"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds that exclude all data.
    #
    # Data: A, C, E. Bounds [X, Z]. Expected: empty.
    # -----------------------------------------------------------------------
    def test_bounds_no_data_in_range(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "E"])
        self.insert_ingest(["C"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="X", upper="Z")

        self.assertEqual(self.scan_forward(cursor), [])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with tombstones inside the range.
    #
    # Stable: A, B, C, D, E. Ingest: tombstone(C).
    # Bounds [B, D]. Expected: B, D (C is deleted).
    # -----------------------------------------------------------------------
    def test_bounds_tombstone_inside(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E"])
        self.remove_ingest(["C"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="D")

        self.assertEqual(self.scan_forward(cursor), ["B", "D"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with tombstones at the bound keys themselves.
    #
    # Stable: A, B, C, D, E. Ingest: tombstone(B), tombstone(D).
    # Bounds [B, D] inclusive. B and D are deleted. Expected: C.
    # -----------------------------------------------------------------------
    def test_bounds_tombstone_at_bounds(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E"])
        self.remove_ingest(["B", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="D")

        self.assertEqual(self.scan_forward(cursor), ["C"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with all data tombstoned in range.
    #
    # Stable: A, B, C, D, E. Ingest: tombstone(B, C, D).
    # Bounds [B, D]. Expected: empty.
    # -----------------------------------------------------------------------
    def test_bounds_all_tombstoned_in_range(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E"])
        self.remove_ingest(["B", "C", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="D")

        self.assertEqual(self.scan_forward(cursor), [])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with tombstones outside the range.
    #
    # Stable: A, B, C, D, E. Ingest: tombstone(A), tombstone(E).
    # Bounds [B, D]. Expected: B, C, D (tombstones outside range).
    # -----------------------------------------------------------------------
    def test_bounds_tombstone_outside(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "B", "C", "D", "E"])
        self.remove_ingest(["A", "E"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="D")

        self.assertEqual(self.scan_forward(cursor), ["B", "C", "D"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Clearing bounds restores full scan.
    # -----------------------------------------------------------------------
    def test_bounds_clear(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="D")

        self.assertEqual(self.scan_forward(cursor), ["C", "D"])

        # Clear bounds and rescan.
        cursor.reset()
        cursor.bound("action=clear")

        self.assertEqual(self.scan_forward(cursor), ["A", "B", "C", "D", "E"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near respects bounds.
    #
    # Data: A, B, C, D, E. Bounds [C, E].
    # search_near("A") should not return A (outside bounds).
    # -----------------------------------------------------------------------
    def test_bounds_search_near(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="E")

        # search_near for a key below bounds should find C.
        cursor.set_key("A")
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        self.assertGreaterEqual(cursor.get_key(), "C")
        self.assertLessEqual(cursor.get_key(), "E")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near respects upper bound.
    #
    # Data: A, B, C, D, E. Bounds [A, C].
    # search_near("E") should not return E (outside bounds).
    # -----------------------------------------------------------------------
    def test_bounds_search_near_upper(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="A", upper="C")

        cursor.set_key("E")
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        self.assertGreaterEqual(cursor.get_key(), "A")
        self.assertLessEqual(cursor.get_key(), "C")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with interleaved data, many keys.
    #
    # Stable: even keys 00-98. Ingest: odd keys 01-99.
    # Bounds [20, 30]. Expected: 20-30.
    # -----------------------------------------------------------------------
    def test_bounds_many_keys(self):
        self.setup_follower()
        self.create_table()

        even_keys = [f"{i:02d}" for i in range(0, 100, 2)]
        odd_keys = [f"{i:02d}" for i in range(1, 100, 2)]
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="20", upper="30")

        expected = [f"{i:02d}" for i in range(20, 31)]
        self.assertEqual(self.scan_forward(cursor), expected)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with many keys, reverse iteration.
    # -----------------------------------------------------------------------
    def test_bounds_many_keys_prev(self):
        self.setup_follower()
        self.create_table()

        even_keys = [f"{i:02d}" for i in range(0, 100, 2)]
        odd_keys = [f"{i:02d}" for i in range(1, 100, 2)]
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="20", upper="30")

        expected = [f"{i:02d}" for i in range(30, 19, -1)]
        self.assertEqual(self.scan_backward(cursor), expected)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds set before constituent cursors are opened.
    #
    # On the follower, if no checkpoint has been picked up yet, the stable
    # cursor may not be open. Setting bounds first should still work.
    # -----------------------------------------------------------------------
    def test_bounds_set_before_data(self):
        self.setup_follower()
        self.create_table()

        # Set bounds on an empty table.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="F")

        # No data yet.
        self.assertEqual(self.scan_forward(cursor), [])

        # Now insert data (some inside, some outside bounds).
        cursor.close()
        self.insert_ingest(["A", "B", "C", "D", "E", "F", "G"])

        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="F")
        self.assertEqual(self.scan_forward(cursor), ["C", "D", "E", "F"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with ingest overriding stable value.
    #
    # Stable: C="old". Ingest: C="new".
    # Bounds [C, C]. Should return C="new".
    # -----------------------------------------------------------------------
    def test_bounds_ingest_overrides_stable(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"], values=["va", "old", "ve"])
        self.insert_ingest(["C"], values=["new"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="C", upper="C")

        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), "C")
        self.assertEqual(cursor.get_value(), "new")
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with adjacent inclusive/exclusive.
    #
    # Data: A, B, C. Bounds (A, C) both exclusive. Expected: B.
    # -----------------------------------------------------------------------
    def test_bounds_adjacent_exclusive(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C"])
        self.insert_ingest(["B"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="A", upper="C",
                        lower_inclusive=False, upper_inclusive=False)

        self.assertEqual(self.scan_forward(cursor), ["B"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Single-point bound (lower == upper, both inclusive).
    #
    # Data: A, B, C. Bounds [B, B]. Expected: B only.
    # -----------------------------------------------------------------------
    def test_bounds_single_point(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C"])
        self.insert_ingest(["B"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="B")

        self.assertEqual(self.scan_forward(cursor), ["B"])
        # Also test prev.
        self.set_bounds(cursor, lower="B", upper="B")
        self.assertEqual(self.scan_backward(cursor), ["B"])
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Bounds with search for key inside range.
    #
    # Data: A-E. Bounds [B, D]. search("C") should succeed.
    # search("A") should fail (outside bounds).
    # -----------------------------------------------------------------------
    def test_bounds_search(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.set_bounds(cursor, lower="B", upper="D")

        cursor.set_key("C")
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "val_C")

        # search for key outside bounds should fail.
        cursor.set_key("A")
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

        cursor.set_key("E")
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Rebinding bounds narrows the range.
    # -----------------------------------------------------------------------
    def test_bounds_rebind(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(["A", "C", "E"])
        self.insert_ingest(["B", "D", "F"])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Wide bounds.
        self.set_bounds(cursor, lower="A", upper="F")
        self.assertEqual(self.scan_forward(cursor), ["A", "B", "C", "D", "E", "F"])

        # Narrow bounds without clearing first (rebind is allowed).
        self.set_bounds(cursor, lower="C", upper="D")
        self.assertEqual(self.scan_forward(cursor), ["C", "D"])
        cursor.close()
