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
#   Test layered cursor search_near correctness with a larger dataset (1000 keys).
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

    nkeys = 1000

    disagg_storages = gen_disagg_storages('test_layered80', disagg_only=True)

    # Each test runs in two scenarios:
    #   - leader:   test operations run on the leader session. The leader's stable cursor
    #               is a regular R/W btree cursor, so search_near returns the nearest key
    #               predictably. This serves as a baseline correctness check.
    #   - follower: test operations run on the follower session. The follower's stable cursor
    #               is a checkpoint cursor, which may return different neighbors from
    #               search_near. This is where XOR normalization bugs manifest.
    #
    # A follower connection is always created (even in leader mode) because insert_stable()
    # needs it to call disagg_advance_checkpoint().
    role_scenarios = [
        ('leader', dict(role='leader')),
        ('follower', dict(role='follower')),
    ]
    scenarios = make_scenarios(disagg_storages, role_scenarios)

    conn_follow = None
    session_follow = None
    ts = 1

    @staticmethod
    def fmt_key(i):
        return f"{i:06d}"

    @staticmethod
    def fmt_val(i):
        return f"val_{i:06d}"

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        """Create follower connection. Required by insert_stable() for checkpoint advance."""
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def get_session(self):
        """Return the session under test (leader or follower based on scenario)."""
        if self.role == 'leader':
            return self.session
        return self.session_follow

    def get_conn(self):
        """Return the connection under test (leader or follower based on scenario)."""
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

    def insert_stable(self, int_keys, values=None):
        """
        Insert keys into stable: write on leader, checkpoint, advance follower.
        After this, the keys are in the stable table of both leader and follower.
        Accepts a list of integers; formats them internally.
        """
        keys = [self.fmt_key(i) for i in int_keys]
        vals = [self.fmt_val(i) for i in int_keys] if values is None else values
        self.insert_keys_on(self.session, keys, vals)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def insert_ingest(self, int_keys, values=None):
        """
        Insert keys into ingest on the session under test.
        Accepts a list of integers; formats them internally.
        """
        keys = [self.fmt_key(i) for i in int_keys]
        vals = [self.fmt_val(i) for i in int_keys] if values is None else values
        self.insert_keys_on(self.get_session(), keys, vals)

    def remove_ingest(self, int_keys):
        """
        Remove keys on the session under test (tombstones in ingest).
        Accepts a list of integers; formats them internally.
        """
        keys = [self.fmt_key(i) for i in int_keys]
        self.remove_keys_on(self.get_session(), keys)

    def search_near_check(self, search_key, expected_key, expected_exact, expected_value=None):
        """
        Open a cursor, call search_near for search_key, and verify the result.
        Accepts pre-formatted string keys.
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
        """Verify search_near returns WT_NOTFOUND. Accepts a pre-formatted string key."""
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
    # Stable gets even keys 0,2,...,998. Ingest gets odd keys 1,3,...,999.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_only(self):
        self.setup_follower()
        self.create_table()

        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_ingest(odd_keys)

        # Exact match on middle odd key
        mid = 501
        self.search_near_check(self.fmt_key(mid), self.fmt_key(mid), 0, self.fmt_val(mid))

        # Key smaller than all ingest keys: search for key 0, should return key 1 (larger)
        self.search_near_check(self.fmt_key(0), self.fmt_key(1), 1)

        # Key larger than all ingest keys: search for key 1000, should return key 999 (smaller)
        self.search_near_check(self.fmt_key(1000), self.fmt_key(999), -1)

        # Key between two odd keys (e.g. 500 is between 499 and 501): should return 501 (larger)
        self.search_near_check(self.fmt_key(500), self.fmt_key(501), 1)

        # Key between two odd keys (e.g. 100 is between 99 and 101): should return 101 (larger)
        self.search_near_check(self.fmt_key(100), self.fmt_key(101), 1)

    # -----------------------------------------------------------------------
    # Test: All data in stable only (everything checkpointed, no new ingest).
    # With only one constituent, the btree search_near may return either
    # neighbor for a non-exact search. Verify exact matches and that non-exact
    # searches return a valid adjacent key.
    # -----------------------------------------------------------------------
    def test_search_near_stable_only(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        self.insert_stable(even_keys)

        # Exact matches always work.
        self.search_near_check(self.fmt_key(500), self.fmt_key(500), 0, self.fmt_val(500))
        self.search_near_check(self.fmt_key(0), self.fmt_key(0), 0, self.fmt_val(0))
        self.search_near_check(self.fmt_key(998), self.fmt_key(998), 0, self.fmt_val(998))

        # Beyond boundaries: only one possible answer.
        # Key before all: search for a key that sorts before "000000"
        self.search_near_check("", self.fmt_key(0), 1)
        # Key after all: search for key 1000 (beyond 998)
        self.search_near_check(self.fmt_key(1000), self.fmt_key(998), -1)

        # Between keys: btree search_near may land on either neighbor.
        # Verify the returned key is a valid neighbor (e.g. search for 501, neighbors are 500 and 502).
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(501))
        exact = cursor.search_near()
        key = cursor.get_key()
        self.assertIn(key, [self.fmt_key(500), self.fmt_key(502)],
            f"search_near({self.fmt_key(501)}): expected {self.fmt_key(500)} or {self.fmt_key(502)}, got {key}")
        if key == self.fmt_key(500):
            self.assertEqual(exact, -1)
        else:
            self.assertEqual(exact, 1)
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Data split across ingest and stable.
    # Stable: even keys 0,2,...,998. Ingest: odd keys 1,3,...,999.
    # search_near for a gap key should find the correct neighbor.
    # -----------------------------------------------------------------------
    def test_search_near_split_data(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        # Exact match on an ingest key
        self.search_near_check(self.fmt_key(501), self.fmt_key(501), 0, self.fmt_val(501))

        # Exact match on a stable key
        self.search_near_check(self.fmt_key(500), self.fmt_key(500), 0, self.fmt_val(500))

        # Key before all
        self.search_near_check("", self.fmt_key(0), 1)

        # Key after all
        self.search_near_check(self.fmt_key(1100), self.fmt_key(999), -1)

        # Search for a key between two adjacent keys (e.g. between 500 and 501).
        # Use a key that sorts between them: "000500x" sorts after "000500" and before "000501".
        # When the cursors land on opposite sides, the XOR normalization may adjust the
        # ingest cursor position. Verify the returned key exists and iteration works.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key("000500x")
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        key = cursor.get_key()
        # Verify iteration from the positioned cursor produces sorted keys.
        keys = [key]
        for _ in range(5):
            self.assertEqual(cursor.next(), 0)
            keys.append(cursor.get_key())
        self.assertEqual(keys, sorted(keys))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Cursors on opposite sides of search key (the XOR normalization).
    #
    # Stable has lower half keys 0-499, ingest has upper half keys 500-999.
    # search_near(fmt_key(500)) should find exact match 500 in ingest.
    # For a gap key, the XOR normalization moves ingest forward via next(),
    # so the result may be the next ingest key after the search key.
    # -----------------------------------------------------------------------
    def test_search_near_opposite_sides(self):
        self.setup_follower()
        self.create_table()

        lower_keys = list(range(0, 500))
        upper_keys = list(range(500, self.nkeys))
        self.insert_stable(lower_keys)
        self.insert_ingest(upper_keys)

        # Exact match in ingest should still work.
        self.search_near_check(self.fmt_key(500), self.fmt_key(500), 0)

        # Search for a gap key: "000499x" is between stable 499 and ingest 500.
        # The XOR normalization may produce different results depending on the role.
        # Verify the result is valid and iteration from it is sorted.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        cursor.set_key("000499x")
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        key = cursor.get_key()
        # The key must be a real key in the table (integer-formatted).
        self.assertTrue(key.isdigit(), f"Expected numeric key, got {key}")
        # Verify iteration from the positioned cursor produces sorted keys.
        keys = [key]
        for _ in range(5):
            self.assertEqual(cursor.next(), 0)
            keys.append(cursor.get_key())
        self.assertEqual(keys, sorted(keys))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Opposite sides, but only one key in each table.
    # Stable has key 498 (smaller), ingest has key 900 (larger).
    # On the follower: XOR normalization moves ingest forward via next() ->
    # NOTFOUND (only key). Falls back to stable -> 498.
    # On the leader: ingest is skipped, stable search_near returns 498.
    # -----------------------------------------------------------------------
    def test_search_near_opposite_sides_farther(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([498])
        self.insert_ingest([900])

        if self.role == 'leader':
            # Leader skips ingest for search_near but sees it via the btree directly.
            # Both 498 and 900 are in the leader's btree. Prefer 900 (larger).
            self.search_near_check(self.fmt_key(500), self.fmt_key(900), 1)
        else:
            # Follower: XOR normalization moves ingest.next() -> NOTFOUND (only key is 900),
            # falls back to stable key 498.
            self.search_near_check(self.fmt_key(500), self.fmt_key(498), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev() path  ingest has a key between stable and search.
    #
    # Stable: key 200 (smaller). Ingest: keys 300, 900 (300 < search < 900).
    # search(500): ingest lands on 900 (cmp>0), stable on 200 (cmp<0). Opposite sides.
    # prev() on ingest from 900 -> 300. Both now < search. Pick bigger: 300 (ingest).
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_ingest_between(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([300, 900])

        if self.role == 'leader':
            # Leader sees all keys. 900 is larger and preferred.
            self.search_near_check(self.fmt_key(500), self.fmt_key(900), 1)
        else:
            # Follower: prev() from 900 -> 300. Both < 500. Pick bigger: 300.
            self.search_near_check(self.fmt_key(500), self.fmt_key(300), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev() path  ingest prev lands before stable.
    #
    # Stable: key 200 (smaller). Ingest: keys 100, 900.
    # search(500): ingest lands on 900 (cmp>0), stable on 200 (cmp<0).
    # prev() on ingest from 900 -> 100. Both < 500. Pick bigger: 200 (stable).
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_ingest_before_stable(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([100, 900])

        if self.role == 'leader':
            self.search_near_check(self.fmt_key(500), self.fmt_key(900), 1)
        else:
            # prev() from 900 -> 100. Both < 500. Pick bigger: 200 (stable).
            self.search_near_check(self.fmt_key(500), self.fmt_key(200), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev()  single key in ingest (NOTFOUND path).
    #
    # Stable: key 200. Ingest: key 900 (only key).
    # search(500): prev() on ingest from 900 -> NOTFOUND. closest = stable(200).
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_notfound(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([900])

        if self.role == 'leader':
            self.search_near_check(self.fmt_key(500), self.fmt_key(900), 1)
        else:
            # prev() from 900 -> NOTFOUND. closest = stable(200).
            self.search_near_check(self.fmt_key(500), self.fmt_key(200), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev()  ingest prev lands on a key still larger
    # than the search key (i.e. prev doesn't cross past the search key).
    #
    # Stable: key 200. Ingest: keys 600, 900.
    # search(500): ingest lands on 900 (cmp>0), stable on 200 (cmp<0).
    # prev() on ingest from 900 -> 600. compare(600, 500) > 0.
    # ingest_cmp = stable_cmp > 0. Now ingest(600) is > search.
    # Both "larger" after normalization  pick closer of 600 vs stable.
    # But stable(200) is actually SMALLER. This tests how the code handles
    # the overwritten stable_cmp.
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_still_larger(self):
        """
        Stable: 200. Ingest: 600, 900. search(500).
        Ingest search_near(500) -> 600 (closest, cmp>0). Stable -> 200 (cmp<0).
        Opposite sides. prev() from 600 -> NOTFOUND (600 is smallest ingest key).
        closest = stable(200).

        To actually test the "prev lands on key still > search" case, we need
        ingest to have a key before the search key so prev() doesn't exhaust.
        Ingest: 300, 600, 900. search_near(500) -> 600 (cmp>0).
        prev() from 600 -> 300. ingest_cmp = compare(300, 500) < 0.
        Both < 500. Pick bigger: 300 (ingest) > 200 (stable) -> 300.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([300, 600, 900])

        if self.role == 'leader':
            # Leader sees all keys. 600 is the closest larger key.
            self.search_near_check(self.fmt_key(500), self.fmt_key(600), 1)
        else:
            # Follower: ingest search_near(500) -> 600 (cmp>0). stable -> 200 (cmp<0).
            # prev() from 600 -> 300. ingest_cmp = compare(300, 500) < 0.
            # Both < 500. Pick bigger: 300 > 200 -> ingest(300).
            self.search_near_check(self.fmt_key(500), self.fmt_key(300), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev() with many keys in ingest.
    #
    # Stable: keys 0-499. Ingest: keys 500-999.
    # search("000499x"): stable lands on 499 (cmp<0), ingest on 500 (cmp>0).
    # prev() on ingest from 500 -> NOTFOUND (500 is ingest's smallest key).
    # closest = stable(499).
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_boundary(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable(list(range(0, 500)))
        self.insert_ingest(list(range(500, self.nkeys)))

        if self.role == 'leader':
            # Leader sees all keys. "000499x" is between 499 and 500.
            # search_near prefers 500 (larger).
            self.search_near_check("000499x", self.fmt_key(500), 1)
        else:
            # Follower: prev() from 500 -> NOTFOUND (500 is first ingest key).
            # closest = stable(499).
            self.search_near_check("000499x", self.fmt_key(499), -1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization prev() with dense ingest keys.
    #
    # Stable: key 200. Ingest: keys 400, 450, 500, 550, 600, 900.
    # search(500): ingest search_near(500) -> exact match 500 (cmp=0).
    # Exact match wins. No XOR normalization needed.
    # -----------------------------------------------------------------------
    def test_search_near_xor_prev_exact_in_ingest(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([400, 450, 500, 550, 600, 900])

        # Exact match in ingest always wins regardless of role.
        self.search_near_check(self.fmt_key(500), self.fmt_key(500), 0)

    # -----------------------------------------------------------------------
    # Test: XOR normalization  stable larger, ingest smaller.
    # This exercises the OTHER branch (stable_cmp > 0, ingest_cmp < 0).
    #
    # Stable: key 800. Ingest: keys 100, 300.
    # search(500): ingest lands on 300 (cmp<0), stable on 800 (cmp>0).
    # next() on ingest from 300 -> NOTFOUND (300 is last ingest key < 500).
    # Wait, 300 is not the last. next from 300 -> nothing if only [100, 300].
    # NOTFOUND -> closest = stable(800).
    # -----------------------------------------------------------------------
    def test_search_near_xor_next_notfound(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([800])
        self.insert_ingest([100, 300])

        if self.role == 'leader':
            self.search_near_check(self.fmt_key(500), self.fmt_key(800), 1)
        else:
            # next() from 300 -> NOTFOUND. closest = stable(800).
            self.search_near_check(self.fmt_key(500), self.fmt_key(800), 1)

    # -----------------------------------------------------------------------
    # Test: XOR normalization  stable larger, ingest next finds a closer key.
    #
    # Stable: key 800. Ingest: keys 100, 300, 600.
    # search(500): ingest lands on 300 (cmp<0), stable on 800 (cmp>0).
    # next() on ingest from 300 -> 600. Both > 500. Pick closer: 600.
    # -----------------------------------------------------------------------
    def test_search_near_xor_next_closer(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([800])
        self.insert_ingest([100, 300, 600])

        # Both roles: ingest next from 300 -> 600. Compare 600 vs 800. 600 < 800 -> pick ingest(600).
        self.search_near_check(self.fmt_key(500), self.fmt_key(600), 1)

    # -----------------------------------------------------------------------
    # Test: Both constituents on same side (both larger).
    # Stable: key 800, Ingest: key 600. Search for 500.
    # Both are larger, should return closer one: 600.
    # -----------------------------------------------------------------------
    def test_search_near_both_larger(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([800])
        self.insert_ingest([600])

        self.search_near_check(self.fmt_key(500), self.fmt_key(600), 1)

    # -----------------------------------------------------------------------
    # Test: Both constituents on same side (both smaller).
    # Stable: key 100, Ingest: key 400. Search for 500.
    # No larger key exists. Should return bigger of the two: 400.
    # -----------------------------------------------------------------------
    def test_search_near_both_smaller(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([100])
        self.insert_ingest([400])

        self.search_near_check(self.fmt_key(500), self.fmt_key(400), -1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable has the same key.
    #
    # Stable: keys 200, 500, 700. Ingest: tombstone(500).
    # search_near(fmt_key(500)):
    #   - ingest_cmp == 0, but value is a tombstone (deleted == true).
    #   - Tombstone walk iterates forward: merge sees 700 in stable. Return 700.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200, 500, 700])
        self.remove_ingest([500])

        # 500 is an exact match in ingest but deleted. Next larger is 700.
        self.search_near_check(self.fmt_key(500), self.fmt_key(700), 1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable has key but nothing
    # larger. Should walk backward.
    #
    # Stable: keys 200, 500. Ingest: tombstone(500).
    # search_near(fmt_key(500)):
    #   - Exact ingest match is tombstone. Walk forward: nothing. Walk backward: 200.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_walk_backward(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200, 500])
        self.remove_ingest([500])

        self.search_near_check(self.fmt_key(500), self.fmt_key(200), -1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, stable does NOT have that
    # key but has neighbors.
    #
    # Stable: keys 300, 700. Ingest: key 500(inserted then deleted).
    # search_near(fmt_key(500)):
    #   - ingest_cmp == 0 (exact on 500 tombstone), deleted == true.
    #   - Walk forward: merge finds 700.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_stable_no_match(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([300, 700])
        self.insert_ingest([500])
        # Insert 500 in ingest and delete it in ingest (no checkpoint).
        self.remove_ingest([500])

        # 500 is tombstoned in ingest, not in stable. Walk forward -> 700.
        self.search_near_check(self.fmt_key(500), self.fmt_key(700), 1)

    # -----------------------------------------------------------------------
    # Test: Exact match in ingest is a tombstone, only ingest has data.
    # All other ingest keys are also tombstoned.
    #
    # Stable: keys 300, 500, 700. Ingest: tombstones for all three.
    # Everything is logically deleted -> WT_NOTFOUND.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_exact_deleted_all_tombstoned(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([300, 500, 700])
        self.remove_ingest([300, 500, 700])

        self.search_near_notfound(self.fmt_key(500))

    # -----------------------------------------------------------------------
    # Test: Tombstone in ingest hides a stable key (exact match).
    #
    # Stable: keys 200, 500, 700. Ingest: tombstone(500).
    # search_near(fmt_key(500)) should NOT return 500. Should return 700 (next larger).
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_exact(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200, 500, 700])
        self.remove_ingest([500])

        self.search_near_check(self.fmt_key(500), self.fmt_key(700), 1)

    # -----------------------------------------------------------------------
    # Test: search_near where the closest key is a tombstone and we must walk
    # backward because nothing larger exists.
    #
    # Stable: keys 200, 500. Ingest: tombstone(500).
    # search_near(fmt_key(500)) -> 500 is deleted, nothing after it -> return 200 (cmp=-1).
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_walk_backward(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200, 500])
        self.remove_ingest([500])

        self.search_near_check(self.fmt_key(500), self.fmt_key(200), -1)

    # -----------------------------------------------------------------------
    # Test: All keys are tombstoned -> WT_NOTFOUND.
    # Insert 1000 keys into stable, remove all of them in ingest.
    # -----------------------------------------------------------------------
    def test_search_near_all_deleted(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(0, self.nkeys))
        self.insert_stable(all_keys)
        self.remove_ingest(all_keys)

        self.search_near_notfound(self.fmt_key(500))

    # -----------------------------------------------------------------------
    # Test: Tombstone in ingest hides stable key that is NOT the search key.
    #
    # Stable: keys 200, 700. Ingest: key 200(overwritten), tombstone(700).
    # search_near(fmt_key(500)):
    #   - ingest may land on 200 (cmp<0). Not a tombstone.
    #   - stable lands on 700 (cmp>0, larger, preferred).
    #   - But 700 is logically deleted by the tombstone in ingest!
    # The XOR normalization should handle this correctly.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_cross_table(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([200, 700])

        # Overwrite 200 in ingest (so it exists in both), and tombstone 700
        self.insert_ingest([200], values=["updated_000200"])
        self.remove_ingest([700])

        # 700 is deleted. 200 is the only live key. search_near(500) -> 200 (cmp=-1).
        self.search_near_check(self.fmt_key(500), self.fmt_key(200), -1)

    # -----------------------------------------------------------------------
    # Test: search_near followed by next/prev iteration produces correct order.
    # This verifies that after search_near positions both constituents, the
    # merge iteration works correctly.
    # Stable: even keys 0,2,...,998. Ingest: odd keys 1,3,...,999.
    # -----------------------------------------------------------------------
    def test_search_near_then_iterate(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near(fmt_key(500)) -> exact match on 500 (in stable)
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(500))

        # Iterate forward: 501, 502, 503, ...
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(501))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(502))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(503))

        cursor.close()

        # Now test backward iteration from search_near
        cursor = session.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        # Iterate backward: 499, 498, 497
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(499))
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(498))
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(497))

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near on a non-exact key, then iterate.
    # Verifies correct merge when the cursor is positioned between keys.
    # Stable: even keys 0,2,...,198. Ingest: keys 200,400,600,800.
    # -----------------------------------------------------------------------
    def test_search_near_nonexact_then_iterate(self):
        self.setup_follower()
        self.create_table()

        # Use interleaved data so both tables have nearby keys to the search key.
        even_keys = list(range(0, self.nkeys, 2))
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near for a non-existent key between 500 and 501.
        # With both tables having dense data nearby, the result should be
        # one of the adjacent keys.
        cursor.set_key("000500x")
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        landed_key = cursor.get_key()

        # Iterate forward from wherever we landed and verify order.
        keys = [landed_key]
        for _ in range(5):
            self.assertEqual(cursor.next(), 0)
            keys.append(cursor.get_key())
        # Keys should be in sorted order.
        self.assertEqual(keys, sorted(keys))

        cursor.close()

        # Test backward iteration: search_near then prev.
        cursor = session.open_cursor(self.uri)
        cursor.set_key("000500x")
        exact = cursor.search_near()
        landed_key = cursor.get_key()

        keys = [landed_key]
        for _ in range(5):
            self.assertEqual(cursor.prev(), 0)
            keys.append(cursor.get_key())
        # Keys should be in reverse sorted order.
        self.assertEqual(keys, sorted(keys, reverse=True))

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near with tombstone, then iterate past it.
    # Stable: all keys 0-999. Ingest: tombstone(500).
    # search_near(fmt_key(500)) -> skip tombstone, land on 501 (cmp=1).
    # prev from 501 -> 499. No 500 should appear.
    # -----------------------------------------------------------------------
    def test_search_near_tombstone_then_iterate(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(0, self.nkeys))
        self.insert_stable(all_keys)
        self.remove_ingest([500])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near(fmt_key(500)) -> 500 is deleted, next is 501
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertEqual(exact, 1)
        self.assertEqual(cursor.get_key(), self.fmt_key(501))

        # prev should go to 499 (500 is deleted)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(499))

        cursor.close()

    # -----------------------------------------------------------------------
    # Test: Multiple tombstones in a row.
    # Stable: all keys 0-999. Ingest: tombstones for 400-600.
    # search_near(fmt_key(500)) -> skip 400-600 -> land on 601 (cmp=1).
    # -----------------------------------------------------------------------
    def test_search_near_consecutive_tombstones(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(0, self.nkeys))
        tombstoned = list(range(400, 601))
        self.insert_stable(all_keys)
        self.remove_ingest(tombstoned)

        # search_near(fmt_key(500)) -> 400-600 deleted, 601 is next non-deleted larger key
        self.search_near_check(self.fmt_key(500), self.fmt_key(601), 1)

        # search_near(fmt_key(400)) -> 400-600 deleted, 601 is next non-deleted larger key
        self.search_near_check(self.fmt_key(400), self.fmt_key(601), 1)

        # search_near(fmt_key(600)) -> 600 deleted, 601 is next
        self.search_near_check(self.fmt_key(600), self.fmt_key(601), 1)

    # -----------------------------------------------------------------------
    # Test: Verify full forward and backward scan with interleaved data.
    # Stable: even keys 0,2,...,998. Ingest: odd keys 1,3,...,999.
    # Full scan should see all 1000 keys in order.
    # -----------------------------------------------------------------------
    def test_search_near_full_scan_interleaved(self):
        self.setup_follower()
        self.create_table()

        even_keys = list(range(0, self.nkeys, 2))
        odd_keys = list(range(1, self.nkeys, 2))
        self.insert_stable(even_keys)
        self.insert_ingest(odd_keys)

        session = self.get_session()
        expected = [self.fmt_key(i) for i in range(self.nkeys)]

        # Position at the start via search_near
        cursor = session.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(0))
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(0))

        # Walk forward
        keys = [self.fmt_key(0)]
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

        # Walk backward from end
        cursor = session.open_cursor(self.uri)
        cursor.set_key(self.fmt_key(999))
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        keys = [self.fmt_key(999)]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, list(reversed(expected)))
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: search_near with duplicate key in both ingest and stable.
    # Stable has old value, ingest has new value. Ingest should win.
    # Use 1000 keys in stable, override all of them in ingest.
    # -----------------------------------------------------------------------
    def test_search_near_ingest_overrides_stable(self):
        self.setup_follower()
        self.create_table()

        all_keys = list(range(0, self.nkeys))
        old_values = [f"old_{self.fmt_key(i)}" for i in all_keys]
        new_values = [f"new_{self.fmt_key(i)}" for i in all_keys]
        self.insert_stable(all_keys, values=old_values)
        self.insert_ingest(all_keys, values=new_values)

        self.search_near_check(self.fmt_key(500), self.fmt_key(500), 0, f"new_{self.fmt_key(500)}")

    # -----------------------------------------------------------------------
    # Test: search_near on a key larger than everything, data in both tables.
    # Stable: lower half 0-499. Ingest: key 700.
    # search_near(fmt_key(1100)) -> should return 700 (closest smaller, from ingest).
    # -----------------------------------------------------------------------
    def test_search_near_beyond_max(self):
        self.setup_follower()
        self.create_table()

        lower_keys = list(range(0, 500))
        self.insert_stable(lower_keys)
        self.insert_ingest([700])

        self.search_near_check(self.fmt_key(1100), self.fmt_key(700), -1)

    # -----------------------------------------------------------------------
    # Test: search_near on a key smaller than everything, data in both tables.
    # Stable: key 500. Ingest: key 800.
    # search_near for a key before all -> should return 500 (closest larger, from stable).
    # -----------------------------------------------------------------------
    def test_search_near_before_min(self):
        self.setup_follower()
        self.create_table()

        self.insert_stable([500])
        self.insert_ingest([800])

        self.search_near_check("", self.fmt_key(500), 1)

    # -----------------------------------------------------------------------
    # Test: next() then search_near then next() — out of order.
    #
    # Without clearing the iteration flags at the start of search_near,
    # the stale ITERATE_NEXT from a prior next() persists. After
    # search_near returns, the next next() call skips repositioning the
    # alternate cursor. The alternate is wherever search_near left it,
    # which may be behind the returned key.
    #
    # Setup:
    #   Stable: even keys 0-998.
    #   Ingest: odd keys 1-999.
    #
    # 1. Iterate forward to key 499 (ITERATE_NEXT set).
    # 2. search_near(700):
    #    - Both cursors reposition near 700.
    #    - Returns 700 (stable exact) or 701 (ingest, larger preferred).
    # 3. next() after search_near:
    #    BUG:  ITERATE_NEXT stale. next() skips repositioning alternate.
    #          Alternate's search_near position from step 2 might be
    #          behind the returned key. next() returns the behind key.
    #    FIX:  ITERATE flags cleared at start. next() repositions.
    # -----------------------------------------------------------------------
    def test_search_near_stale_iterate_flag_out_of_order(self):
        self.setup_follower()
        self.create_table()

        # Stable: keys 0-698 (all below 700).
        # Ingest: keys 700-999.
        # This forces stable search_near(701) to return 698 (cmp < 0).
        self.insert_stable(list(range(0, 699)))
        self.insert_ingest(list(range(700, self.nkeys)))

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Iterate forward to key 498 (sets ITERATE_NEXT).
        for _ in range(499):
            self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(498))

        # search_near(701): ingest has 701 (exact). Stable has no key >= 700.
        # Stable search_near(701) → 698 (cmp < 0, largest stable key).
        # closest = ingest(701, exact). Alternate = stable(698).
        # next(): ITERATE_NEXT stale → skip reposition.
        # Advance current (ingest): 702. Compare with stable(698).
        # min(702, 698) = 698. Out of order: 701, 698.
        cursor.set_key(self.fmt_key(701))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        sn_key = cursor.get_key()

        # next() after search_near must return > sn_key.
        self.assertEqual(cursor.next(), 0)
        next_key = cursor.get_key()
        self.assertGreater(next_key, sn_key,
            f"Out of order: search_near returned {sn_key}, then next() "
            f"returned {next_key}")

        # Continue iteration — all keys must be monotonically increasing.
        keys = [sn_key, next_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, sorted(keys),
            "Iteration after search_near produced out-of-order keys")
        cursor.close()

    # -----------------------------------------------------------------------
    # Test: prev() then search_near then prev() — mirror of forward case.
    #
    # Setup:
    #   Stable: keys 502-999 (all above search key 500).
    #   Ingest: keys 490-499.
    #
    # 1. Position at key 502 via search, then prev to 501 (ITERATE_PREV).
    #    Wait — 501 is not in any table. Let me use a setup where prev
    #    lands on an ingest key.
    #
    # Revised setup:
    #   Stable: keys 502-999.
    #   Ingest: keys 490-499.
    #
    # 1. search(502), prev() to 499 (ingest). ITERATE_PREV set.
    # 2. search_near(500):
    #    - Ingest: returns 499 (cmp < 0).
    #    - Stable: returns 502 (cmp > 0, nearest stable key).
    #    - Both found. Ingest smaller, stable larger. Prefer larger: 502.
    #    Wait — larger is preferred. search_near returns 502 (cmp > 0).
    #    This doesn't help test the prev direction bug.
    #
    # For the prev bug, we need search_near to return a key SMALLER than
    # the search key, then prev() to go further backward. The stale
    # ITERATE_PREV would skip repositioning the alternate (stable at 502,
    # which is AHEAD in prev direction).
    #
    # Setup (revised):
    #   Stable: keys 502-999.
    #   Ingest: keys 490-499.
    #   search_near(500) -> 502 (stable, cmp > 0) preferred over 499.
    #   After search_near returns 502, prev():
    #     BUG: ITERATE_PREV stale. Skip reposition. Alternate (ingest) at
    #          499 from search_near. Advance current (stable): prev -> 501?
    #          No, stable goes 502->next lower. But 501 not in stable.
    #          stable.prev() -> NOTFOUND (502 is smallest stable key? No,
    #          stable has 502-999. prev from 502 -> doesn't exist below 502).
    #          Hmm, this doesn't work well.
    #
    # Actually the clearest test: use the same pattern as forward but in
    # reverse. After search_near returns a key, verify prev() is ordered.
    # -----------------------------------------------------------------------
    def test_search_near_stale_iterate_prev_ordering(self):
        self.setup_follower()
        self.create_table()

        # Stable: only keys above 500.
        self.insert_stable(list(range(502, 1000)))

        # Ingest: only keys below 500.
        self.insert_ingest(list(range(490, 500)))

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Position at 502 via search, then prev() to 499. ITERATE_PREV set.
        cursor.set_key(self.fmt_key(502))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(499))

        # search_near(500): ingest returns 499 (cmp<0), stable returns 502 (cmp>0).
        # Prefer larger: returns 502.
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        sn_key = cursor.get_key()

        # prev() after search_near: must return < sn_key.
        self.assertEqual(cursor.prev(), 0)
        prev_key = cursor.get_key()
        self.assertLess(prev_key, sn_key,
            f"Out of order (reverse): search_near returned {sn_key}, then "
            f"prev() returned {prev_key}")

        # Continue prev — all keys must be monotonically decreasing.
        keys = [sn_key, prev_key]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, sorted(keys, reverse=True),
            "Reverse iteration after search_near produced out-of-order keys")
        cursor.close()
