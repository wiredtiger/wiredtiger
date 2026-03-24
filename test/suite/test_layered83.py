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

# test_layered83.py
#   Test cursor iteration and iteration after search/search_near on layered cursors
#   with a 1000-key dataset (even keys in stable, odd keys in ingest).

@disagg_test_class
class test_layered83(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered83'
    nkeys = 1000

    disagg_storages = gen_disagg_storages('test_layered83', disagg_only=True)

    # Each test runs in two scenarios:
    #   - leader:   test operations run on the leader session (R/W stable cursor).
    #               Baseline check that merged iteration produces correct order.
    #   - follower: test operations run on the follower session (checkpoint stable cursor).
    #               This is where XOR normalization and alternate cursor positioning bugs
    #               cause out-of-order keys.
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

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        """Create follower connection. Required by insert_stable() for checkpoint advance."""
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

    def fmt_key(self, i):
        return f"{i:06d}"

    def fmt_val(self, i):
        return f"val_{i:06d}"

    def key_range(self, lo, hi):
        """Return formatted keys for [lo, hi] inclusive."""
        return [self.fmt_key(i) for i in range(lo, hi + 1)]

    def all_keys(self):
        """Return formatted keys for [0, nkeys-1]."""
        return self.key_range(0, self.nkeys - 1)

    def key_range_reversed(self, lo, hi):
        """Return formatted keys for [hi, lo] descending, inclusive."""
        return [self.fmt_key(i) for i in range(hi, lo - 1, -1)]

    # -----------------------------------------------------------------------
    # Low-level insert/remove helpers.
    # -----------------------------------------------------------------------

    def insert_on(self, session, keys):
        cursor = session.open_cursor(self.uri)
        for k in keys:
            session.begin_transaction()
            cursor[self.fmt_key(k)] = self.fmt_val(k)
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def remove_on(self, session, keys):
        cursor = session.open_cursor(self.uri)
        for k in keys:
            session.begin_transaction()
            cursor.set_key(self.fmt_key(k))
            cursor.remove()
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

    def insert_stable(self, keys):
        """Insert keys (list of ints) into stable via leader checkpoint."""
        self.insert_on(self.session, keys)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def insert_ingest(self, keys):
        """Insert keys (list of ints) into ingest on the session under test."""
        self.insert_on(self.get_session(), keys)

    def remove_ingest(self, keys):
        """Remove keys (list of ints) on the session under test."""
        self.remove_on(self.get_session(), keys)

    # -----------------------------------------------------------------------
    # Data population helpers.
    # -----------------------------------------------------------------------

    def populate_interleaved(self):
        """Even keys in stable, odd keys in ingest."""
        self.insert_stable(list(range(0, self.nkeys, 2)))
        self.insert_ingest(list(range(1, self.nkeys, 2)))

    def populate_all_stable(self):
        """All keys in stable."""
        self.insert_stable(list(range(self.nkeys)))

    # -----------------------------------------------------------------------
    # Cursor helpers.
    # -----------------------------------------------------------------------

    def open_cursor(self):
        """Open a cursor on the session under test."""
        return self.get_session().open_cursor(self.uri)

    def scan_forward(self, cursor):
        """Return all keys from forward scan."""
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        return keys

    def scan_backward(self, cursor):
        """Return all keys from backward scan."""
        keys = []
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        return keys

    def walk_next(self, cursor, n):
        """Call next() n times, returning the list of keys visited."""
        keys = []
        for _ in range(n):
            self.assertEqual(cursor.next(), 0)
            keys.append(cursor.get_key())
        return keys

    def walk_prev(self, cursor, n):
        """Call prev() n times, returning the list of keys visited."""
        keys = []
        for _ in range(n):
            self.assertEqual(cursor.prev(), 0)
            keys.append(cursor.get_key())
        return keys

    def search_key(self, cursor, key_int):
        """Search for a key by integer and verify success."""
        cursor.set_key(self.fmt_key(key_int))
        self.assertEqual(cursor.search(), 0)

    # =====================================================================
    # Basic iteration
    # =====================================================================

    def test_next_full_scan(self):
        """Forward scan of all 1000 interleaved keys."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.assertEqual(self.scan_forward(cursor), self.all_keys())
        cursor.close()

    def test_prev_full_scan(self):
        """Backward scan of all 1000 interleaved keys."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.assertEqual(self.scan_backward(cursor), list(reversed(self.all_keys())))
        cursor.close()

    def test_next_duplicate_keys(self):
        """Ingest value wins when both tables have the same key."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()

        # Override every 10th key in ingest.
        override_keys = list(range(0, self.nkeys, 10))
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        for k in override_keys:
            session.begin_transaction()
            cursor[self.fmt_key(k)] = f"ingest_{k:06d}"
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

        # Verify.
        override_set = set(override_keys)
        cursor = self.open_cursor()
        count = 0
        while cursor.next() == 0:
            k = int(cursor.get_key())
            if k in override_set:
                self.assertEqual(cursor.get_value(), f"ingest_{k:06d}")
            else:
                self.assertEqual(cursor.get_value(), self.fmt_val(k))
            count += 1
        self.assertEqual(count, self.nkeys)
        cursor.close()

    # =====================================================================
    # Iteration with tombstones
    # =====================================================================

    def test_next_skips_tombstones(self):
        """Forward scan skips every 3rd key (tombstoned)."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()

        removed = set(range(0, self.nkeys, 3))
        self.remove_ingest(list(removed))

        cursor = self.open_cursor()
        expected = [self.fmt_key(i) for i in range(self.nkeys) if i not in removed]
        self.assertEqual(self.scan_forward(cursor), expected)
        cursor.close()

    def test_prev_skips_tombstones(self):
        """Backward scan skips every 3rd key (tombstoned)."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()

        removed = set(range(0, self.nkeys, 3))
        self.remove_ingest(list(removed))

        cursor = self.open_cursor()
        expected = [self.fmt_key(i) for i in range(self.nkeys - 1, -1, -1) if i not in removed]
        self.assertEqual(self.scan_backward(cursor), expected)
        cursor.close()

    def test_next_all_tombstoned(self):
        """All keys tombstoned returns NOTFOUND on forward scan."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()
        self.remove_ingest(list(range(self.nkeys)))

        cursor = self.open_cursor()
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_prev_all_tombstoned(self):
        """All keys tombstoned returns NOTFOUND on backward scan."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()
        self.remove_ingest(list(range(self.nkeys)))

        cursor = self.open_cursor()
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # =====================================================================
    # Direction switching
    # =====================================================================

    def test_direction_switch_next_to_prev(self):
        """Forward to key 500, then switch to backward 10 steps."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.walk_next(cursor, 501)
        self.assertEqual(cursor.get_key(), self.fmt_key(500))

        self.walk_prev(cursor, 10)
        self.assertEqual(cursor.get_key(), self.fmt_key(490))
        cursor.close()

    def test_direction_switch_prev_to_next(self):
        """Backward to key 499, then switch to forward 10 steps."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.walk_prev(cursor, 501)
        self.assertEqual(cursor.get_key(), self.fmt_key(499))

        self.walk_next(cursor, 10)
        self.assertEqual(cursor.get_key(), self.fmt_key(509))
        cursor.close()

    def test_direction_zigzag(self):
        """Repeated direction switches oscillate between key 500 and 501."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 500)

        for _ in range(20):
            self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(501)])
            self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(500)])
        cursor.close()

    # =====================================================================
    # Iteration after search
    # =====================================================================

    def test_next_after_search_stable_key(self):
        """search on a stable key (400, even), then next 10 steps."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 400)
        self.assertEqual(cursor.get_value(), self.fmt_val(400))

        self.assertEqual(self.walk_next(cursor, 10), self.key_range(401, 410))
        cursor.close()

    def test_next_after_search_ingest_key(self):
        """search on an ingest key (401, odd), then next 10 steps."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 401)
        self.assertEqual(cursor.get_value(), self.fmt_val(401))

        self.assertEqual(self.walk_next(cursor, 10), self.key_range(402, 411))
        cursor.close()

    def test_prev_after_search(self):
        """search key 500, then prev 10 steps."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 500)

        self.assertEqual(self.walk_prev(cursor, 10), self.key_range_reversed(490, 499))
        cursor.close()

    def test_prev_after_search_at_start(self):
        """search at first key, then prev returns NOTFOUND."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 0)
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_next_after_search_at_end(self):
        """search at last key, then next returns NOTFOUND."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, self.nkeys - 1)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_search_then_direction_switch(self):
        """search 500, forward 5, then switch to prev 10."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 500)

        # Forward 5 steps: 501-505.
        self.assertEqual(self.walk_next(cursor, 5), self.key_range(501, 505))
        # Now at 505. Prev 10 steps: 504-495.
        self.assertEqual(self.walk_prev(cursor, 10), self.key_range_reversed(495, 504))
        cursor.close()

    # =====================================================================
    # Iteration after search_near
    # =====================================================================

    def test_next_after_search_near_exact(self):
        """search_near with exact match at 600, then next 10."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(600))
        self.assertEqual(cursor.search_near(), 0)

        self.assertEqual(self.walk_next(cursor, 10), self.key_range(601, 610))
        cursor.close()

    def test_prev_after_search_near_exact(self):
        """search_near with exact match at 600, then prev 10."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(600))
        self.assertEqual(cursor.search_near(), 0)

        self.assertEqual(self.walk_prev(cursor, 10), self.key_range_reversed(590, 599))
        cursor.close()

    def test_next_after_search_near_larger(self):
        """search_near for key before all data, lands on first key, then next."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        cursor.set_key(" before_all")
        self.assertGreater(cursor.search_near(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(0))

        self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(1)])
        cursor.close()

    def test_prev_after_search_near_smaller(self):
        """search_near for key after all data, lands on last key, then prev."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        cursor.set_key("999999_after")
        self.assertLess(cursor.search_near(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(self.nkeys - 1))

        self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(self.nkeys - 2)])
        cursor.close()

    def test_search_near_then_direction_switch(self):
        """search_near 700, forward 5, then switch to prev."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(700))
        self.assertEqual(cursor.search_near(), 0)

        # Forward 5: 701-705.
        self.assertEqual(self.walk_next(cursor, 5), self.key_range(701, 705))
        # Now at 705. Switch to prev.
        self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(704)])
        cursor.close()

    # =====================================================================
    # Iteration with tombstones after search/search_near
    # =====================================================================

    def test_next_after_search_with_tombstones(self):
        """search 500, next skips tombstoned 501-509, lands on 510."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()
        self.remove_ingest(list(range(501, 510)))

        cursor = self.open_cursor()
        self.search_key(cursor, 500)
        self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(510)])
        cursor.close()

    def test_prev_after_search_with_tombstones(self):
        """search 500, prev skips tombstoned 491-499, lands on 490."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()
        self.remove_ingest(list(range(491, 500)))

        cursor = self.open_cursor()
        self.search_key(cursor, 500)
        self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(490)])
        cursor.close()

    def test_iterate_after_search_near_tombstone(self):
        """search_near on tombstoned 500 lands on 501, prev gives 499."""
        self.setup_follower()
        self.create_table()
        self.populate_all_stable()
        self.remove_ingest([500])

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search_near(), 1)
        self.assertEqual(cursor.get_key(), self.fmt_key(501))

        self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(499)])
        cursor.close()

    # =====================================================================
    # Multiple search + iterate cycles
    # =====================================================================

    def test_repeated_search_iterate(self):
        """Multiple search + iterate cycles on the same cursor."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()

        # Cycle 1: search 100, next 5 -> 101-105.
        self.search_key(cursor, 100)
        self.assertEqual(self.walk_next(cursor, 5), self.key_range(101, 105))

        # Cycle 2: search 800, next 5 -> 801-805.
        self.search_key(cursor, 800)
        self.assertEqual(self.walk_next(cursor, 5), self.key_range(801, 805))

        # Cycle 3: search 300, prev 5 -> 299-295.
        self.search_key(cursor, 300)
        self.assertEqual(self.walk_prev(cursor, 5), self.key_range_reversed(295, 299))
        cursor.close()

    def test_mixed_search_near_and_search(self):
        """search_near + next, then search + prev on same cursor."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()

        cursor.set_key(self.fmt_key(200))
        self.assertEqual(cursor.search_near(), 0)
        self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(201)])

        self.search_key(cursor, 600)
        self.assertEqual(self.walk_prev(cursor, 1), [self.fmt_key(599)])
        cursor.close()

    def test_reset_between_search_iterate(self):
        """reset between search + iterate cycles, then rescan from start."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        self.search_key(cursor, 500)
        self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(501)])

        cursor.reset()
        self.assertEqual(self.walk_next(cursor, 1), [self.fmt_key(0)])
        cursor.close()

    # =====================================================================
    # Edge cases
    # =====================================================================

    def test_next_empty(self):
        """next on empty table returns NOTFOUND."""
        self.setup_follower()
        self.create_table()

        cursor = self.open_cursor()
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_prev_empty(self):
        """prev on empty table returns NOTFOUND."""
        self.setup_follower()
        self.create_table()

        cursor = self.open_cursor()
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_next_after_end_then_rescan(self):
        """Exhaust forward scan, reset, scan again produces same result."""
        self.setup_follower()
        self.create_table()
        self.populate_interleaved()

        cursor = self.open_cursor()
        all_expected = self.all_keys()
        self.assertEqual(self.scan_forward(cursor), all_expected)
        cursor.reset()
        self.assertEqual(self.scan_forward(cursor), all_expected)
        cursor.close()

    def test_next_ingest_only(self):
        """Forward scan with all data in ingest only."""
        self.setup_follower()
        self.create_table()
        self.insert_ingest(list(range(self.nkeys)))

        cursor = self.open_cursor()
        self.assertEqual(self.scan_forward(cursor), self.all_keys())
        cursor.close()

    def test_next_stable_only(self):
        """Forward scan with all data in stable only."""
        self.setup_follower()
        self.create_table()
        self.insert_stable(list(range(self.nkeys)))

        cursor = self.open_cursor()
        self.assertEqual(self.scan_forward(cursor), self.all_keys())
        cursor.close()

    # -----------------------------------------------------------------------
    # Out-of-order iteration tests after search_near.
    #
    # These test the scenario where the XOR normalization in search_near
    # leaves the alternate cursor behind the chosen closest key. If next()
    # doesn't skip stale positions on the alternate, keys come out of order.
    # -----------------------------------------------------------------------

    def test_next_after_search_near_xor_alternate_behind(self):
        """
        Stable: 200. Ingest: 300, 600. search_near(500).
        Ingest search_near(500) -> 600 (cmp>0). Stable -> 200 (cmp<0).
        XOR prev() on ingest: 600 -> 300. Both < 500. Pick bigger: ingest(300).
        Alternate (stable) is at 200, which is BEHIND 300.
        next() must not output 200 before 300. Sequence must be monotonic.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable([200])
        self.insert_ingest([300, 600])

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        first_key = cursor.get_key()

        # Collect all remaining keys via next().
        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        # Verify monotonically increasing order.
        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} >= {keys[i + 1]}. Full: {keys}")
        cursor.close()

    def test_prev_after_search_near_xor_alternate_ahead(self):
        """
        Mirror test for prev(). Stable: 800. Ingest: 400, 700. search_near(500).
        Ingest search_near(500) -> 400 (cmp<0). Stable -> 800 (cmp>0).
        XOR next() on ingest: 400 -> 700. Both > 500. Pick smaller: ingest(700).
        Alternate (stable) is at 800, which is AHEAD of 700.
        prev() must not output 800 after 700. Sequence must be monotonically decreasing.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable([800])
        self.insert_ingest([400, 700])

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertGreater(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} <= {keys[i + 1]}. Full: {keys}")
        cursor.close()

    def test_next_after_search_near_both_smaller(self):
        """
        Non-XOR "both smaller" case. Stable: 300. Ingest: 400. search_near(500).
        Both < 500. Pick bigger: ingest(400). Alternate (stable at 300) is behind.
        next() after 400 must not output 300.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable([300])
        self.insert_ingest([400])

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} >= {keys[i + 1]}. Full: {keys}")
        cursor.close()

    def test_prev_after_search_near_both_larger(self):
        """
        Non-XOR "both larger" case. Stable: 700. Ingest: 600. search_near(500).
        Both > 500. Pick smaller: ingest(600). Alternate (stable at 700) is ahead.
        prev() after 600 must not output 700.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable([700])
        self.insert_ingest([600])

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertGreater(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} <= {keys[i + 1]}. Full: {keys}")
        cursor.close()

    def test_next_after_search_near_xor_many_keys(self):
        """
        Large-scale XOR test. Stable: even keys [0, 400]. Ingest: odd keys [501, 999].
        search_near(450): stable finds ~450, ingest finds 501. Opposite sides.
        XOR normalization moves ingest backward. After search_near, iterate forward
        through ALL remaining keys and verify monotonic order.
        """
        self.setup_follower()
        self.create_table()

        stable_keys = list(range(0, 401, 2))     # 0, 2, 4, ..., 400
        ingest_keys = list(range(501, 1000, 2))   # 501, 503, ..., 999
        self.insert_stable(stable_keys)
        self.insert_ingest(ingest_keys)

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(450))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} >= {keys[i + 1]}")
        cursor.close()

    def test_prev_after_search_near_xor_many_keys(self):
        """
        Large-scale XOR test (reverse). Stable: even keys [600, 999]. Ingest: odd keys [1, 499].
        search_near(550): stable finds ~550, ingest finds ~499. Opposite sides.
        XOR normalization moves ingest forward. After search_near, iterate backward
        through ALL remaining keys and verify monotonically decreasing order.
        """
        self.setup_follower()
        self.create_table()

        stable_keys = list(range(600, 1000, 2))   # 600, 602, ..., 998
        ingest_keys = list(range(1, 500, 2))       # 1, 3, ..., 499
        self.insert_stable(stable_keys)
        self.insert_ingest(ingest_keys)

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(550))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()

        keys = [first_key]
        while cursor.prev() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertGreater(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} <= {keys[i + 1]}")
        cursor.close()

    def test_next_after_search_near_xor_with_tombstones(self):
        """
        XOR + tombstones. Stable: all keys [0, 999]. Ingest: tombstones [400, 600].
        search_near(500): ingest finds tombstone. Tombstone walk starts.
        After resolution, iterate forward and verify monotonic order.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable(list(range(self.nkeys)))
        self.remove_ingest(list(range(400, 601)))

        cursor = self.open_cursor()
        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
        first_key = cursor.get_key()

        # The returned key should be outside the tombstoned range.
        self.assertTrue(first_key < self.fmt_key(400) or first_key > self.fmt_key(600),
            f"search_near returned tombstoned key: {first_key}")

        keys = [first_key]
        while cursor.next() == 0:
            keys.append(cursor.get_key())

        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1],
                f"Out of order at position {i}: {keys[i]} >= {keys[i + 1]}")

        # None of the tombstoned keys should appear.
        tombstoned = set(self.fmt_key(i) for i in range(400, 601))
        for k in keys:
            self.assertNotIn(k, tombstoned, f"Tombstoned key appeared: {k}")
        cursor.close()

    def test_next_after_search_near_interleaved_full_coverage(self):
        """
        Full interleaved coverage test. Even keys in stable, odd in ingest.
        For each search key at positions 0, 100, 250, 500, 750, 999:
        do search_near then iterate forward, verifying strict monotonic order.
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable(list(range(0, self.nkeys, 2)))
        self.insert_ingest(list(range(1, self.nkeys, 2)))

        for search_pos in [0, 100, 250, 500, 750, 999]:
            cursor = self.open_cursor()
            cursor.set_key(self.fmt_key(search_pos))
            exact = cursor.search_near()
            self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
            first_key = cursor.get_key()

            keys = [first_key]
            while cursor.next() == 0:
                keys.append(cursor.get_key())

            for i in range(len(keys) - 1):
                self.assertLess(keys[i], keys[i + 1],
                    f"search_pos={search_pos}: out of order at {i}: "
                    f"{keys[i]} >= {keys[i + 1]}")
            cursor.close()

    def test_prev_after_search_near_interleaved_full_coverage(self):
        """
        Same as above but with prev() (reverse iteration).
        """
        self.setup_follower()
        self.create_table()

        self.insert_stable(list(range(0, self.nkeys, 2)))
        self.insert_ingest(list(range(1, self.nkeys, 2)))

        for search_pos in [0, 100, 250, 500, 750, 999]:
            cursor = self.open_cursor()
            cursor.set_key(self.fmt_key(search_pos))
            exact = cursor.search_near()
            self.assertNotEqual(exact, wiredtiger.WT_NOTFOUND)
            first_key = cursor.get_key()

            keys = [first_key]
            while cursor.prev() == 0:
                keys.append(cursor.get_key())

            for i in range(len(keys) - 1):
                self.assertGreater(keys[i], keys[i + 1],
                    f"search_pos={search_pos}: out of order at {i}: "
                    f"{keys[i]} <= {keys[i + 1]}")
            cursor.close()
