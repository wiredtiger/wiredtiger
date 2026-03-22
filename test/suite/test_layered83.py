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
#   with a large dataset (1000 keys split across ingest and stable).
#
#   Even-numbered keys go into stable, odd-numbered keys go into ingest.
#   This exercises the merge sort across many page boundaries in both
#   constituent btrees.

@disagg_test_class
class test_layered83(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    uri = 'layered:test_layered83'

    nkeys = 1000

    disagg_storages = gen_disagg_storages('test_layered83', disagg_only=True)

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

    def fmt_key(self, i):
        return f"{i:06d}"

    def fmt_val(self, i):
        return f"val_{i:06d}"

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
        """Insert keys into stable via leader checkpoint."""
        self.insert_on(self.session, keys)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def insert_ingest(self, keys):
        """Insert keys into ingest on the session under test."""
        self.insert_on(self.get_session(), keys)

    def remove_ingest(self, keys):
        """Remove keys on the session under test."""
        self.remove_on(self.get_session(), keys)

    def populate(self):
        """
        Populate with nkeys keys: even keys in stable, odd keys in ingest.
        Returns the sorted list of all expected keys as formatted strings.
        """
        even = list(range(0, self.nkeys, 2))
        odd = list(range(1, self.nkeys, 2))
        self.insert_stable(even)
        self.insert_ingest(odd)
        return [self.fmt_key(i) for i in range(self.nkeys)]

    # =====================================================================
    # Basic iteration with large dataset
    # =====================================================================

    def test_next_full_scan(self):
        """Forward scan of all 1000 interleaved keys."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

    def test_prev_full_scan(self):
        """Backward scan of all 1000 interleaved keys."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        keys = []
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, list(reversed(expected)))
        cursor.close()

    def test_next_duplicate_keys(self):
        """Ingest value wins when both tables have the same key."""
        self.setup_follower()
        self.create_table()

        # Put all keys in stable.
        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        # Override every 10th key in ingest with a different value.
        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        override_keys = list(range(0, self.nkeys, 10))
        for k in override_keys:
            session.begin_transaction()
            cursor[self.fmt_key(k)] = f"ingest_{k:06d}"
            session.commit_transaction(f"commit_timestamp={self.timestamp_str(self.next_ts())}")
        cursor.close()

        # Verify.
        override_set = set(override_keys)
        cursor = session.open_cursor(self.uri)
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
        """Forward scan skips tombstoned keys."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        # Remove every 3rd key.
        removed = list(range(0, self.nkeys, 3))
        self.remove_ingest(removed)

        removed_set = set(removed)
        expected = [self.fmt_key(i) for i in range(self.nkeys) if i not in removed_set]

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

    def test_prev_skips_tombstones(self):
        """Backward scan skips tombstoned keys."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        removed = list(range(0, self.nkeys, 3))
        self.remove_ingest(removed)

        removed_set = set(removed)
        expected = [self.fmt_key(i) for i in range(self.nkeys - 1, -1, -1) if i not in removed_set]

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        keys = []
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

    def test_next_all_tombstoned(self):
        """All keys tombstoned returns NOTFOUND."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)
        self.remove_ingest(all_keys)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # =====================================================================
    # Direction switching with large dataset
    # =====================================================================

    def test_direction_switch_next_to_prev(self):
        """Forward to middle, then switch to backward."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Walk forward to position 500.
        for i in range(501):
            self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[500])

        # Switch to prev and walk back 10 steps.
        for i in range(10):
            self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), expected[490])
        cursor.close()

    def test_direction_switch_prev_to_next(self):
        """Backward to middle, then switch to forward."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Walk backward to position 499 from end.
        for i in range(501):
            self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), expected[499])

        # Switch to next and walk forward 10 steps.
        for i in range(10):
            self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[509])
        cursor.close()

    def test_direction_zigzag(self):
        """Repeated direction switches at every step."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Position at key 500.
        cursor.set_key(expected[500])
        self.assertEqual(cursor.search(), 0)

        # Zigzag: next, prev, next, prev — should oscillate between 500 and 501.
        for _ in range(20):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[501])
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), expected[500])
        cursor.close()

    # =====================================================================
    # Iteration after search
    # =====================================================================

    def test_next_after_search_stable_key(self):
        """search on a stable key, then next iterates correctly."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Search for key 400 (even, in stable).
        cursor.set_key(expected[400])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(400))

        # Iterate forward 10 steps.
        for i in range(1, 11):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[400 + i])
        cursor.close()

    def test_next_after_search_ingest_key(self):
        """search on an ingest key, then next iterates correctly."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Search for key 401 (odd, in ingest).
        cursor.set_key(expected[401])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), self.fmt_val(401))

        for i in range(1, 11):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[401 + i])
        cursor.close()

    def test_prev_after_search(self):
        """search then prev iterates backward correctly."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[500])
        self.assertEqual(cursor.search(), 0)

        for i in range(1, 11):
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), expected[500 - i])
        cursor.close()

    def test_prev_after_search_at_start(self):
        """search at first key, then prev returns NOTFOUND."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[0])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_next_after_search_at_end(self):
        """search at last key, then next returns NOTFOUND."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[-1])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_search_then_direction_switch(self):
        """search, next a few, then switch to prev."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[500])
        self.assertEqual(cursor.search(), 0)

        # Forward 5.
        for i in range(1, 6):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[500 + i])

        # Now at 505. Switch to prev 10.
        for i in range(1, 11):
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), expected[505 - i])
        cursor.close()

    # =====================================================================
    # Iteration after search_near
    # =====================================================================

    def test_next_after_search_near_exact(self):
        """search_near with exact match, then next."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[600])
        exact = cursor.search_near()
        self.assertEqual(exact, 0)
        self.assertEqual(cursor.get_key(), expected[600])

        for i in range(1, 11):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[600 + i])
        cursor.close()

    def test_prev_after_search_near_exact(self):
        """search_near with exact match, then prev."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[600])
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        for i in range(1, 11):
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), expected[600 - i])
        cursor.close()

    def test_next_after_search_near_larger(self):
        """search_near lands on larger key, then next."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Search for a key smaller than everything (space is 0x20, before '0' = 0x30).
        cursor.set_key(" before_all")
        exact = cursor.search_near()
        self.assertGreater(exact, 0)
        # Should have landed on key 0.
        key = cursor.get_key()
        self.assertEqual(key, expected[0])

        # next should give key 1.
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[1])
        cursor.close()

    def test_prev_after_search_near_smaller(self):
        """search_near lands on smaller key, then prev."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Search for a key larger than everything.
        cursor.set_key("999999_after")
        exact = cursor.search_near()
        self.assertLess(exact, 0)
        key = cursor.get_key()
        self.assertEqual(key, expected[-1])

        # prev from last key.
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), expected[-2])
        cursor.close()

    def test_search_near_then_direction_switch(self):
        """search_near, iterate forward, then switch to backward."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[700])
        exact = cursor.search_near()
        self.assertEqual(exact, 0)

        # Forward 5.
        for i in range(1, 6):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[700 + i])

        # Now at 705. Switch to prev.
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), expected[704])
        cursor.close()

    # =====================================================================
    # Iteration with tombstones after search/search_near
    # =====================================================================

    def test_next_after_search_with_tombstones(self):
        """search then next skips consecutive tombstones."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        # Remove keys 501-509.
        removed = list(range(501, 510))
        self.remove_ingest(removed)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)

        # next should skip 501-509 and land on 510.
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(510))
        cursor.close()

    def test_prev_after_search_with_tombstones(self):
        """search then prev skips consecutive tombstones."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        # Remove keys 491-499.
        removed = list(range(491, 500))
        self.remove_ingest(removed)

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(self.fmt_key(500))
        self.assertEqual(cursor.search(), 0)

        # prev should skip 499-491 and land on 490.
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(490))
        cursor.close()

    def test_iterate_after_search_near_tombstone(self):
        """search_near on a tombstoned key, then iterate."""
        self.setup_follower()
        self.create_table()

        all_keys = list(range(self.nkeys))
        self.insert_stable(all_keys)

        # Remove key 500.
        self.remove_ingest([500])

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(self.fmt_key(500))
        exact = cursor.search_near()
        # 500 is deleted, should land on 501 (next larger).
        self.assertEqual(exact, 1)
        self.assertEqual(cursor.get_key(), self.fmt_key(501))

        # prev should go to 499 (500 is deleted).
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), self.fmt_key(499))
        cursor.close()

    # =====================================================================
    # Multiple search + iterate cycles
    # =====================================================================

    def test_repeated_search_iterate(self):
        """Multiple search + next cycles on the same cursor."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # Cycle 1: search 100, iterate to 105.
        cursor.set_key(expected[100])
        self.assertEqual(cursor.search(), 0)
        for i in range(1, 6):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[100 + i])

        # Cycle 2: search 800, iterate to 805.
        cursor.set_key(expected[800])
        self.assertEqual(cursor.search(), 0)
        for i in range(1, 6):
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), expected[800 + i])

        # Cycle 3: search 300, prev to 295.
        cursor.set_key(expected[300])
        self.assertEqual(cursor.search(), 0)
        for i in range(1, 6):
            self.assertEqual(cursor.prev(), 0)
            self.assertEqual(cursor.get_key(), expected[300 - i])
        cursor.close()

    def test_mixed_search_near_and_search(self):
        """search_near + iterate, then search + iterate on same cursor."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        # search_near + next.
        cursor.set_key(expected[200])
        self.assertEqual(cursor.search_near(), 0)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[201])

        # search + prev.
        cursor.set_key(expected[600])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), expected[599])
        cursor.close()

    def test_reset_between_search_iterate(self):
        """reset between search + iterate cycles."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        cursor.set_key(expected[500])
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[501])

        # Reset, then full scan from start.
        cursor.reset()
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), expected[0])
        cursor.close()

    # =====================================================================
    # Edge cases
    # =====================================================================

    def test_next_empty(self):
        """next on empty table returns NOTFOUND."""
        self.setup_follower()
        self.create_table()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_prev_empty(self):
        """prev on empty table returns NOTFOUND."""
        self.setup_follower()
        self.create_table()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    def test_next_after_end_then_rescan(self):
        """Exhaust forward scan, reset, scan again."""
        self.setup_follower()
        self.create_table()
        expected = self.populate()

        session = self.get_session()
        cursor = session.open_cursor(self.uri)

        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)

        cursor.reset()
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.assertEqual(keys, expected)
        cursor.close()

    def test_next_ingest_only(self):
        """Forward scan with all data in ingest only."""
        self.setup_follower()
        self.create_table()

        keys = list(range(self.nkeys))
        self.insert_ingest(keys)

        expected = [self.fmt_key(i) for i in range(self.nkeys)]

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        result = []
        while cursor.next() == 0:
            result.append(cursor.get_key())
        self.assertEqual(result, expected)
        cursor.close()

    def test_next_stable_only(self):
        """Forward scan with all data in stable only."""
        self.setup_follower()
        self.create_table()

        keys = list(range(self.nkeys))
        self.insert_stable(keys)

        expected = [self.fmt_key(i) for i in range(self.nkeys)]

        session = self.get_session()
        cursor = session.open_cursor(self.uri)
        result = []
        while cursor.next() == 0:
            result.append(cursor.get_key())
        self.assertEqual(result, expected)
        cursor.close()
