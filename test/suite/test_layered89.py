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

import time
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered89.py
#    Follower ingest chunk garbage collection (replacement for the GC portions of test_layered38).
#
#    Tests the chunk-server driven drop path end-to-end: writes on the follower trigger rollover,
#    leader-side stable checkpoint advances the follower's prune_timestamp, and the chunk server
#    (background thread) drops obsolete oldest ingest chunks. Assertions use layered_ingest stats
#    and metadata inspection rather than eviction, matching the new "cheap obsolete" design.


@disagg_test_class
class test_layered89(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered89', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Table basename must not end in digits (ingest rollover derives chunk names by stripping a
    # trailing ".<digits>" segment; see test_layered88 comment).
    table_name = 'test_layered_eighty_nine'
    uri = 'layered:test_layered_eighty_nine'
    primary_ingest = 'file:test_layered_eighty_nine.wt_ingest'
    chunk1_ingest = 'file:test_layered_eighty_nine.1.wt_ingest'
    chunk2_ingest = 'file:test_layered_eighty_nine.2.wt_ingest'

    conn_base_config = 'cache_size=50MB,statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    chunk_max_ops = 5

    session_follow = None
    conn_follow = None

    def create_follower(self):
        follower_cfg = (
            'disaggregated=(role="follower",'
            f'layered_ingest_chunk_max_ops={self.chunk_max_ops})')
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config + follower_cfg)
        self.session_follow = self.conn_follow.open_session('')

    def setup_leader_seeded_table(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri, None, None)
        c['seed'] = 'leader'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

    def open_follower_after_checkpoint(self):
        self.setup_leader_seeded_table()
        self.create_follower()
        self.disagg_advance_checkpoint(self.conn_follow)

    def conn_stat(self, conn, stat_id):
        """
        Return the conn-scope stat value for stat_id. We cache the cursor per-connection on the
        test instance to avoid opening (and leaking) a new session on every poll, which quickly
        exhausts the default session pool on the tight wait_for_* loops.
        """
        if not hasattr(self, '_stat_cursors'):
            self._stat_cursors = {}
        c = self._stat_cursors.get(id(conn))
        if c is None:
            c = conn.open_session('').open_cursor('statistics:', None, None)
            self._stat_cursors[id(conn)] = c
        c.reset()
        v = c[stat_id][2]
        return v

    def layered_table_config(self, session):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(self.uri)
        self.assertEqual(md.search(), 0)
        value = md.get_value()
        md.close()
        return value

    def metadata_has_key(self, session, key):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(key)
        found = md.search() == 0
        md.close()
        return found

    def follower_puts(self, n, key_offset=0, ts_offset=10, session=None):
        """Follower layered inserts: one put per call counts as one ingest op."""
        if session is None:
            session = self.session_follow
        cursor = session.open_cursor(self.uri, None, None)
        for i in range(n):
            session.begin_transaction()
            k = key_offset + i
            cursor[f'k{k}'] = f'v{k}'
            session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts_offset + i)}')
        cursor.close()

    def advance_leader_stable_and_checkpoint(self, ts):
        bump = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        bump[f'bump-{ts}'] = 'leader'
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        bump.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()

    def wait_for_passes(self, n=2, timeout_s=10.0):
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
            if cur >= start + n:
                return cur
            time.sleep(0.05)
        self.fail(f'chunk server did not complete {n} passes within {timeout_s}s '
                  f'(start={start}, current={cur})')

    def wait_for_drops(self, delta, timeout_s=10.0):
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
            if cur >= start + delta:
                return cur
            time.sleep(0.05)
        self.fail(f'chunks_dropped did not advance by {delta} within {timeout_s}s '
                  f'(start={start}, current={cur})')

    def test_drop_oldest_chunk_after_stable_ts_advance(self):
        """
        Two rollovers create three ingest chunks. Advancing the leader stable past the oldest
        chunk's content (and picking up the resulting checkpoint on the follower) drops only the
        oldest. The second-oldest chunk survives because its content is beyond prune_ts.
        """
        self.open_follower_after_checkpoint()

        # Oldest chunk content: ts 10..14. Rollover fires on the 5th put (index 4).
        self.follower_puts(self.chunk_max_ops, key_offset=0, ts_offset=10)
        # Second chunk content: ts 50..54.
        self.follower_puts(self.chunk_max_ops, key_offset=100, ts_offset=50)
        # Third chunk is now primary; add a couple of ops well into the future so primary is
        # never droppable.
        self.follower_puts(2, key_offset=200, ts_offset=500)

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)
        self.assertIn(self.chunk2_ingest, cfg)

        # Advance stable past the oldest chunk only (14 < stable=30 < 50).
        self.advance_leader_stable_and_checkpoint(30)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_drops(1)
        cfg2 = self.layered_table_config(self.session_follow)
        self.assertNotIn(self.primary_ingest, cfg2,
            'oldest chunk (base name .wt_ingest) should be dropped')
        self.assertIn(self.chunk1_ingest, cfg2,
            'middle chunk (.1.wt_ingest) must survive: content is above prune_ts')
        self.assertIn(self.chunk2_ingest, cfg2,
            'newest chunk (.2.wt_ingest, primary) must survive')

        # Give the server a couple more passes and confirm no further drops.
        passes_before = self.conn_stat(
            self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped), 1,
            'only one drop should occur while middle chunk is not obsolete')

    def test_drops_are_monotonic_oldest_first(self):
        """
        With three chunks and a stable checkpoint that subsumes the oldest two, the chunk server
        drops exactly two chunks in oldest-first order. The primary (newest) is never dropped.
        """
        self.open_follower_after_checkpoint()

        # Two full chunks of obsolete-in-future content at low ts.
        self.follower_puts(self.chunk_max_ops, key_offset=0, ts_offset=10)
        self.follower_puts(self.chunk_max_ops, key_offset=100, ts_offset=20)
        # Primary: well in the future, must not drop.
        self.follower_puts(2, key_offset=200, ts_offset=500)

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)
        self.assertIn(self.chunk2_ingest, cfg)

        # Stable beyond both old chunks' max (24) but well below primary (500).
        self.advance_leader_stable_and_checkpoint(100)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_drops(2)
        cfg2 = self.layered_table_config(self.session_follow)
        # Chunks are dropped oldest first: base, then .1. Primary (.2) stays.
        self.assertNotIn(self.primary_ingest, cfg2)
        self.assertNotIn(self.chunk1_ingest, cfg2)
        self.assertIn(self.chunk2_ingest, cfg2)

    def test_no_drop_below_prune_horizon(self):
        """
        If prune_ts stays below the oldest chunk's max, no drop occurs even after many
        chunk-server passes.
        """
        self.open_follower_after_checkpoint()

        # Oldest chunk's max is 14. Advance stable only to 5.
        self.follower_puts(self.chunk_max_ops, key_offset=0, ts_offset=10)
        self.follower_puts(2, key_offset=100, ts_offset=200)  # primary

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)

        self.advance_leader_stable_and_checkpoint(5)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Several passes with nothing to drop.
        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped), 0,
            'no drop should occur while oldest chunk holds content above prune_ts')

    def test_uncommitted_txn_pins_chunk(self):
        """
        An open, uncommitted transaction with ops on the current primary pins that chunk against
        drop even after it rolls out of primary and the stable checkpoint advances past all its
        ops (the fast obsolete path gates on ingest_gc_pending_ops > 0).
        """
        self.open_follower_after_checkpoint()

        # Start a transaction in session s1 and insert into the current primary (base chunk).
        # Intentionally do NOT commit: this leaves one op outstanding on the ingest btree, which
        # prevents the fast-path obsolete check from firing on that chunk.
        s1 = self.conn_follow.open_session('')
        c1 = s1.open_cursor(self.uri, None, None)
        s1.begin_transaction()
        c1['pinned'] = 'holding'

        # In a different session, fill the remaining ops to drive rollover. The pinned op above
        # already counts toward the threshold, so only chunk_max_ops-1 more are needed.
        s2 = self.conn_follow.open_session('')
        c2 = s2.open_cursor(self.uri, None, None)
        for i in range(self.chunk_max_ops - 1):
            s2.begin_transaction()
            c2[f'r{i}'] = f'v{i}'
            s2.commit_transaction(f'commit_timestamp={self.timestamp_str(10 + i)}')
        c2.close()
        s2.close()

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg, 'rollover must have happened')

        # Advance stable past every committed op ts so the chunk would otherwise be obsolete.
        self.advance_leader_stable_and_checkpoint(100)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Give the chunk server plenty of time to try; the uncommitted op must still pin.
        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped), 0,
            'uncommitted txn on oldest chunk should pin it against drop')

        # Commit s1. The commit_timestamp must be above s2's committed ops (max=13) but below
        # the current prune_ts target (100) so the commit's publish does not push the chunk's
        # tracked max above prune; otherwise the fast obsolete check would still block drop.
        s1.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        c1.close()
        s1.close()

        # The previous disagg_advance_checkpoint could not advance prune_timestamp because s1's
        # open cursor pinned the stable-side dhandle at the picked-up checkpoint. Now that s1
        # has released it, another leader checkpoint + follower pickup lets the prune horizon
        # advance to 100 and the chunk (pending_ops=0, tracked<=20) becomes droppable.
        self.advance_leader_stable_and_checkpoint(110)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_drops(1)

    def test_chunk_server_idle_when_nothing_to_do(self):
        """
        Sanity: with a single ingest chunk (no rollover) and a stable checkpoint, the chunk
        server keeps running passes but never drops, and never errors. Guards against a
        regression where an unconditional drop attempt would loop on the only remaining chunk.
        """
        self.open_follower_after_checkpoint()

        # A handful of puts, not enough to rollover.
        self.follower_puts(self.chunk_max_ops - 1, key_offset=0, ts_offset=10)
        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertNotIn(self.chunk1_ingest, cfg)

        self.advance_leader_stable_and_checkpoint(1000)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped), 0,
            'no drop possible when only the primary chunk exists')
