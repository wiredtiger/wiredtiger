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
from wiredtiger import stat, WT_NOTFOUND
from wtscenario import make_scenarios

# test_layered91.py
#   Tombstone correctness across ingest-chunk boundaries with GC-driven drops.
#
#   Under the "cheap obsolete" design we drop whole ingest chunks (as files) once
#   every op they contain has its commit timestamp below the layered table's
#   prune_timestamp. This test pins down two properties the chunk-level GC must
#   preserve for tombstones:
#
#     1. A tombstone living in a newer (still alive) chunk must continue to hide a
#        key whose only committed insert lived in an older chunk that has since
#        been dropped - i.e. the drop of the insert chunk must not resurrect the
#        key on the follower. The follower's stable-side (leader) btree never saw
#        either op, so the read must come back as not-found.
#
#     2. Once the tombstone's own chunk is also eligible for drop (prune_ts has
#        advanced past the delete timestamp) and both chunks are dropped, the key
#        stays not-found on the follower: no ingest residue is left behind, and
#        the stable side has nothing for it.


@disagg_test_class
class test_layered91(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered91', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Basename must not end with digits (see test_layered88/89 comments).
    table_name = 'test_layered_ninety_one'
    uri = 'layered:' + table_name
    primary_ingest = 'file:' + table_name + '.wt_ingest'
    chunk1_ingest = 'file:' + table_name + '.1.wt_ingest'
    chunk2_ingest = 'file:' + table_name + '.2.wt_ingest'

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
        c = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
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
        if not hasattr(self, '_stat_cursors'):
            self._stat_cursors = {}
        c = self._stat_cursors.get(id(conn))
        if c is None:
            c = conn.open_session('').open_cursor('statistics:', None, None)
            self._stat_cursors[id(conn)] = c
        c.reset()
        return c[stat_id][2]

    def layered_table_config(self, session):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(self.uri)
        self.assertEqual(md.search(), 0)
        value = md.get_value()
        md.close()
        return value

    def follower_put(self, key, value, ts, session=None):
        session = session or self.session_follow
        c = session.open_cursor(self.uri, None, None)
        session.begin_transaction()
        c[key] = value
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        c.close()

    def follower_delete(self, key, ts, session=None):
        session = session or self.session_follow
        c = session.open_cursor(self.uri, None, None)
        session.begin_transaction()
        c.set_key(key)
        self.assertEqual(c.remove(), 0)
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        c.close()

    def follower_read(self, key, at_ts, session=None):
        """Return the follower's view of key at read timestamp at_ts, or None if
        not-found. Raises on any other error."""
        session = session or self.session_follow
        session.begin_transaction(f'read_timestamp={self.timestamp_str(at_ts)}')
        try:
            c = session.open_cursor(self.uri, None, None)
            c.set_key(key)
            r = c.search()
            val = c.get_value() if r == 0 else None
            c.close()
            return val
        finally:
            session.rollback_transaction()

    def advance_leader_stable_and_checkpoint(self, ts):
        bump = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        bump[f'bump-{ts}'] = 'leader'
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        bump.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()

    def wait_for_drops(self, delta, timeout_s=10.0):
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
            if cur >= start + delta:
                return cur
            time.sleep(0.05)
        self.fail(
            f'chunks_dropped did not advance by {delta} within {timeout_s}s '
            f'(start={start}, current={cur})')

    def wait_for_passes(self, n=2, timeout_s=10.0):
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
            if cur >= start + n:
                return cur
            time.sleep(0.05)
        self.fail(
            f'chunk server did not complete {n} passes within {timeout_s}s '
            f'(start={start}, current={cur})')

    def fill_chunk_fillers(self, prefix, start_ts, n=None, session=None):
        """Insert n filler rows on the follower to drive a rollover of the current
        primary chunk. Each insert counts as one ingest op; n defaults to the
        remaining ops needed to reach chunk_max_ops."""
        session = session or self.session_follow
        if n is None:
            n = self.chunk_max_ops
        c = session.open_cursor(self.uri, None, None)
        for i in range(n):
            session.begin_transaction()
            c[f'{prefix}{i}'] = f'v{i}'
            session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(start_ts + i)}')
        c.close()

    def test_tombstone_across_chunks_insert_dropped(self):
        """Insert key K in chunk 0 (oldest), tombstone K in chunk 1, have both roll
        out of primary, then advance prune past chunk 0 only. Chunk 0 drops; chunk 1
        (with the tombstone) survives. Read of K must be not-found on the follower."""
        self.open_follower_after_checkpoint()

        # Chunk 0: insert target K=v1 at ts=10, then 4 fillers at ts=11..14 -> rollover.
        self.follower_put('K', 'v1', ts=10)
        self.fill_chunk_fillers('a', start_ts=11, n=self.chunk_max_ops - 1)

        # Chunk 1: tombstone for K at ts=50, then 4 fillers at ts=51..54 -> rollover.
        self.follower_delete('K', ts=50)
        self.fill_chunk_fillers('b', start_ts=51, n=self.chunk_max_ops - 1)

        # Chunk 2 (current primary): a single op at ts=1000. Keeps primary unaffected
        # by prune horizons we set below.
        self.follower_put('z', 'primary', ts=1000)

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)
        self.assertIn(self.chunk2_ingest, cfg)

        # Sanity: before any drop, K is invisible at a read past the tombstone.
        self.assertIsNone(self.follower_read('K', at_ts=60),
                          'tombstone at ts=50 must hide K at read_ts=60')

        # Advance stable past ts=14 (chunk 0) but well below ts=50 (chunk 1).
        self.advance_leader_stable_and_checkpoint(30)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_drops(1)
        cfg2 = self.layered_table_config(self.session_follow)
        self.assertNotIn(self.primary_ingest, cfg2,
            'chunk 0 (the insert-only chunk) must be dropped')
        self.assertIn(self.chunk1_ingest, cfg2,
            'chunk 1 (holds tombstone, max_ts=54 > prune_ts=30) must survive')
        self.assertIn(self.chunk2_ingest, cfg2,
            'primary (newest) chunk must survive')

        # Read must remain not-found: the insert chunk is gone, and the still-live
        # tombstone in chunk 1 hides any stable-side residue.
        self.assertIsNone(self.follower_read('K', at_ts=60),
                          'K must still read as deleted after its insert-chunk drops')

        # No further drops while chunk 1 still has content above prune_ts.
        before = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped),
            before,
            'chunk 1 must not drop while its max_ts (54) exceeds prune_ts (30)')

    def test_tombstone_chunk_drops_once_prune_passes(self):
        """Extension of the above: once prune_ts also passes the tombstone chunk's
        max ts, that chunk drops too. The key stays not-found on the follower."""
        self.open_follower_after_checkpoint()

        self.follower_put('K', 'v1', ts=10)
        self.fill_chunk_fillers('a', start_ts=11, n=self.chunk_max_ops - 1)
        self.follower_delete('K', ts=50)
        self.fill_chunk_fillers('b', start_ts=51, n=self.chunk_max_ops - 1)
        self.follower_put('z', 'primary', ts=1000)

        # First pass: drop chunk 0 only.
        self.advance_leader_stable_and_checkpoint(30)
        self.disagg_advance_checkpoint(self.conn_follow)
        self.wait_for_drops(1)

        # Now advance prune past chunk 1's max (54).
        self.advance_leader_stable_and_checkpoint(100)
        self.disagg_advance_checkpoint(self.conn_follow)
        self.wait_for_drops(1)

        cfg = self.layered_table_config(self.session_follow)
        self.assertNotIn(self.primary_ingest, cfg,
            'chunk 0 already dropped in earlier step')
        self.assertNotIn(self.chunk1_ingest, cfg,
            'chunk 1 (tombstone-only) must drop once prune_ts > 54')
        self.assertIn(self.chunk2_ingest, cfg,
            'primary (newest) chunk must always survive')

        # Key stays not-found - the stable side never saw K, and every ingest op
        # for it has been retired.
        self.assertIsNone(self.follower_read('K', at_ts=200),
                          'K must still read as deleted after the tombstone chunk drops')

    def test_reinsert_after_tombstone_across_chunks(self):
        """Insert K=v1 (chunk 0), tombstone K (chunk 1), reinsert K=v2 (chunk 2).
        After prune drops chunk 0 only, the follower reads the most-recent chunk-2
        value (v2), because the rolled-out tombstone in chunk 1 is shadowed by the
        newer chunk-2 reinsert at read_ts > reinsert_ts."""
        self.open_follower_after_checkpoint()

        # Chunk 0: insert K=v1 at ts=10, filler ts=11..14.
        self.follower_put('K', 'v1', ts=10)
        self.fill_chunk_fillers('a', start_ts=11, n=self.chunk_max_ops - 1)
        # Chunk 1: tombstone K at ts=50, filler ts=51..54.
        self.follower_delete('K', ts=50)
        self.fill_chunk_fillers('b', start_ts=51, n=self.chunk_max_ops - 1)
        # Chunk 2: reinsert K=v2 at ts=100. Primary, plus a filler.
        self.follower_put('K', 'v2', ts=100)
        self.follower_put('z', 'primary', ts=1000)

        # Sanity pre-drop: at ts=200 we see the reinsert (chunk 2 shadows the
        # tombstone and the original insert).
        self.assertEqual(self.follower_read('K', at_ts=200), 'v2',
                         'reinsert in primary chunk must be visible')

        # Advance prune past chunk 0 only.
        self.advance_leader_stable_and_checkpoint(30)
        self.disagg_advance_checkpoint(self.conn_follow)
        self.wait_for_drops(1)

        cfg = self.layered_table_config(self.session_follow)
        self.assertNotIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)
        self.assertIn(self.chunk2_ingest, cfg)

        # After the insert-chunk drop, the reinsert still wins at read_ts >= 100.
        self.assertEqual(self.follower_read('K', at_ts=200), 'v2',
                         'reinsert must remain visible after chunk 0 drop')
        # And at a read_ts between the tombstone and the reinsert, the tombstone
        # still hides the (dropped) original insert.
        self.assertIsNone(self.follower_read('K', at_ts=60),
                          'tombstone in chunk 1 must still hide K at read_ts=60')
