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

# test_layered92.py
#    Ingest-chunk publish/retention correctness.
#
#    Exercises the per-op timestamp publish path in __txn_ingest_gc_publish_commit and the chunk
#    server fast obsolete path in __layered_ingest_btree_obsolete_for_drop. In particular, the
#    "every op publishes" behaviour must leave the tracked max at or above the highest op
#    timestamp for each GC-tracked btree, so an obsolete chunk is dropped only after the stable
#    checkpoint has advanced past that max.


@disagg_test_class
class test_layered92(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered92', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Table basename must not end in digits (see test_layered88).
    table_name = 'test_layered_ninety_two'
    uri = 'layered:test_layered_ninety_two'
    primary_ingest = 'file:test_layered_ninety_two.wt_ingest'
    chunk1_ingest = 'file:test_layered_ninety_two.1.wt_ingest'

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
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor['seed'] = 'leader'
        cursor.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

    def open_follower_after_checkpoint(self):
        self.setup_leader_seeded_table()
        self.create_follower()
        self.disagg_advance_checkpoint(self.conn_follow)

    def conn_stat(self, conn, stat_id):
        """Cached-per-connection stat cursor to avoid exhausting the session pool on polls."""
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

    def follower_put_ts(self, cursor, key, value, ts):
        self.session_follow.begin_transaction()
        cursor[key] = value
        self.session_follow.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

    def advance_leader_stable_and_checkpoint(self, ts):
        # Insert a bump on the leader so the checkpoint is new and meaningful, then advance
        # stable and take a checkpoint. The follower will pick up the resulting checkpoint
        # timestamp and feed it into prune_timestamp for every ingest btree.
        bump_cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        bump_cursor[f'bump-{ts}'] = 'leader'
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        bump_cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()

    def wait_for_server_passes(self, n=2, timeout_s=10.0):
        """Wait until the follower chunk server has completed at least n additional passes."""
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunk_server_passes)
            if cur >= start + n:
                return cur
            time.sleep(0.05)
        self.fail(f'chunk server passes did not advance by {n} within {timeout_s}s '
                  f'(start={start}, current={cur})')

    def wait_for_drops(self, expected_delta, timeout_s=10.0):
        """Wait until layered_ingest_chunks_dropped advances by at least expected_delta."""
        start = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
            if cur >= start + expected_delta:
                return cur
            time.sleep(0.05)
        self.fail(f'chunks_dropped did not advance by {expected_delta} within {timeout_s}s '
                  f'(start={start}, current={cur})')

    def test_chunk_retained_when_op_ts_above_prune(self):
        """
        An ingest chunk containing an op at ts T must not be dropped while the stable checkpoint
        is still below T. Before the publish-commit fix (commit 4bc78a67bc) the fast-path
        obsolete check could let a chunk drop prematurely when its first op had a lower ts than
        later ops on the same btree. Asserting retention here exercises that invariant.
        """
        self.open_follower_after_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri, None, None)
        # Three ops in the same chunk with a rising commit_timestamp per op (20, 50, 80). After
        # rollover, the full chunk is pinned by the highest ts (80).
        self.follower_put_ts(cursor, 'a', 'va', 20)
        self.follower_put_ts(cursor, 'b', 'vb', 50)
        self.follower_put_ts(cursor, 'c', 'vc', 80)
        # Trigger rollover: two more ops reaches chunk_max_ops=5 puts since follower opened.
        self.follower_put_ts(cursor, 'd', 'vd', 90)
        self.follower_put_ts(cursor, 'e', 've', 100)
        cursor.close()

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.chunk1_ingest, cfg)

        # Stable advances past first op (20) but not past the chunk max (80).
        self.advance_leader_stable_and_checkpoint(60)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Let the chunk server run at least a couple of passes. The oldest chunk holds keys
        # committed at ts=80, which is above prune_ts=60; the chunk must NOT be dropped.
        self.wait_for_server_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped),
            0,
            'oldest chunk was dropped while it still held content above prune_timestamp')
        self.assertIn(self.primary_ingest, self.layered_table_config(self.session_follow))

    def test_chunk_dropped_when_all_op_ts_below_prune(self):
        """
        Same harness but the stable checkpoint advances past every timestamp the oldest chunk
        holds. The chunk server must drop exactly the oldest chunk; the primary chunk stays.
        """
        self.open_follower_after_checkpoint()

        cursor = self.session_follow.open_cursor(self.uri, None, None)
        # Oldest chunk: all ts <= 50.
        self.follower_put_ts(cursor, 'a', 'va', 20)
        self.follower_put_ts(cursor, 'b', 'vb', 30)
        self.follower_put_ts(cursor, 'c', 'vc', 40)
        self.follower_put_ts(cursor, 'd', 'vd', 45)
        self.follower_put_ts(cursor, 'e', 've', 50)
        # First put after the 5th op is what actually rolls the chunk (see __clayered_put).
        # Do two more on the new primary with far-future ts so it is not droppable.
        self.follower_put_ts(cursor, 'f', 'vf', 200)
        self.follower_put_ts(cursor, 'g', 'vg', 210)
        cursor.close()

        cfg = self.layered_table_config(self.session_follow)
        self.assertIn(self.primary_ingest, cfg)
        self.assertIn(self.chunk1_ingest, cfg)

        # Stable well above everything the oldest chunk holds, well below primary content.
        self.advance_leader_stable_and_checkpoint(100)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Exactly one drop: the oldest chunk.
        self.wait_for_drops(1)
        cfg2 = self.layered_table_config(self.session_follow)
        self.assertNotIn(self.primary_ingest, cfg2,
            'the old primary (now oldest) chunk should have been dropped')
        self.assertIn(self.chunk1_ingest, cfg2,
            'the new primary (chunk.1) must still be present')

        # The keys written only to the new primary (chunk.1) remain readable after the drop.
        read = self.session_follow.open_cursor(self.uri, None, None)
        read.set_key('g')
        self.assertEqual(read.search(), 0)
        self.assertEqual(read.get_value(), 'vg')
        read.close()
