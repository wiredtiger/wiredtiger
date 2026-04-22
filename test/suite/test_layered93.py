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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios


# test_layered93.py
#   Ingest-chunk cache-residency under the "cheap obsolete" design.
#
#   The new chunk-server-driven GC requires that ingest btree leaf pages are *never*
#   reconciled or evicted by the normal cache path: obsolete chunks are retired as whole
#   files, not via per-page reconciliation. When natural eviction would otherwise target
#   an ingest leaf, __evict_page_dirty_update refuses (with WT_BTREE_GARBAGE_COLLECT set)
#   and bumps cache_eviction_blocked_prune_timestamp.
#
#   These tests verify the cache-residency invariant indirectly via the per-file recstat
#   counters: after any amount of layered writes and incidental cache churn on the
#   follower, the ingest btree(s) must show rec_pages_eviction == 0 and cache_write == 0
#   while remaining fully readable. The primary ingest chunk and rollover-produced
#   secondary chunks are both covered.


@disagg_test_class
class test_layered93(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered93', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Table basename must not end in digits (ingest rollover derives chunk names by
    # stripping a trailing ".<digits>" segment).
    tablename = 'test_layered_ninety_three'
    uri = 'layered:' + tablename
    primary_ingest = 'file:test_layered_ninety_three.wt_ingest'
    chunk1_ingest = 'file:test_layered_ninety_three.1.wt_ingest'

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
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri, None, None)
        c[0] = 'leader_seed'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

    def open_follower_after_checkpoint(self):
        self.setup_leader_seeded_table()
        self.create_follower()
        self.disagg_advance_checkpoint(self.conn_follow)

    def conn_stat(self, conn, stat_id):
        """Cached-per-connection stat cursor to avoid exhausting the session pool on
        repeated polls."""
        if not hasattr(self, '_stat_cursors'):
            self._stat_cursors = {}
        c = self._stat_cursors.get(id(conn))
        if c is None:
            c = conn.open_session('').open_cursor('statistics:', None, None)
            self._stat_cursors[id(conn)] = c
        c.reset()
        return c[stat_id][2]

    def follower_puts(self, count, key_offset=0, ts_offset=10):
        cursor = self.session_follow.open_cursor(self.uri, None, None)
        for i in range(count):
            self.session_follow.begin_transaction()
            cursor[key_offset + i] = f'v{key_offset + i}'
            self.session_follow.commit_transaction(
                f'commit_timestamp={self.timestamp_str(ts_offset + i)}')
        cursor.close()

    def dsrc_file_stat(self, uri, stat_id):
        """Return a dsrc-scope stat value for a single file URI on the follower."""
        stat_cur = self.session_follow.open_cursor('statistics:' + uri, None, None)
        v = stat_cur[stat_id][2]
        stat_cur.close()
        return v

    def metadata_value(self, session, uri):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(uri)
        self.assertEqual(md.search(), 0)
        v = md.get_value()
        md.close()
        return v

    def assert_ingest_file_cache_resident(self, uri):
        """Confirm a given ingest file URI shows no reconciliation / eviction-write
        activity. Natural eviction refuses to reconcile WT_BTREE_GARBAGE_COLLECT leaf
        pages (the whole-file drop in the chunk server replaces per-page GC), so both
        rec_pages_eviction and cache_write must be zero for the lifetime of the
        ingest chunk."""
        rec_pages_eviction = self.dsrc_file_stat(uri, stat.dsrc.rec_pages_eviction)
        cache_write = self.dsrc_file_stat(uri, stat.dsrc.cache_write)
        self.assertEqual(
            rec_pages_eviction, 0,
            f'{uri} was reconciled by eviction ({rec_pages_eviction}); ingest chunks '
            'must stay cache-resident')
        self.assertEqual(
            cache_write, 0,
            f'{uri} performed {cache_write} cache writes; ingest chunks must not '
            'push any leaf content to disk')

    def test_primary_ingest_stays_cache_resident(self):
        """Single-chunk baseline: after many writes to the primary ingest chunk, the
        ingest btree still shows no eviction reconciliations and no cache writes while
        every key remains readable."""
        self.open_follower_after_checkpoint()

        n = 32
        self.follower_puts(n, key_offset=0, ts_offset=10)

        self.assert_ingest_file_cache_resident(self.primary_ingest)

        c = self.session_follow.open_cursor(self.uri, None, None)
        for i in range(n):
            self.assertEqual(c[i], f'v{i}')
        c.close()

        # Re-check after reads (reads can trigger page-release paths that, on a
        # GC-collect btree, must still refuse to reconcile).
        self.assert_ingest_file_cache_resident(self.primary_ingest)

    def test_rolled_over_chunks_stay_cache_resident(self):
        """After at least one rollover, every ingest chunk btree (the sealed one and
        the current primary) continues to show zero eviction reconciliations and zero
        cache writes, even as writes continue to flow into the new primary."""
        self.open_follower_after_checkpoint()

        # Fill the first chunk to exactly chunk_max_ops, triggering a rollover.
        self.follower_puts(self.chunk_max_ops, key_offset=0, ts_offset=10)
        # Keep writing into the new primary to exercise its cache path, too.
        self.follower_puts(self.chunk_max_ops, key_offset=100, ts_offset=50)

        table_md = self.metadata_value(self.session_follow, self.uri)
        self.assertIn(self.primary_ingest, table_md,
                      'the original primary ingest URI must still be listed after '
                      'rollover (it survives until chunk-server drop)')
        self.assertIn(self.chunk1_ingest, table_md,
                      'rollover should have created a .1.wt_ingest chunk')

        # Both chunks must remain cache-resident.
        self.assert_ingest_file_cache_resident(self.primary_ingest)
        self.assert_ingest_file_cache_resident(self.chunk1_ingest)

        # All keys across both chunks must still read back.
        c = self.session_follow.open_cursor(self.uri, None, None)
        for i in range(self.chunk_max_ops):
            self.assertEqual(c[i], f'v{i}')
        for i in range(self.chunk_max_ops):
            self.assertEqual(c[100 + i], f'v{100 + i}')
        c.close()

        self.assert_ingest_file_cache_resident(self.primary_ingest)
        self.assert_ingest_file_cache_resident(self.chunk1_ingest)
