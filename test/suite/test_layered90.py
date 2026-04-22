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


# test_layered90.py
#   Interaction between prepared transactions and follower ingest-chunk GC.
#
#   Under the "cheap obsolete" design a sealed ingest chunk becomes eligible for the
#   whole-file drop only when:
#     * its max op timestamp < the layered table's prune timestamp, AND
#     * it has zero outstanding in-memory ops (btree->ingest_gc_pending_ops == 0).
#
#   A prepared transaction restored into an ingest chunk by the prepared-discover walk
#   holds a pending op on that chunk until the prepared txn is claimed and resolved.
#   This test exercises the full cycle: restore into primary -> force a rollover so the
#   chunk carrying the prepared artifact becomes sealed -> advance prune past its
#   content -> confirm the chunk is NOT dropped while the prepared txn is outstanding
#   -> resolve -> confirm the chunk is subsequently dropped.


@disagg_test_class
class test_layered90(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered90', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Table basename must not end in digits (ingest rollover derives chunk names by
    # stripping a trailing ".<digits>" segment).
    tablename = 'test_layered_ninety'
    uri = 'layered:' + tablename
    primary_ingest = 'file:test_layered_ninety.wt_ingest'
    chunk1_ingest = 'file:test_layered_ninety.1.wt_ingest'

    conn_base_config = (
        'cache_size=50MB,statistics=(all),precise_checkpoint=true,'
        'preserve_prepared=true,')
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    chunk_max_ops = 5

    conn_follow = None
    session_follow = None

    def tearDown(self):
        # Best-effort cleanup so the automatic verifyLayered (run for test_layered*
        # tests) and the next test's wiredtiger_open() both succeed.
        #
        # Close the follower connection first, then roll back any outstanding
        # prepared transaction on self.session (so the leader btree has no in-memory
        # dirty updates), and run a final checkpoint so schema verify does not see
        # the "Table has dirty data" error.
        try:
            if self.conn_follow is not None:
                self.conn_follow.close()
        except Exception:
            pass
        self.conn_follow = None
        self.session_follow = None
        try:
            if self.session is not None and self.session.this is not None:
                self.session.rollback_transaction(
                    f'rollback_timestamp={self.timestamp_str(1000)}')
        except Exception:
            pass
        try:
            if self.conn is not None and self.conn.is_open():
                cleanup = self.conn.open_session()
                cleanup.checkpoint()
                cleanup.close()
        except Exception:
            pass
        # Close the leader connection explicitly. verifyLayered (in super().tearDown)
        # will reopen it against the now-clean on-disk state; this avoids leaking
        # the leader conn if verifyLayered raises, and prevents "WiredTiger database
        # is already being managed by another thread" on the next test's setUp.
        try:
            if self.conn is not None and self.conn.is_open():
                self.conn.close()
        except Exception:
            pass
        super().tearDown()

    def create_follower(self):
        follower_cfg = (
            'disaggregated=(role="follower",'
            f'layered_ingest_chunk_max_ops={self.chunk_max_ops})')
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config + follower_cfg)
        self.session_follow = self.conn_follow.open_session('')

    def conn_stat(self, conn, stat_id):
        if not hasattr(self, '_stat_cursors'):
            self._stat_cursors = {}
        c = self._stat_cursors.get(id(conn))
        if c is None:
            c = conn.open_session('').open_cursor('statistics:', None, None)
            self._stat_cursors[id(conn)] = c
        c.reset()
        return c[stat_id][2]

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

    def wait_for_drops_to_reach(self, target, timeout_s=10.0):
        """Block until the conn-scope layered_ingest_chunks_dropped stat is >= target."""
        deadline = time.time() + timeout_s
        cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        while cur < target and time.time() < deadline:
            time.sleep(0.05)
            cur = self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        if cur < target:
            self.fail(
                f'chunks_dropped did not reach {target} within {timeout_s}s '
                f'(current={cur})')
        return cur

    def metadata_value(self, session, uri):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(uri)
        self.assertEqual(md.search(), 0)
        v = md.get_value()
        md.close()
        return v

    def metadata_has_key(self, session, key):
        md = session.open_cursor('metadata:', None, None)
        md.set_key(key)
        found = md.search() == 0
        md.close()
        return found

    def leader_commit(self, keyvals, ts):
        """Commit keyvals on the leader. Uses a fresh session so it is safe to call
        even after self.session has an un-resolved prepared transaction."""
        lsess = self.conn.open_session()
        c = lsess.open_cursor(self.uri, None, None)
        lsess.begin_transaction()
        for k, v in keyvals:
            c[k] = v
        lsess.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        c.close()
        lsess.close()

    def leader_advance_and_checkpoint(self, stable_ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(stable_ts)}')
        # self.session may be holding an un-resolved prepared txn; use a dedicated
        # session for the checkpoint call.
        ckpt_sess = self.conn.open_session()
        ckpt_sess.checkpoint()
        ckpt_sess.close()

    def follower_commit(self, keyvals, ts):
        c = self.session_follow.open_cursor(self.uri, None, None)
        self.session_follow.begin_transaction()
        for k, v in keyvals:
            c[k] = v
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(ts)}')
        c.close()

    def setup_leader_with_prepared(self, prepared_id, prepare_ts, stable_after):
        """Create the layered table on the leader, commit a row, prepare a txn at
        prepare_ts, bump stable past the prepare timestamp, and checkpoint. The
        checkpoint captures the prepared artifact on stable (preserve_prepared)."""
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')

        self.session.create(self.uri, 'key_format=i,value_format=S')

        # Committed baseline row (so the table's stable checkpoint is non-trivial).
        self.leader_commit([(0, 'leader_seed')], ts=5)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        # The prepared transaction. We leave it un-resolved on the leader so the
        # stable image that the follower picks up contains the prepared artifact.
        prep = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        prep[100] = 'prepared_value_100'
        prep[101] = 'prepared_value_101'
        self.session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(prepare_ts)},'
            f'prepared_id={self.prepared_id_str(prepared_id)}')
        prep.close()

        self.leader_advance_and_checkpoint(stable_after)

    def test_prepared_pins_sealed_chunk_until_resolved(self):
        """End-to-end: a prepared artifact restored into a chunk that subsequently
        becomes sealed holds that chunk against drop, even once the layered-table
        prune timestamp subsumes the artifact's timestamp. Resolving the prepared
        transaction releases the pin and the chunk server drops the chunk."""

        prepared_id = 1234
        prepare_ts = 100

        # Leader side: create table, prepare a txn, make it stable, checkpoint.
        self.setup_leader_with_prepared(prepared_id=prepared_id,
                                        prepare_ts=prepare_ts,
                                        stable_after=150)

        # Follower side: open, pick up the leader checkpoint.
        self.create_follower()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Open the layered dhandle so the restore path can find it on the handle list.
        c_layered = self.session_follow.open_cursor(self.uri, None, None)
        c_layered.close()

        # Walk prepared-discover on the follower and immediately claim the prepared
        # txn (the cursor refuses to close if any discovered prepared transactions
        # are left un-claimed). The claim session keeps the txn in the prepared
        # state until we commit / rollback it later; in the meantime the restored
        # ops keep ingest_gc_pending_ops > 0 on the chunk that received them.
        claim_sess = self.conn_follow.open_session('')
        discover = self.session_follow.open_cursor('prepared_discover:')
        seen_ids = []
        while discover.next() == 0:
            pid = discover.get_key()
            seen_ids.append(pid)
            claim_sess.begin_transaction(
                f'claim_prepared_id={self.prepared_id_str(pid)}')
        discover.close()
        self.assertEqual(seen_ids, [prepared_id],
                         f'prepared-discover walk should have surfaced the leader '
                         f'prepared_id={prepared_id}, saw {seen_ids}')

        # Now force a rollover so chunk 0 (which holds the restored prepared ops)
        # becomes sealed and chunk 1 becomes the primary. We need chunk_max_ops
        # follower-local writes to trigger rollover (the restore ops themselves
        # are not counted by __clayered_put).
        self.follower_commit([(200 + i, f'follower_{i}')
                              for i in range(self.chunk_max_ops)], ts=200)
        # One more write to land in the new primary chunk.
        self.follower_commit([(300, 'primary_seed')], ts=250)

        self.assertTrue(self.metadata_has_key(self.session_follow, self.chunk1_ingest),
                        'rollover should have produced a .1.wt_ingest chunk')
        self.assertTrue(self.metadata_has_key(self.session_follow, self.primary_ingest),
                        'the sealed chunk (.wt_ingest) must still be tracked prior '
                        'to drop')

        # Advance prune_ts past the prepared timestamp by bumping leader stable and
        # having the follower pick up the new checkpoint.
        self.leader_commit([(0, 'leader_bump')], ts=400)
        self.leader_advance_and_checkpoint(500)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Give the chunk server several passes. It must NOT drop chunk 0 because
        # its ingest_gc_pending_ops is non-zero from the un-resolved prepared txn.
        self.wait_for_passes(n=3)
        drops_pinned = self.conn_stat(
            self.conn_follow, stat.conn.layered_ingest_chunks_dropped)
        self.assertEqual(drops_pinned, 0,
                         'chunk holding un-resolved prepared ops must not be dropped')
        self.assertTrue(self.metadata_has_key(self.session_follow, self.primary_ingest),
                        'sealed chunk must still be present in metadata while the '
                        'prepared transaction is un-resolved')

        # Resolve the prepared transaction on the follower. The op free path
        # decrements ingest_gc_pending_ops back to zero for chunk 0.
        claim_sess.commit_transaction(
            f'commit_timestamp={self.timestamp_str(600)},'
            f'durable_timestamp={self.timestamp_str(610)}')
        claim_sess.close()

        # Advance the prune horizon once more (the stable from the leader must pass
        # the resolution's commit timestamp, too, so the op's own GC max does not
        # stall drop).
        self.leader_advance_and_checkpoint(700)
        self.disagg_advance_checkpoint(self.conn_follow)

        # Now the chunk server should drop chunk 0 (the first obsoleted chunk).
        self.wait_for_drops_to_reach(1)
        self.assertFalse(self.metadata_has_key(self.session_follow, self.primary_ingest),
                         'sealed chunk must be dropped after the prepared transaction '
                         'is resolved and prune_ts advances')
        self.assertTrue(self.metadata_has_key(self.session_follow, self.chunk1_ingest),
                        'current primary chunk must survive (it is never an oldest)')

    def test_prepared_rollback_also_releases_pin(self):
        """Same as above but resolving via rollback_transaction. The chunk still
        drops after resolution+prune; prepared keys remain invisible."""

        prepared_id = 5678
        prepare_ts = 100
        self.setup_leader_with_prepared(prepared_id=prepared_id,
                                        prepare_ts=prepare_ts,
                                        stable_after=150)

        self.create_follower()
        self.disagg_advance_checkpoint(self.conn_follow)

        c_layered = self.session_follow.open_cursor(self.uri, None, None)
        c_layered.close()

        claim_sess = self.conn_follow.open_session('')
        discover = self.session_follow.open_cursor('prepared_discover:')
        while discover.next() == 0:
            pid = discover.get_key()
            claim_sess.begin_transaction(
                f'claim_prepared_id={self.prepared_id_str(pid)}')
        discover.close()

        self.follower_commit([(200 + i, f'follower_{i}')
                              for i in range(self.chunk_max_ops)], ts=200)
        self.follower_commit([(300, 'primary_seed')], ts=250)

        self.leader_advance_and_checkpoint(500)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_passes(n=3)
        self.assertEqual(
            self.conn_stat(self.conn_follow, stat.conn.layered_ingest_chunks_dropped), 0,
            'rollback-resolution: chunk still pinned while prepared is un-resolved')

        claim_sess.rollback_transaction(
            f'rollback_timestamp={self.timestamp_str(600)}')
        claim_sess.close()

        self.leader_advance_and_checkpoint(700)
        self.disagg_advance_checkpoint(self.conn_follow)

        self.wait_for_drops_to_reach(1)
        self.assertFalse(self.metadata_has_key(self.session_follow, self.primary_ingest))
