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

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


# test_layered94.py
#   Prepared-discover walk on a disaggregated follower with a rolled-over ingest side.
#
#   Covers the race-fix in __wti_prepared_discover_restore_and_add_artifact_upd: the
#   restore path must read the primary ingest URI under the per-layered-table
#   ingest_chunk_lock while holding a reference to the layered dhandle, because rollover
#   and drop-oldest mutate layered->ingest_uris[] concurrently. These scenarios exercise
#   the restore path when the follower has already produced multiple ingest chunks, so
#   the primary is no longer the canonical ".wt_ingest" name.


@disagg_test_class
class test_layered94(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered94', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Table basename must not end in digits (ingest rollover derives chunk names by
    # stripping a trailing ".<digits>" segment; see test_layered88 comment).
    tablename = 'test_layered_ninety_four'
    uri = 'layered:' + tablename
    s_config = 'key_format=i,value_format=S'

    conn_base_config = (
        'cache_size=50MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,')

    chunk_max_ops = 5

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def _set_stable(self, ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')

    def _set_oldest(self, ts):
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(ts)}')

    def _seed_prepared_checkpoint(self, prepared_id, prepare_ts=100, stable_after=150):
        """Leader-side setup: create the layered table, commit some rows, prepare a
        transaction, advance stable past the prepare timestamp, and checkpoint so the
        prepared artifact is persisted into the stable image."""
        self._set_oldest(50)
        self._set_stable(50)

        self.session.create(self.uri, self.s_config)
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = 'committed_value_1'
        cursor[2] = 'committed_value_2'
        cursor[3] = 'committed_value_3'
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(60)}')
        self._set_stable(70)

        self.session.begin_transaction()
        cursor[4] = 'prepared_value_4'
        cursor[5] = 'prepared_value_5'
        cursor[6] = 'prepared_value_6'
        self.session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(prepare_ts)},'
            f'prepared_id={self.prepared_id_str(prepared_id)}')

        self._set_stable(stable_after)

        ckpt_sess = self.conn.open_session()
        ckpt_sess.checkpoint()
        ckpt_sess.close()

        # Leave the cursor closed before reopening. The prepared txn stays live on
        # self.session; its artifact is in the stable checkpoint via preserve_prepared.
        cursor.close()

    def _reopen_as_follower(self, with_rollover=True):
        """Close the leader connection and reopen as a follower picking up the
        previously-written stable checkpoint.

        If with_rollover is True, set layered_ingest_chunk_max_ops to chunk_max_ops so
        follower writes trigger rollovers. Otherwise leave it unset (no rotation)."""
        meta = self.disagg_get_complete_checkpoint_meta()
        disagg_kv = [f'role="follower"', f'checkpoint_meta="{meta}"']
        if with_rollover:
            disagg_kv.append(f'layered_ingest_chunk_max_ops={self.chunk_max_ops}')
        follower_cfg = (
            self.conn_base_config + 'disaggregated=(' + ','.join(disagg_kv) + ')')
        self.reopen_conn(config=follower_cfg)

    def _follower_writes(self, key_base, count, ts_base):
        """Commit `count` non-conflicting rows via the (now-follower) default session.
        With rollover configured, chunk_max_ops of these trigger a rollover."""
        wsess = self.conn.open_session()
        wc = wsess.open_cursor(self.uri)
        for i in range(count):
            wsess.begin_transaction()
            wc[key_base + i] = f'follower_v{i}'
            wsess.commit_transaction(
                f'commit_timestamp={self.timestamp_str(ts_base + i)}')
        wc.close()
        wsess.close()

    def _layered_metadata(self):
        md = self.session.open_cursor('metadata:', None, None)
        md.set_key(self.uri)
        self.assertEqual(md.search(), 0)
        v = md.get_value()
        md.close()
        return v

    def _drive_prepared_discover(self, expected_id, resolve):
        """Open the prepared_discover cursor on the follower, verifying it finds
        exactly one artifact with id == expected_id. Invoke resolve(claim_session, pid)
        to claim and commit/rollback the txn."""
        discover = self.session.open_cursor('prepared_discover:')
        claim_sess = self.conn.open_session()
        count = 0
        while discover.next() == 0:
            pid = discover.get_key()
            self.assertEqual(pid, expected_id)
            count += 1
            claim_sess.begin_transaction(
                f'claim_prepared_id={self.prepared_id_str(pid)}')
            resolve(claim_sess, pid)
        discover.close()
        self.assertEqual(count, 1)
        claim_sess.close()

    def _open_layered_once(self):
        """Ensure the layered dhandle is OPEN on this connection before opening the
        prepared_discover cursor. The restore path searches for an OPEN layered
        dhandle matching the stable URI; without this the follower only has the
        stable btree opened (lazily on the walk) and the layered table is never in
        the handle list as OPEN."""
        c = self.session.open_cursor(self.uri)
        c.close()

    def test_restore_into_primary_basic(self):
        """Baseline (no rollover): follower has no ingest writes before the prepared-
        discover walk; the prepared artifact is restored into the canonical primary
        ingest chunk, and a claim+commit makes prepared values visible."""
        self._seed_prepared_checkpoint(prepared_id=123)
        self._reopen_as_follower(with_rollover=False)
        self._open_layered_once()

        def resolve_commit(sess, _pid):
            sess.commit_transaction(
                f'commit_timestamp={self.timestamp_str(200)},'
                f'durable_timestamp={self.timestamp_str(210)}')

        self._drive_prepared_discover(expected_id=123, resolve=resolve_commit)

        self._set_stable(220)
        rsess = self.conn.open_session()
        rc = rsess.open_cursor(self.uri)
        rsess.begin_transaction(f'read_timestamp={self.timestamp_str(200)}')
        for k, v in [(1, 'committed_value_1'), (2, 'committed_value_2'),
                     (3, 'committed_value_3'), (4, 'prepared_value_4'),
                     (5, 'prepared_value_5'), (6, 'prepared_value_6')]:
            self.assertEqual(rc[k], v)
        rsess.rollback_transaction()
        rc.close()
        rsess.close()

    def test_restore_into_rolled_over_primary(self):
        """Follower performs enough writes to trigger at least one rollover before the
        prepared-discover walk runs. The restore path must target the post-rollover
        primary ingest URI, and a claim+commit must succeed and make the prepared
        values visible."""
        self._seed_prepared_checkpoint(prepared_id=321)
        self._reopen_as_follower(with_rollover=True)
        self._open_layered_once()

        # Write just enough to trigger one rollover (chunk_max_ops) plus a couple in
        # the new primary. This keeps the test under a second and still places the
        # restore target on a non-canonical URI.
        self._follower_writes(key_base=1000, count=self.chunk_max_ops + 2,
                              ts_base=300)

        md_text = self._layered_metadata()
        self.assertIn(f'file:{self.tablename}.1.wt_ingest', md_text,
                      'expected at least one rollover-produced ingest chunk before '
                      'the prepared-discover walk runs')

        def resolve_commit(sess, _pid):
            sess.commit_transaction(
                f'commit_timestamp={self.timestamp_str(400)},'
                f'durable_timestamp={self.timestamp_str(410)}')

        self._drive_prepared_discover(expected_id=321, resolve=resolve_commit)

        self._set_stable(420)
        rsess = self.conn.open_session()
        rc = rsess.open_cursor(self.uri)
        rsess.begin_transaction(f'read_timestamp={self.timestamp_str(400)}')
        for k, v in [(1, 'committed_value_1'), (4, 'prepared_value_4'),
                     (5, 'prepared_value_5'), (6, 'prepared_value_6'),
                     (1000, 'follower_v0'),
                     (1000 + self.chunk_max_ops, f'follower_v{self.chunk_max_ops}')]:
            self.assertEqual(rc[k], v)
        rsess.rollback_transaction()
        rc.close()
        rsess.close()

    def test_rollback_resolution_after_rollover(self):
        """As above, but resolve with rollback_transaction. Prepared keys must NOT
        become visible; the follower's non-prepared writes and the leader-committed
        rows remain readable."""
        self._seed_prepared_checkpoint(prepared_id=555)
        self._reopen_as_follower(with_rollover=True)
        self._open_layered_once()

        self._follower_writes(key_base=2000, count=self.chunk_max_ops + 2,
                              ts_base=300)

        def resolve_rollback(sess, _pid):
            sess.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(400)}')

        self._drive_prepared_discover(expected_id=555, resolve=resolve_rollback)

        self._set_stable(420)
        rsess = self.conn.open_session()
        rc = rsess.open_cursor(self.uri)
        rsess.begin_transaction(f'read_timestamp={self.timestamp_str(400)}')
        self.assertEqual(rc[1], 'committed_value_1')
        self.assertEqual(rc[2000], 'follower_v0')
        for k in (4, 5, 6):
            rc.set_key(k)
            self.assertEqual(rc.search(), wiredtiger.WT_NOTFOUND)
        rsess.rollback_transaction()
        rc.close()
        rsess.close()
