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

# test_layered103.py
#   When two prepared sessions share the same prepared_id, a follower
#   stepping up to leader must resolve all of them.
#
#   The follower holds two prepared sessions with prepared_id=42:
#   claimed_session (reclaimed from the checkpoint, T1 writes only) and
#   live_session (freshly prepared, T2 writes only, prepare_ts after the
#   checkpoint). At step-up the session scan may hit claimed_session first;
#   without the fix it stops there and leaves live_session's T2 writes
#   unresolved, causing step-up to fail with a conflict.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@wttest.skip_for_hook("tiered", "Layered tables are not supported with tiered storage")
@disagg_test_class
class test_layered103(wttest.WiredTigerTestCase):
    uri_t1 = 'layered:test_layered103_t1'
    uri_t2 = 'layered:test_layered103_t2'

    resolve_scenarios = [
        ('commit',   dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    disagg_storages = gen_disagg_storages('test_layered103', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, resolve_scenarios)

    conn_base_config = (
        'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,')

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def _open_follower(self, checkpoint_meta):
        conn = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        conn.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')
        return conn

    def _checkpoint(self, conn):
        s = conn.open_session()
        s.checkpoint()
        s.close()

    def test_split_prepared_survives_step_up(self):
        """
        When two prepared sessions share the same prepared_id (one reclaimed
        from the checkpoint, one freshly prepared on the follower), step-up
        must succeed and both sessions must be resolvable afterward.
        """
        # ---- Phase 1 (Leader) ----------------------------------------
        # Commit base values, then prepare T1 only (not T2) and checkpoint
        # so the prepare is captured on disk. Roll back on the leader so
        # the follower can reclaim it via claim_prepared_id.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50))

        self.session.create(self.uri_t1, 'key_format=i,value_format=S')
        self.session.create(self.uri_t2, 'key_format=i,value_format=S')
        c1 = self.session.open_cursor(self.uri_t1)
        c2 = self.session.open_cursor(self.uri_t2)

        self.session.begin_transaction()
        for i in range(1, 4):
            c1[i] = f'base_t1_{i}'
            c2[i] = f'base_t2_{i}'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))

        # T2 gets no prepared content so the follower can prepare T2 with
        # the same prepared_id=42 later, forming the split scenario.
        self.session.begin_transaction()
        for i in range(4, 7):
            c1[i] = f'split_t1_{i}'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(100) +
            ',prepared_id='      + self.prepared_id_str(42))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))
        self._checkpoint(self.conn)
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()

        self.session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(210))
        c1.close()
        c2.close()
        self.conn.close('debug=(skip_checkpoint=true)')

        # ---- Phase 2 (Follower: set up split-prepared state) ----------
        conn_f = self._open_follower(checkpoint_meta)

        # Open claimed_session first so it is scanned before live_session
        # at step-up. Reclaim the checkpointed prepare into claimed_session.
        claimed_session = conn_f.open_session()
        disc_session = conn_f.open_session()
        disc_cursor = disc_session.open_cursor('prepared_discover:')

        discovered = []
        while disc_cursor.next() == 0:
            pid = disc_cursor.get_key()
            discovered.append(pid)
            claimed_session.begin_transaction(
                'claim_prepared_id=' + self.prepared_id_str(pid))

        self.assertEqual(discovered, [42], 'expected exactly one discovered prepared_id')
        disc_cursor.close()

        # Open live_session before closing disc_session so it cannot reuse
        # disc_session's position and be scanned before claimed_session.
        live_session = conn_f.open_session()
        disc_session.close()

        # Prepare T2 with the same prepared_id=42. prepare_ts=160 is after
        # the checkpoint's stable timestamp (150) so these writes are not in
        # the checkpoint and must be carried through step-up.
        c2_live = live_session.open_cursor(self.uri_t2)
        live_session.begin_transaction()
        for i in range(4, 7):
            c2_live[i] = f'split_t2_{i}'
        live_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(160) +
            ',prepared_id='      + self.prepared_id_str(42))

        # ---- Phase 3 (Step up) ----------------------------------------
        # Without the fix, claimed_session matches by prepared_id but has no
        # T2 writes; the scan stops there and live_session's writes are left
        # unresolved, causing step-up to fail with a conflict.
        conn_f.reconfigure('disaggregated=(role="leader")')

        # ---- Phase 4 (Resolve both prepared transactions) -------------
        if self.commit:
            claimed_session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(200) +
                ',durable_timestamp=' + self.timestamp_str(210))
            claimed_session.commit_transaction()

            live_session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(200) +
                ',durable_timestamp=' + self.timestamp_str(210))
            live_session.commit_transaction()
        else:
            claimed_session.rollback_transaction(
                'rollback_timestamp=' + self.timestamp_str(210))
            live_session.rollback_transaction(
                'rollback_timestamp=' + self.timestamp_str(210))

        c2_live.close()
        live_session.close()
        claimed_session.close()

        conn_f.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        self._checkpoint(conn_f)

        # ---- Phase 5 (Verify) ----------------------------------------
        read_s = conn_f.open_session()
        rc1 = read_s.open_cursor(self.uri_t1)
        rc2 = read_s.open_cursor(self.uri_t2)

        # Base values (committed at ts=60) are always visible.
        read_s.begin_transaction('read_timestamp=' + self.timestamp_str(60))
        for i in range(1, 4):
            self.assertEqual(rc1[i], f'base_t1_{i}')
            self.assertEqual(rc2[i], f'base_t2_{i}')
        for i in range(4, 7):
            rc1.set_key(i)
            self.assertEqual(rc1.search(), wiredtiger.WT_NOTFOUND)
            rc2.set_key(i)
            self.assertEqual(rc2.search(), wiredtiger.WT_NOTFOUND)
        read_s.rollback_transaction()

        # Keys 4-6 are visible on commit, absent on rollback.
        read_s.begin_transaction('read_timestamp=' + self.timestamp_str(200))
        for i in range(4, 7):
            rc1.set_key(i)
            rc2.set_key(i)
            if self.commit:
                self.assertEqual(rc1.search(), 0)
                self.assertEqual(rc1.get_value(), f'split_t1_{i}')
                self.assertEqual(rc2.search(), 0)
                self.assertEqual(rc2.get_value(), f'split_t2_{i}')
            else:
                self.assertEqual(rc1.search(), wiredtiger.WT_NOTFOUND)
                self.assertEqual(rc2.search(), wiredtiger.WT_NOTFOUND)
        read_s.rollback_transaction()

        rc1.close()
        rc2.close()
        read_s.close()
        conn_f.close()
