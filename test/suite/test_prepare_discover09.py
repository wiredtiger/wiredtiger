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
#
# test_prepare_discover09.py
#   A prepared transaction captured in a checkpoint, then reclaimed on a follower via
#   "claim_prepared_id", must survive step-up. A reclaimed transaction owns no per-session
#   transaction id  the only identifier its session carries is the prepared id  so when the
#   ingest drain runs as part of step-up it must locate the owning session by prepared id when
#   patching the in-flight operations from the ingest btree to the stable btree.
#
#   This test exercises that step-up path end-to-end: discover, reclaim, step up, then resolve.
#   The fix is the match-by-prepared-id branch in the ingest drain's session walk; without it,
#   the drain skips the reclaim session and either crashes during ingest truncate or leaves
#   stale operation pointers behind.
#
#   Scenario dimensions:
#     resolve:     commit | rollback   how the reclaimed transaction is resolved post-step-up
#     multi_table: True  | False       whether the prepared transaction spans two layered tables

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_prepare_discover09(wttest.WiredTigerTestCase):
    tablename = 'test_prepare_discover09'
    uri = 'layered:' + tablename

    resolve_scenarios = [
        ('commit',   dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    multi_table_scenarios = [
        ('single_table', dict(multi_table=False)),
        ('multi_table',  dict(multi_table=True)),
    ]
    disagg_storages = gen_disagg_storages('test_prepare_discover09', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, resolve_scenarios, multi_table_scenarios)

    conn_base_config = (
        'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,')

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    @property
    def uri_b(self):
        return 'layered:' + self.tablename + '_b'

    @property
    def _uris(self):
        """URI(s) exercised by this run -- one table or two, based on the multi_table scenario."""
        return [self.uri] + ([self.uri_b] if self.multi_table else [])

    def _open_follower(self, checkpoint_meta):
        """Open a follower connection and apply the given checkpoint metadata."""
        conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        conn_follow.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')
        return conn_follow

    def _checkpoint(self, conn):
        """Take a checkpoint on the given connection."""
        session = conn.open_session()
        session.checkpoint()
        session.close()

    def test_claimed_prepare_insert_survives_step_up(self):
        """
        A prepared INSERT (keys 4-6) captured in the leader's checkpoint is loaded by the
        follower, reclaimed via claim_prepared_id, and then resolved after step-up. The
        reclaim session has no transaction id, so the ingest drain at step-up must match
        the owning session by prepared id when patching its in-flight operations from the
        ingest btree to the stable btree.

        Verifies the resolve sequence completes without error and the connection closes
        cleanly. When multi_table=True the prepared transaction also covers a second
        layered table; both tables are exercised by the same reclaim.
        """
        prepared_id = 12345
        uris = self._uris

        # ---- Phase 1 (Leader) ----
        # Commit baseline keys 1-3, prepare INSERT on keys 4-6, advance stable past the
        # prepare timestamp, and checkpoint so the prepare is captured on disk. Roll back
        # the leader's prepare afterward; the follower reclaims it by prepared id below.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp='  + self.timestamp_str(50))

        for uri in uris:
            self.session.create(uri, 'key_format=i,value_format=S')
        leader_cursors = [self.session.open_cursor(uri) for uri in uris]

        self.session.begin_transaction()
        for c in leader_cursors:
            c[1] = 'committed_value_1'
            c[2] = 'committed_value_2'
            c[3] = 'committed_value_3'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))

        self.session.begin_transaction()
        for c in leader_cursors:
            c[4] = 'prepared_value_4'
            c[5] = 'prepared_value_5'
            c[6] = 'prepared_value_6'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(100) +
            ',prepared_id='      + self.prepared_id_str(prepared_id))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))
        self._checkpoint(self.conn)
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()
        self.session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(210))

        for c in leader_cursors:
            c.close()
        # Close without a final checkpoint so the follower loads exactly the state above.
        self.conn.close('debug=(skip_checkpoint=true)')

        # ---- Phase 2 (Follower: discover and reclaim) ----
        # Open the follower from the captured checkpoint, walk the prepared_discover cursor,
        # and reclaim each surfaced prepared id on a dedicated session via claim_prepared_id.
        # Reclaiming sets the session's prepared id but deliberately leaves its transaction
        # id unset, which is the case the step-up callback must handle.
        conn_follow = self._open_follower(checkpoint_meta)
        discover_session = conn_follow.open_session()
        discover_cursor = discover_session.open_cursor('prepared_discover:')
        claim_session = conn_follow.open_session()

        discovered = []
        while discover_cursor.next() == 0:
            pid = discover_cursor.get_key()
            discovered.append(pid)
            # The discover cursor must not close until every surfaced prepared id has been
            # claimed; reclaim now and leave the session active until after step-up.
            claim_session.begin_transaction(
                'claim_prepared_id=' + self.prepared_id_str(pid))

        self.assertEqual(discovered, [prepared_id])
        discover_cursor.close()
        discover_session.close()

        # ---- Phase 3 (Step up while the reclaim is live) ----
        # The ingest drain that runs as part of step-up walks active sessions, finds the
        # session that owns the prepared transaction, and patches its operations so they
        # apply to the stable btree. The reclaim session has no transaction id, so this
        # match must succeed by prepared id.
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # ---- Phase 4 (Resolve the reclaimed transaction) ----
        # The resolve must complete without error. Without the step-up fix, the drain skips
        # the reclaim session, leaving its operations pointing at the about-to-be-truncated
        # ingest btree; the resolve below would then trip on the inconsistent state.
        if self.commit:
            claim_session.timestamp_transaction(
                'commit_timestamp=' + self.timestamp_str(200) +
                ',durable_timestamp=' + self.timestamp_str(210))
            claim_session.commit_transaction()
        else:
            claim_session.rollback_transaction(
                'rollback_timestamp=' + self.timestamp_str(210))
        claim_session.close()

        # Advance stable past the resolve and checkpoint to flush the resolved state.
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        self._checkpoint(conn_follow)

        conn_follow.close()
