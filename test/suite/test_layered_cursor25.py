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
# A follower's read-timestamped walk can be parked on a key that lives only in
# the stable constituent. If the leader later removes that key and its
# tombstone becomes obsolete from the *leader's own* perspective (its own
# oldest_timestamp passes the tombstone's commit timestamp), the leader's
# next checkpoint physically drops the row. That checkpoint can still be
# fully valid for the follower's reader -- its oldest_timestamp need not
# exceed the reader's pinned read_timestamp at all, since obsolescence is a
# purely leader-local decision that never consults any follower's readers.
# When the still-open reader's cursor tries to carry its position forward
# into that checkpoint, the row is genuinely gone.
#
# The leader and follower advance their oldest/stable timestamps in lockstep
# throughout, and a second key is replicated into the follower's ingest table
# like any live write, to show that ordinary replication and checkpoint-only
# delivery of the leader's obsolescence decision coexist without conflict.
#
# No concurrency is required to hit this: the whole sequence is driven
# synchronously by one script, one step at a time.

import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_cursor25(wttest.WiredTigerTestCase):

    test_name = __qualname__
    uri = 'layered:' + test_name

    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def commit(self, session, key, value, ts):
        c = session.open_cursor(self.uri)
        session.begin_transaction()
        c[key] = value
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c.close()

    # Mirror a leader write onto the follower's ingest table at the same commit
    # timestamp, as ordinary replication would. Must be called before any reader on
    # conn_follow is active at or above ts: WiredTiger asserts (WT_DIAGNOSTIC_TXN_VISIBILITY)
    # that a commit timestamp is after every active read timestamp on the same connection,
    # so a follower can only ever replicate ops that predate its readers' pins -- exactly
    # matching real replication, where a reader is never permitted to pin a timestamp the
    # follower hasn't already caught up to.
    def replicate(self, key, value, ts):
        self.commit(self.session, key, value, ts)
        self.commit(self.session_follow, key, value, ts)

    # Advance oldest_timestamp on both the leader and the follower together, keeping
    # their retention in lockstep like a follower whose own tracking keeps pace with
    # the leader's.
    def advance_oldest(self, ts):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(ts))
        self.conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(ts))

    # Reproduces the "upgrading a positioned stable cursor" assertion in
    # __clayered_reopen_stable via a legitimate, panic-free sequence: the
    # checkpoint the follower adopts never violates the reader's pin.
    def test_reopen_stable_key_pruned_by_leader_local_obsolescence(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        self.advance_oldest(1)

        # Checkpoint A: keys a, m, z committed at ts=10, checkpointed at stable=100.
        # 'a' is old enough that on the follower it will only ever live in stable,
        # never touched on the follower's ingest side.
        for k in ('a', 'm', 'z'):
            self.commit(self.session, k, 'v-' + k, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Ordinary replication continues past checkpoint A: 'n' is written by the leader
        # at ts=120 and replicated into the follower's ingest table at the same timestamp,
        # well before any reader opens. This is the common case this fix doesn't change.
        self.replicate('n', 'v-n', 120)

        # Follower reader: read_timestamp=150. 'n' is visible via ingest replication; 'a'
        # comes from stable, since it predates any ingest activity and was never
        # replicated. Parks on 'a' first.
        cursor = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(150))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'a')
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'm')
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'n')
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'm')
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'a')

        # Leader removes 'a' at commit_ts=140 (< reader's read_timestamp=150). Unlike 'n'
        # above, this op is deliberately NOT replicated into the follower's ingest table --
        # it can't be, since the reader is already pinned at 150 on that connection, and in
        # any case this is exactly the kind of materialization lag disaggregated followers
        # are expected to tolerate. The leader then advances oldest_timestamp to 145 (>=
        # 140, so the tombstone is fully obsolete for the leader's own reconciliation --
        # nothing to do with the follower's reader) on both connections together, and
        # checkpoints again at stable=300.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c.set_key('a')
        self.assertEqual(c.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(140))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(300))
        self.advance_oldest(145)
        self.session.checkpoint()

        # The follower's own pinned timestamp is min(its own oldest_timestamp, every
        # active reader's read_timestamp) = min(145, 150) = 145, which does not exceed
        # the checkpoint's oldest_timestamp (145), so picking it up does not trip the
        # checkpoint-pickup pinned-timestamp panic.
        self.disagg_advance_checkpoint(self.conn_follow)

        # The still-open reader continues its walk. Its cursor is still parked on 'a' in
        # the old checkpoint; carrying that position into the new checkpoint fails because
        # the row is genuinely gone there, unrelated to this reader's own pin or to the
        # ordinary replication of 'n'.
        try:
            ret = cursor.next()
            self.pr('cursor.next() after checkpoint pickup returned ' + str(ret))
        finally:
            self.session_follow.rollback_transaction()
            cursor.close()
