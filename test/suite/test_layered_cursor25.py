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

    # Reproduces the "upgrading a positioned stable cursor" assertion in
    # __clayered_reopen_stable via a legitimate, panic-free sequence: the
    # checkpoint the follower adopts never violates the reader's pin.
    def test_reopen_stable_key_pruned_by_leader_local_obsolescence(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # Checkpoint A: keys a, m, z committed at ts=10, checkpointed at stable=100.
        # 'a' is old enough that on the follower it will only ever live in stable,
        # never touched on the follower's ingest side.
        for k in ('a', 'm', 'z'):
            self.commit(self.session, k, 'v-' + k, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Follower reader: read_timestamp=150, well above checkpoint A's stable=100 --
        # an entirely ordinary follower read (ingest covers anything more recent; 'a'
        # itself comes from stable because it predates the checkpoint). Parks on 'a'.
        cursor = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(150))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'a')

        # Leader removes 'a' at commit_ts=140 (< reader's read_timestamp=150), then
        # advances its own oldest_timestamp to 145 (>= 140, so the tombstone is fully
        # obsolete for the leader's own reconciliation -- nothing to do with the
        # follower's reader) and checkpoints again at stable=300.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c.set_key('a')
        self.assertEqual(c.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(140))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(300))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(145))
        self.session.checkpoint()

        # The follower's own pinned timestamp is min(its own oldest_timestamp, every
        # active reader's read_timestamp). Its own oldest_timestamp is still 1 from
        # setUp, so without this it would (correctly) trigger the checkpoint-pickup
        # pinned-timestamp panic -- a follower's own retention has to track along with
        # its readers to accept new checkpoints at all. Advance it to just below the
        # reader's read_timestamp, matching a properly-tracking follower.
        self.conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(145))

        # The follower adopts this checkpoint without incident: its pinned timestamp
        # (min(145, the reader's 150) = 145) does not exceed the checkpoint's
        # oldest_timestamp (145), so this does not trip the checkpoint-pickup
        # pinned-timestamp panic.
        self.disagg_advance_checkpoint(self.conn_follow)

        # The still-open reader continues its walk. Its cursor is still parked on 'a'
        # in the old checkpoint; carrying that position into the new checkpoint fails
        # because the row is genuinely gone there, unrelated to this reader's own pin.
        try:
            ret = cursor.next()
            self.pr('cursor.next() after checkpoint pickup returned ' + str(ret))
        finally:
            self.session_follow.rollback_transaction()
            cursor.close()
