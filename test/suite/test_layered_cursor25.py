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
# On a layered follower, a write op must report a write conflict (WT_ROLLBACK) when a committed
# update newer than the transaction's read timestamp exists on the target key, even when the
# cursor was last positioned on a different key.
#
# Setup on the leader, captured in a single checkpoint (oldest pinned low so nothing is obsolete):
#     key A = "value_a1" @ commit_ts=10                              (one version)
#     key B = "value_b1" @ commit_ts=10, then "value_b2" @ commit_ts=200  (two versions)
#
# On the follower, in a snapshot transaction at read_ts=100 (so value_b2@200 is committed but not
# visible): search(A), then a write op on B without an intervening reset.
#
# Expected outcome for every op is WT_ROLLBACK, since a committed update newer than read_ts
# exists on B. The one exception is a non-overwrite insert: it finds the visible B (value_b1) and
# returns WT_DUPLICATE_KEY before any write is attempted.

import wiredtiger
import wttest
from helpers.helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_cursor25(wttest.WiredTigerTestCase):
    test_name = __qualname__

    disagg_storages = gen_disagg_storages(disagg_only = True)
    # Run every op with the follower cursor in both overwrite and non-overwrite mode.
    overwrite_modes = [
        ('overwrite', dict(follower_curcfg=None)),
        ('no_overwrite', dict(follower_curcfg='overwrite=false')),
    ]
    scenarios = make_scenarios(disagg_storages, overwrite_modes)

    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader"),'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower"),'

    uri = f'layered:{test_name}'
    keyA = 'A'
    keyB = 'B'

    def create_follower(self):
        self.conn_follow = self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',create,' + self.conn_config_follower)
        self.session_follow = self.conn_follow.open_session('')

    # Build the A/B chain on the leader and let the follower pick up the checkpoint.
    def setup_leader_and_follower(self):
        self.session.create(self.uri, 'key_format=S,value_format=S,')
        self.create_follower()

        # Pin oldest low so both of B's committed versions stay on the chain through the
        # checkpoint.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        c = self.session.open_cursor(self.uri, None, None)

        self.session.begin_transaction()
        c[self.keyA] = 'value_a1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        self.session.begin_transaction()
        c[self.keyB] = 'value_b1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        self.session.begin_transaction()
        c[self.keyB] = 'value_b2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(200))
        c.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    # Apply the named write op to key B on the follower cursor.
    def apply_op(self, fc, op_name):
        fc.set_key(self.keyB)
        if op_name == 'modify':
            return fc.modify([wiredtiger.Modify('X', 0, 1)])
        if op_name == 'remove':
            return fc.remove()
        if op_name == 'update':
            fc.set_value('value_bnew')
            return fc.update()
        if op_name == 'insert':
            fc.set_value('value_bnew')
            return fc.insert()
        if op_name == 'reserve':
            return fc.reserve()
        raise Exception('unknown op ' + op_name)

    # On the follower: position on A, then run a write op on B at read_ts=100 without an
    # intervening reset. Return a label for what the op returned.
    def probe(self, op_name):
        fc = self.session_follow.open_cursor(self.uri, None, self.follower_curcfg)
        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(100))

        fc.set_key(self.keyA)
        self.assertEqual(fc.search(), 0)

        # Target a different key (B) without an intervening reset.
        try:
            ret = self.apply_op(fc, op_name)
            if ret == wiredtiger.WT_NOTFOUND:
                outcome = 'WT_NOTFOUND'
            elif ret == wiredtiger.WT_DUPLICATE_KEY:
                outcome = 'WT_DUPLICATE_KEY'
            elif ret == 0:
                outcome = 'OK'
            else:
                outcome = 'ret=%d' % ret
        except wiredtiger.WiredTigerRollbackError:
            outcome = 'WT_ROLLBACK'
        except wiredtiger.WiredTigerError as e:
            s = str(e)
            if 'WT_DUPLICATE_KEY' in s:
                outcome = 'WT_DUPLICATE_KEY'
            elif 'WT_NOTFOUND' in s:
                outcome = 'WT_NOTFOUND'
            else:
                raise
        self.session_follow.rollback_transaction()
        fc.close()
        return outcome

    def check(self, op_name):
        actual = self.probe(op_name)
        # A non-overwrite insert finds the visible B (value_b1) and returns WT_DUPLICATE_KEY. Every
        # other op surfaces the newer committed update on B as WT_ROLLBACK.
        no_overwrite = self.follower_curcfg == 'overwrite=false'
        expected = 'WT_DUPLICATE_KEY' if (op_name == 'insert' and no_overwrite) else 'WT_ROLLBACK'
        self.assertEqual(actual, expected)

    def test_conflict_check_modify(self):
        self.setup_leader_and_follower()
        self.check('modify')

    def test_conflict_check_remove(self):
        self.setup_leader_and_follower()
        self.check('remove')

    def test_conflict_check_update(self):
        self.setup_leader_and_follower()
        self.check('update')

    def test_conflict_check_insert(self):
        self.setup_leader_and_follower()
        self.check('insert')

    def test_conflict_check_reserve(self):
        self.setup_leader_and_follower()
        self.check('reserve')

if __name__ == '__main__':
    wttest.run()
