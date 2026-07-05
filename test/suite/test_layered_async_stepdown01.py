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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

# test_layered_async_stepdown01.py
#    Elegant (async) step-down routing for layered tables, the "detect and roll back" plan.
#
#    On a leader, writes go to the stable constituent. The server arms a planned step-down by
#    setting a cutoff timestamp (step_down_ts). From that moment:
#      - new writes are directed to the ingest constituent (so they survive the demotion), and
#        reads on the leader consult ingest first, merged over the still-live stable table;
#      - a transaction that began before the cutoff was armed is a "straddler": its next write
#        rolls back with WT_ROLLBACK so the server can retry it cleanly after the cutoff.
@disagg_test_class
class test_layered_async_stepdown01(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:async_stepdown'
    ingest_uri = 'file:async_stepdown.wt_ingest'

    def set_global_ts(self, oldest, stable):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest) +
                                ',stable_timestamp=' + self.timestamp_str(stable))

    # Insert a batch of keys in a single transaction committed at commit_ts.
    def insert_at(self, keys, value, commit_ts):
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        for k in keys:
            cursor[k] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    # Return the keys visible through the given cursor URI at read_ts. Reading the ingest
    # constituent (file:...wt_ingest) directly is the ground truth for where a write was routed.
    def keys_at(self, cursor_uri, read_ts):
        cursor = self.session.open_cursor(cursor_uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        keys = set()
        while cursor.next() == 0:
            keys.add(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return keys

    # Return the key/value map visible through the layered cursor at read_ts.
    def kv_at(self, read_ts):
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        kv = {}
        while cursor.next() == 0:
            kv[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return kv

    # The headline test: writes split at the cutoff and the leader reads ingest-first.
    def test_routing_and_ingest_first_reads(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        pre = {'pre' + str(i) for i in range(5)}
        post = {'post' + str(i) for i in range(5)}

        # Before the cutoff, writes go to stable. The ingest constituent stays empty.
        self.insert_at(pre, 'stable', 10)
        self.assertEqual(self.keys_at(self.ingest_uri, 15), set(),
            'pre-arm writes must not be in the ingest table')
        self.assertEqual(self.keys_at(self.uri, 15), pre)

        # Arm the planned step-down at the current frontier.
        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(20))

        # After the cutoff, writes are directed to ingest. The transaction begins after arming, so
        # it is not a straddler.
        self.insert_at(post, 'ingest', 30)

        # The leader now reads ingest-first, merged over the live stable table: it sees both halves.
        self.assertEqual(self.keys_at(self.uri, 40), pre | post)

        # Ground truth: post-arm keys landed in ingest, pre-arm keys did not.
        self.assertEqual(self.keys_at(self.ingest_uri, 40), post)

    def step_down_rollbacks(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        count = stat_cursor[stat.conn.txn_rollback_step_down][2]
        stat_cursor.close()
        return count

    # A transaction that began before the cutoff was armed is rolled back on its next write, and the
    # retry after the cutoff commits cleanly to ingest.
    def test_straddler_rollback(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Begin a transaction and write before the cutoff is armed (this lands in stable).
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddle'] = 'before'

        # The server arms the step-down while this transaction is still in flight.
        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(20))
        self.assertEqual(self.step_down_rollbacks(), 0)

        # The next write by the straddling transaction must roll back: its data sits in stable but
        # would commit after the cutoff, where it belongs in ingest. The Python exception carries the
        # generic WT_ROLLBACK string; the specific reason is on the session's last error.
        def straddle_write():
            cursor['straddle2'] = 'after'
        self.assertRaisesException(wiredtiger.WiredTigerError, straddle_write,
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        err, _, err_msg = self.session.get_last_error()
        self.assertEqual(err, wiredtiger.WT_ROLLBACK)
        self.assertTrue('started before a planned step-down' in err_msg,
            'expected the step-down rollback reason, got: ' + err_msg)
        # The dedicated statistic counts the straddler rollback.
        self.assertEqual(self.step_down_rollbacks(), 1)
        self.session.rollback_transaction()
        cursor.close()

        # The server retries the transaction after the cutoff: it now routes cleanly to ingest.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddle'] = 'after'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.keys_at(self.ingest_uri, 40), {'straddle'})
        self.assertEqual(self.keys_at(self.uri, 40), {'straddle'})

    # Routing is keyed on whether a step-down is armed, not on the operation, so update, modify and
    # remove of pre-cutoff (stable) keys all land in ingest after arming, exactly like insert.
    def test_update_modify_remove_route_to_ingest(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.insert_at(['k1', 'k2', 'k3'], 'base', 10)
        self.assertEqual(self.keys_at(self.ingest_uri, 15), set(),
            'pre-arm writes must not be in the ingest table')

        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(20))

        cursor = self.session.open_cursor(self.uri, None, None)

        # Update k1.
        self.session.begin_transaction()
        cursor['k1'] = 'updated'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        # Modify k3: build the new value on the stable base, write the result to ingest.
        self.session.begin_transaction()
        cursor.set_key('k3')
        cursor.modify([wiredtiger.Modify('X', 0, 1)])  # 'base' -> 'Xase'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))

        # Remove k2: a tombstone record over stable's k2.
        self.session.begin_transaction()
        cursor.set_key('k2')
        cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(32))
        cursor.close()

        # Merged result on the leader: update and modify reflected, remove hides the stable key.
        kv = self.kv_at(40)
        self.assertEqual(kv.get('k1'), 'updated')
        self.assertEqual(kv.get('k3'), 'Xase')
        self.assertNotIn('k2', kv)

        # All three writes landed in ingest (the remove as a tombstone record shadowing stable).
        self.assertEqual(self.keys_at(self.ingest_uri, 40), {'k1', 'k2', 'k3'})

    # The straddler rollback is keyed on the write, not the operation type: a remove by a transaction
    # that began before the cutoff rolls back just like an insert would.
    def test_straddler_rollback_non_insert(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_at(['k1'], 'base', 10)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k1')

        # The server arms the step-down while this transaction is in flight.
        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(20))

        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.remove(),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        err, _, err_msg = self.session.get_last_error()
        self.assertEqual(err, wiredtiger.WT_ROLLBACK)
        self.assertTrue('started before a planned step-down' in err_msg,
            'expected the step-down rollback reason, got: ' + err_msg)
        self.session.rollback_transaction()
        cursor.close()

    # Reads and the ingest content survive the demotion: after stepping down to follower, the
    # post-cutoff writes (held in ingest) are still readable.
    def test_content_survives_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(20))
        post = {'post' + str(i) for i in range(5)}
        self.insert_at(post, 'ingest', 30)
        self.assertEqual(self.keys_at(self.ingest_uri, 40), post)

        # Before completing the step-down the server advances stable to the cutoff (the step-down
        # checkpoint lands at stable == cutoff); WiredTiger asserts this at step-down.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        # Step down to follower. The cutoff is cleared and the node demotes; ingest content stays.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrue(post.issubset(self.keys_at(self.uri, 40)),
            'post-cutoff (ingest) content must survive the step-down')
