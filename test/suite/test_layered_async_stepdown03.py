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

# test_layered_async_stepdown03.py
#    Commit-time / arm-timing races for async (elegant) step-down. A straddler that does all its
#    writes before the cutoff is armed and only commits afterwards performs no further cursor write,
#    so the write-time check cannot see it; the commit-time guard catches it instead. Also covers
#    the happy path (post-cutoff transactions committing above the cutoff) and read-only survivors.
@disagg_test_class
class test_layered_async_stepdown03(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:race'
    ingest_uri = 'file:race.wt_ingest'

    def set_global_ts(self, oldest, stable):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest) +
                                ',stable_timestamp=' + self.timestamp_str(stable))

    def arm(self, ts):
        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(ts))

    def kv_at(self, read_ts):
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        kv = {}
        while cursor.next() == 0:
            kv[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return kv

    def keys_at(self, cursor_uri, read_ts):
        cursor = self.session.open_cursor(cursor_uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        keys = set()
        while cursor.next() == 0:
            keys.add(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return keys

    def assert_step_down_rollback(self):
        err, _, err_msg = self.session.get_last_error()
        self.assertEqual(err, wiredtiger.WT_ROLLBACK)
        self.assertTrue('started before a planned step-down' in err_msg,
            'expected the step-down rollback reason, got: ' + err_msg)

    # The race: a transaction writes before the cutoff, the cutoff is armed, then it commits with no
    # further write. The commit must roll back, and the retry after the cutoff lands in ingest.
    def test_arm_just_before_commit_rolls_back(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'                       # pre-arm write to stable; no further write follows

        self.arm(20)                             # armed right before commit

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30)),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        self.assert_step_down_rollback()
        cursor.close()

        # The rolled-back write left nothing behind, in either constituent.
        self.assertEqual(self.kv_at(40), {})
        self.assertEqual(self.keys_at(self.ingest_uri, 40), set())

        # The retry runs after the cutoff and commits cleanly to ingest.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.kv_at(40), {'k1': 'v'})
        self.assertEqual(self.keys_at(self.ingest_uri, 40), {'k1'})

    # The guard is keyed on straddling the arm, not on the commit timestamp: a straddler rolls back
    # even when it would commit at or below the cutoff (where its content could in principle stay in
    # stable). This matches the design choice to roll back every concurrent writer.
    def test_straddler_commit_below_cutoff_also_rolls_back(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'

        self.arm(20)

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(15)),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        self.assert_step_down_rollback()
        cursor.close()

    # The happy path: once armed, transactions that begin after the cutoff and commit above it are
    # never rolled back and land in ingest.
    def test_post_arm_commits_above_cutoff_succeed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        for i, commit_ts in enumerate((30, 40, 50)):
            self.session.begin_transaction()
            cursor['post%d' % i] = 'v%d' % commit_ts
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

        self.assertEqual(self.kv_at(60), {'post0': 'v30', 'post1': 'v40', 'post2': 'v50'})
        self.assertEqual(self.keys_at(self.ingest_uri, 60), {'post0', 'post1', 'post2'})

    # A read-only transaction that straddles the arm commits normally: the guard only fires for
    # transactions that performed writes, so reads are never disturbed.
    def test_readonly_straddler_commits_fine(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        wcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        wcur['k1'] = 'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        wcur.close()

        # A read-only transaction begins before the cutoff and commits after it.
        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(rcur['k1'], 'v')
        self.arm(20)
        self.assertEqual(rcur['k1'], 'v')
        # Committing a read-only transaction must succeed (no WT_ROLLBACK).
        self.session.commit_transaction()
        rcur.close()
