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

# helper_layered_stepdown.py
#   Shared helpers for the async (planned) step-down layered table tests.

import wiredtiger
from wiredtiger import stat

# Shared helpers for the layered async step-down test suite.
class LayeredStepdownMixin:
    # Substring of the WT_ROLLBACK last-error reason.
    STRADDLER_REASON = 'started before the step-down timestamp was set'
    
    # FIXME-WT-17895: remove this skip once the planned step-down implementation lands.
    def setUp(self):
        self.skipTest('elegant step-down is not implemented yet')
        super().setUp()
    # Set the global oldest and stable timestamps.
    def set_global_ts(self, oldest, stable):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest) +
                                ',stable_timestamp=' + self.timestamp_str(stable))

    # Arm a planned step-down at the given cutoff timestamp.
    def arm(self, ts):
        self.conn.set_timestamp('step_down_timestamp=' + self.timestamp_str(ts))

    # Complete an armed step-down: advance stable to the cutoff, take the step-down checkpoint
    # and demote to follower.
    def complete_step_down(self, cutoff):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(cutoff))
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.conn.reconfigure('disaggregated=(role="follower")')

    # The file URI of a layered table's ingest constituent.
    def ingest_uri(self, uri):
        return 'file:' + uri.split(':', 1)[1] + '.wt_ingest'

    # The file URI of a layered table's stable constituent.
    def stable_uri(self, uri):
        return 'file:' + uri.split(':', 1)[1] + '.wt_stable'

    # The connection's all_durable timestamp as an integer.
    def all_durable(self):
        return int(self.conn.query_timestamp('get=all_durable'), 16)

    # Write k/v pairs (dict) to a table in one transaction committed at commit_ts.
    def write_at(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    # The key/value map visible through a cursor on uri at read_ts.
    def read_kvs_at(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        kv = {}
        while cursor.next() == 0:
            kv[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return kv

    # The set of keys visible through a cursor on uri at read_ts.
    def read_keys_at(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        keys = set()
        while cursor.next() == 0:
            keys.add(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return keys

    # The connection-wide count of step-down transaction rollbacks.
    def step_down_rollbacks(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        count = stat_cursor[stat.conn.txn_rollback_stepdown][2]
        stat_cursor.close()
        return count

    # Run op and expect WT_ROLLBACK.
    def assert_step_down_rollback(self, op, reason=STRADDLER_REASON):
        before = self.step_down_rollbacks()
        self.assertRaisesException(wiredtiger.WiredTigerError, op,
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        err, _, err_msg = self.session.get_last_error()
        self.assertEqual(err, wiredtiger.WT_ROLLBACK)
        if reason is not None:
            self.assertTrue(reason in err_msg,
                'expected a step-down rollback reason, got: ' + err_msg)
        self.assertEqual(self.step_down_rollbacks(), before + 1)
