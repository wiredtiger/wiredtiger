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
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# Prepared transactions resolving across step-down survive the role change without pending prepares.
@disagg_test_class
class test_layered_prepare14(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),preserve_prepared=true,precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    resolution_times = [
        ('before_checkpoint', dict(resolve_after_checkpoint=False)),
        ('after_checkpoint', dict(resolve_after_checkpoint=True)),
    ]
    scenarios = make_scenarios(disagg_storages, resolution_times)

    test_name = __qualname__
    uri = f'layered:{test_name}'

    def checkpoint_at(self, ts):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts))
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

    def checkpoint_at_step_down_timestamp(self):
        self.checkpoint_at(11)

    def resolve(self, session, commit, ts):
        if commit:
            session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts) +
                ',durable_timestamp=' + self.timestamp_str(ts))
        else:
            session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(ts))

    def test_prepared_commit_straddler_survives_step_down(self):
        expected = {'straddler': 'straddler-value'}
        ingest_uri = self.ingest_uri(self.uri)
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddler'] = 'straddler-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))

        self.set_step_down_ts(11)
        if self.resolve_after_checkpoint:
            self.checkpoint_at_step_down_timestamp()

        self.session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(12) + ',durable_timestamp=' + self.timestamp_str(12))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 20), expected)
        self.assertEqual(self.read_kvs_at(ingest_uri, 20), expected)

        if not self.resolve_after_checkpoint:
            self.checkpoint_at_step_down_timestamp()
        self.conn.reconfigure('disaggregated=(role="follower")')

        self.assertEqual(self.read_kvs_at(self.uri, 20), expected)
        self.assertEqual(self.read_kvs_at(ingest_uri, 20), expected)

        # A checkpoint taken after commit still excludes a write durable beyond its boundary.
        if not self.resolve_after_checkpoint:
            self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(self.uri), 20), set())

    def test_prepared_rollback_straddler_does_not_resurface_on_step_up(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddler'] = 'straddler-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))

        self.set_step_down_ts(11)
        if self.resolve_after_checkpoint:
            self.checkpoint_at_step_down_timestamp()
        self.session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(12))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 20), {})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 20), set())

        if not self.resolve_after_checkpoint:
            self.checkpoint_at_step_down_timestamp()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs_at(self.uri, 20), {})

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        prepared_before_step_up = stat_cursor[wiredtiger.stat.conn.txn_prepared_updates][2]
        stat_cursor.close()

        # Step-up must not rediscover the transaction that was rolled back.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.read_kvs_at(self.uri, 20), {})
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        prepared_discovered = stat_cursor[wiredtiger.stat.conn.txn_prepared_updates][2] - prepared_before_step_up
        stat_cursor.close()
        self.assertEqual(prepared_discovered, 0)

        self.set_global_ts(20, 20)
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

    # A key whose prepared transaction rolled back across one step-down is written by another
    # prepared transaction that resolves across the next.
    def check_straddler_after_rolled_back_straddler(self, commit):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None, None)

        self.session.begin_transaction()
        cursor['straddler'] = 'first-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))
        self.set_step_down_ts(11)
        self.resolve(self.session, False, 12)
        cursor.close()
        self.complete_step_down(11)

        self.conn.reconfigure('disaggregated=(role="leader")')
        self.checkpoint_at(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddler'] = 'second-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(30) + ',prepared_id=' + self.prepared_id_str(2))
        self.set_step_down_ts(31)
        if self.resolve_after_checkpoint:
            self.checkpoint_at(31)
        self.resolve(self.session, commit, 32)
        cursor.close()

        expected = {'straddler': 'second-value'} if commit else {}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        if not self.resolve_after_checkpoint:
            self.checkpoint_at(31)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.checkpoint_at(40)

    def test_prepared_commit_straddler_after_rolled_back_straddler(self):
        self.check_straddler_after_rolled_back_straddler(commit=True)

    def test_prepared_rollback_straddler_after_rolled_back_straddler(self):
        self.check_straddler_after_rolled_back_straddler(commit=False)

    # A prepared transaction that wrote keys more than once is recovered from a checkpoint and
    # resolves after a step-down is announced.
    def check_recovered_straddler_with_repeated_keys(self, commit):
        ingest_uri = self.ingest_uri(self.uri)
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'kept': 'old-value', 'updated': 'old-value'}, 5)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        for key in ('inserted', 'updated'):
            cursor[key] = 'new-value'
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))
        self.checkpoint_at(15)
        cursor.close()

        self.reopen_conn()
        discover_cursor = self.session.open_cursor('prepared_discover:')
        self.assertEqual(discover_cursor.next(), 0)
        self.assertEqual(discover_cursor.get_key(), 1)
        claim_session = self.conn.open_session()
        claim_session.begin_transaction('claim_prepared_id=' + self.prepared_id_str(1))
        self.assertEqual(discover_cursor.next(), wiredtiger.WT_NOTFOUND)
        discover_cursor.close()

        self.set_step_down_ts(20)
        if self.resolve_after_checkpoint:
            self.checkpoint_at(20)
        self.resolve(claim_session, commit, 25)
        claim_session.close()

        # A committed remove is recorded in ingest; a rolled back transaction leaves nothing there.
        expected = {'kept': 'old-value'}
        if not commit:
            expected['updated'] = 'old-value'
        expected_ingest = {'inserted', 'updated'} if commit else set()
        self.assertEqual(self.read_kvs_at(self.uri, 30), expected)
        self.assertEqual(self.read_keys_at(ingest_uri, 30), expected_ingest)

        if not self.resolve_after_checkpoint:
            self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs_at(self.uri, 30), expected)
        self.assertEqual(self.read_keys_at(ingest_uri, 30), expected_ingest)

    def test_recovered_prepared_commit_with_repeated_keys(self):
        self.check_recovered_straddler_with_repeated_keys(commit=True)

    def test_recovered_prepared_rollback_with_repeated_keys(self):
        self.check_recovered_straddler_with_repeated_keys(commit=False)

if __name__ == '__main__':
    wttest.run()
