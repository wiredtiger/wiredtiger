#!/usr/bin/env python
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
# After reopening the connection, "prepared_discover:" must discover and
# allow claims of the persisted prepared transaction even when no cursor
# has yet been opened on the layered table on the new connection. Exercised
# for both roles the connection can come back as. Until the transaction is
# resolved, its keys cannot be written by anyone else.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_prepare_discover08(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    tablename = test_name
    uri = 'layered:' + tablename

    # A leader's writes after a step-down is announced are either mirrored to
    # the stable table or go to the ingest table alone.
    role_scenarios = [
        ('standby', dict(reopen_role='follower', step_down_ts=None, write_mirroring=True)),
        ('primary', dict(reopen_role='leader', step_down_ts=None, write_mirroring=True)),
        ('primary_claim_during_stepdown',
            dict(reopen_role='leader', step_down_ts=175, write_mirroring=True)),
        ('primary_claim_during_unmirrored_stepdown',
            dict(reopen_role='leader', step_down_ts=175, write_mirroring=False)),
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, role_scenarios)

    conn_base_config = ('cache_size=10MB,statistics=(all),'
                        'precise_checkpoint=true,preserve_prepared=true,')

    def leader_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader",' + \
            f'stepdown_write_mirroring={str(self.write_mirroring).lower()})'

    def conn_config(self):
        return self.leader_config()

    def test_prepare_discover_first_cursor_on_connection(self):
        prepared_id = 123

        # Leader: commit some data, prepare a later transaction, advance stable
        # past the prepare, then checkpoint so the prepared update is included.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        for i in range(1, 4):
            cursor[i] = f'committed_value_{i}'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))

        self.session.begin_transaction()
        for i in range(4, 7):
            cursor[i] = f'prepared_value_{i}'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(100) +
            ',prepared_id=' + self.prepared_id_str(prepared_id))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))

        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

        cursor.close()

        # Capture checkpoint metadata while the original leader connection is
        # still open, then reopen in the role this scenario targets.
        (checkpoint_meta, checkpoint_lsn) = self.disagg_get_checkpoint_install()
        if self.reopen_role == 'follower':
            reopen_config = (self.conn_base_config +
                             'disaggregated=(role="follower",'
                             f'checkpoint_meta="{checkpoint_meta}",'
                             f'checkpoint_lsn={checkpoint_lsn})')
        else:
            reopen_config = self.leader_config()
        self.reopen_conn(config=reopen_config)

        # Opening "prepared_discover:" must work as the first cursor on the
        # reopened connection; successful discovery must not depend on a
        # prior cursor having been opened on the layered table.
        prepared_discover_cursor = self.session.open_cursor('prepared_discover:')

        self.assertEqual(prepared_discover_cursor.next(), 0)
        self.assertEqual(prepared_discover_cursor.get_key(), prepared_id)
        if self.step_down_ts is not None:
            self.set_step_down_ts(self.step_down_ts)

        claim_session = self.conn.open_session()
        claim_session.begin_transaction('claim_prepared_id=' + self.prepared_id_str(prepared_id))

        # Another transaction cannot write a key the prepared transaction holds.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor.set_key(4)
        cursor.set_value('competing_value')
        self.expect_conflict_rollback(cursor.update)
        self.session.rollback_transaction()

        claim_session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(200) +
            ',durable_timestamp=' + self.timestamp_str(210))

        self.assertEqual(prepared_discover_cursor.next(), wiredtiger.WT_NOTFOUND)
        prepared_discover_cursor.close()
        claim_session.close()

        if self.step_down_ts is not None:
            self.complete_step_down(self.step_down_ts)
            self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 250),
                {i: f'prepared_value_{i}' for i in range(4, 7)})

        # Once the transaction is resolved, a transaction reading its commit can write the key.
        expected = {i: f'committed_value_{i}' for i in range(1, 4)}
        expected.update({i: f'prepared_value_{i}' for i in range(4, 7)})
        self.assertEqual(self.read_kvs_at(self.uri, 250), expected)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(250))
        cursor[4] = 'later_value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(260))
        cursor.close()
        expected[4] = 'later_value'
        self.assertEqual(self.read_kvs_at(self.uri, 270), expected)
