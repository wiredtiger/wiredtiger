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

# test_layered_arm06.py
#    A prepared transaction that began armed stays open across the demotion. The successor applies
#    the same prepare as replication would, takes the lead with it unresolved, and resolves it; the
#    demoted node resolves its own copy as oplog application would. The demotion is not refused for
#    it, the follower reads it as a prepared update, and once resolved its outcome is readable on
#    the follower, after the follower adopts the successor's checkpoint, after it takes the lead
#    again, and on a fresh follower of its checkpoint.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm06(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages(disagg_only=True)
    resolutions = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    resolve_points = [
        ('before_pickup', dict(resolve_before_pickup=True)),
        ('after_pickup', dict(resolve_before_pickup=False)),
    ]
    # With preserve_prepared, a checkpoint writes a prepared update at or below the stable
    # timestamp as a prepared cell.
    preserve = [
        ('discard', dict(preserve_prepared=False)),
        ('preserve', dict(preserve_prepared=True)),
    ]
    scenarios = make_scenarios(disagg_storages, resolutions, resolve_points, preserve)

    @property
    def conn_base_config(self):
        return 'statistics=(all),precise_checkpoint=true,' + \
            ('preserve_prepared=true,' if self.preserve_prepared else '')

    @property
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    uri = 'layered:test_layered_arm06'

    # The value of key at read_ts, 'conflict' for a prepare conflict, None if absent.
    def read_at(self, key, read_ts, conn=None):
        session = (conn or self.conn).open_session('')
        cursor = session.open_cursor(self.uri)
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        cursor.set_key(key)
        try:
            value = cursor.get_value() if cursor.search() == 0 else None
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' not in str(e):
                raise
            value = 'conflict'
        session.rollback_transaction()
        session.close()
        return value

    def prepare(self, conn):
        prepared = conn.open_session()
        cursor = prepared.open_cursor(self.uri)
        prepared.begin_transaction()
        cursor['K'] = 'v2'
        cursor['K2'] = 'w2'
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.timestamp_str(1))
        cursor.close()
        return prepared

    def resolve(self, prepared):
        if self.commit:
            prepared.commit_transaction('commit_timestamp=' + self.timestamp_str(40) +
                ',durable_timestamp=' + self.timestamp_str(40))
        else:
            prepared.rollback_transaction('rollback_timestamp=' + self.timestamp_str(40))
        prepared.close()

    def expected(self, resolved):
        kv = {'K': 'v1', 'K2': 'w1', 'S': 'straddler', 'M': 'armed', 'B': 'successor'}
        if resolved and self.commit:
            kv.update({'K': 'v2', 'K2': 'w2'})
        return kv

    def test_prepared_across_demote(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'K': 'v1', 'K2': 'w1'}, 10)
        self.checkpoint_at(10)

        # A straddler's commit is stable only and raises plain_high; an armed commit does not.
        straddler = self.conn.open_session()
        cursor = straddler.open_cursor(self.uri)
        straddler.begin_transaction()
        cursor['S'] = 'straddler'
        self.arm()
        straddler.commit_transaction('commit_timestamp=' + self.timestamp_str(15))
        straddler.close()
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 15)
        self.write_at(self.uri, {'M': 'armed'}, 18)
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 15)

        prepared = self.prepare(self.conn)

        # The demotion goes ahead with the armed prepared transaction open.
        refused = self.conn_stat(stat.conn.disagg_step_down_refused_prepared)
        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.conn_stat(stat.conn.disagg_role_leader), 0)
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_refused_prepared), refused)

        # The follower serves the prepared update from ingest: invisible below the prepare
        # timestamp, a prepare conflict above it.
        for key, before in (('K', 'v1'), ('K2', 'w1')):
            self.assertEqual(self.read_at(key, 15), before)
            self.assertEqual(self.read_at(key, 25), 'conflict')

        # The successor adopts the demotion checkpoint, which holds no committed version of the
        # prepared transaction's updates, applies the prepare and takes the lead with it
        # unresolved, so its drain meets the prepared updates in ingest.
        conn_b = self.open_node('node_b', config=self.conn_base_config)
        conn_b.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(19))
        self.disagg_advance_checkpoint_and_wait(conn_b)
        self.assertEqual(self.read_at('K', 25, conn_b), 'v1')
        self.assertEqual(self.read_at('K2', 25, conn_b), 'w1')
        prepared_b = self.prepare(conn_b)
        self.assertEqual(self.read_at('K', 25, conn_b), 'conflict')
        self.promote(conn_b)
        self.assertEqual(self.read_at('K', 15, conn_b), 'v1')
        self.assertEqual(self.read_at('K', 25, conn_b), 'conflict')
        session_b = conn_b.open_session('')
        self.write_at(self.uri, {'B': 'successor'}, 30, session_b)
        self.checkpoint_at(30, conn_b)

        # The successor resolves it and the follower applies the same outcome. The follower's
        # armed updates in the stepped-down stable tree are left behind; ingest carries them.
        if self.resolve_before_pickup:
            self.resolve(prepared_b)
            self.resolve(prepared)
            self.assertEqual(self.read_at('K', 35), 'v1')
            self.assertEqual(self.read_at('K', 45), self.expected(True)['K'])

        # The adoption prunes ingest at or below the successor's checkpoint timestamp 30. The
        # resolved commit is durable at 40 and the prepared updates are unresolved, so both keep
        # their ingest copies.
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        if not self.resolve_before_pickup:
            self.assertEqual(self.read_at('K', 25), 'conflict')
            self.resolve(prepared_b)
            self.resolve(prepared)
        self.assertEqual(self.read_kvs_at(self.uri, 45, session_b), self.expected(True))
        self.assertEqual(self.conn_stat(stat.conn.txn_prepared_updates_stepped_down), 2)
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 15)
        self.assertEqual(self.read_at('K', 35), 'v1')
        self.assertEqual(self.read_kvs_at(self.uri, 45), self.expected(True))

        # The demoted node takes the lead again. Its drain moves the ingest copies into stable.
        session_b.close()
        conn_b.close('debug=(skip_checkpoint=true)')
        self.promote()
        self.assertEqual(self.read_kvs_at(self.uri, 45), self.expected(True))
        self.assertEqual(self.read_at('K', 35), 'v1')
        self.checkpoint_at(45)

        conn_c = self.open_node('node_c', config=self.conn_base_config)
        conn_c.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(45))
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 45, session_c), self.expected(True))
        session_c.close()
        conn_c.close()

if __name__ == '__main__':
    wttest.run()
