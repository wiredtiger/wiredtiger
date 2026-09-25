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

# test_layered_arm07.py
#    An armed prepared transaction held across the demotion modifies a key, removes a key, or
#    inserts into a table created while armed. It is resolved on the follower, or left unresolved
#    until the demoted node takes the lead again and resolved there, committed or rolled back. Its
#    outcome is readable on the node and on a fresh follower of the node's next checkpoint. And a
#    table created while armed, so with no stable constituent in the demotion checkpoint, keeps a
#    write at or below that checkpoint's timestamp when the node takes the lead again.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm07(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages(disagg_only=True)
    ops = [
        ('modify', dict(op='modify')),
        ('remove', dict(op='remove')),
        ('armed_create', dict(op='armed_create')),
    ]
    resolutions = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    resolve_points = [
        ('on_follower', dict(resolve_on_follower=True)),
        ('after_stepup', dict(resolve_on_follower=False)),
    ]
    preserve = [
        ('discard', dict(preserve_prepared=False)),
        ('preserve', dict(preserve_prepared=True)),
    ]
    scenarios = make_scenarios(disagg_storages, ops, resolutions, resolve_points, preserve)

    @property
    def conn_base_config(self):
        return 'statistics=(all),precise_checkpoint=true,' + \
            ('preserve_prepared=true,' if self.preserve_prepared else '')

    @property
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    base_uri = 'layered:test_layered_arm07'
    created_uri = 'layered:test_layered_arm07_created'

    @property
    def uri(self):
        return self.created_uri if self.op == 'armed_create' else self.base_uri

    def read_at(self, read_ts, conn=None):
        session = (conn or self.conn).open_session('')
        cursor = session.open_cursor(self.uri)
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        cursor.set_key('K')
        try:
            ret = cursor.search()
            value = cursor.get_value() if ret == 0 else None
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' not in str(e):
                raise
            value = 'conflict'
        session.rollback_transaction()
        session.close()
        return value

    def before(self):
        return None if self.op == 'armed_create' else 'v1'

    def after(self):
        if not self.commit:
            return self.before()
        return {'modify': 'v1X', 'remove': None, 'armed_create': 'v2'}[self.op]

    def resolve(self, prepared):
        if self.commit:
            prepared.commit_transaction('commit_timestamp=' + self.timestamp_str(30) +
                ',durable_timestamp=' + self.timestamp_str(30))
        else:
            prepared.rollback_transaction('rollback_timestamp=' + self.timestamp_str(30))
        prepared.close()

    def check(self, conn=None):
        self.assertEqual(self.read_at(15, conn), self.before())
        self.assertEqual(self.read_at(35, conn), self.after())
        self.assertEqual(self.read_kvs_at(self.base_uri, 35, (conn or self.conn).open_session()),
            {'K': 'v1', 'L': 'v1'} if self.op == 'armed_create' else
            {k: v for k, v in {'K': self.after(), 'L': 'v1'}.items() if v is not None})

    def test_prepared_op_across_demote(self):
        self.set_global_ts(1, 1)
        self.session.create(self.base_uri, 'key_format=S,value_format=S')
        self.write_at(self.base_uri, {'K': 'v1', 'L': 'v1'}, 10)
        self.checkpoint_at(10)
        self.arm()
        if self.op == 'armed_create':
            self.session.create(self.created_uri, 'key_format=S,value_format=S')

        prepared = self.conn.open_session()
        cursor = prepared.open_cursor(self.uri)
        prepared.begin_transaction()
        cursor.set_key('K')
        if self.op == 'modify':
            self.assertEqual(cursor.modify([wiredtiger.Modify('X', 2, 0)]), 0)
        elif self.op == 'remove':
            self.assertEqual(cursor.remove(), 0)
        else:
            cursor.set_value('v2')
            self.assertEqual(cursor.insert(), 0)
        cursor.close()
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.timestamp_str(1))
        self.assertEqual(self.read_at(15), self.before())
        self.assertEqual(self.read_at(25), 'conflict')

        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_at(15), self.before())
        self.assertEqual(self.read_at(25), 'conflict')

        if self.resolve_on_follower:
            self.resolve(prepared)
            self.check()

        # Taking the lead again drains ingest, with the prepared updates unresolved or not.
        self.promote()
        if not self.resolve_on_follower:
            self.assertEqual(self.read_at(15), self.before())
            self.assertEqual(self.read_at(25), 'conflict')
            self.resolve(prepared)
        self.check()
        # Whether drained while resolved or resolved after the drain, a commit is now stable only.
        if self.commit:
            self.assertEqual(self.conn_stat(stat.conn.disagg_unmirrored_durable_ts), 30)

        self.checkpoint_at(35)
        conn_c = self.open_node('node_c', config=self.conn_base_config)
        conn_c.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(35))
        self.disagg_advance_checkpoint_and_wait(conn_c)
        self.check(conn_c)
        conn_c.close()

@disagg_test_class
class test_layered_arm07_create(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages(disagg_only=True)
    preserve = [
        ('discard', dict(preserve_config='')),
        ('preserve', dict(preserve_config='preserve_prepared=true,')),
    ]
    scenarios = make_scenarios(disagg_storages, preserve)

    @property
    def conn_base_config(self):
        return 'statistics=(all),precise_checkpoint=true,' + self.preserve_config

    @property
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    uri = 'layered:test_layered_arm07_create'

    def test_armed_create_below_checkpoint(self):
        self.set_global_ts(1, 1)
        self.arm()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k': 'v'}, 15)
        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs_at(self.uri, 25), {'k': 'v'})

        self.promote()
        self.assertEqual(self.read_kvs_at(self.uri, 25), {'k': 'v'})
        self.checkpoint_at(25)

        conn_c = self.open_node('node_c', config=self.conn_base_config)
        conn_c.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(25))
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 25, session_c), {'k': 'v'})
        session_c.close()
        conn_c.close()

if __name__ == '__main__':
    wttest.run()
