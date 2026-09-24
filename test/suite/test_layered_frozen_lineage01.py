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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered_frozen_lineage01.py
#    A demoted node that adopts a successor's checkpoint must abandon its own records when it steps
#    up again, even when a frozen tree survives the pickup: one for a table the successor dropped,
#    or one whose checkpoint the successor did not change.
@disagg_test_class
class test_layered_frozen_lineage01(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    drop_scenarios = [
        ('drop', dict(drop=True)),
        ('no_drop', dict(drop=False)),
    ]
    scenarios = make_scenarios(disagg_storages, drop_scenarios)

    uri1 = 'layered:test_layered_frozen_lineage01_t1'
    uri2 = 'layered:test_layered_frozen_lineage01_t2'

    def rows(self, tag):
        return {f'{tag}{i}': tag for i in range(10)}

    def test_frozen_after_foreign_adoption(self):
        self.set_global_ts(1, 1)
        for uri in (self.uri1, self.uri2):
            self.session.create(uri, 'key_format=S,value_format=S')
            self.write_at(uri, self.rows('a'), 10)
        self.checkpoint_at(10)
        for uri in (self.uri1, self.uri2):
            self.write_at(uri, self.rows('b'), 20)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.demote()
        self.assertGreater(self.connection_stat(stat.conn.disagg_frozen_handles), 0)

        # The successor applies the demoted node's window, or drops the first table instead so its
        # checkpoint does not list it.
        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.promote(conn_b)
        self.write_at(self.uri2, self.rows('b'), 20, session_b)
        if self.drop:
            session_b.drop(self.uri1)
        else:
            self.write_at(self.uri1, self.rows('b'), 20, session_b)
        self.write_at(self.uri2, self.rows('c'), 30, session_b)
        self.checkpoint_at(30, conn_b)

        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        expected1 = {**self.rows('a'), **self.rows('b')}
        expected2 = {**expected1, **self.rows('c')}
        self.assertEqual(self.read_kvs(self.uri2), expected2)
        # A dropped table's frozen tree survives the pickup and keeps serving the window.
        self.assertEqual(self.read_kvs(self.uri1), expected1)

        session_b.close()
        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

        abandoned = self.connection_stat(stat.conn.disagg_abandon_checkpoint_succeed)
        self.promote()
        self.ignoreStdoutPatternIfExists(
            'discards modified frozen tree file:test_layered_frozen_lineage01_t1')
        self.assertGreater(
            self.connection_stat(stat.conn.disagg_abandon_checkpoint_succeed), abandoned,
            'step-up after adopting a foreign checkpoint did not abandon')
        self.assertGreaterEqual(
            self.connection_stat(stat.conn.disagg_step_up_frozen_after_foreign_adoption), 1)

        # The reaper finishes the successor's drop on the new leader.
        if self.drop:
            self.session.drop(self.uri1)
        self.write_at(self.uri2, self.rows('d'), 40)
        self.checkpoint_at(40)
        expected2 = {**expected2, **self.rows('d')}

        conn_c = self.open_node('reader', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs(self.uri2, session_c), expected2)
        if self.drop:
            self.assertNotIn(self.uri1, self.layered_tables(conn_c))
        else:
            self.assertEqual(self.read_kvs(self.uri1, session_c), expected1)
        session_c.close()
        conn_c.close()
