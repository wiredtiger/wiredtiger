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

# test_layered_arm04.py
#    Hand the lead A -> B -> A across planned step-downs. The demoted node keeps nothing above its
#    checkpoint, so when it takes the lead back after adopting the successor's checkpoint its step-up
#    abandons, and every commit is readable from a fresh node, including when the successor dropped
#    one of the tables.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm04(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    drop_scenarios = [
        ('drop', dict(drop=True)),
        ('no_drop', dict(drop=False)),
    ]
    scenarios = make_scenarios(disagg_storages, drop_scenarios)

    uri1 = 'layered:test_layered_arm04_t1'
    uri2 = 'layered:test_layered_arm04_t2'

    def rows(self, tag):
        return {f'{tag}{i}': tag for i in range(10)}

    def test_hand_back(self):
        self.set_global_ts(1, 1)
        for uri in (self.uri1, self.uri2):
            self.session.create(uri, 'key_format=S,value_format=S')
            self.write_at(uri, self.rows('a'), 10)
        self.checkpoint_at(10)

        # A's armed window sits above its step-down checkpoint, in ingest alone.
        self.arm()
        for uri in (self.uri1, self.uri2):
            self.write_at(uri, self.rows('b'), 20)
        self.checkpoint_at(15)
        self.conn.reconfigure('disaggregated=(role="follower")')
        expected1 = {**self.rows('a'), **self.rows('b')}
        for uri in (self.uri1, self.uri2):
            self.assertEqual(self.read_kvs(uri), expected1)

        # The successor applies A's window as replication would, takes the lead, and drops the
        # first table or writes it again.
        conn_b = self.open_node('node_b', config=self.conn_base_config)
        conn_b.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(15))
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        for uri in (self.uri1, self.uri2):
            self.write_at(uri, self.rows('b'), 20, session_b)
        abandoned_b = self.conn_stat(stat.conn.disagg_abandon_checkpoint_succeed, conn_b)
        self.promote(conn_b)
        self.assertGreater(
            self.conn_stat(stat.conn.disagg_abandon_checkpoint_succeed, conn_b), abandoned_b)
        if self.drop:
            # A table with data above the last checkpoint cannot be dropped.
            self.checkpoint_at(20, conn_b)
            self.dropUntilSuccess(session_b, self.uri1)
        else:
            self.write_at(self.uri1, self.rows('c'), 30, session_b)
        self.write_at(self.uri2, self.rows('c'), 30, session_b)
        self.checkpoint_at(30, conn_b)

        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        expected2 = {**expected1, **self.rows('c')}
        self.assertEqual(self.read_kvs(self.uri2), expected2)
        if not self.drop:
            self.assertEqual(self.read_kvs(self.uri1), expected2)

        # B's planned step-down takes a final checkpoint, which A adopts before taking the lead.
        session_b.close()
        self.demote(30, conn_b)
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

        # A takes the lead back: its step-up abandons, whatever it held before.
        abandoned = self.conn_stat(stat.conn.disagg_abandon_checkpoint_succeed)
        self.promote()
        self.assertGreater(self.conn_stat(stat.conn.disagg_abandon_checkpoint_succeed), abandoned,
            'step-up after adopting the successor checkpoint did not abandon')

        # Replication finishes the successor's drop on the new leader.
        if self.drop:
            self.checkpoint_at(30)
            self.dropUntilSuccess(self.session, self.uri1)
        self.write_at(self.uri2, self.rows('d'), 40)
        self.checkpoint_at(40)
        expected2 = {**expected2, **self.rows('d')}

        conn_c = self.open_node('node_c', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs(self.uri2, session_c), expected2)
        if self.drop:
            self.assertNotIn(self.uri1, self.layered_tables(conn_c))
        else:
            self.assertEqual(self.read_kvs(self.uri1, session_c),
                {**expected1, **self.rows('c')})
        session_c.close()
        conn_c.close('debug=(skip_checkpoint=true)')

if __name__ == '__main__':
    wttest.run()
