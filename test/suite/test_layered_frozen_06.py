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
from wtscenario import make_scenarios

# test_layered_frozen_06.py
#    Two full hand-overs between two nodes: each leader demotes with commits above its final
#    checkpoint, the other node takes over, writes and checkpoints, and the demoted node picks that
#    up and later takes over again. Every node reads the full history after every step.
@disagg_test_class
class test_layered_frozen_06(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_frozen_06'

    def check(self, conn, session, expected, history, where):
        self.assertEqual(self.read_kvs(self.uri, session), expected, f'{where}: newest')
        for ts, rows in history:
            self.assertEqual(self.read_kvs_at(self.uri, ts, session), rows, f'{where}: at {ts}')

    def test_two_cycles(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        conn_b = self.open_node('node_b', config=self.conn_base_config)
        session_b = conn_b.open_session('')
        a, sa = self.conn, self.session
        b, sb = conn_b, session_b

        expected = {}
        history = []

        def commit(items, ts, *sessions):
            for session in sessions:
                self.write_at(self.uri, items, ts, session)
            expected.update(items)
            history.append((ts, dict(expected)))

        # Cycle 1: A leads, checkpoints, commits above the checkpoint and demotes.
        commit({'a1': 'A'}, 10, sa)
        self.checkpoint_at(20, a)
        self.disagg_advance_checkpoint_and_wait(b, a)
        commit({'a2': 'A', 'a1': 'A2'}, 30, sa, sb)
        self.demote(a)
        self.check(a, sa, expected, history, 'A demoted')
        self.check(b, sb, expected, history, 'B following')

        # B takes over, commits, checkpoints, and commits again above that checkpoint.
        self.promote(b)
        commit({'b1': 'B'}, 40, sb, sa)
        self.checkpoint_at(40, b)
        self.disagg_advance_checkpoint_and_wait(a, b)
        self.check(a, sa, expected, history, 'A after first pickup')
        commit({'b2': 'B', 'a2': 'B2'}, 50, sb, sa)
        self.demote(b)
        self.check(b, sb, expected, history, 'B demoted')
        self.check(a, sa, expected, history, 'A following')

        # Cycle 2: A takes over again from its picked-up checkpoint and its applied log.
        self.promote(a)
        self.check(a, sa, expected, history, 'A promoted again')
        commit({'a3': 'A'}, 60, sa, sb)
        self.checkpoint_at(60, a)
        self.disagg_advance_checkpoint_and_wait(b, a)
        self.check(b, sb, expected, history, 'B after pickup')
        commit({'a4': 'A'}, 70, sa, sb)
        self.demote(a)
        self.check(a, sa, expected, history, 'A demoted again')
        self.check(b, sb, expected, history, 'B following again')

        # A fresh node sees everything once B covers the last window.
        self.promote(b)
        self.checkpoint_at(70, b)
        conn_c = self.open_node('node_c', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c, b)
        session_c = conn_c.open_session('')
        self.check(conn_c, session_c, expected, history, 'fresh node')
        self.disagg_advance_checkpoint_and_wait(a, b)
        self.check(a, sa, expected, history, 'A after the last pickup')

        session_c.close()
        conn_c.close('debug=(skip_checkpoint=true)')
        self.demote(b)
        session_b.close()
        conn_b.close('debug=(skip_checkpoint=true)')
