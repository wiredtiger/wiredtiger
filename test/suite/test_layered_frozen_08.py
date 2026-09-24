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
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# test_layered_frozen_08.py
#    A pickup whose checkpoint timestamp is below the newest commit in the frozen tree does not
#    cover it. It must not take the frozen commits away: the demoted node keeps reading them until a
#    covering checkpoint arrives, and then reads that checkpoint.
@disagg_test_class
class test_layered_frozen_08(LayeredStepdownMixin, wttest.WiredTigerTestCase, suite_subprocess):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_frozen_08'
    base = {f'k{i}': 'base' for i in range(5)}
    window = {f'w{i}': 'window' for i in range(5)}

    def demote_with_window(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, self.base, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, self.window, 30)
        self.demote()

    # Hand over to a successor that has applied the window, and pick up its covering checkpoint.
    def pick_up_covering(self, conn_b, session_b, expected):
        self.write_at(self.uri, self.window, 30, session_b)
        self.write_at(self.uri, {'b2': 'successor'}, 40, session_b)
        self.checkpoint_at(40, conn_b)
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        expected = {**expected, **self.window, 'b2': 'successor'}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)

    # The demoted node is handed its own final checkpoint back.
    def test_own_final_checkpoint(self):
        self.demote_with_window()
        self.write_at(self.uri, {'f': 'follower'}, 35)
        expected = {**self.base, **self.window, 'f': 'follower'}

        # Whether this is deferred or adopted as a no-op, it must not be waited on: a deferred
        # pickup is only adopted once a covering checkpoint arrives.
        self.disagg_advance_checkpoint(self.conn)
        self.ignoreStdoutPatternIfExists('Picking up the same checkpoint again')
        self.assertEqual(self.read_kvs_at(self.uri, 35), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 20), self.base)

        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.write_at(self.uri, {'f': 'follower'}, 35, session_b)
        self.promote(conn_b)
        self.pick_up_covering(conn_b, session_b, expected)
        session_b.close()
        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

    # A successor checkpoint below the frozen commits, constructed by promoting a node that never
    # applied them. The pickup must be deferred rather than adopted. Runs in a subprocess because
    # the engine currently panics on such a pickup; until it defers instead, this fails.
    def subprocess_successor_checkpoint_below_frozen(self):
        self.demote_with_window()
        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.promote(conn_b)
        self.write_at(self.uri, {'b1': 'successor'}, 25, session_b)
        self.checkpoint_at(25, conn_b)

        self.disagg_advance_checkpoint(self.conn, conn_b)
        expected = {**self.base, **self.window}
        self.assertEqual(self.read_kvs_at(self.uri, 30), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)

        self.pick_up_covering(conn_b, session_b, {**expected, 'b1': 'successor'})
        session_b.close()
        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

    def test_successor_checkpoint_below_frozen(self):
        self.set_global_ts(1, 1)
        rc, _ = self.run_subprocess_function('SUBPROCESS',
            'test_layered_frozen_08.test_layered_frozen_08.subprocess_successor_checkpoint_below_frozen',
            silent=True, scenario=self.scenario_name)
        self.assertEqual(rc, 0, f'the non-covering pickup failed in the subprocess, rc={rc}')
