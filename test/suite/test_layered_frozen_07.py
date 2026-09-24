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

# test_layered_frozen_07.py
#    A demoted node goes down before any pickup. Its frozen commits above the final checkpoint are
#    gone with the process: it restarts as a follower from the final checkpoint, re-applies the lost
#    commits from the log, and then picks up the successor's checkpoint.
@disagg_test_class
class test_layered_frozen_07(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    follower_config = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    restarts = [
        ('keep_local_files', dict(lose_local_files=False)),
        ('lose_local_files', dict(lose_local_files=True)),
    ]
    scenarios = make_scenarios(disagg_storages, restarts)

    uri = 'layered:test_layered_frozen_07'

    def test_restart_demoted_node_before_pickup(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        base = {f'k{i}': 'base' for i in range(5)}
        self.write_at(self.uri, base, 10)
        self.checkpoint_at(20)
        window = {f'w{i}': 'window' for i in range(5)}
        self.write_at(self.uri, window, 30)
        self.demote()
        self.assertEqual(self.read_kvs(self.uri), {**base, **window})

        if self.lose_local_files:
            self.restart_without_local_files(config=self.follower_config)
        else:
            self.reopen_conn(config=self.follower_config)
            self.disagg_advance_checkpoint_and_wait(self.conn)

        self.assertEqual(self.read_kvs(self.uri), base,
            'the restarted node must serve exactly the final checkpoint')
        self.assertEqual(self.read_kvs_at(self.uri, 30), base)

        # Log application re-delivers the lost commits.
        self.write_at(self.uri, window, 30)
        self.assertEqual(self.read_kvs_at(self.uri, 30), {**base, **window})

        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.write_at(self.uri, window, 30, session_b)
        self.promote(conn_b)
        self.write_at(self.uri, {'b': 'successor'}, 40, session_b)
        self.checkpoint_at(40, conn_b)

        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        expected = {**base, **window, 'b': 'successor'}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 20), base)

        session_b.close()
        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')
