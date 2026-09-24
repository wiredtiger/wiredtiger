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

# test_layered_frozen_03.py
#    A failed step-down: the node demotes with commits above its final checkpoint and steps back up
#    with no pickup in between. Those commits must reach its next checkpoint, so a fresh follower
#    picking that checkpoint up sees them.
@disagg_test_class
class test_layered_frozen_03(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # Whether the demoted node applies a follower write before stepping back up, which the step-up
    # drains from ingest on top of the un-frozen tree.
    follower_writes = [
        ('frozen_only', dict(follower_write=False)),
        ('with_ingest', dict(follower_write=True)),
    ]
    scenarios = make_scenarios(disagg_storages, follower_writes)

    uri = 'layered:test_layered_frozen_03'

    def test_step_back_up_keeps_window(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        base = {f'k{i}': 'base' for i in range(5)}
        self.write_at(self.uri, base, 10)
        self.checkpoint_at(20)

        window = {f'w{i}': 'window' for i in range(5)}
        self.write_at(self.uri, window, 30)
        self.write_at(self.uri, {'k0': 'window-update'}, 31)
        self.remove_at(self.uri, ['k1'], 32)
        expected = {**base, **window, 'k0': 'window-update'}
        del expected['k1']

        self.demote()
        self.assertEqual(self.read_kvs_at(self.uri, 32), expected)
        if self.follower_write:
            self.write_at(self.uri, {'f': 'follower'}, 35)
            expected['f'] = 'follower'
        self.assertEqual(self.read_kvs(self.uri), expected)

        self.promote()
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {})
        self.write_at(self.uri, {'after': 'leader'}, 40)
        expected['after'] = 'leader'
        self.checkpoint_at(40)

        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(self.uri), 40), expected)
        conn_c = self.open_node('fresh', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 40, session_c), expected,
            'the checkpoint after stepping back up must hold the frozen commits')
        self.assertEqual(self.read_kvs(self.uri, session_c), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 20, session_c), base)
        session_c.close()
        conn_c.close('debug=(skip_checkpoint=true)')
