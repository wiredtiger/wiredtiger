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

# test_layered_async_stepdown09.py
#   A planned step-down holds stable at the final checkpoint until the demotion, so the commits above
#   that checkpoint stay out of it and the demoted node serves them from its frozen tree. Demoting
#   with stable past the last checkpoint is refused and leaves the node a working leader.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# Each case runs in a subprocess and is judged by its exit status, because a refused demotion that
# is not recoverable takes the process down.
@disagg_test_class
class test_layered_async_stepdown09(LayeredStepdownMixin, wttest.WiredTigerTestCase,
                                    suite_subprocess):
    conn_config = 'statistics=(all),precise_checkpoint=true,disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # A demote may happen at any stable timestamp: content committed after the last checkpoint is
    # kept in the frozen tree and stays valid across the following step-up, so the demote does not
    # require stable to sit at the checkpoint. The behind case (stable ahead of the checkpoint) and
    # the no-checkpoint case (the frozen tree is the only copy) must both preserve the data.
    checkpoints = [
        ('at_stable',     dict(checkpoint_ts=20)),
        ('behind_stable', dict(checkpoint_ts=15)),
        ('no_checkpoint', dict(checkpoint_ts=None)),
    ]
    scenarios = make_scenarios(disagg_storages, checkpoints)

    test_name = __qualname__

    uri = f'layered:{test_name}'
    stable = 20

    def subprocess_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v1'}, 10)
        if self.checkpoint_ts is not None:
            self.checkpoint_at(self.checkpoint_ts)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.stable))
        self.write_at(self.uri, {'k2': 'v2'}, 30)

        expected = {'k1': 'v1', 'k2': 'v2'}
        self.demote()
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)

    def test_step_down_checkpoint_boundary(self):
        # Precise checkpoint requires a stable timestamp when the parent connection closes.
        self.set_global_ts(1, 1)
        rc, _ = self.run_subprocess_function(
            'SUBPROCESS',
            'test_layered_async_stepdown09.test_layered_async_stepdown09.subprocess_step_down',
            silent=True,
            scenario=self.scenario_name)
        self.assertEqual(rc, 0, f'the step-down case failed in the subprocess, rc={rc}')
