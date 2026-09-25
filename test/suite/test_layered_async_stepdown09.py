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
#   A step-down requires the last checkpoint to cover every commit that was not mirrored to ingest,
#   because that checkpoint is what the next leader picks up. Without one the step-down is refused,
#   the node stays a working leader, and a retry after a covering checkpoint succeeds.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_async_stepdown09(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_config = 'precise_checkpoint=true,disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    checkpoints = [
        ('above_commit',  dict(checkpoint_ts=20,   refused=False)),
        ('at_commit',     dict(checkpoint_ts=10,   refused=False)),
        ('below_commit',  dict(checkpoint_ts=5,    refused=True)),
        ('no_checkpoint', dict(checkpoint_ts=None, refused=True)),
    ]
    scenarios = make_scenarios(disagg_storages, checkpoints)

    uri = 'layered:test_layered_async_stepdown09'

    def test_step_down_checkpoint_boundary(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v1'}, 10)
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 10)

        self.arm()
        if self.checkpoint_ts is not None:
            self.checkpoint_at(self.checkpoint_ts)

        if self.refused:
            self.expect_demote_refused(stat.conn.disagg_step_down_refused_plain_high)
            self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_armed), 1)
            # The leader keeps accepting writes after a refusal.
            self.write_at(self.uri, {'k2': 'v2'}, 12)
            self.checkpoint_at(12)

        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.conn_stat(stat.conn.disagg_role_leader), 0)
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_armed), 0)

if __name__ == '__main__':
    wttest.run()
