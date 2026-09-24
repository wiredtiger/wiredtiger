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

# test_layered_frozen_02.py
#    A demoted node serves its commits above the final checkpoint before any pickup, and the
#    successor's covering checkpoint supersedes them once picked up.
@disagg_test_class
class test_layered_frozen_02(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_frozen_02'

    # The ops committed above the final checkpoint, as (commit_ts, key, value) with None removing.
    window_ops = [(30, 'w1', 'w'), (30, 'w2', 'w'), (31, 'k1', 'k1-new'), (32, 'k2', None)]

    def apply(self, ops, session=None):
        for ts, key, value in ops:
            if value is None:
                self.remove_at(self.uri, [key], ts, session)
            else:
                self.write_at(self.uri, {key: value}, ts, session)

    def expected_at(self, base, ops, read_ts=None):
        rows = dict(base)
        for ts, key, value in ops:
            if read_ts is not None and ts > read_ts:
                continue
            if value is None:
                rows.pop(key, None)
            else:
                rows[key] = value
        return rows

    def test_successor_checkpoint_supersedes_frozen(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        base = {f'k{i}': f'k{i}' for i in range(1, 6)}
        self.write_at(self.uri, base, 10)
        self.checkpoint_at(20)
        self.apply(self.window_ops)
        self.demote()

        refusals = self.refusal_counts()
        for read_ts in (20, 30, 31, 32):
            self.assertEqual(self.read_kvs_at(self.uri, read_ts),
                self.expected_at(base, self.window_ops, read_ts), f'read at {read_ts}')
        window_view = self.expected_at(base, self.window_ops)
        self.assertEqual(self.read_kvs(self.uri), window_view)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {})
        self.assertEqual(self.refusal_counts(), refusals)

        # The successor has applied the window from the log before it takes over. Its first
        # checkpoint removes a window key and adds one of its own, neither of which this node's
        # frozen tree or ingest holds.
        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.apply(self.window_ops, session_b)
        self.promote(conn_b)
        successor_ops = [(40, 'b1', 'b'), (41, 'w1', None)]
        self.apply(successor_ops, session_b)
        self.checkpoint_at(41, conn_b)

        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        final = self.expected_at(window_view, successor_ops)
        self.assertEqual(self.read_kvs_at(self.uri, 41), final)
        self.assertEqual(self.read_kvs(self.uri), final)
        self.assertEqual(self.read_kvs_at(self.uri, 32), window_view)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 50), {})
        self.assertEqual(self.refusal_counts(), refusals)

        session_b.close()
        conn_b.close('debug=(skip_checkpoint=true)')
