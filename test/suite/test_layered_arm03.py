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

# test_layered_arm03.py
#    Disarming abandons a planned step-down: the mirrored ingest copies are discarded, so a later
#    arm and demotion serve the stable writes made in between rather than stale copies. And a
#    prepared transaction that is not armed blocks the demotion until it resolves and a checkpoint
#    covers its commit.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm03(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),precise_checkpoint=true,disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_arm03'

    def rows(self, tag, n=10):
        return {f'k{i:03}': tag for i in range(n)}

    def test_disarm(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, self.rows('base'), 10)

        self.arm()
        self.write_at(self.uri, self.rows('mirrored'), 20)
        self.write_at(self.uri, {'extra': 'mirrored'}, 21)
        self.assertEqual(set(self.read_kvs_at(self.ingest_uri(self.uri), 30)),
            set(self.rows('mirrored')) | {'extra'})

        self.disarm()
        self.assertEqual(self.step_down_is_armed(), 0)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 30), {})

        # Stable writes after the disarm overwrite and remove what the mirrored copies held.
        self.write_at(self.uri, self.rows('after'), 30)
        self.remove_at(self.uri, ['extra'], 31)
        expected = self.rows('after')
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {})

        # A later planned step-down serves the stable writes, not the discarded copies.
        self.arm()
        self.write_at(self.uri, {'k000': 'rearmed'}, 40)
        expected['k000'] = 'rearmed'
        self.checkpoint_at(35)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 36), self.rows('after'))

    def test_disarm_then_rearm_covers_mirrored(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, self.rows('base'), 10)
        self.checkpoint_at(10)

        # Armed writes lose their ingest copies at the disarm and are left in stable alone.
        self.arm()
        self.write_at(self.uri, self.rows('mirrored'), 20)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.disarm()

        self.arm()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.reconfigure('disaggregated=(role="follower")'),
            '/step-down refused: last checkpoint .* is below the newest unmirrored commit/')
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_refused_plain_high), 1)
        self.expect_demote_refused(stat.conn.disagg_step_down_refused_plain_high)

        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs(self.uri), self.rows('mirrored'))

    def test_disarm_refused_with_armed_transaction(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm()

        session = self.conn.open_session()
        session.begin_transaction()
        cursor = session.open_cursor(self.uri)
        cursor['k'] = 'v'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.disarm(),
            '/disarm refused: an armed transaction is in flight/')
        self.assertEqual(self.step_down_is_armed(), 1)
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()
        session.close()

        self.disarm()
        self.assertEqual(self.read_kvs(self.uri), {'k': 'v'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 20), {})
        self.checkpoint_at(10)

    def test_disarm_refused_with_armed_create(self):
        self.set_global_ts(1, 1)
        self.arm()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k': 'v'}, 10)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.disarm(),
            '/disarm refused: .* was created while armed and has no stable constituent/')
        self.assertEqual(self.step_down_is_armed(), 1)

        self.complete_step_down(10)
        self.assertEqual(self.read_kvs(self.uri), {'k': 'v'})

    def test_prepared_straddler(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'base': 'v'}, 10)

        # A transaction that began before the arm prepares after it.
        prepared = self.conn.open_session()
        cursor = prepared.open_cursor(self.uri)
        prepared.begin_transaction()
        cursor['prepared'] = 'v'
        self.arm()
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.timestamp_str(1))

        self.write_at(self.uri, {'armed': 'v'}, 25)
        self.checkpoint_at(15)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.reconfigure('disaggregated=(role="follower")'),
            '/step-down refused: a prepared transaction that is not armed is unresolved/')
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_refused_prepared), 1)
        self.assertEqual(self.conn_stat(stat.conn.disagg_role_leader), 1)

        # Resolving it raises plain_high, so the checkpoint must now cover its commit.
        prepared.commit_transaction('commit_timestamp=' + self.timestamp_str(30) +
            ',durable_timestamp=' + self.timestamp_str(30))
        cursor.close()
        prepared.close()
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 30)
        self.expect_demote_refused(stat.conn.disagg_step_down_refused_plain_high)

        self.checkpoint_at(30)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs(self.uri), {'base': 'v', 'prepared': 'v', 'armed': 'v'})

if __name__ == '__main__':
    wttest.run()
