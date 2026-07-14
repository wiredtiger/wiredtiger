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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown01.py
#    Write routing and lifecycle: pre-arm writes route to stable, post-arm writes route to ingest.
@disagg_test_class
class test_layered_async_stepdown01(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:async_stepdown'

    # Pre-arm writes go to stable, post-arm writes go to ingest.
    def test_pre_post_arm_write_routing(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        pre = {'pre' + str(i) for i in range(5)}
        post = {'post' + str(i) for i in range(5)}

        # Before the arm, writes go to stable. The ingest constituent stays empty.
        self.write_at(self.uri, {k: 'stable' for k in pre}, 10)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 15), set(),
            'pre-arm writes must not be in the ingest table')
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 15), pre,
            'pre-arm writes must land in the stable table')
        self.assertEqual(self.read_keys_at(self.uri, 15), pre)

        self.arm(20)

        # After the arm, writes are directed to ingest. 
        self.write_at(self.uri, {k: 'ingest' for k in post}, 30)

        # The leader now reads ingest-first, merged over the live stable table: it sees both halves.
        self.assertEqual(self.read_keys_at(self.uri, 40), pre | post)

        # Post-arm keys landed in ingest and never reached stable.
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), post)
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 40), pre,
            'post-arm writes must not be in the stable table')

    # Update/modify/remove of stable keys route to ingest after arm, like insert.
    def test_post_arm_update_modify_remove_routing(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.write_at(self.uri, {'k1': 'base', 'k2': 'base', 'k3': 'base'}, 10)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 15), set(),
            'pre-arm writes must not be in the ingest table')
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 15), {'k1', 'k2', 'k3'},
            'pre-arm writes must land in the stable table')

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)

        # Update k1.
        self.session.begin_transaction()
        cursor['k1'] = 'updated'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        # Remove k2: a tombstone record over stable's k2.
        self.session.begin_transaction()
        cursor.set_key('k2')
        cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))

        # Modify k3: build the new value on the stable base, write the result to ingest.
        self.session.begin_transaction()
        cursor.set_key('k3')
        cursor.modify([wiredtiger.Modify('X', 0, 1)])  # 'base' -> 'Xase'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(32))
        cursor.close()

        # Merged result on the leader: update and modify reflected, remove hides the stable key.
        kv = self.read_kvs_at(self.uri, 40)
        self.assertEqual(kv.get('k1'), 'updated')
        self.assertEqual(kv.get('k3'), 'Xase')
        self.assertNotIn('k2', kv)

        # All three writes landed in ingest (the remove as a tombstone record shadowing stable);
        # the stable versions are untouched.
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1', 'k2', 'k3'})
        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 40),
            {'k1': 'base', 'k2': 'base', 'k3': 'base'},
            'post-arm update/modify/remove must not touch the stable table')

    # All tables share the same cutoff; arming once routes all their post-arm writes to ingest.
    def test_multiple_tables_share_cutoff(self):
        uri1 = 'layered:multi1'
        uri2 = 'layered:multi2'
        self.set_global_ts(1, 1)
        self.session.create(uri1, 'key_format=S,value_format=S')
        self.session.create(uri2, 'key_format=S,value_format=S')

        self.write_at(uri1, {'a': 'pre'}, 10)
        self.write_at(uri2, {'b': 'pre'}, 10)

        self.arm(20)

        self.write_at(uri1, {'c': 'post'}, 30)
        self.write_at(uri2, {'d': 'post'}, 30)

        self.assertEqual(self.read_keys_at(self.ingest_uri(uri1), 40), {'c'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(uri2), 40), {'d'})
        self.assertEqual(self.read_keys_at(self.stable_uri(uri1), 40), {'a'})
        self.assertEqual(self.read_keys_at(self.stable_uri(uri2), 40), {'b'})
        self.assertEqual(self.read_kvs_at(uri1, 40), {'a': 'pre', 'c': 'post'})
        self.assertEqual(self.read_kvs_at(uri2, 40), {'b': 'pre', 'd': 'post'})

    # Ingest content survives the full step-down sequence.
    def test_content_survives_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        pre = {'pre' + str(i) for i in range(5)}
        self.write_at(self.uri, {k: 'stable' for k in pre}, 10)

        self.arm(20)
        post = {'post' + str(i) for i in range(5)}
        self.write_at(self.uri, {k: 'ingest' for k in post}, 30)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), post)
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 40), pre,
            'post-arm writes must not be in the stable table')

        # The server advances stable to the cutoff and takes the step-down checkpoint: everything
        # at or below the cutoff becomes durable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

        # Step down to follower. The cutoff is cleared and the node demotes; ingest content stays.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), post,
            'post-arm (ingest) content must survive the step-down')

        # Pick up checkpoint; verify merged view: pre-arm from checkpoint, post-arm from ingest.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')
        self.disagg_advance_checkpoint(self.conn)
        expected = {k: 'stable' for k in pre} | {k: 'ingest' for k in post}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected,
            'the full merged view must survive the step-down after checkpoint pickup')
