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

# Leader create/drop/create of the same layered table, observed by a pure follower through
# checkpoint pickups only, using the publish API and schema epochs.
#
# The follower picks up a checkpoint between the drop and the recreate: the drop pickup must
# discard the table's local metadata, and the recreate pickup must bring in the new incarnation
# (which has a fresh btree ID) so the follower reads the recreated data.
#
# Also checks the other direction: a table the follower created via the publish API must survive
# picking up a checkpoint that predates the create.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema14(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    uri2 = f'layered:{test_name}_b'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def read_all(self, session, uri=None):
        """Return the follower's view of the table as a key to value dict."""
        cursor = session.open_cursor(self.uri if uri is None else uri)
        result = {}
        while cursor.next() == 0:
            result[cursor.get_key()] = cursor.get_value()
        cursor.close()
        return result

    def layered_in_local_metadata(self, conn, uri):
        """Return True if the layered: entry itself is present in conn's local metadata."""
        session = conn.open_session('')
        cursor = session.open_cursor('metadata:')
        cursor.set_key(uri)
        found = cursor.search() == 0
        cursor.close()
        session.close()
        return found

    def test_id_conflict_create_drop_create(self):
        # The leader drives every schema operation. The follower only picks up checkpoints, so its
        # metadata operation queue stays empty and the pickup relies entirely on the shared vs.
        # local metadata diff.

        # Leader creates the table, writes the first generation of data, and checkpoints.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 10)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(10):
            cursor[i] = 'aaa'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()
        self.set_stable_epoch(10)
        self.leader_checkpoint(10)

        # Follower picks up the checkpoint and sees the first generation.
        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertEqual(self.read_all(session_follow), {i: 'aaa' for i in range(10)})

        # Leader drops the table and advances the stable schema epoch past the drop so the remove is
        # flushed out of shared metadata.
        self.session.drop(self.uri)
        self.publish(self.uri, 25)
        self.set_stable_epoch(25)
        self.leader_checkpoint(20)

        # Follower picks up the post-drop checkpoint. The table is gone from the shared metadata
        # and the pickup must discard it from the follower's local metadata as well.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))

        # Leader recreates the table under the same name. It gets a fresh btree ID and a new
        # generation of data.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 40)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(10):
            cursor[i] = 'bbb'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(40))
        cursor.close()
        self.set_stable_epoch(40)
        self.leader_checkpoint(50)

        # Follower picks up the recreate as a new table and must read back the recreated data.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertEqual(self.read_all(session_follow), {i: 'bbb' for i in range(10)})

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_pickup_keeps_locally_published_create(self):
        # Leader creates a first table and checkpoints so the follower has something to pick up.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 10)
        self.set_stable_epoch(10)
        self.leader_checkpoint(10)

        conn_follow, session_follow = self.open_follower()

        # Both nodes create a second table at epoch 25 (the follower applies the same operation
        # through the publish API), but the leader checkpoints with the stable epoch still at 10,
        # so the create has not reached the shared metadata yet.
        self.session.create(self.uri2, self.table_config)
        self.publish(self.uri2, 25)
        session_follow.create(self.uri2, self.table_config)
        self.publish(self.uri2, 25, session_follow)
        self.set_stable_epoch(10)
        self.leader_checkpoint(20)

        # The pickup sees the new table locally but not in the shared metadata. The pending local
        # create must prevent the pickup from discarding it as a dropped table.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri2))
        self.assertTrue(self.layered_in_local_metadata(conn_follow, self.uri2))
        cursor = session_follow.open_cursor(self.uri2)
        cursor.close()

        # Once the create epoch reaches a shared checkpoint, the table converges: the leader's
        # data becomes readable on the follower.
        cursor = self.session.open_cursor(self.uri2)
        self.session.begin_transaction()
        for i in range(10):
            cursor[i] = 'ccc'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.set_stable_epoch(25)
        self.leader_checkpoint(40)
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.layered_in_local_metadata(conn_follow, self.uri2))
        self.assertEqual(self.read_all(session_follow, self.uri2), {i: 'ccc' for i in range(10)})

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
