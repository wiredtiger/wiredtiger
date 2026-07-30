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

# Test that a table drop queued as a pending metadata operation survives a
# step-down and is completed by publishing the old queue entries after a
# step-up, rather than being discarded on step-down.
#
# A drop enqueues a REMOVE in the shared metadata queue. Like a pending CREATE,
# that intent is role-independent and must survive the leader to follower
# transition, so a later step-up can publish it and the next covering checkpoint
# can apply it. This covers both the two-phase drop of an already-published table
# and a create/drop pair that nets out once published.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema20(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    uri2 = f'layered:{test_name}_uncovered'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def local_metadata_keys(self, conn):
        """Return the local metadata keys naming the table or its constituents."""
        tablename = self.uri[len('layered:'):]
        session = conn.open_session('')
        mc = session.open_cursor('metadata:')
        keys = [k for k, _ in mc if tablename in k]
        mc.close()
        session.close()
        return keys

    def test_covered_table_keeps_data_uncovered_dropped(self):
        """
        The step-down drop only sheds uncovered stable constituents, which hold no
        data. A table whose data the step-down checkpoint covers keeps its stable
        constituent and its data across the step-down; an uncovered empty table's
        stable constituent is dropped.
        """
        # Covered table: create, write data, publish, and cover it with a checkpoint.
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(10):
            cursor[i] = 'covered'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        cursor.close()
        self.publish(self.uri, 20)
        self.set_stable_epoch(20)
        self.leader_checkpoint(10)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        # Uncovered table: created after the checkpoint, so no checkpoint covers it.
        self.session.create(self.uri2, self.table_config)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri2))

        # Step down: the covered table's stable and data survive; the uncovered
        # table's stable constituent is dropped.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri2))
        cursor = self.session.open_cursor(self.uri)
        self.assertEqual({k: v for k, v in cursor}, {i: 'covered' for i in range(10)})
        cursor.close()

    def test_two_phase_drop_after_step_down(self):
        """
        Publish and checkpoint a table so it is in shared metadata, then drop it
        and publish the drop at a higher epoch. Step down before any checkpoint
        covers the drop: the pending REMOVE must survive, and after a step-up the
        covering checkpoint must remove the table from shared metadata.
        """
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 20)
        self.set_stable_epoch(20)
        self.leader_checkpoint(1)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        # Drop and publish the drop at an epoch no checkpoint has covered yet.
        self.session.drop(self.uri)
        self.publish(self.uri, 30)
        self.assertEqual(self.local_metadata_keys(self.conn), [])

        # Step down and back up: the pending REMOVE survives the role change.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="leader")')

        # A checkpoint covering the drop applies the surviving REMOVE.
        self.set_stable_epoch(30)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # A follower picking up that checkpoint must not see the table.
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_create_drop_pair_after_step_down(self):
        """
        Create a table and drop it while its create is still pending (never covered
        by a checkpoint), publishing both at the same epoch. Step down with the
        create/remove pair queued: both must survive, and after a step-up the
        covering checkpoint must net them out without publishing the table.
        """
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 20)

        # A checkpoint below the publish epoch leaves the CREATE pending.
        self.leader_checkpoint(1)
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # Drop and publish the drop at the same epoch as the create.
        self.session.drop(self.uri)
        self.publish(self.uri, 20)
        self.assertEqual(self.local_metadata_keys(self.conn), [])

        # Step down and back up: the create/remove pair survives.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))

        # A checkpoint covering the pair nets it out: the table is neither
        # published to shared metadata nor left in the local metadata.
        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # A follower picking up that checkpoint must not see the table.
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
