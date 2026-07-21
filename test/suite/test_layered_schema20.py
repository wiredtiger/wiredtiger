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

# Test the two-phase drop of a layered table on the leader: a table created and
# dropped at different published epochs, with checkpoints splitting the two.
#
# A checkpoint whose stable schema epoch covers the create but not the drop must
# include the table in shared metadata -- a follower picking up that checkpoint
# sees the table -- and defer the drop to a later covering checkpoint. Only a
# drop that was never published may cancel the queued create/remove pair. A
# create through the table: prefix enqueues two CREATE entries for the table,
# so the pairing must treat the URI's entries as a group.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema20(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    layered_uri = f'layered:{test_name}'
    table_uri = f'table:{test_name}'
    table_config = 'key_format=i,value_format=S'
    table_config_layered = table_config + ',type=layered,block_manager=disagg'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def stable_uri(self, uri):
        """Return the stable component URI, accepting layered: and table: URIs."""
        return 'file:' + uri.split(':', 1)[1] + '.wt_stable'

    def get_conn_stat(self, stat_key):
        cursor = self.session.open_cursor('statistics:')
        val = cursor[stat_key][2]
        cursor.close()
        return val

    def pair_canceled_count(self):
        return self.get_conn_stat(stat.conn.checkpoint_disagg_metadata_pair_canceled)

    def metadata_unstable_count(self):
        return self.get_conn_stat(stat.conn.checkpoint_disagg_metadata_unstable)

    def check_follower_sees_table(self, uri):
        """Open a follower on the latest checkpoint and check it sees the table."""
        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.uri_in_shared_metadata(conn_follow, uri))
        self.assertTrue(self.uri_in_local_metadata(conn_follow, uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def two_phase_drop(self, uri, config, creates_per_table):
        """
        Create a table, publish the create at epoch 20, drop it, publish the
        drop at epoch 30, and checkpoint in between: the checkpoint must
        include the table in shared metadata and defer the drop, and the next
        covering checkpoint must apply it.
        """
        self.set_stable_epoch(10)
        self.session.create(uri, config)
        self.publish(uri, 20)
        self.session.drop(uri)
        self.publish(uri, 30)

        # A checkpoint below the publish epochs defers every queued entry;
        # the delta pins down how many CREATE entries the create enqueued.
        unstable_before = self.metadata_unstable_count()
        self.leader_checkpoint(1)
        self.assertEqual(self.metadata_unstable_count() - unstable_before,
                         creates_per_table + 1)

        # A checkpoint covering the create but not the drop must include the
        # table in shared metadata and defer the REMOVE; nothing is canceled.
        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, uri))
        self.assertEqual(self.pair_canceled_count(), 0)

        # A follower picking up that checkpoint sees the table.
        self.check_follower_sees_table(uri)

        # A checkpoint covering the drop removes the table from shared metadata.
        self.set_stable_epoch(30)
        self.leader_checkpoint(3)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uri))
        self.assertEqual(self.pair_canceled_count(), 0)

    def test_two_phase_drop(self):
        self.two_phase_drop(self.layered_uri, self.table_config, 1)

    def test_two_phase_drop_table_prefix(self):
        self.two_phase_drop(self.table_uri, self.table_config_layered, 2)

    def test_drop_unpublished_table_prefix(self):
        """
        Drop a table: prefix table whose create was published but never covered,
        without publishing the drop. The checkpoint covering the create must
        cancel the whole queued group -- both CREATE entries and the REMOVE --
        so neither entry publishes the dropped table to shared metadata.
        """
        self.set_stable_epoch(10)
        self.session.create(self.table_uri, self.table_config_layered)
        self.publish(self.table_uri, 20)
        self.session.drop(self.table_uri)

        # The checkpoint covers the CREATE entries but the REMOVE can never be
        # published: the pair cancellation must consume the whole group.
        self.set_stable_epoch(20)
        with self.expectedStderrPattern('canceling'):
            self.leader_checkpoint(1)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.table_uri))
        self.assertEqual(self.pair_canceled_count(), 1)

        # A later checkpoint stays clean: the queue holds nothing for the table.
        self.set_stable_epoch(30)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.table_uri))
        self.assertEqual(self.pair_canceled_count(), 1)
