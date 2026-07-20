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

# Test dropping a layered table on an ex-leader after a step-down, before any
# checkpoint covered the table's creation.
#
# A table created while leader leaves a CREATE in the shared metadata queue with
# the stable constituent's metadata captured, and the local stable constituent
# exists. Both survive a step-down. Dropping the table as a follower must succeed
# and remove all local constituents, and the dropped table must stay dead: a later
# step-up must not recreate the stable constituent and a later leader checkpoint
# must neither publish the table to shared metadata nor fail. This holds whether
# the create was never published (the rolled-back create case) or was published at
# an epoch no checkpoint has covered yet.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema18(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
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

    def test_drop_never_published_after_step_down(self):
        """
        Create a table as leader without publishing it, step down, then drop it
        (as a drop-pending reaper would for a rolled-back create). The drop must
        remove all local constituents, and a subsequent fail-back and checkpoint
        must not resurrect the table.
        """
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)

        # Committed but unstable data, as a rolled-back create would have.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'rolled_back'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))
        cursor.close()

        # A checkpoint below the table's (unpublished) epoch: the CREATE stays
        # pending and the table is absent from shared metadata.
        self.leader_checkpoint(1)
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # Step down: the pending CREATE and the local stable constituent survive.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))

        # Drop as follower: all local constituents must go away.
        self.session.drop(self.uri)
        self.assertEqual(self.local_metadata_keys(self.conn), [])
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))

        # Fail back: step-up must not recreate the dropped stable constituent.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))

        # A later covering checkpoint must succeed and must not publish the
        # dropped table to shared metadata.
        self.set_stable_epoch(20)
        self.leader_checkpoint(60)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # A follower picking up that checkpoint must not see the table.
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def pair_canceled_count(self):
        """Return the connection's canceled create/remove pair statistic."""
        cursor = self.session.open_cursor('statistics:')
        val = cursor[stat.conn.checkpoint_disagg_metadata_pair_canceled][2]
        cursor.close()
        return val

    def test_drop_published_create_after_step_down(self):
        """
        Create and publish a table as leader at an epoch above the last checkpoint's,
        step down, drop it as follower (the drop stays unpublished), fail back, and
        checkpoint at an epoch covering the create. The checkpoint must cancel the
        queued create/remove pair rather than publish the dropped table.
        """
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 20)

        # A checkpoint below the publish epoch: the CREATE stays pending and the table
        # is absent from shared metadata.
        self.leader_checkpoint(1)
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # Step down and drop: the REMOVE is enqueued but never published.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.session.drop(self.uri)
        self.assertEqual(self.local_metadata_keys(self.conn), [])

        # Fail back: step-up must not recreate the dropped stable constituent.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertEqual(self.pair_canceled_count(), 0)

        # Checkpoint at an epoch covering the CREATE but not the unpublished REMOVE:
        # the pair must be canceled instead of resurrecting the table in shared
        # metadata.
        self.set_stable_epoch(20)
        with self.expectedStderrPattern('canceling both operations'):
            self.leader_checkpoint(60)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))
        self.assertEqual(self.pair_canceled_count(), 1)

        # The queue is left clean: a later checkpoint and a step-down/step-up cycle
        # must not revisit the pair.
        self.set_stable_epoch(30)
        self.leader_checkpoint(70)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.leader_checkpoint(80)
        self.assertEqual(self.pair_canceled_count(), 1)
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # A follower picking up the final checkpoint must not see the table.
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
