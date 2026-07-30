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

# Test that a pending table create survives a fail-back (step-down then step-up)
# and is published by the next covering checkpoint.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema21(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def create_with_pending_publish(self, publish_epoch):
        """
        Create and publish the table at publish_epoch, then checkpoint at stable epoch 10.
        The publish epoch is above the checkpoint's, so the CREATE stays pending in the
        shared metadata queue and the table is absent from shared metadata.
        """
        self.set_stable_epoch(10)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, publish_epoch)
        self.leader_checkpoint(1)

        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

    def step_down(self):
        self.conn.reconfigure('disaggregated=(role="follower")')

    def step_up(self):
        self.conn.reconfigure('disaggregated=(role="leader")')

    def test_failback_no_pickup(self):
        """
        A pending publish survives a step-down followed by an immediate step-up with no
        checkpoint pickup in between; the next covering checkpoint publishes the table.
        """
        self.create_with_pending_publish(20)

        # Step down: the uncovered local stable constituent is dropped, but the pending
        # CREATE survives in the queue to explain and rebuild it.
        self.step_down()
        self.assertFalse(self.uri_stable_exists(self.conn, self.uri))

        # Step back up without picking up any checkpoint: the surviving CREATE recreates
        # the stable constituent.
        self.step_up()
        self.assertTrue(self.uri_stable_exists(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # A checkpoint at a covering epoch publishes the table.
        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        # A follower picking up that checkpoint sees the table.
        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_failback_repeated(self):
        """
        A pending CREATE survives several step-down and step-up cycles, its stable
        constituent rebuilt on each step-up, and a later covering checkpoint publishes it.
        """
        self.create_with_pending_publish(20)

        for _ in range(3):
            self.step_down()
            self.assertFalse(self.uri_stable_exists(self.conn, self.uri))
            self.step_up()
            self.assertTrue(self.uri_stable_exists(self.conn, self.uri))
            self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_failback_noncovering_pickup(self):
        """
        A pending publish survives a step-down and a subsequent pickup of a checkpoint
        that does not cover its epoch; after a fail-back the next covering checkpoint
        publishes the table.
        """
        self.create_with_pending_publish(30)

        # Open a second node, which picks up the checkpoint at epoch 10 and does not see
        # the table.
        conn_follow, session_follow = self.open_follower()
        session_follow.close()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))

        # Swap roles: this node steps down with the CREATE (epoch 30) still pending.
        self.conn.reconfigure('disaggregated=(role="follower")')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # The new leader checkpoints at epoch 20, which does not cover the pending CREATE.
        self.set_stable_epoch(20, conn_follow)
        session_follow = conn_follow.open_session('')
        self.leader_checkpoint(2, conn_follow, session_follow)
        session_follow.close()

        # This node dropped its uncovered stable constituent on step-down; the pending
        # CREATE survives, and the non-covering pickup neither rebuilds nor prunes it.
        self.disagg_advance_checkpoint(self.conn, conn_follow)
        self.assertFalse(self.uri_stable_exists(self.conn, self.uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri))

        # Fail back to this node and checkpoint at a covering epoch: the table is
        # published to shared metadata.
        conn_follow.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.set_stable_epoch(30)
        self.leader_checkpoint(3)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        # The other node picks up the covering checkpoint and sees the table.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri))
        conn_follow.close('debug=(skip_checkpoint=true)')
