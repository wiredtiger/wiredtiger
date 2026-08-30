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

# Test that publishing at a schema epoch at or below the last adopted
# checkpoint's epoch is rejected.
#
# A checkpoint claims to cover every schema operation published at or below its
# epoch, so an operation published there can never reach shared metadata. A
# follower's stable schema epoch legitimately lags the checkpoint it adopted,
# leaving a range the stable epoch alone does not reject.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema32(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    uri2 = f'layered:{test_name}_b'
    table_config = 'key_format=i,value_format=S'

    # The epoch the leader's checkpoint is stamped with, and so the epoch the
    # follower adopts.
    ckpt_epoch = 10

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def last_epoch(self, conn):
        """The schema epoch of the most recently adopted or written checkpoint."""
        return int(conn.query_timestamp('get=last_disaggregated_schema_epoch'), 16)

    def setup_follower_behind_checkpoint(self):
        """
        Open a follower whose stable schema epoch sits below the checkpoint it adopted.

        This is the ordinary state of a follower: the stable epoch tracks what the node has
        applied, while the adopted checkpoint comes from a leader that is ahead of it.
        """
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, self.ckpt_epoch)
        self.set_stable_epoch(self.ckpt_epoch)
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower_epoch(1)
        self.assertEqual(self.last_epoch(conn_follow), self.ckpt_epoch)
        return conn_follow, session_follow

    def assert_publish_rejected(self, session, uri, epoch):
        """Publishing at this epoch fails, and fails specifically as a disaggregated conflict."""
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.publish(uri, epoch, session))
        _, sub_level_err, err_msg = session.get_last_error()
        self.assertEqual(sub_level_err, wiredtiger.WT_CONFLICT_DISAGG)
        self.assertIn('at or below the last checkpoint schema epoch', err_msg)

    def test_publish_below_last_checkpoint_epoch_rejected(self):
        """
        A follower create published in the gap between its stable epoch and the epoch of the
        checkpoint it adopted is rejected, at the boundary and below it.
        """
        conn_follow, session_follow = self.setup_follower_behind_checkpoint()

        session_follow.create(self.uri2, self.table_config)

        # Both epochs clear the follower's stable epoch of 1, so only the checkpoint's epoch
        # rejects them.
        self.assert_publish_rejected(session_follow, self.uri2, self.ckpt_epoch - 5)
        self.assert_publish_rejected(session_follow, self.uri2, self.ckpt_epoch)

        # Above it the same publish succeeds, leaving the table published.
        self.publish(self.uri2, self.ckpt_epoch + 1, session_follow)

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_publish_below_last_checkpoint_epoch_rejected_for_drop(self):
        """A drop is subject to the same limit as a create."""
        conn_follow, session_follow = self.setup_follower_behind_checkpoint()

        session_follow.drop(self.uri)

        self.assert_publish_rejected(session_follow, self.uri, self.ckpt_epoch)
        self.publish(self.uri, self.ckpt_epoch + 1, session_follow)

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_rejected_range_grows_with_each_pickup(self):
        """
        The rejected range grows as the follower adopts newer checkpoints: an epoch that was
        publishable before a pickup is rejected after it.
        """
        conn_follow, session_follow = self.setup_follower_behind_checkpoint()

        # The leader moves on to a later epoch and checkpoints again.
        later_epoch = self.ckpt_epoch + 10
        self.set_stable_epoch(later_epoch)
        self.leader_checkpoint(2)

        session_follow.create(self.uri2, self.table_config)
        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.last_epoch(conn_follow), later_epoch)

        # This epoch cleared the previously adopted checkpoint but not the new one.
        self.assert_publish_rejected(session_follow, self.uri2, self.ckpt_epoch + 1)
        self.publish(self.uri2, later_epoch + 1, session_follow)

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_leader_publish_above_own_checkpoint_epoch(self):
        """
        A leader publishing above the checkpoint it wrote is unaffected, which is the
        ordinary path.
        """
        self.set_stable_epoch(1)
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, self.ckpt_epoch)
        self.set_stable_epoch(self.ckpt_epoch)
        self.leader_checkpoint(1)
        self.assertEqual(self.last_epoch(self.conn), self.ckpt_epoch)

        self.session.create(self.uri2, self.table_config)
        self.publish(self.uri2, self.ckpt_epoch + 1)
