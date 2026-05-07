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

# test_layered102.py
#   Test WT_SESSION::publish for disaggregated storage (WT-17087).
#
#   Schema operations (create, drop) on a leader now enqueue with
#   WT_SCHEMA_EPOCH_UNPUBLISHED and are withheld from followers until the
#   caller explicitly publishes them with a schema epoch.  The stable
#   disaggregated schema epoch acts as the cut-off: only operations whose
#   epoch <= stable epoch are processed during checkpoint.
#
#   Tests:
#     - table create: deferred until published, then visible after checkpoint
#     - table drop: deferred until published, checkpoint succeeds without panic
#     - error cases: zero epoch, epoch not newer than stable, follower, invalid URI
#     - publish success / fail statistics
#
#   Known limitation: follower drop visibility (FIXME-WT-17089) is not yet
#   implemented.  After the leader publishes and checkpoints a DROP, the
#   follower's local metadata is not cleaned up, so the table remains visible
#   on the follower.  test_drop_deferred_until_publish only verifies that
#   publish and checkpoint succeed without error.

import time
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat


@disagg_test_class
class test_layered102(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages('test_layered102', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # ------------------------------------------------------------------ helpers

    def set_stable_epoch(self, epoch):
        """Advance stable_disaggregated_schema_epoch; epoch is a positive integer."""
        self.conn.set_timestamp(
            'stable_disaggregated_schema_epoch=' + self.timestamp_str(epoch))

    def leader_checkpoint(self, stable_ts):
        """Set stable_timestamp and take a timestamped checkpoint."""
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(stable_ts) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

    def open_follower(self):
        return self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_config_follower)

    def table_visible_on_follower(self, uri):
        """
        Advance the follower to the latest leader checkpoint and return True if
        a cursor can be opened on uri, False if the table is absent.
        """
        conn_f = self.open_follower()
        self.disagg_advance_checkpoint(conn_f)
        session_f = conn_f.open_session('')
        visible = True
        try:
            c = session_f.open_cursor(uri)
            c.close()
        except wiredtiger.WiredTigerError:
            visible = False
        session_f.close()
        conn_f.close()
        return visible

    def publish(self, uri, epoch, session=None):
        if session is None:
            session = self.session
        session.publish(uri, 'disaggregated=(schema_epoch=' + self.timestamp_str(epoch) + ')')

    def get_stat(self, stat_name):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[stat_name][2]
        stat_cursor.close()
        return value

    def assertStatEqual(self, stat_name, expected_value, retries=10):
        # Stats may be updated asynchronously, so retry a few times if the expected value is not
        # observed.
        for attempt in range(retries):
            value = self.get_stat(stat_name)
            if value == expected_value:
                return
            if attempt < retries - 1:
                time.sleep(0.1)
        self.assertEqual(value, expected_value)

    # ------------------------------------------------------------------ end-to-end tests

    def test_create_deferred_until_publish(self):
        """
        A table created after a stable epoch is set is invisible to followers
        until published; one checkpoint after publish makes it visible.
        """
        uri = 'layered:test_layered102_create'

        # Set a stable epoch so the checkpoint filters UNPUBLISHED entries.
        self.set_stable_epoch(5)
        self.session.create(uri, 'key_format=i,value_format=S')

        # Checkpoint 1: CREATE is deferred (UNPUBLISHED > cur_epoch=5).
        self.leader_checkpoint(1)
        self.assertFalse(self.table_visible_on_follower(uri),
            'table should be invisible before publish')

        # Publish with epoch 10 (must be > current stable epoch 5).
        self.publish(uri, 10)
        self.set_stable_epoch(10)

        # Checkpoint 2: CREATE is processed (entry epoch 10 <= cur_epoch=10).
        self.leader_checkpoint(2)
        self.assertTrue(self.table_visible_on_follower(uri),
            'table should be visible after publish and checkpoint')

    def test_drop_deferred_until_publish(self):
        """
        A drop of a published table is deferred until the drop is itself
        published; one checkpoint after drop-publish removes the table.
        """
        uri = 'layered:test_layered102_drop'

        # Create and publish the table so it lands on the follower.
        self.session.create(uri, 'key_format=i,value_format=S')
        self.publish(uri, 10)
        self.set_stable_epoch(10)
        self.leader_checkpoint(1)
        self.assertTrue(self.table_visible_on_follower(uri),
            'table should be visible after create publish')

        # Drop the table (new REMOVE entry gets UNPUBLISHED epoch).
        # Advance stable epoch but do NOT publish the drop yet.
        self.session.drop(uri)
        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        # DROP deferred: follower still sees the table.
        self.assertTrue(self.table_visible_on_follower(uri),
            'table should still be visible before drop is published')

        # Publish the drop with epoch 30 (> stable epoch 20).
        self.publish(uri, 30)
        self.set_stable_epoch(30)
        self.leader_checkpoint(3)
        # FIXME-WT-17089: Follower drop visibility is not yet implemented.
        # The leader processes the DROP in shared metadata, but the follower's
        # local metadata is not cleaned up, so the table remains visible on the
        # follower.  Once follower publish support is added, assert that the
        # table is invisible here.

    # ------------------------------------------------------------------ error cases

    def test_publish_error_zero_epoch(self):
        """schema_epoch=0 is rejected with EINVAL before touching the queue."""
        uri = 'layered:test_layered102_err_zero'
        self.session.create(uri, 'key_format=i,value_format=S')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.publish(uri, 'disaggregated=(schema_epoch=0)'),
            '/zero not permitted/')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
                                ',oldest_timestamp=' + self.timestamp_str(1))

    def test_publish_error_epoch_not_newer_than_stable(self):
        """
        schema_epoch <= stable_disaggregated_schema_epoch is rejected with EINVAL.
        The stable check fires before any queue access.
        """
        uri = 'layered:test_layered102_err_old'
        self.session.create(uri, 'key_format=i,value_format=S')
        self.set_stable_epoch(10)

        # Epoch equal to stable must fail.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.publish(uri, 10),
            '/Cannot publish with a schema epoch that is older/')
        # Epoch older than stable must fail.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.publish(uri, 5),
            '/Cannot publish with a schema epoch that is older/')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
                                ',oldest_timestamp=' + self.timestamp_str(1))

    def test_publish_error_follower(self):
        """
        Calling publish on a follower session returns ENOTSUP.

        The table is created and checkpointed without a stable epoch so that
        the CREATE entry is fully processed (no UNPUBLISHED entries remain in
        the queue) before the follower test runs, avoiding unrelated panics.
        """
        uri = 'layered:test_layered102_err_follower'
        self.session.create(uri, 'key_format=i,value_format=S')
        # No stable epoch set: checkpoint processes the CREATE unconditionally.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
                                ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

        conn_f = self.open_follower()
        self.disagg_advance_checkpoint(conn_f)
        session_f = conn_f.open_session('')
        # The leader check fires before any queue access, so no panic risk.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.publish(uri, 20, session=session_f),
            '/not supported for followers/')
        session_f.close()
        conn_f.close()

    def test_publish_error_invalid_uri(self):
        """An unsupported URI prefix (not table: or layered:) returns ENOTSUP."""
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.publish(
                'file:test_layered102_err_uri',
                'disaggregated=(schema_epoch=1)'),
            '/only supported for table: and layered:/')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
                                ',oldest_timestamp=' + self.timestamp_str(1))

    # ------------------------------------------------------------------ statistics

    def test_publish_stats(self):
        """
        session_table_publish_success increments on a no-op publish (no
        schema_epoch supplied); session_table_publish_fail increments on EINVAL.
        """
        uri = 'layered:test_layered102_stats'
        self.session.create(uri, 'key_format=i,value_format=S')

        success_before = self.get_stat(stat.conn.session_table_publish_success)
        fail_before    = self.get_stat(stat.conn.session_table_publish_fail)

        # No schema_epoch in config: no-op, returns success.
        self.session.publish(uri, '')
        self.assertStatEqual(stat.conn.session_table_publish_success, success_before + 1)
        self.assertStatEqual(stat.conn.session_table_publish_fail, fail_before)

        # Zero epoch: returns EINVAL (fail stat increments).
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.publish(uri, 'disaggregated=(schema_epoch=0)'),
            '/zero not permitted/')
        self.assertStatEqual(stat.conn.session_table_publish_success, success_before + 1)
        self.assertStatEqual(stat.conn.session_table_publish_fail, fail_before + 1)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
                                ',oldest_timestamp=' + self.timestamp_str(1))


if __name__ == '__main__':
    wttest.run()
