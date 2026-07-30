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

# Test that a pending table drop survives a step-down and is applied after a
# step-up, instead of being discarded when the shared metadata queue is preserved.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema20(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def uri(self, name):
        """Return a distinct layered table URI within this test."""
        return f'layered:{self.test_name}_{name}'

    def create_covered(self, uri, publish_epoch, stable_ts, commit_ts, rows=0):
        """Create, publish, and cover a table with a checkpoint so it reaches shared metadata."""
        self.session.create(uri, self.table_config)
        if rows:
            cursor = self.session.open_cursor(uri)
            self.session.begin_transaction()
            for i in range(rows):
                cursor[i] = 'covered'
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
            cursor.close()
        self.publish(uri, publish_epoch)
        self.set_stable_epoch(publish_epoch)
        self.leader_checkpoint(stable_ts)
        self.assertTrue(self.uri_in_shared_metadata(self.conn, uri))

    def step_down(self):
        self.conn.reconfigure('disaggregated=(role="follower")')

    def step_up(self):
        self.conn.reconfigure('disaggregated=(role="leader")')

    def local_metadata_keys(self, conn, uri):
        """Return the local metadata keys naming the table or its constituents."""
        tablename = uri[len('layered:'):]
        session = conn.open_session('')
        mc = session.open_cursor('metadata:')
        keys = [k for k, _ in mc if tablename in k]
        mc.close()
        session.close()
        return keys

    def assert_follower_absent(self, uri):
        """A fresh follower picking up the latest checkpoint must not see the table."""
        conn_follow, session_follow = self.open_follower()
        self.assertFalse(self.uri_in_local_metadata(conn_follow, uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, uri))
        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    def test_covered_table_keeps_data_uncovered_dropped(self):
        """
        Step-down keeps the stable constituent and data of a covered table and drops
        the stable constituent of an uncovered one, keeping the uncovered ingest half.
        """
        self.set_stable_epoch(10)
        covered = self.uri('covered')
        self.create_covered(covered, 20, stable_ts=10, commit_ts=5, rows=10)

        # Uncovered table: created after the checkpoint, so no checkpoint covers it.
        uncovered = self.uri('uncovered')
        self.session.create(uncovered, self.table_config)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uncovered))

        self.step_down()
        self.assertTrue(self.uri_stable_exists(self.conn, covered))
        self.assertFalse(self.uri_stable_exists(self.conn, uncovered))
        cursor = self.session.open_cursor(covered)
        self.assertEqual({k: v for k, v in cursor}, {i: 'covered' for i in range(10)})
        cursor.close()

    def test_multiple_covered_and_uncovered(self):
        """
        Step-down sheds every uncovered stable constituent and keeps every covered
        one, regardless of how many tables of each kind exist.
        """
        self.set_stable_epoch(10)
        covered = [self.uri(f'covered{i}') for i in range(3)]
        for i, uri in enumerate(covered):
            self.create_covered(uri, 20 + i, stable_ts=10 * (i + 1), commit_ts=10 * (i + 1) - 5, rows=5)

        uncovered = [self.uri(f'uncovered{i}') for i in range(3)]
        for uri in uncovered:
            self.session.create(uri, self.table_config)

        self.step_down()
        for uri in covered:
            self.assertTrue(self.uri_stable_exists(self.conn, uri))
        for uri in uncovered:
            self.assertFalse(self.uri_stable_exists(self.conn, uri))
            # The ingest half survives so a later step-up can rebuild the table.
            self.assertTrue(self.uri_in_local_metadata(self.conn, uri))

    def test_two_phase_drop_after_step_down(self):
        """
        A drop published above the last checkpoint epoch leaves a pending REMOVE. It
        survives a step-down and back up, and the next covering checkpoint removes the
        table from shared metadata.
        """
        uri = self.uri('drop')
        self.set_stable_epoch(10)
        self.create_covered(uri, 20, 1, 5)

        # Drop and publish the drop at an epoch no checkpoint has covered yet.
        self.session.drop(uri)
        self.publish(uri, 30)
        self.assertEqual(self.local_metadata_keys(self.conn, uri), [])

        self.step_down()
        self.step_up()

        self.set_stable_epoch(30)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uri))
        self.assert_follower_absent(uri)

    def test_two_phase_drop_survives_repeated_failback(self):
        """
        A pending REMOVE survives several step-down and step-up cycles before a
        covering checkpoint applies it.
        """
        uri = self.uri('drop_repeat')
        self.set_stable_epoch(10)
        self.create_covered(uri, 20, 1, 5)

        self.session.drop(uri)
        self.publish(uri, 30)

        for _ in range(3):
            self.step_down()
            self.step_up()

        self.set_stable_epoch(30)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uri))
        self.assert_follower_absent(uri)

    def test_create_drop_pair_after_step_down(self):
        """
        A create and drop published at the same pending epoch survive a step-down and
        back up as a queued pair, and the next covering checkpoint nets them out
        without publishing the table.
        """
        uri = self.uri('pair')
        self.set_stable_epoch(10)
        self.session.create(uri, self.table_config)
        self.publish(uri, 20)

        # A checkpoint below the publish epoch leaves the CREATE pending.
        self.leader_checkpoint(1)
        self.assertTrue(self.uri_in_local_metadata(self.conn, uri))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uri))

        self.session.drop(uri)
        self.publish(uri, 20)
        self.assertEqual(self.local_metadata_keys(self.conn, uri), [])

        self.step_down()
        self.step_up()
        self.assertFalse(self.uri_in_local_metadata(self.conn, uri))

        self.set_stable_epoch(20)
        self.leader_checkpoint(2)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uri))
        self.assert_follower_absent(uri)
