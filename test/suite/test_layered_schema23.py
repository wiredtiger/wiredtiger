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

# Checkpoint pickup drops a local layered table the checkpoint describes under a different
# btree id, which is what a drop and a create under the same name produce. The discard runs while
# the application is still using the node, so a cursor open on the table holds the adoption up
# until it closes, and a table the follower created itself must not be mistaken for a stale one.

import re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema23(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    # A cached cursor keeps the table in use, which is not what these tests are about.
    conn_base_config = 'statistics=(all),cache_cursors=false,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def write_one(self, value, commit_ts, session=None, uri=None):
        """Write a single row, so the table holds something the other node can read back."""
        if session is None:
            session = self.session
        if uri is None:
            uri = self.uri
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        cursor[1] = value
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def test_open_cursor_holds_up_the_pickup(self):
        """
        A cursor open on a table the leader has replaced keeps the node on its current checkpoint,
        because the drop cannot take the exclusive handle it needs. The node moves on once the
        application is done with the table.
        """
        # Only the busy handle may hold the adoption up, so any other reason is a failure.
        self.ignoreStdoutPattern('deferred checkpoint pickup failed: Device or resource busy|' +
                                 'Picking up the same checkpoint')

        self.session.create(self.uri, self.table_config)
        self.write_one('before the recreate', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        held = session_follow.open_cursor(self.uri)
        self.assertEqual(held[1], 'before the recreate')

        # The leader replaces the table while the follower still reads the old one.
        self.dropUntilSuccess(self.session, self.uri)
        self.session.create(self.uri, self.table_config)
        self.write_one('after the recreate', 4)
        self.leader_checkpoint(4)
        second_id = self.stable_btree_id(self.conn, self.uri)
        self.assertGreater(second_id, first_id)
        self.disagg_advance_checkpoint(conn_follow)

        # Reading the metadata rather than the held cursor is what makes this an observation of the
        # node: a cursor answers from the table it was pinned to whether or not the pickup ran.
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)
        self.assertEqual(held[1], 'before the recreate')
        held.close()

        # With the table free the adoption goes through on the retry that runs in the background.
        self.disagg_wait_for_adoption(conn_follow)
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), second_id)

        session_follow.close()
        session_follow = conn_follow.open_session('')
        cursor = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor[1], 'after the recreate')
        cursor.close()
        self.close_follower(conn_follow, session_follow)

    def deferred_pickups(self, conn):
        """Return how many checkpoint pick-ups this node has put off."""
        session = conn.open_session('')
        cursor = session.open_cursor('statistics:')
        deferred = cursor[wiredtiger.stat.conn.disagg_checkpoint_defer][2]
        cursor.close()
        session.close()
        return deferred

    def test_a_held_up_pickup_leaves_every_table_alone(self):
        """
        A checkpoint that has been delivered but not yet adopted applies to no table at all, so no
        node ever serves part of one. The three names bracket the table being recreated, since the
        pick-up walks them in name order and would otherwise reach one before it and one after.
        """
        # Only the busy handle may hold the adoption up, so any other reason is a failure.
        self.ignoreStdoutPattern('deferred checkpoint pickup failed: Device or resource busy|' +
                                 'Picking up the same checkpoint')

        before = 'layered:aaa_before'
        after = 'layered:zzz_after'
        for uri in (before, self.uri, after):
            self.session.create(uri, self.table_config)
            self.write_one('first', 2, uri=uri)
        self.leader_checkpoint(2)

        conn_follow, session_follow = self.open_follower()
        adopted = {uri: self.stable_config(conn_follow, uri) for uri in (before, after)}
        deferred = self.deferred_pickups(conn_follow)

        # A cursor on the middle table holds the pick-up off.
        held = session_follow.open_cursor(self.uri)
        self.assertEqual(held[1], 'first')

        # The leader moves all three on, replacing the middle one under a new btree id.
        self.write_one('second', 4, uri=before)
        self.write_one('second', 4, uri=after)
        self.dropUntilSuccess(self.session, self.uri)
        self.session.create(self.uri, self.table_config)
        self.write_one('second', 4)
        self.leader_checkpoint(4)
        self.disagg_advance_checkpoint(conn_follow)

        # The pick-up really was held off rather than quietly completing, which is what makes the
        # assertions below meaningful.
        self.assertGreater(self.deferred_pickups(conn_follow), deferred)

        # None of the checkpoint has been applied. The local metadata is what shows it: the walk
        # updates each table's stable entry as it goes, so an entry still naming the old checkpoint
        # belongs to a table the pick-up never got to.
        for uri in (before, after):
            self.assertEqual(self.stable_config(conn_follow, uri), adopted[uri])
            cursor = session_follow.open_cursor(uri)
            self.assertEqual(cursor[1], 'first')
            cursor.close()

        held.close()
        self.disagg_wait_for_adoption(conn_follow)

        # With the table free the whole checkpoint lands at once, entries included.
        session_follow.close()
        session_follow = conn_follow.open_session('')
        for uri in (before, after):
            self.assertNotEqual(self.stable_config(conn_follow, uri), adopted[uri])
        for uri in (before, self.uri, after):
            cursor = session_follow.open_cursor(uri)
            self.assertEqual(cursor[1], 'second')
            cursor.close()
        self.close_follower(conn_follow, session_follow)

    def test_a_failed_pickup_unrolls_what_it_had_adopted(self):
        """
        A pick-up that fails part way applies none of the checkpoint: metadata tracking unrolls the
        tables the walk had already adopted. A cursor on the ingest constituent of the recreated
        table refuses the drop the walk needs, on a name that sorts after a healthy table.
        """
        healthy = 'layered:aaa_healthy'
        recreated = 'layered:zzz_recreated'

        for uri in (healthy, recreated):
            self.session.create(uri, self.table_config)
            self.write_one('first', 2, uri=uri)
        self.leader_checkpoint(2)

        conn_follow, session_follow = self.open_follower()
        adopted = self.stable_config(conn_follow, healthy)

        # The leader moves the healthy table on and replaces the other under a new btree id.
        self.write_one('second', 4, uri=healthy)
        self.dropUntilSuccess(self.session, recreated)
        self.session.create(recreated, self.table_config)
        self.write_one('second', 4, uri=recreated)
        self.leader_checkpoint(4)

        # Hold the ingest constituent, which the drop has to close.
        held = session_follow.open_cursor('file:zzz_recreated.wt_ingest')

        self.assertRaises(wiredtiger.WiredTigerError,
                          lambda: self.disagg_advance_checkpoint(conn_follow))
        self.ignoreStderrPatternIfExists('WT_VERB_ERROR_RETURNS|Resource busy')
        self.ignoreStdoutPatternIfExists('Resource busy')

        # The healthy table sorts first, so the walk adopted it before it failed. Its stable entry
        # naming the old checkpoint again is the unroll.
        self.assertEqual(self.stable_config(conn_follow, healthy), adopted)

        held.close()
        self.close_follower(conn_follow, session_follow)

    def test_follower_create_is_not_a_dropped_table(self):
        """A table the follower created itself has no stable constituent to disagree with."""
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower()
        session_follow.create(self.uri, self.table_config)
        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))

        self.session.create(self.uri, self.table_config)
        self.write_one('from the leader', 3)
        self.leader_checkpoint(3)
        self.disagg_advance_checkpoint(conn_follow)

        self.assertTrue(self.stable_in_local_metadata(conn_follow, self.uri))
        cursor = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor[1], 'from the leader')
        cursor.close()
        self.close_follower(conn_follow, session_follow)

    def stable_btree_id(self, conn, uri):
        """Return the btree id recorded for a table's stable constituent."""
        config = self.stable_config(conn, uri)
        match = re.search(r'(?:^|,)id=(\d+)', config)
        self.assertIsNotNone(match, f'no btree id in {config}')
        return int(match.group(1))

    def test_uncheckpointed_recreate_yields_to_the_checkpoint(self):
        """
        A recreate that was never checkpointed was never published, so the checkpoint's btree id
        wins even though the local one is larger.

        The step-down here declares no timestamp, which is what leaves the recreate unpublished.
        A graceful step-down would carry it into the checkpoint instead, which
        test_recreate_published_before_a_graceful_step_down covers.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        # The follower briefly leads and recreates the table, but never checkpoints the recreate.
        self.step_down()
        self.step_up(conn_follow)
        self.dropUntilSuccess(session_follow, self.uri)
        session_follow.create(self.uri, self.table_config)
        self.write_one('from the second leader', 4, session=session_follow)
        second_id = self.stable_btree_id(conn_follow, self.uri)
        self.assertGreater(second_id, first_id)
        self.step_down(conn_follow)

        # The original leader takes over again and checkpoints the table it still holds, so the
        # newest checkpoint names the smaller id. The unpublished recreate loses to it.
        self.step_up()
        self.leader_checkpoint(6)
        self.assertEqual(self.stable_btree_id(self.conn, self.uri), first_id)

        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        session_follow.close()
        session_follow = conn_follow.open_session('')
        cursor = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor[1], 'from the first leader')
        cursor.close()

        self.close_follower(conn_follow, session_follow)

    def test_strict_validation_accepts_a_discarded_table(self):
        """
        Strict validation asks the operation queue to account for a table held only by the shared
        metadata. One the same pickup dropped is accounted for by the pickup itself. Tables it did
        not discard are still validated, which test_layered_schema17 covers.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        conn_follow.reconfigure('disaggregated=(strict_checkpoint_metadata=true)')

        # The follower briefly leads and recreates the table, but never checkpoints the recreate.
        self.step_down()
        self.step_up(conn_follow)
        self.dropUntilSuccess(session_follow, self.uri)
        session_follow.create(self.uri, self.table_config)
        self.write_one('from the second leader', 4, session=session_follow)
        self.assertGreater(self.stable_btree_id(conn_follow, self.uri), first_id)
        self.step_down(conn_follow)

        # The original leader checkpoints the table it still holds, so the pickup has to discard the
        # unpublished recreate and adopt the checkpoint's table with strict validation on.
        self.step_up()
        self.leader_checkpoint(6)
        self.disagg_advance_checkpoint(conn_follow)
        self.disagg_wait_for_adoption(conn_follow)

        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)
        self.close_follower(conn_follow, session_follow)

    def test_recreate_published_before_a_graceful_step_down(self):
        """
        A graceful step-down lands its checkpoint on the step-down timestamp, so a table the
        outgoing leader recreated is published and both nodes settle on its btree id.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        # Hand leadership over and recreate the table under a new btree id.
        self.step_down()
        self.step_up(conn_follow)
        self.set_stable_epoch(1, conn_follow)
        self.dropUntilSuccess(session_follow, self.uri)
        session_follow.create(self.uri, self.table_config)
        self.write_one('from the second leader', 4, session=session_follow)
        second_id = self.stable_btree_id(conn_follow, self.uri)
        self.assertGreater(second_id, first_id)

        # Publish the recreate, then step down on a declared boundary: the checkpoint has to land on
        # the step-down timestamp, so it carries the recreate with it.
        self.publish(self.uri, 10, session=session_follow)
        self.set_stable_epoch(10, conn_follow)
        conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                  ',stable_timestamp=' + self.timestamp_str(5) +
                                  ',step_down_timestamp=' + self.timestamp_str(5))
        session_follow.checkpoint()
        conn_follow.reconfigure('disaggregated=(role="follower")')

        # The first leader adopts the recreated table before it may lead again.
        self.disagg_advance_checkpoint(self.conn, conn_follow)
        self.assertEqual(self.stable_btree_id(self.conn, self.uri), second_id)

        self.close_follower(conn_follow, session_follow)
