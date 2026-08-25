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

# A follower drops its copy of a layered table when the checkpoint names the same table under a
# different btree id, which is what dropping and creating the table produces. These tests check
# which copy wins, that the follower keeps a table it created itself, and that a pick-up applies
# the whole checkpoint or none of it.

import re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema23(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    # Disable the cursor cache: a cached cursor keeps a table in use and holds up the pick-up.
    conn_base_config = 'statistics=(all),cache_cursors=false,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def write_one(self, value, commit_ts, session=None, uri=None):
        """Write one row, giving the other node something to read back."""
        if session is None:
            session = self.session
        if uri is None:
            uri = self.uri
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        cursor[1] = value
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def assert_reads(self, session, uri, value):
        """Check that a node reads the row it should."""
        cursor = session.open_cursor(uri)
        self.assertEqual(cursor[1], value)
        cursor.close()

    def stable_btree_id(self, conn, uri):
        """Return the btree id a node records for a table."""
        config = self.stable_config(conn, uri)
        match = re.search(r'(?:^|,)id=(\d+)', config)
        self.assertIsNotNone(match, f'no btree id in {config}')
        return int(match.group(1))

    def deferred_pickups(self, conn):
        """Return how many pick-ups this node has put off."""
        session = conn.open_session('')
        cursor = session.open_cursor('statistics:')
        deferred = cursor[wiredtiger.stat.conn.disagg_checkpoint_defer][2]
        cursor.close()
        session.close()
        return deferred

    def recreate(self, uri, value, commit_ts, session=None):
        """Drop and create a table under one name, giving it a new btree id."""
        if session is None:
            session = self.session
        self.dropUntilSuccess(session, uri)
        session.create(uri, self.table_config)
        self.write_one(value, commit_ts, session=session, uri=uri)

    def test_a_held_up_pickup_leaves_every_table_alone(self):
        """
        Hold a cursor on a table the leader has replaced, and the follower waits: it applies none
        of the checkpoint until the cursor closes. The three names sort around the replaced table,
        so the pick-up would reach one before it and one after.
        """
        # Accept only a busy handle as the reason to wait.
        self.ignoreStdoutPattern('deferred checkpoint pickup failed: Device or resource busy|' +
                                 'Picking up the same checkpoint')

        before = 'layered:aaa_before'
        after = 'layered:zzz_after'
        for uri in (before, self.uri, after):
            self.session.create(uri, self.table_config)
            self.write_one('first', 2, uri=uri)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        adopted = {uri: self.stable_config(conn_follow, uri) for uri in (before, after)}
        deferred = self.deferred_pickups(conn_follow)

        held = session_follow.open_cursor(self.uri)
        self.assertEqual(held[1], 'first')

        # Advance all three tables, replacing the middle one.
        self.write_one('second', 4, uri=before)
        self.write_one('second', 4, uri=after)
        self.recreate(self.uri, 'second', 4)
        self.leader_checkpoint(4)
        second_id = self.stable_btree_id(self.conn, self.uri)
        self.assertGreater(second_id, first_id)
        self.disagg_advance_checkpoint(conn_follow)

        # Confirm the follower waited instead of quietly finishing.
        self.assertGreater(self.deferred_pickups(conn_follow), deferred)

        # Read the metadata, not the tables: the pick-up updates each entry as it goes, so an
        # entry naming the old checkpoint marks a table it never reached.
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)
        for uri in (before, after):
            self.assertEqual(self.stable_config(conn_follow, uri), adopted[uri])
            self.assert_reads(session_follow, uri, 'first')

        held.close()
        self.disagg_wait_for_adoption(conn_follow)

        # Release the table and the whole checkpoint lands.
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), second_id)
        session_follow.close()
        session_follow = conn_follow.open_session('')
        for uri in (before, self.uri, after):
            self.assert_reads(session_follow, uri, 'second')
        self.close_follower(conn_follow, session_follow)

    def test_a_failed_pickup_unrolls_what_it_had_adopted(self):
        """
        Fail a pick-up part way and it takes back the tables it had already applied. A cursor on
        the ingest constituent refuses the drop, and the name sorts after a healthy table so the
        pick-up applies that one first. Holding the layered table would make it wait, not fail.
        """
        healthy = 'layered:aaa_healthy'
        recreated = 'layered:zzz_recreated'

        for uri in (healthy, recreated):
            self.session.create(uri, self.table_config)
            self.write_one('first', 2, uri=uri)
        self.leader_checkpoint(2)

        conn_follow, session_follow = self.open_follower()
        adopted = self.stable_config(conn_follow, healthy)

        self.write_one('second', 4, uri=healthy)
        self.recreate(recreated, 'second', 4)
        self.leader_checkpoint(4)

        held = session_follow.open_cursor(self.ingest_uri(recreated))
        self.assertRaises(wiredtiger.WiredTigerError,
                          lambda: self.disagg_advance_checkpoint(conn_follow))
        self.ignoreStderrPatternIfExists('WT_VERB_ERROR_RETURNS|Resource busy')
        self.ignoreStdoutPatternIfExists('Resource busy')

        # The healthy table went first, so its entry naming the old checkpoint again is the
        # pick-up taking it back.
        self.assertEqual(self.stable_config(conn_follow, healthy), adopted)
        self.assert_reads(session_follow, healthy, 'first')

        held.close()
        self.close_follower(conn_follow, session_follow)

    def test_follower_create_is_not_a_dropped_table(self):
        """A follower keeps a table it created itself: it has no stable copy to disagree."""
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower()
        session_follow.create(self.uri, self.table_config)
        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))

        self.session.create(self.uri, self.table_config)
        self.write_one('from the leader', 3)
        self.leader_checkpoint(3)
        self.disagg_advance_checkpoint(conn_follow)
        self.disagg_wait_for_adoption(conn_follow)

        self.assertTrue(self.stable_in_local_metadata(conn_follow, self.uri))
        self.assert_reads(session_follow, self.uri, 'from the leader')
        self.close_follower(conn_follow, session_follow)

    def test_uncheckpointed_recreate_yields_to_the_checkpoint(self):
        """
        A replacement nobody checkpointed was never published, so the checkpoint wins even though
        the local btree id is larger. The step-down declares no timestamp, which is what leaves
        the replacement unpublished. test_recreate_published_before_a_graceful_step_down covers a
        step-down that carries it into the checkpoint instead.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        # Let the follower lead briefly and replace the table without checkpointing it.
        self.step_down()
        self.step_up(conn_follow)
        self.recreate(self.uri, 'from the second leader', 4, session=session_follow)
        second_id = self.stable_btree_id(conn_follow, self.uri)
        self.assertGreater(second_id, first_id)
        self.step_down(conn_follow)

        # The first leader takes over and checkpoints the table it kept, so the newest checkpoint
        # names the smaller id and the unpublished copy loses.
        self.step_up()
        self.leader_checkpoint(6)
        self.assertEqual(self.stable_btree_id(self.conn, self.uri), first_id)

        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        session_follow.close()
        session_follow = conn_follow.open_session('')
        self.assert_reads(session_follow, self.uri, 'from the first leader')

        self.close_follower(conn_follow, session_follow)

    def test_strict_validation_accepts_a_discarded_table(self):
        """
        Strict validation accepts a table the pick-up replaced on the way past. It still rejects a
        table it did not replace, which test_layered_schema17 covers.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        conn_follow.reconfigure('disaggregated=(strict_checkpoint_metadata=true)')

        # Let the follower lead briefly and replace the table without checkpointing it.
        self.step_down()
        self.step_up(conn_follow)
        self.recreate(self.uri, 'from the second leader', 4, session=session_follow)
        self.assertGreater(self.stable_btree_id(conn_follow, self.uri), first_id)
        self.step_down(conn_follow)

        # The first leader checkpoints the table it kept, so the follower has to replace its own
        # copy with strict validation on.
        self.step_up()
        self.leader_checkpoint(6)
        self.disagg_advance_checkpoint(conn_follow)
        self.disagg_wait_for_adoption(conn_follow)

        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)
        self.close_follower(conn_follow, session_follow)

    def test_recreate_published_before_a_graceful_step_down(self):
        """
        A graceful step-down lands its checkpoint on the step-down timestamp, publishing a table
        the outgoing leader replaced. Both nodes then settle on the replacement's btree id.
        """
        self.session.create(self.uri, self.table_config)
        self.write_one('from the first leader', 2)
        self.leader_checkpoint(2)
        first_id = self.stable_btree_id(self.conn, self.uri)

        conn_follow, session_follow = self.open_follower()
        self.assertEqual(self.stable_btree_id(conn_follow, self.uri), first_id)

        # Hand leadership over and replace the table.
        self.step_down()
        self.step_up(conn_follow)
        self.set_stable_epoch(1, conn_follow)
        self.recreate(self.uri, 'from the second leader', 4, session=session_follow)
        second_id = self.stable_btree_id(conn_follow, self.uri)
        self.assertGreater(second_id, first_id)

        # Publish the replacement, then step down on a declared boundary. The checkpoint has to
        # land on the step-down timestamp, so it carries the replacement with it.
        self.publish(self.uri, 10, session=session_follow)
        self.set_stable_epoch(10, conn_follow)
        conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                  ',stable_timestamp=' + self.timestamp_str(5) +
                                  ',step_down_timestamp=' + self.timestamp_str(5))
        session_follow.checkpoint()
        conn_follow.reconfigure('disaggregated=(role="follower")')

        # The first leader takes the replacement before it may lead again.
        self.disagg_advance_checkpoint(self.conn, conn_follow)
        self.assertEqual(self.stable_btree_id(self.conn, self.uri), second_id)

        self.close_follower(conn_follow, session_follow)
