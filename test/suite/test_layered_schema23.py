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

# Checkpoint pickup lets go of a layered table the leader has dropped. The discard runs while the
# application is still using the node, so a cursor open on the table holds the adoption up until it
# closes, and a table the follower created itself must not be mistaken for a dead incarnation.

import time
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

    def test_dropped_table_is_let_go(self):
        """A table dropped on the leader leaves nothing behind on the follower."""
        self.session.create(self.uri, self.table_config)
        self.write_one('before the drop', 2)
        self.leader_checkpoint(2)

        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.stable_in_local_metadata(conn_follow, self.uri))

        self.dropUntilSuccess(self.session, self.uri)
        self.leader_checkpoint(3)
        self.disagg_advance_checkpoint(conn_follow)

        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))
        self.assertFalse(self.uri_in_local_metadata(conn_follow, self.uri))
        self.close_follower(conn_follow, session_follow)

    def test_open_cursor_holds_up_the_pickup(self):
        """
        A cursor open on a table the leader has replaced keeps the node on its current checkpoint:
        the drop cannot take the exclusive handle it needs, so the adoption is retried rather than
        applied in part. The node moves on once the application is done with the table.
        """
        self.ignoreStdoutPattern('deferred checkpoint pickup failed|Picking up the same checkpoint')

        self.session.create(self.uri, self.table_config)
        self.write_one('before the recreate', 2)
        self.leader_checkpoint(2)

        conn_follow, session_follow = self.open_follower()
        held = session_follow.open_cursor(self.uri)
        self.assertEqual(held[1], 'before the recreate')

        # The leader replaces the table while the follower still reads the old one.
        self.dropUntilSuccess(self.session, self.uri)
        self.session.create(self.uri, self.table_config)
        self.write_one('after the recreate', 4)
        self.leader_checkpoint(4)
        self.disagg_advance_checkpoint(conn_follow)

        # The held cursor still reads its own incarnation, and no new one has been adopted.
        self.assertEqual(held[1], 'before the recreate')
        held.close()

        # With the table free the adoption goes through, but the retry that carries it runs in the
        # background, so give it a bounded number of checkpoints to land.
        value = None
        for attempt in range(20):
            self.leader_checkpoint(5 + attempt)
            self.disagg_advance_checkpoint(conn_follow)
            session_follow.close()
            session_follow = conn_follow.open_session('')
            cursor = session_follow.open_cursor(self.uri)
            value = cursor[1]
            cursor.close()
            if value == 'after the recreate':
                break
            time.sleep(0.1)

        self.assertEqual(value, 'after the recreate')
        self.close_follower(conn_follow, session_follow)

    def test_follower_create_is_not_a_dead_incarnation(self):
        """
        A table the follower creates itself has no stable constituent, so the leader's entry is
        adopted rather than compared against a local btree id that was never assigned here.
        """
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
