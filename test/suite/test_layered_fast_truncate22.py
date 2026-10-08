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

import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_fast_truncate import LayeredFastTruncateConfigMixin
from wtscenario import make_scenarios

# A follower must see a key reinserted after a range truncate when it picks up the leader's
# checkpoint, while untouched keys in the range remain deleted.
@disagg_test_class
class test_layered_fast_truncate22(LayeredFastTruncateConfigMixin, wttest.WiredTigerTestCase):

    test_name = __qualname__
    conn_config = 'disaggregated=(role="leader")'
    uri = f'layered:{test_name}'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def setUp(self):
        super().setUp()

        # Seed the leader's checkpoint with keys inside and outside the truncate range.
        self.session.create(self.uri, self.session_create_config())
        self.populate([50, 100, 150, 200, 250, 300, 350], value='old', commit_timestamp=10)
        self.leader_checkpoint(10)
        self.conn_follow, self.session_follow = self.open_follower()
        self.disagg_wait_for_adoption(self.conn_follow)

    def test_reinsert_after_checkpoint_pickup(self):
        conn, sess = self.conn_follow, self.session_follow

        # Truncate on both nodes while the follower still reads the initial checkpoint.
        self.truncate(100, 300, commit_timestamp=20)
        self.truncate(100, 300, commit_timestamp=20, session=sess)
        self.assertEqual(self.visible_keys(session=sess, read_timestamp=25), [50, 350])
        self.assertEqual(self.visible_keys(forward=False, session=sess, read_timestamp=25), [350, 50])

        # Reinsert on the leader only, so the follower reads the new values from the checkpoint.
        self.populate([150, 250], value='new', commit_timestamp=30)
        self.leader_checkpoint(30)
        self.disagg_advance_checkpoint_and_wait(conn, self.conn)

        # Only the reinserted keys return; untouched keys stay deleted and exterior keys are unchanged.
        for key in [150, 250]:
            ret, val = self.search_at(sess, key, 40)
            self.assertEqual(ret, 0)
            self.assertEqual(val, 'new')
        for key in [100, 200, 300]:
            ret, _ = self.search_at(sess, key, 40)
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        for key in [50, 350]:
            ret, val = self.search_at(sess, key, 40)
            self.assertEqual(ret, 0)
            self.assertEqual(val, 'old')

        # Both scan directions return the surviving keys in order.
        self.assertEqual(self.visible_keys(session=sess, read_timestamp=40), [50, 150, 250, 350])
        self.assertEqual(self.visible_keys(forward=False, session=sess, read_timestamp=40),
            [350, 250, 150, 50])

        # Search-near finds a reinserted key exactly or a live neighbor of a deleted key.
        exact, key = self.search_near_key(150, session=sess, read_timestamp=40)
        self.assertEqual(exact, 0)
        self.assertEqual(key, 150)
        exact, key = self.search_near_key(200, session=sess, read_timestamp=40)
        self.assertIn(exact, [-1, 1])
        self.assertIn(key, [150, 250])

        # Reads before the truncate still see the original values, including the reinserted keys.
        for key in [100, 150, 200, 250, 300]:
            ret, val = self.search_at(sess, key, 15)
            self.assertEqual(ret, 0)
            self.assertEqual(val, 'old')

    def test_truncate_above_checkpoint_hides_range(self):
        conn, sess = self.conn_follow, self.session_follow

        # Pick up the new values before either node truncates the range.
        self.populate([150, 250], value='new', commit_timestamp=20)
        self.leader_checkpoint(20)
        self.disagg_advance_checkpoint_and_wait(conn, self.conn)

        # A truncate newer than the picked-up checkpoint must hide all keys in its range.
        self.truncate(100, 300, commit_timestamp=30)
        self.truncate(100, 300, commit_timestamp=30, session=sess)
        for key in [150, 250]:
            ret, _ = self.search_at(sess, key, 40)
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.visible_keys(session=sess, read_timestamp=40), [50, 350])
        self.assertEqual(self.visible_keys(forward=False, session=sess, read_timestamp=40), [350, 50])

    def test_checkpoint_pickup_with_old_cursor(self):
        conn, sess = self.conn_follow, self.session_follow
        gc_removed = wiredtiger.stat.conn.layered_truncate_list_gc_entries_removed

        # Keep the initial checkpoint in use after the reader's transaction ends.
        with self.auto_closing_cursor(session=sess) as cursor:
            with self.transaction(session=sess):
                self.assertEqual(cursor.next(), 0)
                self.assertEqual(cursor.get_key(), 50)

            removed = self.get_stat(gc_removed, session=sess)

            # Reinsert on both nodes so the old cursor can see the new values before advancing.
            self.truncate(100, 300, commit_timestamp=20)
            self.truncate(100, 300, commit_timestamp=20, session=sess)
            self.populate([150, 250], value='new', commit_timestamp=30)
            self.populate([150, 250], value='new', commit_timestamp=30, session=sess)
            self.leader_checkpoint(30)
            self.disagg_advance_checkpoint_and_wait(conn, self.conn)

            # Pickup must retain the deletions while the old cursor finishes its scan.
            self.assertEqual(self.get_stat(gc_removed, session=sess), removed)
            self.assertEqual([key for key, _ in cursor], [150, 250, 350])

        # Closing the old cursor allows the next pickup to retire the truncate entry.
        self.leader_checkpoint(40)
        self.disagg_advance_checkpoint_and_wait(conn, self.conn)
        self.assertStatGreaterSoon(gc_removed, removed, session=sess, timeout=60)
        self.assertEqual(self.visible_keys(session=sess, read_timestamp=40), [50, 150, 250, 350])
        self.assertEqual(self.visible_keys(forward=False, session=sess, read_timestamp=40),
            [350, 250, 150, 50])
