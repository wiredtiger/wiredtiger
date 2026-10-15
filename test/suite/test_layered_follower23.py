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

# A follower that defers a delivered checkpoint reports how many checkpoints are
# waiting, how long the oldest has waited and how many sessions block it, and it
# adopts the checkpoint well before the pick-up server's backstop timer once the
# last blocking snapshot ends.

import time
import wttest
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_follower23(wttest.WiredTigerTestCase):
    test_name = __qualname__

    uri = f'layered:{test_name}'
    table_config = 'key_format=S,value_format=S'
    conn_base_config = ',create,statistics=(all),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Well below the pick-up server's 10 second backstop timer.
    adoption_deadline_sec = 8

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def follower_config(self):
        return self.extensionsConfig() + self.conn_base_config + \
            'disaggregated=(role="follower")'

    def put(self, session, kv, ts):
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        for key, value in kv.items():
            cursor[key] = value
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        cursor.close()

    def leader_checkpoint(self, ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()

    def get_stat(self, conn, stat_id):
        session = conn.open_session('')
        cursor = session.open_cursor('statistics:')
        value = cursor[stat_id][2]
        cursor.close()
        session.close()
        return value

    def read(self, session, key):
        # Read through a newly opened cursor, so the stable constituent binds now.
        cursor = session.open_cursor(self.uri)
        cursor.set_key(key)
        self.assertEqual(cursor.search(), 0)
        cursor.close()

    def begin_reader(self, conn):
        session = conn.open_session('')
        session.begin_transaction()
        self.read(session, 'key')
        return session

    def end_reader(self, session):
        session.rollback_transaction()
        session.close()

    def test_blocked_adoption_stats(self):
        queued = stat.conn.disagg_checkpoint_defer_queued
        blocking = stat.conn.disagg_checkpoint_defer_blocking_sessions
        oldest_wait = stat.conn.disagg_checkpoint_defer_oldest_wait
        adopt_wait = stat.conn.disagg_checkpoint_defer_adopt_wait
        pickups = stat.conn.layered_table_manager_checkpoints_disagg_pick_up_succeed

        self.session.create(self.uri, self.table_config)
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')
        self.put(self.session, {'key': 'v1'}, 10)
        self.leader_checkpoint(10)

        conn_follow = self.wiredtiger_open('follower', self.follower_config())
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, self.table_config)
        self.put(session_follow, {'key': 'v1'}, 10)
        self.disagg_advance_checkpoint(conn_follow)

        # Nothing is waiting yet.
        self.assertEqual(self.get_stat(conn_follow, queued), 0)
        self.assertEqual(self.get_stat(conn_follow, blocking), 0)
        self.assertEqual(self.get_stat(conn_follow, oldest_wait), 0)

        # Two snapshots predate the next checkpoint.
        reader_a = self.begin_reader(conn_follow)
        reader_b = self.begin_reader(conn_follow)

        self.put(self.session, {'key': 'v2'}, 20)
        self.leader_checkpoint(20)
        self.put(session_follow, {'key': 'v2'}, 20)
        self.disagg_advance_checkpoint(conn_follow)

        self.assertEqual(self.get_stat(conn_follow, queued), 1)
        self.assertEqual(self.get_stat(conn_follow, blocking), 2)

        # The wait keeps growing while the checkpoint is blocked.
        time.sleep(1.1)
        self.assertGreaterEqual(self.get_stat(conn_follow, oldest_wait), 1000)

        # Releasing one reader leaves the other blocking it.
        self.end_reader(reader_a)
        self.assertEqual(self.get_stat(conn_follow, queued), 1)
        self.assertEqual(self.get_stat(conn_follow, blocking), 1)

        # Releasing the last blocker adopts the checkpoint long before the backstop timer.
        pickups_before = self.get_stat(conn_follow, pickups)
        released = time.time()
        self.end_reader(reader_b)
        while self.get_stat(conn_follow, pickups) == pickups_before:
            self.assertLess(time.time() - released, self.adoption_deadline_sec,
                'the deferred checkpoint was not adopted soon after its last blocker ended')
            time.sleep(0.05)

        self.assertEqual(self.get_stat(conn_follow, queued), 0)
        self.assertEqual(self.get_stat(conn_follow, blocking), 0)
        self.assertEqual(self.get_stat(conn_follow, oldest_wait), 0)
        self.assertGreaterEqual(self.get_stat(conn_follow, adopt_wait), 1000)

        session_follow.close()
        conn_follow.close()
