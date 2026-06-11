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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper import WiredTigerCursor, statistic_uri
from wtscenario import make_scenarios
from wiredtiger import stat

# test_layered_checkpoint16.py
#     Test that picking up the same checkpoint a second time does not update file metadata.
#     The disagg_pick_up_file_meta_updated stat tracks actual metadata writes.
#     Re-applying an identical checkpoint must leave the stat unchanged.

@disagg_test_class
class test_layered_checkpoint16(wttest.WiredTigerTestCase):
    num_items = 100

    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered_checkpoint16', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_checkpoint16'

    def insert_data(self, session, value_prefix, ts):
        session.begin_transaction()
        cursor = session.open_cursor(self.uri)
        for i in range(self.num_items):
            cursor[str(i)] = value_prefix + str(i)
        cursor.close()
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

    def check_data(self, session, value_prefix):
        cursor = session.open_cursor(self.uri)
        for i in range(self.num_items):
            self.assertEqual(cursor[str(i)], value_prefix + str(i))
        cursor.close()

    def get_stats(self, session):
        with WiredTigerCursor(session, statistic_uri()) as stat_cur:
            inserted = stat_cur[stat.conn.disagg_pick_up_file_meta_inserted][2]
            updated = stat_cur[stat.conn.disagg_pick_up_file_meta_updated][2]
        return inserted, updated

    def test_layered_checkpoint16(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_data(self.session, 'v1-', 10)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')

        # First pick-up: Entries are new to the follower's local metadata, so they are inserted.
        self.disagg_advance_checkpoint(conn_follow)
        self.check_data(session_follow, 'v1-')
        inserted_after_first, updated_after_first = self.get_stats(session_follow)
        self.assertGreater(inserted_after_first, 0)
        self.assertEqual(updated_after_first, 0)

        # Create a new checkpoint without changing the table data, then pick it up.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(15)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)
        self.check_data(session_follow, 'v1-')
        inserted_after_repeat, updated_after_repeat = self.get_stats(session_follow)
        self.assertEqual(inserted_after_repeat, inserted_after_first)
        self.assertEqual(updated_after_repeat, 0)

        # Now change the table, create a checkpoint, and pick it up. The metadata must be updated.
        self.insert_data(self.session, 'v2-', 20)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(20)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)
        self.check_data(session_follow, 'v2-')
        inserted_after_new, updated_after_new = self.get_stats(session_follow)
        self.assertEqual(inserted_after_new, inserted_after_first)
        self.assertGreater(updated_after_new, 0)

        session_follow.close()
        conn_follow.close()
