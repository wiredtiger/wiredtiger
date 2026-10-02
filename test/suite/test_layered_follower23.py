#!/usr/bin/env python
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
from wtscenario import make_scenarios

# test_layered_follower23.py
#    A running follower picks up a shared non-layered table that the leader creates after the
# follower's previous pickup.
@disagg_test_class
class test_layered_follower23(wttest.WiredTigerTestCase):
    test_name = __qualname__
    nitems = 100

    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    uri = f'table:{test_name}'
    shared_config = 'block_manager=disagg,log=(enabled=false)'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def in_metadata(self, session, key):
        cursor = session.open_cursor('metadata:')
        cursor.set_key(key)
        found = cursor.search() == 0
        cursor.close()
        return found

    def test_pick_up_new_shared_table(self):
        # The follower picks up a checkpoint without the table, so its next pickup has a
        # previous checkpoint to compare with.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,' + self.conn_config_follower)
        session_follow = conn_follow.open_session('')
        self.disagg_advance_checkpoint_and_wait(conn_follow)
        self.assertFalse(self.in_metadata(session_follow, self.uri))

        # The leader creates and fills the table, then checkpoints.
        self.session.create(self.uri, 'key_format=S,value_format=SS,' + self.shared_config)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(self.nitems):
            cursor[str(i)] = ('a' + str(i), 'b' + str(i))
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

        self.disagg_advance_checkpoint_and_wait(conn_follow)

        # The follower has the table's entries and reads its data.
        self.assertTrue(self.in_metadata(session_follow, self.uri))
        self.assertTrue(self.in_metadata(session_follow, f'colgroup:{self.test_name}'))

        cursor = session_follow.open_cursor(self.uri)
        for i in range(self.nitems):
            self.assertEqual(cursor[str(i)], ['a' + str(i), 'b' + str(i)])
        cursor.close()

        session_follow.close()
        conn_follow.close()
