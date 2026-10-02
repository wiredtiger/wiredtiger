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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

# test_layered_follower24.py
#    Without schema epochs, a follower that drops a layered table the leader keeps skips the table
# while the drop is queued. Once a pickup clears the queue, a pickup that changes the table fails
# rather than add back the table that the follower dropped.
@disagg_test_class
class test_layered_follower24(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    nitems = 100

    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def put_data(self, value, ts):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(self.nitems):
            cursor[i] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def test_local_drop_not_added_back(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.put_data('a', 10)
        self.leader_checkpoint(10)

        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.stable_in_local_metadata(conn_follow, self.uri))

        # The follower drops the table, which the leader keeps.
        session_follow.drop(self.uri)
        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))

        # The queued drop keeps the table out of the follower's local metadata.
        self.put_data('b', 20)
        self.leader_checkpoint(20)
        self.disagg_advance_checkpoint(conn_follow)
        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))

        # That pickup cleared the queue, so no queued drop explains the missing table now.
        self.put_data('c', 30)
        self.leader_checkpoint(30)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.disagg_advance_checkpoint(conn_follow),
            '/is not in the local metadata, and the queue holds no drop of the table/')
        self.assertFalse(self.stable_in_local_metadata(conn_follow, self.uri))

        self.close_follower(conn_follow, session_follow)
