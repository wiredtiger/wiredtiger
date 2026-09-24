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

import time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Once a covering checkpoint supersedes the trees frozen at demote, their handles
# are closed even though the commits after the final checkpoint left them dirty.
@disagg_test_class
class test_layered_frozen_core03(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),' \
        'file_manager=(close_scan_interval=1,close_idle_time=1,close_handle_minimum=0),'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def write(self, session, uri, items, commit_ts):
        cursor = session.open_cursor(uri, None, None)
        session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def read(self, session, uri, read_ts):
        cursor = session.open_cursor(uri, None, None)
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        found = {}
        while cursor.next() == 0:
            found[cursor.get_key()] = cursor.get_value()
        session.rollback_transaction()
        cursor.close()
        return found

    def get_stat(self, stat):
        cursor = self.session.open_cursor('statistics:')
        value = cursor[stat][2]
        cursor.close()
        return value

    def test_superseded_frozen_handles_closed(self):
        uri = 'layered:' + self.test_name
        expect = {'a': '1', 'b': '2', 'c': '3', 'd': '4'}
        self.session.create(uri, 'key_format=S,value_format=S')

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')

        self.write(self.session, uri, {'a': '1', 'b': '2'}, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()
        self.disagg_advance_checkpoint_and_wait(conn_follow)

        self.write(self.session, uri, {'c': '3', 'd': '4'}, 20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        frozen = self.get_stat(wiredtiger.stat.conn.disagg_frozen_handles)
        self.assertGreater(frozen, 0)
        self.assertEqual(self.read(self.session, uri, 20), expect)

        # The other node leads, applies the same commits and checkpoints them.
        conn_follow.reconfigure('disaggregated=(role="leader")')
        self.write(session_follow, uri, {'c': '3', 'd': '4'}, 20)
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        session_follow.checkpoint()
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_follow)
        self.assertEqual(self.read(self.session, uri, 20), expect)

        # Only the table's checkpoint changed: the history store stays frozen, the table's
        # superseded handle is closed.
        deadline = time.time() + 60
        while self.get_stat(wiredtiger.stat.conn.disagg_frozen_handles) != frozen - 1:
            self.assertLess(time.time(), deadline, 'superseded frozen handle was not closed')
            time.sleep(0.2)
        self.assertEqual(self.read(self.session, uri, 20), expect)

        session_follow.close()
        conn_follow.close()
