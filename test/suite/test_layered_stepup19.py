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
from wtscenario import make_scenarios

# The checkpoint LSN installed with the checkpoint metadata reaches the page log when the node
# abandons its incomplete checkpoint on step-up: the page log keeps the installed checkpoint and
# drops the newer one.
@disagg_test_class
class test_layered_stepup19(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_config = 'create,statistics=(all),disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def open_leader(self):
        return self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' +
            'statistics=(all),disaggregated=(role="leader")')

    def insert(self, session, prefix, ts):
        session.begin_transaction()
        cursor = session.open_cursor(self.uri)
        for i in range(100):
            cursor[str(i)] = prefix + str(i)
        cursor.close()
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

    def complete_checkpoint_lsn(self, conn):
        return self.disagg_get_complete_checkpoint_ext(conn)[0]

    def test_installed_checkpoint_lsn(self):
        conn2 = self.open_leader()
        session2 = conn2.open_session('')
        session2.create(self.uri, 'key_format=S,value_format=S')
        self.insert(session2, 'v1-', 10)
        conn2.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        session2.checkpoint()
        meta1 = self.disagg_get_complete_checkpoint_meta(conn2)
        lsn1 = self.complete_checkpoint_lsn(conn2)

        # A second checkpoint that the new leader will not install.
        self.insert(session2, 'v2-', 20)
        conn2.set_timestamp(f'stable_timestamp={self.timestamp_str(20)}')
        session2.checkpoint()
        lsn2 = self.complete_checkpoint_lsn(conn2)
        self.assertGreater(lsn2, lsn1)

        # The main connection installs the first checkpoint and is told that checkpoint's LSN.
        self.conn.reconfigure(
            f'disaggregated=(checkpoint_meta="{meta1}",checkpoint_lsn={lsn1})')
        self.assertEqual(self.complete_checkpoint_lsn(self.conn), lsn2)

        # The new leader abandons above the installed checkpoint, not above the newest one.
        conn2.reconfigure('disaggregated=(role="follower")')
        conn2.close()
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.complete_checkpoint_lsn(self.conn), lsn1)
