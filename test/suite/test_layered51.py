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


import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


# test_layered51.py
#    Test accessing ingest tables (WT-15301)

@disagg_test_class
class test_layered51(wttest.WiredTigerTestCase, DisaggConfigMixin):
    disagg_storages = gen_disagg_storages('test_layered51', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = 'disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    table_cfg = 'key_format=S,value_format=S,block_manager=disagg'

    session_follow = None
    conn_follow = None

    uri = 'layered:test_layered51'
    # Use internals to test a specific edge case scenario.
    ingest_uri = 'file:test_layered51.wt_ingest'

    msg = '/writes to ingest tables are disallowed on leader nodes/'

    def test_illegal_cursor_open(self):
        # Create a layered table on the leader
        self.session.create(self.uri, self.table_cfg)
        self.session.checkpoint()

        # Open an ingest cursor on leader. We bypass __clayered_put and attempt to
        # open a cursior externally through __session_open_cursor to write to leader.
        # This should fail.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.ingest_uri, None, None), self.msg)

    def test_switch_role_ingest_access(self):
        # Start as leader. Ingest access should be blocked
        self.session.create(self.uri, self.table_cfg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.ingest_uri, None, None), self.msg)

        # Switch to follower role
        self.conn.reconfigure('disaggregated=(role="follower")')

        # Now ingest access should work
        ingest_cursor = self.session.open_cursor(self.ingest_uri, None, None)
        ingest_cursor.close()

    def test_ingest_cursor_cfgs(self):
        self.session.create(self.uri, self.table_cfg)

        # Test that all cursor cfgs fail on a leader ingest table
        ops = [
            lambda: self.session.open_cursor(self.ingest_uri, None, None),
            lambda: self.session.open_cursor(self.ingest_uri, None, "readonly=false"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "overwrite"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "overwrite=false"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "append"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "append=false"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "bulk"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "bulk=false"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "raw"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "raw=false"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "read_once"),
            lambda: self.session.open_cursor(self.ingest_uri, None, "read_once=false"),
        ]

        for op in ops:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError, op, self.msg)

        # We are able to open readonly cursors on leader ingest tables
        readonly_cursor = self.session.open_cursor(self.ingest_uri, None, "readonly=true")

        readonly_cursor.close()
