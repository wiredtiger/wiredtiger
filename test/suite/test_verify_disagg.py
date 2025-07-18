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

import os, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_verify_disagg.py
#    SESSION::verify() testing for disagg storage
@disagg_test_class
class test_verify_disagg(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    # TODO-DONT-MERGE: CHECK WHETHER WE NEED ALL THESE CONFIGS AND VARIABLES !!!
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    table_cfg = 'key_format=S,value_format=S,block_manager=disagg,log=(enabled=false)'

    session_follow = None
    conn_follow = None

    uri = 'layered:test_verify_disagg'

    disagg_storages = gen_disagg_storages('test_verify_disagg', disagg_only = True)
    # TODO-DONT-MERGE: DO WE NEED make_scenarios HERE ???
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def leader_put_data(self, value_prefix = '', low = 0, high = nitems):
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(low, high):
            cursor[str(i)] = value_prefix + str(i)
        cursor.close()

    def verify(self, sessions, err_msg = None):
        for session in sessions:
            try:
                session.verify(self.uri)
            except wiredtiger.WiredTigerError as e:
                if (err_msg):
                    self.assertTrue(err_msg == str(e))
                else:
                    raise(e)


    def create_follower(self):
        self.conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def test_verify_disagg(self):
        # TODO-DONT-MERGE: Verify HS ???
        # TODO-DONT-MERGE: Imitate Oplog to check dirty ingest table case for the follower ???

        # Create a table in the leader
        self.session.create(self.uri, self.table_cfg)
        # Verify the empty leader's table
        self.verify([self.session])

        # Create a follower
        self.create_follower()
        # The leader's table stays empty, the follower creation doesn't mean loading tables from the leader (it requires reconfiguration)
        self.verify([self.session])
        self.verify([self.session_follow], "No such file or directory")

        # Create an empty checkpoint
        self.session.checkpoint()
        self.verify([self.session])

        self.disagg_advance_checkpoint(self.conn_follow)
        # Now both connections should have empty tables
        self.verify([self.session, self.session_follow])

        # Put some data to the leader
        self.leader_put_data()
        # That's not allowed to perform verification if there is some dirty data
        self.verify([self.session], "Device or resource busy")

        # Checkpoint the data on the leader
        self.session.checkpoint()
        # Verify the leader's populated table
        self.verify([self.session])

        # Load the latest checkpoint to the follower
        self.disagg_advance_checkpoint(self.conn_follow)
        # Verify both the leader's and the follower's populated tables
        self.verify([self.session, self.session_follow])

        self.session_follow.close()
        self.conn_follow.close()

        self.verify([self.session]) # the leader is still alive
