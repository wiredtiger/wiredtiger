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

import os, os.path, shutil, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages

# test_layered31.py
#    Test using history store on follower.
@disagg_test_class
class test_layered31(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = ',create,statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'checkpoint=(precise=true),disaggregated=(page_log=palm),'
    def conn_config(self):
        return self.conn_base_config + self.extensionsConfig() + ',disaggregated=(role="leader")'

    uri = "layered:test_layered31"

    scenarios = gen_disagg_storages('test_layered23', disagg_only = True)

    def check_data(self, session, uri, ts):
        session.begin_transaction(f'read_timestamp={self.timestamp_str(ts)}')
        cursor = session.open_cursor(uri)
        for i in range(0, 10):
            self.assertEqual(cursor[str(i)], str(ts))
        cursor.close()
        session.rollback_transaction()

    # Test history
    def test_layered31(self):
        # Create table on leader.
        lsession = self.session # leader session
        lsession.create(self.uri, 'key_format=S,value_format=S')

        # Insert data at timestamp 10.
        lcursor = lsession.open_cursor(self.uri)
        lsession.begin_transaction()
        for i in range(0, 10):
            lcursor[str(i)] = str(10)
        lcursor.close()
        lsession.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        lsession.checkpoint()

        # Start the follower.
        conn_follow = self.wiredtiger_open('follower',
                                           self.conn_base_config + self.extensionsConfig() \
                                           + ',disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(conn_follow)
        fsession = conn_follow.open_session('')

        # Check that the btree made it over and that it has the expected data at timestamp 10.
        self.check_data(fsession, self.uri, 10)

        # Back on the leader, update the data at timestamp 20.
        lcursor = lsession.open_cursor(self.uri)
        lsession.begin_transaction()
        for i in range(0, 10):
            lcursor[str(i)] = str(20)
        lcursor.close()
        lsession.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        lsession.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # On leader and follower, make sure we can read at timestamp 10 and 20.
        for reopen in [False, True]:
            if reopen:
                self.reopen_conn()
                lsession = self.session # leader session
                conn_follow.close()
                conn_follow = self.wiredtiger_open('follower',
                                                   self.conn_base_config + self.extensionsConfig() \
                                                   + ',disaggregated=(role="follower")')
                fsession = conn_follow.open_session('')

            for role in [['leader', lsession], ['follower', fsession]]:
                for ts in [10, 20]:
                    role_name = role[0]
                    role_session = role[1]
                    self.pr(f'Reading {role_name} at ts {ts}')
                    self.check_data(role_session, self.uri, ts)
