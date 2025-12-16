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

import threading, time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered70.py
#    Test timestamped metadata updates.
@disagg_test_class
class test_layered70(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'precise_checkpoint=true,' \
                     + 'timing_stress_for_test=[checkpoint_slow],'
    conn_config = conn_base_config + 'disaggregated=(role="follower")'

    create_session_config = 'key_format=S,value_format=S,type=layered'

    uri = "table:test_layered70"

    disagg_storages = gen_disagg_storages('test_layered70', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Test creating an empty table while a checkpoint is running.
    def test_layered70(self):
        # The node started as a follower, so step it up as the leader
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Avoid checkpoint error with precise checkpoint
        self.conn.set_timestamp('stable_timestamp=1')

        # Create a table with some data, at a given timestamp
        self.session.create(self.uri, self.create_session_config +
                            f',disaggregated=(timestamp={self.timestamp_str(15)})')
        self.session.begin_transaction()
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor['a'] = 'b'
        cursor.close()
        self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(15)}")

        # Now open the follower connection
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
                                           self.conn_config)
        session_follow = conn_follow.open_session('')

        #
        # Part 1: Check the new table in the follower
        #

        # Create a checkpoint before creating the new table
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Check that the table does not yet exist in the follower
        l = lambda: session_follow.open_cursor(self.uri, None, None)
        self.assertRaisesException(wiredtiger.WiredTigerError, l, '/No such file or directory/')

        #
        # Part 2: Advance the checkpoint past the table creation timestamp
        #

        # Create a checkpoint before creating the new table
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(20)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Check that the table does not yet exist in the follower
        cursor = session_follow.open_cursor(self.uri, None, None)
        self.assertEqual(cursor['a'], 'b')
        cursor.close()

        # Clean up
        session_follow.close()
        conn_follow.close()
