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

# test_layered98.py
#    Test that iterating a layered cursor after step-up works correctly when the
#    cursor was reset on the follower before the step-up.
@disagg_test_class
class test_layered98(wttest.WiredTigerTestCase):
    uri = "layered:test_layered98"

    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages('test_layered98', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_cursor_reset_then_stepup(self):
        # Create a layered table on the leader, populate it, and checkpoint.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(10):
            cursor[str(i)] = 'value' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Create a follower and pick up the leader's checkpoint.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_config_follower)
        session_follow = conn_follow.open_session('')
        self.disagg_advance_checkpoint(conn_follow)

        # Open a cursor on the follower, iterate once to open the stable cursor on the checkpointed btree, then reset it.
        cursor_follow = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor_follow.next(), 0)
        cursor_follow.reset()

        # Close the leader and step up the follower.
        self.session.close()
        self.close_conn()
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Iterate the cursor after step-up. The stable cursor must be upgraded to the new writable stable before use.
        cursor_follow.next()

        session_follow.close()
        conn_follow.close()
