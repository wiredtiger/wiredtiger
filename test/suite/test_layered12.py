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

import os, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_layered12.py
#    Pick up different checkpoints.
@disagg_test_class
class test_layered12(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_base_config = 'layered_table_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    uri = "layered:test_layered12"

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', 'palm')
        self.pr(f"{extlist=}")

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    # Test inserting records into a follower that turned into a leader
    def test_layered12(self):
        self.session.create(self.uri, self.create_session_config)

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, self.create_session_config)

        value1 = "aaa"
        value2 = "bbb"

        # Create version 1 of the data
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            cursor[str(i)] = value1
            if i % 250 == 0:
                time.sleep(1)
        cursor.close()
        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        checkpoint1 = self.disagg_get_complete_checkpoint_meta()

        # Create version 2 of the data
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            if i % 10 == 0:
                cursor[str(i)] = value2
            if i % 250 == 0:
                time.sleep(1)
        cursor.close()
        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        checkpoint2 = self.disagg_get_complete_checkpoint_meta()

        # Pick up the first version and check
        conn_follow.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint1}")')
        cursor = session_follow.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            self.assertEquals(cursor[str(i)], value1)
        cursor.close()

        # Pick up the second version and check
        conn_follow.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint2}")')
        cursor = session_follow.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            if i % 10 == 0:
                self.assertEquals(cursor[str(i)], value2)
            else:
                self.assertEquals(cursor[str(i)], value1)
        cursor.close()

        session_follow.close()
        conn_follow.close()
