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

import os, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_layered11.py
#    Create an artificial delay in materializing pages from the page service.
@disagg_test_class
class test_layered11(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 5000
    uri_base = "test_layered11"
    conn_base_config = 'layered_table_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader"),'

    uri = "layered:" + uri_base

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        config='materialization_delay_ms=3000'  # 3 seconds
        extlist.extension('page_log', 'palm', f'(config="({config})")')

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    # Test inserting a record into a layered tree
    def test_layered11(self):
        base_create = 'key_format=S,value_format=S'

        self.pr("create layered tree")
        self.session.create(self.uri, base_create)

        with self.expectedStdoutPattern('retry', maxchars=100000):
            self.pr('opening cursor')
            cursor = self.session.open_cursor(self.uri, None, None)

            for i in range(self.nitems):
                cursor["Hello " + str(i)] = "World"
                cursor["Hi " + str(i)] = "There"
                cursor["OK " + str(i)] = "Go"
                if i % 10000 == 0:
                    time.sleep(1)

            cursor.reset()

            cursor.close()
            time.sleep(1)
            self.reopen_conn()

            self.pr('opening cursor')
            item_count = 0
            cursor = self.session.open_cursor(self.uri, None, None)
            while cursor.next() == 0:
                item_count += 1

            self.pr('read cursor saw: ' + str(item_count))
            self.assertEqual(item_count, self.nitems * 3)
            cursor.close()

        self.session.checkpoint()

        # FIXME SLS-759: Check that we also retry reading the checkpoint metadata, e.g.
        # with self.expectedStdoutPattern('retry', maxchars=100000):
        if True:
            self.pr('opening the follower')
            conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() \
                                               + ',create,' + self.conn_base_config \
                                               + 'disaggregated=(role="follower")')
            session_follow = conn_follow.open_session('')
            self.disagg_advance_checkpoint(conn_follow)

            item_count = 0
            cursor = session_follow.open_cursor(self.uri, None, None)
            while cursor.next() == 0:
                item_count += 1

            self.pr('read cursor saw: ' + str(item_count))
            self.assertEqual(item_count, self.nitems * 3)
            cursor.close()
