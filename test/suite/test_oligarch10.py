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

# test_oligarch10.py
#    Additional oligarch table & cursor methods.
class test_oligarch10(wttest.WiredTigerTestCase):
    nitems = 100_000

    conn_base_config = 'oligarch_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,storage_source=dir_store),'
    conn_config = conn_base_config + 'oligarch=(role="leader")'

    uri = "oligarch:test_oligarch10"

    # Load the directory store extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', 'dir_store')
        self.pr(f"{extlist=}")

    # Custom test case setup
    def early_setup(self):
        # FIXME: This shouldn't take an absolute path
        os.mkdir('foo') # Hard coded to match library for now.
        os.mkdir('bar') # Hard coded to match library for now.
        os.mkdir('follower')
        os.mkdir('follower/foo')
        os.mkdir('follower/bar')

        # TODO shouldn't need these - work around code in __wt_open_fs that handles
        # non fixed-location files
        os.mkdir('follower/foo/follower')
        os.mkdir('follower/bar/follower')

    # Test additional oligarch table / cursor operations
    def test_oligarch10(self):
        session_config = 'key_format=i,value_format=S'

        self.session.create(self.uri, session_config)

        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + "oligarch=(role=\"follower\")")
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, session_config)

        # Check the largest key before populating the data
        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(cursor.largest_key(), wiredtiger.WT_NOTFOUND)

        # Populate
        for i in range(self.nitems):
            cursor[i + 1] = f'Value {i}'

        # Check the largest key
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), self.nitems)

        # Check the largest key again after a checkpoint and a bit of a wait
        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), self.nitems)

        # Ensure that all data makes it to the follower
        conn_follow.reconfigure('disaggregated=(checkpoint_id=1)') # TODO Use a real checkpoint ID

        # Check the largest key at the follower
        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        self.assertEqual(cursor_follow.largest_key(), 0)
        self.assertEqual(cursor_follow.get_key(), self.nitems)

        # Cleanup
        cursor.close()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()
