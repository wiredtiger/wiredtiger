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

import os, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat

# test_layered28.py
#    Test to ensure that dropping layered tables works and subsequent sweep doesn't crash
@disagg_test_class
class test_layered28(wttest.WiredTigerTestCase):
    uri_base = "test_layered28"
    conn_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),disaggregated=(role="leader"),' \
                + 'file_manager=(close_scan_interval=1)'

    uri = "layered:" + uri_base

    disagg_storages = gen_disagg_storages('test_key_provider_disagg02', disagg_only = True)

    # Test simple create and drop on leader mode.
    def test_create_drop(self):
        base_create = 'key_format=S,value_format=S,type=layered'

        self.pr("create layered tree")
        self.session.create(self.uri, base_create)

        cursor = self.session.open_cursor(self.uri)
        for i in range(1000):
            cursor[str(i)] = str(i)
        cursor.close()

        self.session.checkpoint()

        self.session.drop(self.uri, "")
        database_home = os.path.join('kv_home', 'pages_000041.db')
        self.assertFalse(os.path.isfile(database_home))

    # Test create and drop with a subsequent checkpoint and enough time for sweep to come through
    def test_create_drop_checkpoint(self):
        base_create = 'key_format=S,value_format=S'

        # Use a session so it can be closed which releases the reference to the dhandle and
        # allows the sweep thread to close out the handle
        custom_session = self.conn.open_session()
        self.pr("create layered tree")
        custom_session.create(self.uri, base_create)

        cursor = self.session.open_cursor(self.uri)
        for i in range(1000):
            cursor[str(i)] = str(i)
        cursor.close()

        custom_session.checkpoint()
        custom_session.drop(self.uri, "")
        custom_session.close()
        database_home = os.path.join('/kv_home', 'pages_000041.db')
        self.assertFalse(os.path.isfile(database_home))

    # Test create and drop on follower mode.
    def test_create_drop_follower(self):
        base_create = 'key_format=S,value_format=S,type=layered'

        self.pr("create layered tree")
        self.session.create(self.uri, base_create)

        cursor = self.session.open_cursor(self.uri)
        for i in range(1000):
            cursor[str(i)] = str(i)
        cursor.close()

        self.session.checkpoint()
        # Get the checkpoint metadata before closing
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()

        # Configure as follower with checkpoint pickup (not using backup)
        follower_config = 'disaggregated=(role="follower",' + \
                         f'checkpoint_meta="{checkpoint_meta}")'

        # Switch to follower mode.
        self.reopen_conn(config=follower_config)
        self.session.drop(self.uri, "")
        database_home = os.path.join('kv_home', 'pages_000041.db')
        self.assertTrue(os.path.isfile(database_home))
