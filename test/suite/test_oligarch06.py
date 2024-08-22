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

StorageSource = wiredtiger.StorageSource  # easy access to constants

# test_oligarch06.py
#    Start a second WT that shares the stable content with the first.
class test_oligarch06(wttest.WiredTigerTestCase):

    # conn_config = 'log=(enabled),verbose=[oligarch:5]'
    conn_config = 'log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true)'
    # conn_config = 'log=(enabled)'

    # TODO do Python tests expect a field named uri?
    uri = "oligarch:test_oligarch06"

    # Load the directory store extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', 'dir_store')
        self.pr(f"{extlist=}")

    # Test records into an oligarch tree and restarting
    def test_oligarch06(self):
        nitems = 100000
        leader_create = 'key_format=S,value_format=S,role=leader'
        follower_create = 'key_format=S,value_format=S,role=follower,stable_follower_prefix=foo'
        os.mkdir('foo') # Hard coded to match library for now.
        os.mkdir('bar') # Hard coded to match library for now.
        os.mkdir('follower')
        os.mkdir('follower/foo')
        os.mkdir('follower/bar')

        # TODO shouldn't need these - work around code in __wt_open_fs that handles
        # non fixed-location files
        os.mkdir('follower/foo/follower')
        os.mkdir('follower/bar/follower')

        self.pr("create oligarch tree")
        self.session.create(self.uri, leader_create)

        self.pr("create second WT")
        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', 'extensions=["../../ext/storage_sources/dir_store/libwiredtiger_dir_store.so"],create,' + self.conn_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, follower_create)

        self.pr('opening cursor')
        cursor = self.session.open_cursor(self.uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            if i % 10000 == 0:
                time.sleep(1)

        cursor.reset()

        self.pr('opening cursor')
        cursor.close()
        time.sleep(1)

        cursor = self.session.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor.next() == 0:
            item_count += 1

        self.assertEqual(item_count, self.nitems * 3)
        cursor.close()

        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 3)
        cursor_follow.close()
