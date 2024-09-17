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
    nitems = 100000

    # conn_config = 'log=(enabled),verbose=[oligarch:5]'
    conn_base_config = 'log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'oligarch=(role="leader")'

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
        session_config = 'key_format=S,value_format=S,stable_prefix=.'
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

        #
        # Part 1: Create an oligarch table and check that follower has all the data.
        #

        self.pr("create oligarch tree")
        self.session.create(self.uri, session_config)

        self.pr("create second WT")
        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + "oligarch=(role=\"follower\")")
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, session_config)

        self.pr('opening cursor')
        cursor = self.session.open_cursor(self.uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            if i % 10000 == 0:
                time.sleep(1)
                #session_follow.checkpoint()
                if i == 10000:
                    cursor_follow1 = session_follow.open_cursor(self.uri, None, None) # TODO needed so we make the metadata watcher thread earlier

        cursor.reset()

        self.pr('opening cursor')
        cursor.close()
        time.sleep(2)

        cursor = self.session.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor.next() == 0:
            item_count += 1

        self.assertEqual(item_count, self.nitems * 3)
        cursor.close()

        # Ensure that all data makes it to the follower before we check.
        self.session.checkpoint()
        session_follow.checkpoint()
        time.sleep(2)

        cursor_follow2 = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow2.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 3)

        #
        # Part 2: Add a second set of items to ensure that the follower can pick them up.
        #

        self.pr('add another set of items')
        cursor = self.session.open_cursor(self.uri, None, None)

        for i in range(self.nitems):
            cursor["** Hello " + str(i)] = "World"
            cursor["** Hi " + str(i)] = "There"
            cursor["** OK " + str(i)] = "Go"
            if i % 10000 == 0:
                time.sleep(1)
                #session_follow.checkpoint()

        cursor.reset()
        cursor.close()
        time.sleep(2)

        # Ensure that all data makes it to the follower before we check.
        self.session.checkpoint()
        session_follow.checkpoint()
        time.sleep(2)

        cursor_follow3 = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow3.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 6)

        # Close cursors
        cursor_follow1.close()
        cursor_follow2.close()
        cursor_follow3.close()

        #
        # Part 3: Check stats.
        #

        cursor = self.session.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor.next() == 0:
            item_count += 1

        self.assertEqual(item_count, self.nitems * 6)
        cursor.close()

        # Allow time for stats to be updated
        time.sleep(2)

        #
        # Part 4: Reopen the follower to ensure it can open an existing table.
        #

        # Checkpoint to ensure that we don't miss any items after we reopen the connection below.
        self.session.checkpoint()
        session_follow.checkpoint()
        time.sleep(2)

        # Reopen the follower connection.
        session_follow.close()
        conn_follow.close()
        self.pr("reopen the follower")
        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', 'extensions=["../../ext/storage_sources/dir_store/libwiredtiger_dir_store.so"],create,' + self.conn_config)
        session_follow = conn_follow.open_session('')

        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 6)

        cursor_follow.close()
        session_follow.close()
        conn_follow.close()
