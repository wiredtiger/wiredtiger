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
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered06.py
#    Start a second WT that shares the stable content with the first.
@disagg_test_class
class test_layered06(wttest.WiredTigerTestCase, DisaggConfigMixin):

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    scenarios = make_scenarios([
        ('1k', dict(nitems=1000)),
        ('100k', dict(nitems=100000)),
    ])

    # TODO do Python tests expect a field named uri?
    uri = "layered:test_layered06"

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', 'palm')

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    # Test records into a layered tree and restarting
    def test_layered06(self):
        session_config = 'key_format=S,value_format=S'

        #
        # Part 1: Create a layered table and check that follower has all the data.
        #

        self.pr("create layered tree")
        self.session.create(self.uri, session_config)

        self.pr("create second WT")
        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + "disaggregated=(role=\"follower\")")
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, session_config)

        # Sanity-check that a stats cursor says we have nothing in the tree.
        stat_cur = self.session.open_cursor('statistics:' + self.uri, None, None)
        self.assertEqual(stat_cur[stat.dsrc.btree_entries][2], 0)
        stat_cur.close()

        self.pr('opening cursor')
        cursor = self.session.open_cursor(self.uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            if i % 25000 == 0:
                time.sleep(1)

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
        self.disagg_advance_checkpoint(conn_follow)

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
            if i % 25000 == 0:
                time.sleep(1)

        cursor.reset()
        cursor.close()
        time.sleep(2)

        # Ensure that all data makes it to the follower before we check.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        cursor_follow3 = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow3.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 6)

        # Close cursors
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

        # Make sure the stats agree that the leader has everything.
        stat_cur = self.session.open_cursor('statistics:' + self.uri, None, None)
        self.assertEqual(stat_cur[stat.dsrc.btree_entries][2], self.nitems * 6)
        stat_cur.close()


        # Allow time for stats to be updated
        time.sleep(2)

        #
        # Part 4: Reopen the follower to ensure it can open an existing table.
        #

        # Checkpoint to ensure that we don't miss any items after we reopen the connection below.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Reopen the follower connection.
        session_follow.close()
        conn_follow.close()
        self.pr("reopen the follower")
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',' + self.conn_base_config + "disaggregated=(role=\"follower\")")
        session_follow = conn_follow.open_session('')

        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        item_count = 0
        while cursor_follow.next() == 0:
            item_count += 1
        self.assertEqual(item_count, self.nitems * 6)

        cursor_follow.close()

        # Make sure the stats agree that the follower has everything.
        stat_cur = session_follow.open_cursor('statistics:' + self.uri, None, None)
        self.assertEqual(stat_cur[stat.dsrc.btree_entries][2], self.nitems * 6)
        stat_cur.close()

        session_follow.close()
        conn_follow.close()
