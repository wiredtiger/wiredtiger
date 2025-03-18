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
from wtscenario import make_scenarios

# test_layered36.py
#    Test creating missing stable tables.
@disagg_test_class
class test_layered36(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_base_config = 'statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'checkpoint=(precise=true),disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="follower")'

    create_session_config = 'key_format=S,value_format=S'

    table_name_preexisting = "test_layered36a"
    table_name_new_empty = "test_layered36b"
    table_name_new_filled = "test_layered36c"

    disagg_storages = gen_disagg_storages('test_layered36', disagg_only = True)
    scenarios = make_scenarios(disagg_storages, [
        ('layered-prefix', dict(prefix='layered:', table_config='')),
        ('layered-type', dict(prefix='table:', table_config='block_manager=disagg,type=layered')),
    ])

    num_restarts = 0

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    # Restart the node without local files
    def restart_without_local_files(self):
        # Close the current connection
        self.close_conn()

        # Move the local files to another directory
        self.num_restarts += 1
        dir = f'SAVE.{self.num_restarts}'
        os.mkdir(dir)
        for f in os.listdir():
            if os.path.isdir(f):
                continue
            if f.startswith('WiredTiger') or f.startswith('test_'):
                os.rename(f, os.path.join(dir, f))

        # Also save the PALM database (to aid debugging)
        shutil.copytree('kv_home', os.path.join(dir, 'kv_home'))

        # Reopen the connection
        self.open_conn()

    # A simple test with a single node.
    def test_layered36(self):

        # Create an empty table.
        uri_new_empty = self.prefix + self.table_name_new_empty
        config = self.create_session_config + ',' + self.table_config
        self.session.create(uri_new_empty, config)

        # Create a table with some data.
        uri_new_filled = self.prefix + self.table_name_new_filled
        config = self.create_session_config + ',' + self.table_config
        self.session.create(uri_new_filled, config)

        # FIXME-SLS-760
        if False:
            self.session.begin_transaction() # Draining requires timestamps
            cursor = self.session.open_cursor(uri_new_filled, None, None)
            cursor['a'] = 'b'
            cursor.close()
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Step up and checkpoint.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.session.checkpoint()

        # Restart without local files to check that the tables are created and have correct data.
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()
        self.restart_without_local_files()
        self.conn.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')

        # Check the tables
        cursor = self.session.open_cursor(uri_new_empty, None, None)
        item_count = 0
        while cursor.next() == 0:
            item_count += 1
        cursor.close()
        self.assertEqual(item_count, 0)

        cursor = self.session.open_cursor(uri_new_filled, None, None)
        # FIXME-SLS-760
        if False:
            self.assertEqual(cursor['a'], 'b')
        cursor.close()

    # A more complicated test with preexisting tables and switching node roles.
    def test_layered36_ext(self):
        # FIXME-SLS-760 This test triggers SLS-760.
        self.skipTest('Skipped due to SLS-760.')

        # The node started as a follower, so step it up as the leader
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Create the first table, with some data
        uri_preexisting = self.prefix + self.table_name_preexisting
        config = self.create_session_config + ',' + self.table_config
        self.session.create(uri_preexisting, config)
        cursor = self.session.open_cursor(uri_preexisting, None, None)
        cursor['a'] = 'b'
        cursor.close()

        # Create a checkpoint
        self.session.checkpoint()

        # Start the follower
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_config)
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')

        # Check the new table in the follower
        cursor = session_follow.open_cursor(uri_preexisting, None, None)
        self.assertEqual(cursor['a'], 'b')
        cursor.close()

        #
        # Part 1: Create new tables in the follower (e.g., due to oplog application)
        #

        # An empty table
        uri_new_empty = self.prefix + self.table_name_new_empty
        config = self.create_session_config + ',' + self.table_config
        session_follow.create(uri_new_empty, config)

        # A table with some data
        uri_new_filled = self.prefix + self.table_name_new_filled
        config = self.create_session_config + ',' + self.table_config
        session_follow.create(uri_new_filled, config)

        session_follow.begin_transaction() # Draining requires timestamps
        cursor = session_follow.open_cursor(uri_new_filled, None, None)
        cursor['a'] = 'b'
        cursor.close()
        session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        #
        # Part 2: Close the original connection, and step up the follower
        #

        # We didn't create tables in the leader, because we're using clean shutdown here.
        self.close_conn()

        # Step up the follower, which should result in the tables being created in SLS.
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Create a checkpoint, to ensure the data is committed to SLS.
        session_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        session_follow.checkpoint()

        #
        # Part 3: Open a new follower and check that the tables are there
        #

        # Start the a follower
        self.restart_without_local_files()
        self.open_conn()
        self.disagg_advance_checkpoint(self.conn, conn_follow)

        # Check the tables in the follower
        cursor = self.session.open_cursor(uri_preexisting, None, None)
        self.assertEqual(cursor['a'], 'b')
        cursor.close()

        cursor = self.session.open_cursor(uri_new_empty, None, None)
        item_count = 0
        while cursor.next() == 0:
            item_count += 1
        cursor.close()
        self.assertEqual(item_count, 0)

        cursor = self.session.open_cursor(uri_new_filled, None, None)
        self.assertEqual(cursor['a'], 'b')
        cursor.close()

        #
        # Clean up
        #

        session_follow.close()
        conn_follow.close()
