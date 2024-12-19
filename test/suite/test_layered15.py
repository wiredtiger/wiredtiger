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

import os, os.path, shutil, time, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered15.py
#    Start without local files.
class test_layered15(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                + 'disaggregated=(stable_prefix=.,page_log=palm,role="follower"),'

    create_session_config = 'key_format=S,value_format=S'

    layered_uris = ["layered:test_layered15a", "layered:test_layered15b"]
    file_uris = ["file:test_layered15c"]
    table_uris = ["table:test_layered15d"]
    all_uris = layered_uris + file_uris + table_uris

    update_uris = [table_uris[0]] + ([layered_uris[0]] if len(layered_uris) > 0 else [])
    same_uris = list(set(all_uris) - set(update_uris))

    disagg_storages = gen_disagg_storages('test_layered15', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    num_restarts = 0

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Custom test case setup
    def early_setup(self):
        os.mkdir('kv_home')

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

    # Ensure that the metadata cursor has all the expected URIs.
    def check_metadata_cursor(self, expect_contains, expect_missing = []):
        cursor = self.session.open_cursor('metadata:', None, None)
        metadata = {}
        while cursor.next() == 0:
            metadata[cursor.get_key()] = cursor.get_value()
        for uri in expect_contains:
            self.assertTrue(uri in metadata)
        for uri in expect_missing:
            self.assertFalse(uri in metadata)
        cursor.close()

    # Ensure that the shared metadata has all the expected URIs.
    def check_shared_metadata(self, expect_contains, expect_missing = []):
        cursor = self.session.open_cursor('file:WiredTigerShared.wt_stable', None, None)
        metadata = {}
        while cursor.next() == 0:
            metadata[cursor.get_key()] = cursor.get_value()
        for uri in expect_contains:
            self.assertTrue(uri in metadata)
        for uri in expect_missing:
            self.assertFalse(uri in metadata)
        cursor.close()

    # Test starting without local files.
    def test_layered15(self):
        # The node started as a follower, so step it up as the leader
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Create tables
        for uri in self.all_uris:
            cfg = self.create_session_config
            if not uri.startswith('layered'):
                cfg += ',block_manager=disagg,layered_table_log=(enabled=false),log=(enabled=false)'
            self.session.create(uri, cfg)

        # Put data to tables
        value_prefix = 'aaa'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        checkpoint_id = self.disagg_get_complete_checkpoint()

        # Ensure that the shared metadata table has all the expected URIs
        self.check_shared_metadata(self.all_uris)

        #
        # ------------------------------ Restart 1 ------------------------------
        #

        # Reopen the connection
        self.restart_without_local_files()

        # There should be no shared URIs in the metadata table at this point
        self.check_metadata_cursor([], self.all_uris)

        # Pick up the checkpoint
        self.conn.reconfigure(f'disaggregated=(checkpoint_id={checkpoint_id})')

        # Ensure that the shared metadata table has all the expected URIs
        self.check_shared_metadata(self.all_uris)

        # Ensure that the metadata cursor has all the expected URIs
        self.check_metadata_cursor(self.all_uris)

        # Check tables after the restart, but before we step up as a leader
        # FIXME-SLS-760: Opening a layered table here would cause a failure in __assert_ckpt_matches
        # for uri in self.all_uris:
        for uri in self.table_uris + self.file_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()

        # Become the leader (skip a few extra checkpoint IDs just in case)
        self.conn.reconfigure(f'disaggregated=(role="leader",next_checkpoint_id={checkpoint_id+2})')

        # Check tables again after stepping up
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()

        # Do a few more updates to ensure that the tables continue to be writable
        value_prefix2 = 'bbb'
        for uri in self.update_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                if i % 10 == 0:
                    cursor[str(i)] = value_prefix2 + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        # Ensure that the leader sees its own writes before a checkpoint
        for uri in self.update_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                if i % 10 == 0:
                    self.assertEquals(cursor[str(i)], value_prefix2 + str(i))
                else:
                    self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()
        for uri in self.same_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()

        # Ensure that the shared metadata table has all the expected URIs
        self.check_shared_metadata(self.all_uris)

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        checkpoint_id = self.disagg_get_complete_checkpoint()

        # Ensure that the shared metadata table has all the expected URIs after the checkpoint
        self.check_shared_metadata(self.all_uris)

        # Ensure that the leader sees its own writes after a checkpoint
        for uri in self.update_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                if i % 10 == 0:
                    self.assertEquals(cursor[str(i)], value_prefix2 + str(i))
                else:
                    self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()
        for uri in self.same_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()

        # Ensure that the shared metadata table has all the expected URIs
        self.check_shared_metadata(self.all_uris)

        # Ensure that the metadata cursor has all the expected URIs
        self.check_metadata_cursor(self.all_uris)

        #
        # ------------------------------ Restart 2 ------------------------------
        #

        # Reopen the connection
        self.restart_without_local_files()

        # Pick up the checkpoint
        self.conn.reconfigure(f'disaggregated=(checkpoint_id={checkpoint_id})')

        # Become the leader (skip a few extra checkpoint IDs just in case)
        self.conn.reconfigure(f'disaggregated=(role="leader",next_checkpoint_id={checkpoint_id+2})')

        # Ensure that the shared metadata table has all the expected URIs
        self.check_shared_metadata(self.all_uris)

        # Ensure that the metadata cursor has all the expected URIs
        self.check_metadata_cursor(self.all_uris)

        # Check tables after the restart
        for uri in self.update_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                if i % 10 == 0:
                    self.assertEquals(cursor[str(i)], value_prefix2 + str(i))
                else:
                    self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()
        for uri in self.same_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()
