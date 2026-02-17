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

import re
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Test file IDs for tables that should have predefined IDs.
@disagg_test_class
class test_layered75(wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages('test_layered75', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    conn_config = 'disaggregated=(role="leader")'
    conn_config_follower = 'disaggregated=(role="follower")'

    session_follow = None
    conn_follow = None

    def create_follower(self):
        self.conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
                                                self.conn_config_follower)
        self.session_follow = self.conn_follow.open_session('')

    def check_predefined_ids(self, session):
        """
        Checks file IDs for files that should have predefined IDs.
        """
        TEST_WT_BTREE_ID_NAMESPACE_SPECIAL = 2

        # Calculate namespaced IDs: (index << 3) | namespaces
        expected_ids = {
            'file:WiredTigerShared.wt_stable': (0 << 3) | TEST_WT_BTREE_ID_NAMESPACE_SPECIAL, # 2
            'file:WiredTigerSharedHS.wt_stable': (1 << 3) | TEST_WT_BTREE_ID_NAMESPACE_SPECIAL # 10
        }

        cursor = session.open_cursor('metadata:', None, None)
        found_files = {}

        # Iterate through metadata entries
        for key, value in cursor:
            if key.startswith('file:') and key in expected_ids:
                # Parse the file ID from the configuration string
                match = re.search(r',id=(\d+)', value)
                self.assertTrue(match, "All `file:` prefixed metadata entries are expected to have IDs")
                file_id = int(match.group(1))

                # Check if this is one of our expected files
                found_files[key] = file_id
                self.pr(f"Found {key}: id={file_id}, expected={expected_ids[key]}")

                # Verify the ID matches expected
                self.assertEqual(file_id, expected_ids[key],
                                   f"File {key} has unexpected ID {file_id}, expected {expected_ids[key]}")

        cursor.close()

        # Verify we found all expected files
        for key in expected_ids:
            self.assertIn(key, found_files, f"Expected file {key} not found in metadata")

    def test_empty_database(self):
        # Check the leader
        self.check_predefined_ids(self.session)

        self.create_follower()
        # Check the follower
        self.check_predefined_ids(self.session_follow)

    def test_populate_table_on_leader(self):
        self.session.create("layered:test_layered75", 'key_format=S,value_format=S')

        # Check the leader
        self.check_predefined_ids(self.session)

        self.create_follower()
        # Check the follower
        self.check_predefined_ids(self.session_follow)

    def test_populate_table_on_leader_pick_up_on_follower(self):
        self.session.create("layered:test_layered75", 'key_format=S,value_format=S')
        # Check the leader
        self.check_predefined_ids(self.session)

        self.create_follower()
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        # Check the follower
        self.check_predefined_ids(self.session_follow)
