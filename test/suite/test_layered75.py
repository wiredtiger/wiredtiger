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

# Helper variables - have the same values as namespaces in WT.
TEST_NAMESPACE_LOCAL = 0
TEST_NAMESPACE_SHARED = 1
TEST_NAMESPACE_SPECIAL = 2

TEST_NAMESPACE_BITS = 3

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

    def find_id(self, metadata_value):
        """
        Parse the file ID from the configuration string.
        """

        match = re.search(r',id=(\d+)', metadata_value)
        self.assertTrue(match, "All `file:` or `metadata:` prefixed metadata entries are expected to have IDs")
        return int(match.group(1))

    def check_prohibited_ids(self, file_id):
        """
        Checks that file ID is not in the prohibited IDs.
        """
        # These IDs are a part of the special namespace but they exist only on the PALI level
        # and don't have corresponding metadata entries so we shouldn't have metadata entries with
        # these IDs to avoid conflicts on the PALI level.
        prohibited_ids = (
            (0 << TEST_NAMESPACE_BITS) | TEST_NAMESPACE_SPECIAL, # WT_SPECIAL_PALI_TURTLE_FILE_ID
            (3 << TEST_NAMESPACE_BITS) | TEST_NAMESPACE_SPECIAL, # WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID
        )

        self.assertFalse(file_id in prohibited_ids, f"Prohibited id {file_id} detected")

    def check_metadata_ids(self, session, explicit_dynamic_id_tables = {}):
        """
        Checks that that the metadata contain only expected entries and their IDs are valid.
        """

        # Shared metadata and shared history store should have fixed IDs within the special namespace.
        # Local metadata is a special case that should always have 0 table ID.
        # BE CAREFUL!!! CHANGING THESE IDS WILL PROBABLY CAUSE A BACKWARD COMPATIBILITY BREAK!!!
        # Calculate namespaced IDs: (ID << 3) | namespaces
        expected_fixed_id_tables = {
            'file:WiredTigerShared.wt_stable': (1 << TEST_NAMESPACE_BITS) | TEST_NAMESPACE_SPECIAL, # 10
            'file:WiredTigerSharedHS.wt_stable': (2 << TEST_NAMESPACE_BITS) | TEST_NAMESPACE_SPECIAL, # 18
            'metadata:': 0
        }

        # These tables are getting created by WT implicitly but they don't have predefined ID so
        # they use counter to get their IDs. They still should be located in the corresponding namespace.
        implicit_dynamic_id_tables = {
            'file:WiredTigerHS.wt' : TEST_NAMESPACE_LOCAL
        }

        expected_dynamic_ids = {**implicit_dynamic_id_tables, **explicit_dynamic_id_tables}

        cursor = session.open_cursor('metadata:', None, None)
        found_files = {}
        found_ids = {TEST_NAMESPACE_LOCAL: set(), TEST_NAMESPACE_SHARED: set()}

        # Iterate through metadata entries
        for key, value in cursor:
            if not key.startswith('file:') and not key == 'metadata:':
                continue

            file_id = self.find_id(value)
            self.check_prohibited_ids(file_id)

            self.assertTrue(key not in found_files, f"Duplicated table {key}")
            found_files[key] = file_id

            if key in expected_fixed_id_tables:
                # Check that tables with predefined IDs matches the expected IDs
                expected_id = expected_fixed_id_tables[key]
                self.assertEqual(file_id, expected_id,
                                 f"File {key} has unexpected ID {file_id}, expected {expected_id}")
            elif key in expected_dynamic_ids:
                # Check that tables with dynamic IDs have the expected namespace and track counters
                expected_namespace = expected_dynamic_ids[key]
                namespace = file_id & ((1 << TEST_NAMESPACE_BITS) - 1)
                id = file_id >> TEST_NAMESPACE_BITS

                self.assertTrue(id not in found_ids[namespace], f"Duplicated ID {id} within the namespace {namespace}")
                self.assertEqual(namespace, expected_namespace,
                                 f"File {key} has unexpected namespace {namespace}, expected {expected_namespace}")

                # Add unnamespaced IDs to the hash table to track counters
                found_ids[namespace].add(id)
            else:
                # Fail on unexpected entries
                self.assertTrue(False, f"Unexpected file: or metadata: entry {key}")

        cursor.close()

        # Verify we found all expected files
        for key in expected_fixed_id_tables.keys() | expected_dynamic_ids.keys():
            self.assertIn(key, found_files, f"Expected file {key} not found in metadata")

        # Check that counters for each namespace are getting assigned incrementally
        for namespace, namespace_ids in found_ids.items():
            if not namespace_ids:
                continue
            max_id = max(namespace_ids)
            for id in range(max_id, 1):
                self.assertTrue(id in namespace_ids, f"id {id} for namespace {namespace} does not exist in the metadata")

    def test_empty_database(self):
        # Check the leader
        self.check_metadata_ids(self.session)

        self.create_follower()
        # Check the follower
        self.check_metadata_ids(self.session_follow)

    def test_populate_table_on_leader(self):
        self.session.create("layered:test_layered75", 'key_format=S,value_format=S')
        # Check the leader
        expected_files = {"file:test_layered75.wt_stable": TEST_NAMESPACE_SHARED,
                          "file:test_layered75.wt_ingest": TEST_NAMESPACE_LOCAL}
        self.check_metadata_ids(self.session, expected_files)

        self.create_follower()
        # Check the follower
        self.check_metadata_ids(self.session_follow)

    def test_populate_table_on_leader_pick_up_on_follower(self):
        expected_files = {"file:test_layered75.wt_stable": TEST_NAMESPACE_SHARED,
                          "file:test_layered75.wt_ingest": TEST_NAMESPACE_LOCAL}

        self.session.create("layered:test_layered75", 'key_format=S,value_format=S')
        # Check the leader
        self.check_metadata_ids(self.session, expected_files)

        self.create_follower()
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        # Check the follower
        self.check_metadata_ids(self.session_follow, expected_files)

    def test_populate_10_tables_on_leader_pick_up_on_follower(self):
        expected_files = {}

        for i in range(10):
            table_name = f"test_layered75_{i}"
            self.session.create(f"layered:{table_name}", 'key_format=S,value_format=S')
            expected_files[f"file:{table_name}.wt_stable"] = TEST_NAMESPACE_SHARED
            expected_files[f"file:{table_name}.wt_ingest"] = TEST_NAMESPACE_LOCAL

        # Check the leader
        self.check_metadata_ids(self.session, expected_files)

        self.create_follower()
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        # Check the follower
        self.check_metadata_ids(self.session_follow, expected_files)
