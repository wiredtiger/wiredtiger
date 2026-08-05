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

# Validate immutable metadata configuration fields on checkpoint pickup.
# A matched pickup passes; a table dropped and recreated on the leader (new btree id) and a table
# created with different key formats on the two nodes both panic.

import os
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema22(wttest.WiredTigerTestCase, suite_subprocess, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def run_panic_subprocess(self, name):
        """
        Run subprocess_<name> in a subprocess and assert it died from the metadata
        mismatch panic.
        """
        [returncode, home] = self.run_subprocess_function(f'SUBPROCESS_{name}',
            f'{self.test_name}.{self.test_name}.subprocess_{name}', silent=True)
        self.assertNotEqual(returncode, 0)
        self.check_file_contains(os.path.join(home, 'stderr.txt'),
            'checkpoint pickup metadata mismatch')

    def test_matched_pickup_ok(self):
        """Repeated pickups of an unchanged table configuration pass the validation."""
        self.session.create(self.uri, self.table_config)
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower()
        session_follow.close()

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        cursor.close()
        self.leader_checkpoint(2)
        self.disagg_advance_checkpoint(conn_follow)

        self.assertTrue(self.uri_stable_exists(conn_follow, self.uri))
        conn_follow.close('debug=(skip_checkpoint=true)')

    def subprocess_recreated_table_panics(self):
        """Subprocess body for the btree id mismatch test; expected to panic/abort."""
        self.session.create(self.uri, self.table_config)
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower()
        session_follow.close()

        # Drop and recreate the table on the leader with an identical configuration: the new table
        # gets a new btree id. The follower never applies the drop, expected to panic.
        self.dropUntilSuccess(self.session, self.uri)
        self.session.create(self.uri, self.table_config)
        self.leader_checkpoint(2)
        self.disagg_advance_checkpoint(conn_follow)

    def test_recreated_table_panics(self):
        """A table recreated on the leader carries a new btree id: pickup panics."""
        self.run_panic_subprocess('recreated_table_panics')

    def subprocess_mismatched_format_panics(self):
        """Subprocess body for the key format mismatch test; expected to panic/abort."""
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower()
        # Create the same table on both nodes with different key formats.
        session_follow.create(self.uri, 'key_format=S,value_format=S')
        session_follow.close()

        self.session.create(self.uri, self.table_config)
        self.leader_checkpoint(2)
        self.disagg_advance_checkpoint(conn_follow)

    def test_mismatched_format_panics(self):
        """The same table created with different key formats on each node: pickup panics."""
        self.run_panic_subprocess('mismatched_format_panics')
