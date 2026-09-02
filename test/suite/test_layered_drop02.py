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

# Dropping a layered table must tolerate its stable data having already been
# reclaimed by the page server.
#
# Trimming a layered table's stable pages before the drop becomes durable is by
# design: it prevents leaking the table's data if the leader crashes after the
# drop is durable but before the trim runs. After restarting from the newest
# checkpoint, two-phase drop guarantees that replay of a dropped table only ever
# reissues drop(), so drop must handle pages the page server has already
# discarded rather than aborting on the missing root-page read.
#
# Two scenarios, both issuing a first drop (which sends the trim), hitting a
# failure, then replaying the drop on reclaimed pages:
#   test_replayed_drop_after_close_conn - the first drop completes, then closing
#     without checkpointing rolls back the local drop. The pages are reclaimed in
#     palite, then on reopening from the shared checkpoint the table is restored
#     and the drop is replayed.
#   test_replayed_drop_after_post_trim_crash - the debug failpoint aborts the
#     process after the trim is sent but before the constituents are dropped.
#     The pages are reclaimed in palite, then the crashed home is reopened and
#     the drop replayed.
#
# Palite's trim_table is a no-op, so both tests stand in for a page server that
# has already reclaimed the pages by deleting the stable constituent's rows from
# palite's pages table while the connection is closed. The pre-fix abort takes
# down the process, so the replay runs in a subprocess and the parent asserts a
# zero exit.

import os, re, signal
import wttest
from helper_disagg import (
    disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin, DisaggCorruptionMixin)
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_drop02(
  wttest.WiredTigerTestCase, suite_subprocess, DisaggSchemaEpochMixin, DisaggCorruptionMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'

    uri = 'layered:drop02'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def stable_btree_id(self):
        cursor = self.session.open_cursor('metadata:')
        cursor.set_key(self.stable_uri(self.uri))
        self.assertEqual(cursor.search(), 0)
        value = cursor.get_value()
        cursor.close()
        return int(re.search(r'id=(\d+)', value).group(1))

    def setup_table_and_checkpoint(self):
        """Create, publish, and checkpoint the layered table; return its stable btree id."""
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, self.table_config)
        self.set_stable_epoch(1)
        self.publish(self.uri, 2)
        self.set_stable_epoch(2)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'stable_value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        cursor.close()
        self.leader_checkpoint(2)
        return self.stable_btree_id()

    def replay_drop(self, home='.'):
        """Open (or reopen) the home and replay the drop."""
        self.ignoreStdoutPattern('WT_VERB_METADATA')
        self.open_conn(home)
        self.session.drop(self.uri)

    def subprocess_replay_after_close_conn(self):
        btree_id = self.setup_table_and_checkpoint()

        # First drop completes (trim is a no-op in palite); close without checkpointing simulates a
        # crash that rolls back the local drop.
        self.session.drop(self.uri)
        self.close_conn('debug=(skip_checkpoint=true)')
        self.delete_all_table_pages(btree_id)
        self.replay_drop()
        os._exit(0)

    def subprocess_crash_after_trim(self):
        self.setup_table_and_checkpoint()

        # The crash point aborts after the trim is sent, before the constituents are dropped.
        self.conn.reconfigure('debug_mode=(crash_point=(after_drop_trim=true))')
        self.session.drop(self.uri)

    def subprocess_replay_after_post_trim_crash(self):
        # Phase 1: spawn a subprocess that sets up the table and crashes mid-drop at the crash
        # point, after the trim is sent.
        [returncode, home] = self.run_subprocess_function('SUBPROCESS_crash_after_trim',
            f'{self.test_name}.{self.test_name}.subprocess_crash_after_trim', silent=True)
        self.assert_crashed(returncode, signal.SIGABRT)

        # Phase 2: reopen the crashed home to read the stable btree id from the restored local
        # metadata, close it without checkpointing, reclaim the pages, then reopen and replay.
        self.close_conn('debug=(skip_checkpoint=true)')
        self.ignoreStdoutPattern('WT_VERB_METADATA')
        self.open_conn(home)
        btree_id = self.stable_btree_id()
        self.close_conn('debug=(skip_checkpoint=true)')
        self.delete_all_table_pages(btree_id, home=home)
        self.replay_drop(home)
        os._exit(0)

    def run_replay_subprocess(self, name):
        """Run a replay subprocess and assert it exits cleanly (the drop must not abort)."""
        # Advance the parent past the subprocess's epochs so the fixture can close cleanly.
        self.set_stable_epoch(10)
        self.leader_checkpoint(1)
        [returncode, _] = self.run_subprocess_function(f'SUBPROCESS_{name}',
            f'{self.test_name}.{self.test_name}.subprocess_{name}', silent=True)
        self.assertEqual(returncode, 0,
            'replayed drop() aborted when the stable data was already discarded')

    def test_replayed_drop_after_close_conn(self):
        self.run_replay_subprocess('replay_after_close_conn')

    def test_replayed_drop_after_post_trim_crash(self):
        self.run_replay_subprocess('replay_after_post_trim_crash')
