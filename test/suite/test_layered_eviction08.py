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

import time
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_eviction08.py
#    A step-down leaves dirty pages resident on a btree it marks read-only. Those pages can never
#    be written to shared storage, so the eviction server has to reach them and discard them. If
#    the walk skips the tree because it is read-only, the pages are stranded and the cache fills.
@disagg_test_class
class test_layered_eviction08(wttest.WiredTigerTestCase):
    # A dirty target low enough that the stranded content alone keeps the eviction server running
    # dirty passes, and a trigger high enough that the loading transactions never stall.
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
                     + 'cache_size=20MB,eviction_dirty_target=1,eviction_dirty_trigger=20,' \
                     + 'eviction_updates_target=1,eviction_updates_trigger=10,' \
                     + 'eviction=(threads_min=1,threads_max=1),' \
                     + 'disaggregated=(lose_all_my_data=true),'
    conn_config = conn_base_config + 'disaggregated=(role="follower")'

    create_session_config = 'key_format=i,value_format=S,leaf_page_max=4KB,' \
                          + 'block_manager=disagg,log=(enabled=false)'

    table_name = 'test_layered_eviction08'

    nrows = 1000
    # Comfortably more than one dirty leaf page's worth, so the drain is not a marginal thing a
    # slower variant can miss.
    stranded_rows = 1000
    value = 'v' * 1000

    batch = 250
    drain_timeout = 120

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_stat(self, name):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[name][2]
        stat_cursor.close()
        return value


    def write_rows(self, uri, rows, value, ts):
        """Write rows in batches, committing and advancing the timestamps as we go."""
        cursor = self.session.open_cursor(uri, None, None)
        for start in range(1, rows + 1, self.batch):
            self.session.begin_transaction()
            for i in range(start, min(start + self.batch, rows + 1)):
                cursor[i] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
            ts += 1
        cursor.close()
        return ts

    def load(self, uri):
        """Fill the table and checkpoint it, so the whole tree is materialized and clean."""
        ts = self.write_rows(uri, self.nrows, self.value, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts) +
            ',oldest_timestamp=' + self.timestamp_str(ts))
        self.session.checkpoint()
        return ts + 1

    def step_up(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.conn.reconfigure('disaggregated=(role="leader")')

    def test_outdated_tree_dirty_pages_drain(self):
        self.step_up()
        uri = 'table:' + self.table_name
        self.session.create(uri, self.create_session_config)
        ts = self.load(uri)

        # Everything written so far is checkpointed, so whatever dirty content the connection
        # still reports belongs to other trees. That is the floor the drain has to come back to.
        baseline = self.conn_stat(wiredtiger.stat.conn.cache_bytes_dirty)

        # Dirty part of the tree above the checkpoint timestamp and do NOT checkpoint. This
        # content belongs to the current leader era and is never flushed, so it is still dirty and
        # resident when the step-down strands it.
        ts = self.write_rows(uri, self.stranded_rows, self.value.upper(), ts)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts) +
            ',oldest_timestamp=' + self.timestamp_str(ts))

        self.assertGreater(self.conn_stat(wiredtiger.stat.conn.cache_bytes_dirty), baseline,
            'the tree holds no dirty content, so the step-down strands nothing')

        skips_before = self.conn_stat(wiredtiger.stat.conn.eviction_server_skip_trees_read_only)

        self.conn.reconfigure('disaggregated=(role="follower")')

        # Release every reference to the tree. A page on an outdated tree cannot be re-read from
        # shared storage, so eviction deliberately holds it resident while any reader has the
        # handle open; while a session still holds the handle the pages stay put however the walk
        # behaves. Closing the session drops its cached handle reference.
        self.session.close()
        self.session = self.conn.open_session()

        # The stranded content must drain. Poll rather than sleep: the eviction server owns this.
        deadline = time.time() + self.drain_timeout
        while self.conn_stat(wiredtiger.stat.conn.cache_bytes_dirty) > baseline:
            self.assertLess(time.time(), deadline,
                'eviction did not discard the dirty pages stranded on the outdated tree')
            time.sleep(0.5)

        # The tree was reached because it is outdated, not in spite of being read-only.
        self.assertGreater(
            self.conn_stat(wiredtiger.stat.conn.eviction_server_walk_outdated_disagg_trees), 0,
            'the walk never took the outdated-tree path')
        self.assertEqual(
            self.conn_stat(wiredtiger.stat.conn.eviction_server_skip_trees_read_only),
            skips_before,
            'the walk skipped a read-only tree that still held stranded dirty content')

    def test_read_only_tree_is_skipped(self):
        # The other half of the change: a read-only tree with nothing stranded is still skipped on
        # a pass looking only for dirty pages.
        self.step_up()
        uri = 'table:' + self.table_name
        self.session.create(uri, self.create_session_config)
        self.load(uri)

        self.conn.reconfigure('disaggregated=(role="follower")')
        self.session.close()
        self.session = self.conn.open_session()

        deadline = time.time() + self.drain_timeout
        while self.conn_stat(wiredtiger.stat.conn.eviction_server_skip_trees_read_only) == 0:
            self.assertLess(time.time(), deadline,
                'the eviction walk never skipped the clean read-only tree')
            time.sleep(0.5)
