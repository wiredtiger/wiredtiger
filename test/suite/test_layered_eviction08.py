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
#    Content written before a step-down stays resident on the table the step-down makes read-only.
#    Eviction has to reach and discard it, or it is stranded and the cache fills up.
@disagg_test_class
class test_layered_eviction08(wttest.WiredTigerTestCase):
    # A low dirty target keeps eviction on dirty-only passes, which is where a read-only table is
    # skipped. The high updates target keeps those passes from also counting as update passes.
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
                     + 'cache_size=20MB,eviction_dirty_target=1,eviction_dirty_trigger=20,' \
                     + 'eviction_updates_target=50,eviction_updates_trigger=60,' \
                     + 'eviction=(threads_min=1,threads_max=1),'
    conn_config = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

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

    def dirty_bytes(self):
        return self.conn_stat(wiredtiger.stat.conn.cache_bytes_dirty)

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

    def strand_content(self, extra_tables=0):
        """
        Fill and checkpoint a table as leader, write to it again above the stable timestamp, then
        step down, leaving that second write resident on a table now marked read-only.

        Returns the connection's dirty bytes before and after that second write.
        """
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.conn.reconfigure('disaggregated=(role="leader")')

        uri = 'table:' + self.table_name
        self.session.create(uri, self.create_session_config)

        # These are checkpointed and then left alone, so the step-down strands nothing on them.
        quiet_uris = ['table:' + self.table_name + '_quiet%d' % i for i in range(extra_tables)]
        for quiet_uri in quiet_uris:
            self.session.create(quiet_uri, self.create_session_config)

        ts = self.write_rows(uri, self.nrows, self.value, 10)
        for quiet_uri in quiet_uris:
            ts = self.write_rows(quiet_uri, self.batch, self.value, ts)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts) +
            ',oldest_timestamp=' + self.timestamp_str(ts))
        self.session.checkpoint()

        # Everything so far is durable, so what is still dirty belongs elsewhere: the drain floor.
        baseline = self.dirty_bytes()

        # Leave stable behind this write so it cannot be written out before the step-down.
        self.write_rows(uri, self.stranded_rows, self.value.upper(), ts + 1)
        peak = self.dirty_bytes()
        self.assertGreater(peak, baseline,
            'the table holds nothing for the step-down to strand')

        self.conn.reconfigure('disaggregated=(role="follower")')

        # Content that cannot be re-read from shared storage is deliberately held resident while
        # anyone still has the table open, so drop this session's handles before expecting a drain.
        self.session.close()
        self.session = self.conn.open_session()

        return baseline, peak

    def test_stranded_content_drains(self):
        # The only user table here, so eviction ends its passes parked on it and has to drain it
        # anyway.
        baseline, peak = self.strand_content()

        # Eviction stops once under the dirty target, so the last of it legitimately stays: allow a
        # wide margin rather than expecting the exact floor.
        drained = baseline + (peak - baseline) // 4
        deadline = time.time() + self.drain_timeout
        while self.dirty_bytes() > drained:
            self.assertLess(time.time(), deadline,
                'the content stranded by the step-down was never discarded')
            time.sleep(0.5)

        self.assertGreater(
            self.conn_stat(wiredtiger.stat.conn.eviction_server_walk_outdated_disagg_trees), 0,
            'eviction never reached the stepped-down table')

        # Nobody holds the table, so the drain should not still be waiting on a reader.
        stale = self.conn_stat(wiredtiger.stat.conn.eviction_server_skip_stale_disagg_pages)
        time.sleep(1)
        self.assertEqual(
            self.conn_stat(wiredtiger.stat.conn.eviction_server_skip_stale_disagg_pages), stale,
            'eviction is still holding pages back for a reader that has gone')

    def test_stranded_content_drains_alongside_quiet_tables(self):
        # The same drain with other tables in the cache, so the walk has somewhere else to go.
        baseline, peak = self.strand_content(extra_tables=4)

        drained = baseline + (peak - baseline) // 4
        deadline = time.time() + self.drain_timeout
        while self.dirty_bytes() > drained:
            self.assertLess(time.time(), deadline,
                'the content stranded by the step-down was never discarded')
            time.sleep(0.5)
