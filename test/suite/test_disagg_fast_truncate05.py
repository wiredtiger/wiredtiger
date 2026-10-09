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

import threading, time
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios


@disagg_test_class
class test_disagg_fast_truncate05(wttest.WiredTigerTestCase):
    """Check that a clean resident internal page can be skipped safely."""

    uri = "table:test_disagg_fast_truncate05"
    nrows = 1000
    value = "a" * 50
    trunc_start = 100
    trunc_stop = 900

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_config = ('cache_size=1GB,statistics=(all),precise_checkpoint=true,'
        'checkpoint_cleanup=(wait=100000),disaggregated=(role="leader"),')

    def read_stat(self, stat_key):
        with wttest.open_cursor(
            self.session, "statistics:" + self.uri, config="statistics=(fast)"
        ) as stat_cursor:
            return stat_cursor[stat_key][2]

    def read_conn_stat(self, stat_key):
        with wttest.open_cursor(self.session, "statistics:") as stat_cursor:
            return stat_cursor[stat_key][2]

    def scan(self, read_ts, forward=True):
        values = []
        self.session.begin_transaction("read_timestamp=" + self.timestamp_str(read_ts))
        with wttest.open_cursor(self.session, self.uri) as cursor:
            step = cursor.next if forward else cursor.prev
            while step() == 0:
                values.append((cursor.get_key(), cursor.get_value()))
        self.session.rollback_transaction()
        return values

    def evict_leaves(self):
        with (
            wttest.open_cursor(self.session, self.uri, config="debug=(release_evict)") as cursor,
            self.transaction(rollback=True),
        ):
            for key in range(1, self.nrows + 1):
                cursor.set_key(key)
                self.assertEqual(cursor.search(), 0)
                cursor.reset()

    def create_deleted_subtree(self):
        self.session.create(
            self.uri,
            "key_format=i,value_format=S,block_manager=disagg,log=(enabled=false),"
            "allocation_size=512,leaf_page_max=512,internal_page_max=512,"
            "memory_page_max=4096",
        )
        self.conn.set_timestamp("oldest_timestamp=" + self.timestamp_str(1))
        with (
            wttest.open_cursor(self.session, self.uri) as cursor,
            self.transaction(commit_timestamp=10),
        ):
            for key in range(1, self.nrows + 1):
                cursor[key] = self.value
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(10))
        self.session.checkpoint()
        self.evict_leaves()

        with (
            wttest.open_cursor(self.session, self.uri) as start,
            wttest.open_cursor(self.session, self.uri) as stop,
            self.transaction(commit_timestamp=20),
        ):
            start.set_key(self.trunc_start)
            stop.set_key(self.trunc_stop)
            self.session.truncate(None, start, stop, None)
        self.assertGreater(self.read_stat(stat.dsrc.rec_page_delete_fast), 0)

        surviving = self.nrows - (self.trunc_stop - self.trunc_start + 1)
        self.assertEqual(len(self.scan(25)), surviving)
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(20))
        self.session.checkpoint()
        self.assertEqual(self.read_stat(stat.dsrc.cache_eviction_internal), 0)
        return surviving

    def test_skip_clean_resident_internal_page(self):
        surviving = self.create_deleted_subtree()
        before = self.read_stat(stat.dsrc.cursor_tree_walk_resident_del_internal_page_skip)
        self.assertEqual(len(self.scan(25)), surviving)
        after = self.read_stat(stat.dsrc.cursor_tree_walk_resident_del_internal_page_skip)
        self.assertGreater(after, before, "clean resident internal page was not skipped")

        # A new update makes one child resident and dirty without dirtying its internal parent. The
        # next walk must descend into that parent rather than trust its old aggregate.
        insert_key = self.nrows // 2
        with (
            wttest.open_cursor(self.session, self.uri) as cursor,
            self.transaction(commit_timestamp=24),
        ):
            cursor[insert_key] = "new value"
        values = self.scan(25)
        self.assertIn((insert_key, "new value"), values)
        self.assertEqual(len(values), surviving + 1)

    def test_scan_while_checkpoint_clears_tree_modified(self):
        # A checkpoint clears the tree's modified flag before it writes the dirty child, so for
        # that window the tree reads as clean while the parent's aggregate is still stale. A walk
        # inside the window must still return the new record.
        surviving = self.create_deleted_subtree()
        insert_key = self.nrows // 2
        with (
            wttest.open_cursor(self.session, self.uri) as cursor,
            self.transaction(commit_timestamp=24),
        ):
            cursor[insert_key] = "new value"

        # Keep the updated leaf in use so its parent cannot be evicted during the checkpoint.
        pin_session = self.conn.open_session()
        pin_session.begin_transaction("read_timestamp=" + self.timestamp_str(25))
        pin = pin_session.open_cursor(self.uri)
        pin.set_key(insert_key)
        self.assertEqual(pin.search(), 0)

        errors = []
        def checkpoint():
            ckpt_session = self.conn.open_session()
            try:
                ckpt_session.checkpoint()
            except Exception as e:
                errors.append(e)
            finally:
                ckpt_session.close()

        self.conn.reconfigure("timing_stress_for_test=[checkpoint_sync_file_delay]")
        ckpt_thread = threading.Thread(target=checkpoint)
        ckpt_thread.start()
        try:
            deadline = time.time() + 60
            while self.read_conn_stat(stat.conn.checkpoint_sync_file_stress_active) == 0:
                self.assertLess(time.time(), deadline, "checkpoint did not reach the pause")
                time.sleep(0.1)
            forward = self.scan(25)
            backward = self.scan(25, forward=False)
            self.assertEqual(self.read_conn_stat(stat.conn.checkpoint_sync_file_stress_active), 1,
                "scans finished after the checkpoint pause ended")
        finally:
            ckpt_thread.join()
            self.conn.reconfigure("timing_stress_for_test=[]")
            pin_session.close()
        self.assertEqual(errors, [])

        self.assertIn(insert_key, [key for key, _ in forward])
        self.assertIn((insert_key, "new value"), forward)
        self.assertEqual(len(forward), surviving + 1)
        self.assertEqual(backward, list(reversed(forward)))


if __name__ == "__main__":
    wttest.run()
