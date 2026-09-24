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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Inserts after the final checkpoint split pages and write them to the page log before
# demote. Stepping straight back up abandons those writes; the next checkpoint must not
# depend on them.
@disagg_test_class
class test_layered_frozen_core01(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    nstable = 2000
    nwindow = 20000
    value = 'v' * 200

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def key(self, i):
        return 'k%08d' % i

    def insert(self, uri, keys, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        for batch in range(0, len(keys), 500):
            self.session.begin_transaction()
            for k in keys[batch:batch + 500]:
                cursor[k] = self.value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def evict_all(self, uri):
        cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        self.session.begin_transaction()
        while cursor.next() == 0:
            pass
        self.session.rollback_transaction()
        cursor.close()

    def count(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        n = 0
        while cursor.next() == 0:
            self.assertEqual(cursor.get_value(), self.value)
            n += 1
        self.session.rollback_transaction()
        cursor.close()
        return n

    def get_stat(self, stat):
        cursor = self.session.open_cursor('statistics:')
        value = cursor[stat][2]
        cursor.close()
        return value

    def test_split_in_window_then_step_up(self):
        uri = 'layered:' + self.test_name
        stable_uri = 'file:' + self.test_name + '.wt_stable'
        self.session.create(uri, 'key_format=S,value_format=S')

        # Spread the stable rows so the window's inserts land between them on every page.
        stride = self.nwindow // self.nstable + 1
        self.insert(uri, [self.key(i * stride) for i in range(self.nstable)], 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # Stable stays at the final checkpoint: the window's commits are not written, but
        # the splits they cause move stable rows onto new pages that are.
        writes = self.get_stat(wiredtiger.stat.conn.rec_page_full_image_leaf)
        self.insert(uri, [self.key(i) for i in range(self.nwindow) if i % stride != 0], 20)
        self.evict_all(stable_uri)
        self.assertGreater(self.get_stat(wiredtiger.stat.conn.rec_page_full_image_leaf), writes)

        pinned = self.get_stat(wiredtiger.stat.conn.disagg_step_down_out_of_lineage_pinned)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertGreater(
            self.get_stat(wiredtiger.stat.conn.disagg_step_down_out_of_lineage_pinned), pinned)
        total = self.nstable + self.nwindow - (self.nwindow + stride - 1) // stride
        self.assertEqual(self.count(uri, 20), total)

        # No pickup: step straight back up and checkpoint the window's commits.
        rebased = self.get_stat(wiredtiger.stat.conn.disagg_step_up_out_of_lineage_rebased)
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertGreater(
            self.get_stat(wiredtiger.stat.conn.disagg_step_up_out_of_lineage_rebased), rebased)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()
        self.assertEqual(self.count(uri, 20), total)

        self.evict_all(stable_uri)
        self.assertEqual(self.count(uri, 20), total)
