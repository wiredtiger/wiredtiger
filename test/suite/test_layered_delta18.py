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

import wttest
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Test that reconciliation writes a full page instead of a delta once the full page
# image falls below the configured full_page_min_size threshold.
@disagg_test_class
class test_layered_delta18(wttest.WiredTigerTestCase):
    test_name = __qualname__
    nitems = 100

    # A delta percentage of 100 is an arbitrary large value, intended to produce
    # deltas whenever full_page_min_size does not veto them.
    conn_base_config = 'statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'precise_checkpoint=true,' \
                     + 'page_delta=(delta_pct=100,full_page_min_size=1MB),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'
    uri = f'layered:{test_name}'

    disagg_storages = gen_disagg_storages(disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def test_full_page_min_size_forces_full_page(self):
        self.session.create(self.uri, self.create_session_config)
        cursor = self.session.open_cursor(self.uri, None, None)
        value1 = "a" * 100

        self.session.begin_transaction()
        for i in range(self.nitems):
            cursor[str(i)] = value1
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(5)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(5)}')
        self.session.checkpoint()

        self.session.begin_transaction()
        for i in range(self.nitems):
            if i % 10 == 0:
                cursor[str(i)] = "b" * 100
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        # The disk images in this test are far smaller than the 1MB threshold, so
        # every eligible page write is rejected for being below full_page_min_size
        # and no leaf deltas are produced.
        self.assertGreater(self.get_stat(stat.conn.rec_page_delta_rejected_min_page_size), 0)
        self.assertEqual(self.get_stat(stat.conn.rec_page_delta_leaf), 0)

    def test_full_page_min_size_capped_by_split_size(self):
        # A small leaf page size keeps the table's split size well below the
        # configured full_page_min_size, so the cap must apply for deltas to be
        # written at all.
        self.session.create(self.uri,
            self.create_session_config + ',allocation_size=4KB,leaf_page_max=4KB')
        cursor = self.session.open_cursor(self.uri, None, None)
        value1 = "a" * 100

        self.session.begin_transaction()
        for i in range(self.nitems):
            cursor[str(i)] = value1
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(5)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(5)}')
        self.session.checkpoint()

        self.session.begin_transaction()
        for i in range(self.nitems):
            if i % 10 == 0:
                cursor[str(i)] = "b" * 100
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        # Without the cap, full_page_min_size=1MB would reject every page on this
        # table. The cap limits the effective threshold to the table's split size,
        # so deltas are written as usual.
        self.assertGreater(self.get_stat(stat.conn.rec_page_delta_leaf), 0)
