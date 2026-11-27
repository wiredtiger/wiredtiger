#!/usr/bin/env python
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
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

# A leaf page that is larger than the maximum page size before compression is always written in
# full, even when a small change to it would otherwise produce a small delta.
@disagg_test_class
class test_layered_delta_large_page01(wttest.WiredTigerTestCase):

    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_base_config = 'cache_size=32MB,statistics=(all),'
    conn_delta_config = 'disaggregated=(role="leader"),' \
        'page_delta=(leaf_page_delta=true,internal_page_delta=false,delta_pct=50),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # The small rows of the large page stay under half of the split size, so the large value is
    # added to the same page instead of starting a new one.
    page_sizes = [
        ('large', dict(nkeys=10, large_value_size=40000, expect_delta=False, compressor='none',
            leaf_page_max=4096)),
        ('normal', dict(nkeys=30, large_value_size=0, expect_delta=True, compressor='none',
            leaf_page_max=4096)),
        # Compressible rows that stay under the maximum page size before compression, and rows
        # that take the page past it, although they compress to well under it.
        ('compressed_small', dict(nkeys=200, large_value_size=0, expect_delta=True,
            compressor='snappy', leaf_page_max=32768)),
        ('compressed_large', dict(nkeys=600, large_value_size=0, expect_delta=False,
            compressor='snappy', leaf_page_max=32768)),
    ]
    scenarios = make_scenarios(disagg_storages, page_sizes)

    def table_config(self):
        return (
            'key_format=S,value_format=S,'
            f'allocation_size=4096,leaf_page_max={self.leaf_page_max},'
            f'block_compressor={self.compressor},'
            'block_manager=disagg'
        )

    def conn_extensions(self, extlist):
        if self.compressor != 'none':
            extlist.skip_if_missing = True
            extlist.extension('compressors', self.compressor)
        DisaggConfigMixin.conn_extensions(self, extlist)
    VALUE_SIZE = 100

    def conn_config(self):
        return self.conn_base_config + self.conn_delta_config

    def make_key(self, i):
        return f'key{i:04d}'

    def get_stat(self, statistic):
        stat_cursor = self.session.open_cursor('statistics:' + self.uri, None, None)
        val = stat_cursor[statistic][2]
        stat_cursor.close()
        return val

    def evict_page(self):
        s = self.conn.open_session()
        s.begin_transaction()
        cursor = s.open_cursor(self.uri, None, 'debug=(release_evict)')
        for i in range(self.nkeys + 1):
            cursor.set_key(self.make_key(i))
            cursor.search()
            cursor.reset()
        s.rollback_transaction()
        cursor.close()
        s.close()

    def test_no_delta_for_large_page(self):
        self.session.create(self.uri, self.table_config())
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.session.begin_transaction()
            cursor[self.make_key(i)] = 'v' * self.VALUE_SIZE
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        if self.large_value_size != 0:
            self.session.begin_transaction()
            cursor[self.make_key(self.nkeys)] = 'x' * self.large_value_size
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(10)},stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()
        self.reopen_disagg_conn(self.conn_config())

        # Change one small row on the page.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[self.make_key(0)] = 'w' * self.VALUE_SIZE
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        cursor.close()
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(20)},stable_timestamp={self.timestamp_str(20)}')
        self.evict_page()
        self.session.checkpoint()

        deltas = self.get_stat(stat.dsrc.rec_page_delta_leaf)
        rejected = self.get_stat(stat.dsrc.rec_page_delta_rejected_large_page)
        if self.expect_delta:
            self.assertGreater(deltas, 0)
            self.assertEqual(rejected, 0)
        else:
            self.assertEqual(deltas, 0)
            self.assertGreater(rejected, 0)

if __name__ == '__main__':
    wttest.run()
