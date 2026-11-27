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
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

# A page log message carries a page together with all of its deltas, so the delta chain is not
# allowed to grow past the maximum disaggregated write size. A delta that would take it past the
# limit is replaced by a full page write.
@disagg_test_class
class test_layered_delta_chain_size01(wttest.WiredTigerTestCase):

    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_config = 'cache_size=1GB,statistics=(all),disaggregated=(role="leader"),' \
        'page_delta=(leaf_page_delta=true,internal_page_delta=false,delta_pct=100),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # The large value is added to the same page as the small rows, leaving about 30KB of the 32MB
    # limit for deltas, which is several of the deltas written by each round below.
    page_kinds = [
        ('small_page', dict(large_value_size=0, expect_rejected=False)),
        ('near_limit', dict(large_value_size=32 * 1024 * 1024 - 40 * 1024, expect_rejected=True)),
    ]
    scenarios = make_scenarios(disagg_storages, page_kinds)

    TABLE_CONFIG = (
        'key_format=S,value_format=S,'
        'allocation_size=4096,leaf_page_max=65536,memory_page_max=64MB,'
        'block_manager=disagg'
    )
    NKEYS = 100
    NMODIFY = 50
    VALUE_SIZE = 100
    ROUNDS = 8

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
        for i in range(self.NKEYS):
            cursor.set_key(self.make_key(i))
            cursor.search()
            cursor.reset()
        s.rollback_transaction()
        cursor.close()
        s.close()

    def test_delta_chain_size(self):
        self.session.create(self.uri, self.TABLE_CONFIG)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.NKEYS):
            self.session.begin_transaction()
            cursor[self.make_key(i)] = 'v' * self.VALUE_SIZE
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        if self.large_value_size != 0:
            self.session.begin_transaction()
            cursor['zlarge'] = 'x' * self.large_value_size
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(10)},stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()
        self.reopen_disagg_conn(self.conn_config + ',')

        # Each round changes some of the small rows, which writes a delta unless the chain would
        # become too large.
        for r in range(self.ROUNDS):
            ts = 20 + r
            cursor = self.session.open_cursor(self.uri)
            for i in range(self.NMODIFY):
                self.session.begin_transaction()
                cursor[self.make_key(i)] = chr(ord('a') + r) * self.VALUE_SIZE
                self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
            cursor.close()
            self.conn.set_timestamp(
                f'oldest_timestamp={self.timestamp_str(ts)},stable_timestamp={self.timestamp_str(ts)}')
            self.evict_page()
            self.session.checkpoint()

        deltas = self.get_stat(stat.dsrc.rec_page_delta_leaf)
        rejected = self.get_stat(stat.dsrc.rec_page_delta_rejected_chain_size)
        self.assertGreater(deltas, 0)
        if self.expect_rejected:
            self.assertGreater(rejected, 0)
        else:
            self.assertEqual(rejected, 0)

if __name__ == '__main__':
    wttest.run()
