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

# The delta size threshold can be measured against either the full page image just
# built or the configured maximum page size. A page that is well under its maximum
# gets a much smaller delta byte budget under the former. Modify a slice of such a
# page sized to clear the threshold only against the maximum page size, and check
# that the delta is written only when the switch selects that denominator.

@disagg_test_class
class test_layered_delta18(wttest.WiredTigerTestCase):

    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_base_config = 'cache_size=32MB,statistics=(all),'
    conn_delta_config = 'disaggregated=(role="leader"),' \
        'page_delta=(leaf_page_delta=true,internal_page_delta=false,delta_pct=20),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    denominator = [
        ('image_size', dict(use_target=False)),
        ('target_size', dict(use_target=True)),
    ]
    scenarios = make_scenarios(disagg_storages, denominator)

    # All rows fit on one leaf page that is roughly a quarter of the configured maximum,
    # so a delta covering a third of the rows is about 8% of the maximum page size but
    # about 33% of the image: over the threshold against the image, under it against
    # the target.
    TABLE_CONFIG = (
        'key_format=S,value_format=S,'
        'allocation_size=4096,leaf_page_max=131072,'
        'block_manager=disagg'
    )
    NKEYS = 60
    VALUE_SIZE = 500
    NMODIFY = 20

    def conn_config(self):
        return (self.conn_base_config + self.conn_delta_config +
            f'page_delta=(delta_pct_of_target_page_size={str(self.use_target).lower()}),')

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

    def test_delta_pct_denominator(self):
        self.session.create(self.uri, self.TABLE_CONFIG)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.NKEYS):
            self.session.begin_transaction()
            cursor[self.make_key(i)] = ('v' * self.VALUE_SIZE) + f'{i:04d}'
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(10)},stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()
        self.reopen_disagg_conn(self.conn_config())

        cursor = self.session.open_cursor(self.uri)
        for i in range(self.NMODIFY):
            self.session.begin_transaction()
            cursor[self.make_key(i)] = ('w' * self.VALUE_SIZE) + f'{i:04d}'
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        cursor.close()
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(20)},stable_timestamp={self.timestamp_str(20)}')
        self.evict_page()
        self.session.checkpoint()

        deltas = self.get_stat(stat.dsrc.rec_page_delta_leaf)
        rejected = self.get_stat(stat.dsrc.rec_page_delta_rejected_size_threshold)
        if self.use_target:
            self.assertGreater(deltas, 0)
            self.assertEqual(rejected, 0)
        else:
            self.assertEqual(deltas, 0)
            self.assertGreater(rejected, 0)

if __name__ == '__main__':
    wttest.run()
