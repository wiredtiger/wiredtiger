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
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_eviction07.py
# The dirty-index ring is on by default, but disaggregated btrees opt out of it
# (eviction_dirty_index_disagg, default false): the ring's re-queue churn fights
# disaggregated storage's checkpoint materialization lag. This test pins that
# gate. On a leader, a disaggregated table takes cursor writes and must not feed
# its ring while the option is off, and must feed it once the option is turned on.
@disagg_test_class
class test_eviction07(wttest.WiredTigerTestCase):

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'table:test_eviction07'
    nrows = 5000
    value_size = 1500

    def conn_config(self):
        return ('cache_size=200MB,statistics=(all),eviction_dirty_index=true,'
                'disaggregated=(role="leader"),')

    def dsrc_stat(self, stat_key):
        c = self.session.open_cursor('statistics:' + self.uri)
        val = c[stat_key][2]
        c.close()
        return val

    def _write_rows(self, start, count):
        value = 'x' * self.value_size
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, start + count):
            self.session.begin_transaction()
            cursor[i] = value
            self.session.commit_transaction()
        cursor.close()

    def test_disagg_ring_gated_off_by_default(self):
        self.session.create(
            self.uri, 'key_format=i,value_format=S,block_manager=disagg,leaf_page_max=4KB')

        # Default: the disaggregated table does not feed the ring.
        self._write_rows(0, self.nrows)
        self.assertEqual(
            self.dsrc_stat(stat.dsrc.cache_eviction_dirty_index_insert), 0,
            "disaggregated btree fed the ring while eviction_dirty_index_disagg was off")

        # Opt in: the same table now feeds the ring. The ring is allocated at
        # open, so flipping the runtime flag is enough -- no reopen needed.
        self.conn.reconfigure('eviction_dirty_index_disagg=true')
        self._write_rows(self.nrows, self.nrows)
        self.assertGreater(
            self.dsrc_stat(stat.dsrc.cache_eviction_dirty_index_insert), 0,
            "disaggregated btree did not feed the ring after opting in")

if __name__ == '__main__':
    wttest.run()
