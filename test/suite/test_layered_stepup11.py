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

import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_stepup11.py
# The step-up ingest-table clear should write non-transactional, globally visible tombstones,
# so eviction reconciling the cleared ingest table under a concurrent pinned reader should succeed.
@disagg_test_class
class test_layered_stepup11(wttest.WiredTigerTestCase):
    conn_base_config = ',create,cache_size=10GB,statistics=(all),' \
        'statistics_log=(wait=1,json=true,on_close=true),' \
        'disaggregated=(lose_all_my_data=true),precise_checkpoint=true,'

    disagg_storages = gen_disagg_storages('test_layered_stepup11', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_stepup11'
    ingest_uri = 'file:test_layered_stepup11.wt_ingest'
    create_config = 'allocation_size=512,leaf_page_max=512,key_format=S,value_format=S'
    nitems = 1000

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def force_evict_ingest(self, session):
        evict_cursor = session.open_cursor(self.ingest_uri, None, "debug=(release_evict)")
        for i in range(self.nitems):
            evict_cursor.set_key(str(i))
            ret = evict_cursor.search()
            self.assertTrue(ret == 0 or ret == wiredtiger.WT_NOTFOUND)
            evict_cursor.reset()
        evict_cursor.close()

    def test_ingest_clear_evict_with_pinned_reader(self):
        ts = 10

        # Write as a follower so the keys live only in the ingest table.
        self.session.create(self.uri, self.create_config)
        self.conn.reconfigure('disaggregated=(role="follower")')

        write_session = self.conn.open_session('')
        cursor = write_session.open_cursor(self.uri)
        write_session.begin_transaction()
        for i in range(self.nitems):
            cursor[str(i)] = 'value' + str(i)
        write_session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        cursor.close()
        write_session.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')

        # Hold the oldest id across step-up.
        pin_session = self.conn.open_session('')
        pin_session.begin_transaction()
        pin_cursor = pin_session.open_cursor(self.uri)
        pin_cursor.set_key(str(0))
        pin_cursor.search()

        # Step up: drains then clears the ingest table.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Evict while a reader still pins the oldest id: that is the window in which a
        # transactional truncate's tombstones would not yet be globally visible, and attempting to
        # evict would result in a crash. Using non-transactional truncate prevents this.
        evict_session = self.conn.open_session('')
        self.force_evict_ingest(evict_session)
        evict_session.close()

        pin_cursor.close()
        pin_session.rollback_transaction()
        pin_session.close()

        check_cursor = self.session.open_cursor(self.uri)
        count = 0
        while check_cursor.next() == 0:
            count += 1
        self.assertEqual(count, self.nitems)
        check_cursor.close()
