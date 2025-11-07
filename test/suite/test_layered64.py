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

import random, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from helper import WiredTigerStat
from wiredtiger import stat
import time


# test_layered64.py
# Test that we write internal page deltas.

@disagg_test_class
class test_layered64(wttest.WiredTigerTestCase):

    delta = [
        ('write_none', dict(delta_config='page_delta=(internal_page_delta=false,leaf_page_delta=false)', delta_type='none')),
    ]

    conn_base_config = 'cache_size=5G,transaction_sync=(enabled,method=fsync),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'page_delta=(delta_pct=100),'
    disagg_storages = gen_disagg_storages('test_layered64', disagg_only = True)

    nrows = 1000
    uri='file:test_layered64'

    # Make scenarios for different cloud service providers
    scenarios = make_scenarios(disagg_storages, delta)

    def session_create_config(self):
        # The delta percentage of 100 is an arbitrary large value, intended to produce
        # deltas a lot of the time.
        cfg = 'key_format=S,value_format=S,allocation_size=512,leaf_page_max=512,internal_page_max=512,block_manager=disagg'
        return cfg

    def conn_config(self):
        return self.conn_base_config + f'disaggregated=(role="leader"),{self.delta_config},'

    def insert(self, kv, ts):
        cursor = self.session.open_cursor(self.uri, None, None)
        for k, v in kv.items():
            self.session.begin_transaction()
            cursor[k] = v
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(ts))
        cursor.close()

    def test_uncommit_eviction(self):
        """
        Scenario:
            Evict pages with uncommitted updates.
        """
        self.session.create(self.uri, self.session_create_config())
        
        cursor = self.session.open_cursor(self.uri, None, None)
        with WiredTigerStat(self.session, 'statistics:'+self.uri) as stat_cursor:
            cache_put_before = stat_cursor[stat.dsrc.cache_write][2]
        self.session.begin_transaction()

        # Populate the table with nrows.
        inital_value = "xyz" * 10
        kv = {str(i): inital_value for i in range(1, self.nrows + 1)}
        cursor = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        idx = 0
        for k, v in kv.items():
            if idx % 1000 == 0:
                print(f"Inserting record {idx}/{self.nrows}")
            idx += 1
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        
        # Populate the table with nrows.
        inital_value = "abc" * 10
        kv = {str(i): inital_value for i in range(1, self.nrows + 1)}
        cursor = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        idx = 0
        for k, v in kv.items():
            if idx % 1000 == 0:
                print(f"Inserting record {idx}/{self.nrows}")
            idx += 1
            cursor[k] = v
        print("Inserted initial data")
        # Evict the data.
        session = self.conn.open_session("debug=(release_evict_page)")
        evict_cursor = session.open_cursor(self.uri, None, None)
        evict_cursor.set_key('a')
        print("Evict set call")
        evict_cursor.search()
        print("Evict search call")
        evict_cursor.close()
        print("Evict cursor closed")
        # Monitor under un-committed status.
        with WiredTigerStat(self.session, 'statistics:'+self.uri) as stat_cursor:
            cache_put_after = stat_cursor[stat.dsrc.cache_write][2]
        print(f"Cache write before: {cache_put_before}, after: {cache_put_after}")