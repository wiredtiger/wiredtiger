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

# test_cross_checkpoint_caching.py

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_cross_checkpoint_caching(wttest.WiredTigerTestCase):

    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    uri = 'layered:test_cross_checkpoint_caching'
    nrows = 10

    disagg_storages = gen_disagg_storages('test_cross_checkpoint_caching', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def setUp(self):
        super().setUp()
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        table_cfg = 'key_format=S,value_format=S'
        self.session.create(self.uri, table_cfg)
        self.session_follow.create(self.uri, table_cfg)

    def tearDown(self):
        if hasattr(self, 'conn_follow'):
            self.session_follow.close()
            self.conn_follow.close()
        super().tearDown()

    def get_stat(self, stat_key, session):
        stat_cursor = session.open_cursor('statistics:')
        val = stat_cursor[stat_key][2]
        stat_cursor.close()
        return val

    def test_put_and_get(self):
        # Write data and checkpoint so the follower has something to read.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor[str(i).zfill(4)] = 'value_' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Follower scans at checkpoint N: pages come from disk and are put into the cache.
        self.disagg_advance_checkpoint(self.conn_follow)
        c = self.session_follow.open_cursor(self.uri)
        while c.next() == 0:
            pass
        c.close()
        self.assertGreater(
            self.get_stat(wiredtiger.stat.conn.cache_shared_dsk_miss, self.session_follow), 0)

        # Leader makes checkpoint N+1. Follower advances and scans again.
        # Any page that goes to disk must be a hit.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        misses_before = self.get_stat(wiredtiger.stat.conn.cache_shared_dsk_miss, self.session_follow)
        c = self.session_follow.open_cursor(self.uri)
        while c.next() == 0:
            pass
        c.close()
        self.assertEqual(
            self.get_stat(wiredtiger.stat.conn.cache_shared_dsk_miss, self.session_follow),
            misses_before)
