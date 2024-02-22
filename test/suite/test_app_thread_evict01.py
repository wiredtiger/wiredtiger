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

import wiredtiger, wttest
from wtscenario import make_scenarios

# test_app_thread_evict01.py
# Test to trigger application threads to perform eviction.
class test_app_thread_evict01(wttest.WiredTigerTestCase):
    uri = "table:test_app_thread_evict001"

    # 100MB cache, 52MB trigger, 50MB target
    conn_config = "cache_size=100MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true)," \
        "eviction=(threads_max=1)," \
        "eviction_updates_trigger=52,eviction_dirty_trigger=52,eviction_trigger=52," \
        "eviction_updates_target=50,eviction_dirty_target=50,eviction_target=50,"

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def test_app_thread_evict01(self):
        format='key_format=i,value_format=S'
        self.session.create(self.uri, format)

        # For our target stat to be incremented we need our application thread to evict a page, but
        # this is probabilistic as the application thread is always racing against the internal
        # eviction threads. Give the application thread a few chances to beat the internal thread
        for _ in range(0, 20):
            # Insert 40MB of data and perform lots of small inserts so we'll have a lot of
            # pages to evict. We are below target levels so no eviction takes place
            cursor = self.session.open_cursor(self.uri)
            for i in range(40 * 1024):
                cursor[i+1] = 'a' * 1024

            # Perform two large updates. The first causes us to exceed trigger levels,
            # and on the second insert the app thread is pulled into eviction since
            # trigger levels are exceeded.
            self.session.begin_transaction()
            cursor[100001] = 'a' * 20 * 1024 * 1024
            cursor[100002] = 'a' * 20 * 1024 * 1024
            self.session.commit_transaction()

            num_app_evict_snapshot_refreshed = self.get_stat(wiredtiger.stat.conn.application_evict_snapshot_refreshed)
            cursor.close()

            if num_app_evict_snapshot_refreshed > 0:
                break

        self.assertGreater(self.get_stat(wiredtiger.stat.conn.application_evict_snapshot_refreshed), 0)

    def test_app_read_uncommitted_thread_evict01(self):
        format='key_format=i,value_format=S'
        self.session.create(self.uri, format)

        # For our target stat to be incremented we need our application thread to evict a page, but
        # this is probabilistic as the application thread is always racing against the internal
        # eviction threads. Give the application thread a few chances to beat the internal thread
        for _ in range(0, 20):
            # Insert 40MB of data and perform lots of small inserts so we'll have a lot of
            # pages to evict. We are below target levels so no eviction takes place
            cursor = self.session.open_cursor(self.uri)
            cursor2 = self.session.open_cursor(self.uri)
            for i in range(40 * 1024):
                cursor[i+1] = 'a' * 1024

            # Perform two large updates which will cause us to exceed clean trigger levels. We need
            # to make sure that the page doesn't get pulled into eviction. Place another cursor
            # under the same page, to disable eviction on the page.
            self.session.begin_transaction()
            cursor[100001] = 'a' * 20 * 1024 * 1024
            cursor2.set_key(100001)
            cursor2.search()
            cursor[100002] = 'a' * 20 * 1024 * 1024
            self.session.commit_transaction()
            cursor2.close()

            # Open up a read-uncommitted reader, and perform a next. The next() call should get
            # pulled into eviction.
            self.session.begin_transaction("isolation=read-uncommitted")
            read_cursor = self.session.open_cursor(self.uri)
            read_cursor.next()
            self.session.commit_transaction()
            read_cursor.close()

            num_app_evict_snapshot_read_uncommitted = self.get_stat(wiredtiger.stat.conn.application_evict_snapshot_read_uncommitted)
            if num_app_evict_snapshot_read_uncommitted > 0:
                break

        self.assertGreater(self.get_stat(wiredtiger.stat.conn.application_evict_snapshot_read_uncommitted), 0)

if __name__ == '__main__':
    wttest.run()
