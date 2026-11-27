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
from wtscenario import make_scenarios

# test_hs_scrub_evict01.py
#
# When the cache is below its eviction target, dirty history store pages are evicted with scrub:
# they are written and kept in the cache as clean pages. Run a workload that moves many old values
# into the history store while dirty eviction runs with plenty of room in the cache, and check that
# every older version stays readable at its timestamp, across checkpoints and a restart.
class test_hs_scrub_evict01(wttest.WiredTigerTestCase):
    uri = 'table:hs_scrub_evict01'
    nrows = 2000
    nversions = 5

    # A large cache with low dirty and update targets: dirty eviction runs well before the cache is
    # anywhere near its eviction target, which is when history store pages are scrubbed.
    eviction = [
        ('room', dict(cache='cache_size=500MB,eviction_dirty_target=1,eviction_dirty_trigger=40,'
            'eviction_updates_target=1,eviction_updates_trigger=40')),
        ('pressure', dict(cache='cache_size=20MB')),
    ]
    scenarios = make_scenarios(eviction)

    def conn_config(self):
        return self.cache + ',statistics=(all)'

    def value(self, key, version):
        return '{}-{}-'.format(key, version) + 'v' * 500

    def write_version(self, version):
        cursor = self.session.open_cursor(self.uri)
        for key in range(1, self.nrows + 1):
            self.session.begin_transaction()
            cursor[key] = self.value(key, version)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10 * version))
        cursor.close()

    def check_versions(self):
        cursor = self.session.open_cursor(self.uri)
        for version in range(1, self.nversions + 1):
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(10 * version))
            for key in range(1, self.nrows + 1, 7):
                self.assertEqual(cursor[key], self.value(key, version))
            self.session.rollback_transaction()
        cursor.close()

    def test_hs_scrub_evict(self):
        self.session.create(self.uri, 'key_format=i,value_format=S,log=(enabled=false)')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        for version in range(1, self.nversions + 1):
            self.write_version(version)
            # Checkpoints and eviction move the older versions into the history store.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10 * version))
            self.session.checkpoint()

        self.assertGreater(self.get_stat(stat.conn.cache_hs_insert), 0)
        self.check_versions()

        # Read the history store a few more times while eviction keeps running.
        for _ in range(3):
            self.check_versions()
            self.session.checkpoint()

        self.reopen_conn()
        self.check_versions()

if __name__ == '__main__':
    wttest.run()
