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
from wtdataset import SimpleDataSet

# test_update_obsolete_skip01.py
#
# While the oldest update on a chain that isn't globally visible is only waiting for its timestamp
# (its transaction is globally visible, but the pinned timestamp is older), updates to the page skip
# the obsolete update check for a bounded number of updates. Check that reads at older timestamps
# are not affected, that chains are trimmed again once the timestamps move forward, and that the
# forced eviction of pages with more than a thousand updates to a single item still happens.
class test_update_obsolete_skip01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=200MB,statistics=(all)'
    uri = 'table:update_obsolete_skip01'
    nkeys = 50

    def setUp(self):
        super().setUp()
        # Disable table logging so timestamped updates are valid.
        ds = SimpleDataSet(self, self.uri, 0, key_format='i', value_format='S',
            config='log=(enabled=false)')
        ds.populate()

    def update(self, cursor, key, value, ts):
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))

    def pin_timestamps(self, ts):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(ts) +
            ',stable_timestamp=' + self.timestamp_str(ts))

    def value(self, key, i):
        return 'key{}-update{}'.format(key, i)

    def check_read_at(self, key, ts, expected):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        self.assertEqual(cursor[key], expected)
        self.session.rollback_transaction()
        cursor.close()

    def test_history_reads_and_trimming(self):
        self.pin_timestamps(10)
        cursor = self.session.open_cursor(self.uri)

        # Grow a long chain on key 1 while the pinned timestamp stays behind every update, with
        # updates to other keys on the same page in between.
        nupdates = 400
        for i in range(1, nupdates + 1):
            self.update(cursor, 1, self.value(1, i), 10 + i)
            if i % 10 == 0:
                key = 2 + i // 10 % (self.nkeys - 1)
                self.update(cursor, key, self.value(key, i), 10 + i)

        # Every version of key 1 is still readable at its timestamp.
        for i in list(range(1, 21)) + list(range(nupdates - 20, nupdates + 1)):
            self.check_read_at(1, 10 + i, self.value(1, i))
        for i in range(10, nupdates + 1, 10):
            key = 2 + i // 10 % (self.nkeys - 1)
            self.check_read_at(key, 10 + i, self.value(key, i))

        # Move the timestamps forward with each update: the chain must be trimmed again (within the
        # skip budget) and never reach the length that forces the page out of the cache.
        forced = self.get_stat(stat.conn.eviction_force_long_update_list)
        ts = 10 + nupdates
        for i in range(nupdates + 1, nupdates + 1201):
            ts += 1
            self.update(cursor, 1, self.value(1, i), ts)
            self.pin_timestamps(ts)
        self.assertEqual(self.get_stat(stat.conn.eviction_force_long_update_list), forced)
        self.assertEqual(cursor[1], self.value(1, nupdates + 1200))
        cursor.close()

    def test_long_chain_forces_eviction(self):
        self.pin_timestamps(10)
        cursor = self.session.open_cursor(self.uri)
        forced = self.get_stat(stat.conn.eviction_force_long_update_list)
        for i in range(1, 1201):
            self.update(cursor, 1, self.value(1, i), 10 + i)
        self.assertGreater(self.get_stat(stat.conn.eviction_force_long_update_list), forced)
        self.assertEqual(cursor[1], self.value(1, 1200))
        self.check_read_at(1, 11, self.value(1, 1))
        cursor.close()

    def test_prepared_update_on_page(self):
        self.pin_timestamps(10)
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 101):
            self.update(cursor, 1, self.value(1, i), 10 + i)

        # A prepared update to another key on the page is never skipped over as globally visible.
        prep_session = self.conn.open_session()
        prep_cursor = prep_session.open_cursor(self.uri)
        prep_session.begin_transaction()
        prep_cursor[2] = 'prepared'
        prep_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(200))

        for i in range(101, 301):
            self.update(cursor, 1, self.value(1, i), 200 + i)

        prep_session.commit_transaction('commit_timestamp=' + self.timestamp_str(250) +
            ',durable_timestamp=' + self.timestamp_str(250))
        prep_session.close()

        self.check_read_at(2, 250, 'prepared')
        self.check_read_at(1, 10 + 50, self.value(1, 50))
        self.check_read_at(1, 200 + 300, self.value(1, 300))

        # Advance the timestamps past everything and keep updating: the page is still usable.
        self.pin_timestamps(600)
        for i in range(301, 501):
            self.update(cursor, 1, self.value(1, i), 600 + i)
        self.assertEqual(cursor[1], self.value(1, 500))
        self.assertEqual(cursor[2], 'prepared')
        cursor.close()

if __name__ == '__main__':
    wttest.run()
