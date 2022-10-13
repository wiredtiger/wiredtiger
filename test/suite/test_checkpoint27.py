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

from wiredtiger import stat, wiredtiger_strerror, WiredTigerError, WT_ROLLBACK
from wtdataset import SimpleDataSet
import wttest

# test_checkpoint27.py
# Test that checkpoint cursors can't evict large metadata pages.
# We don't allow eviction if we're in a checkpoint cursor transaction, but checkpoint cursors and
# metadata pages are both a little bit special, so test them together.
class test_checkpoint27(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=10MB,statistics=(all)'

    def large_updates(self, uri, value, ds, nrows, commit_ts):
        # Update a large number of records.
        session = self.session
        try:
            cursor = session.open_cursor(uri)
            for i in range(1, nrows + 1):
                session.begin_transaction()
                cursor[ds.key(i)] = value
                session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
            cursor.close()
        except WiredTigerError as e:
            rollback_str = wiredtiger_strerror(WT_ROLLBACK)
            if rollback_str in str(e):
                session.rollback_transaction()
            raise(e)

    def get_stat(self, session, stat):
        stat_cursor = session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def test_checkpoint_evict_page(self):
        self.key_format='r'
        self.value_format='8t'

        # Create a table that has a checkpoint with a lot of stable data in the history store.
        stable_uri = "table:test_checkpoint27"
        nrows = 50000
        ds = SimpleDataSet(self, stable_uri, 0, key_format=self.key_format,
                           value_format=self.value_format, config=',allocation_size=512,leaf_page_max=512')
        ds.populate()

        value_a = 97
        value_b = 98
        value_c = 99
        value_d = 100

        # Perform several updates.
        self.large_updates(stable_uri, value_a, ds, nrows, 10)
        self.large_updates(stable_uri, value_b, ds, nrows, 20)
        self.large_updates(stable_uri, value_c, ds, nrows, 30)
        self.large_updates(stable_uri, value_d, ds, nrows, 40)

        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50) +
            ',stable_timestamp=' + self.timestamp_str(50))

        # Assert that a decent chunk of stuff made it into the history store.
        self.session.checkpoint()
        hs_writes = self.get_stat(self.session, stat.conn.cache_hs_insert)
        self.assertTrue(hs_writes > nrows / 2)

        # Create a lot of tables to generate a large metadata page
        for i in range(0, 2000):
            temp_uri = 'table:test_checkpoint27_' + str(i)
            self.session.create(temp_uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
            self.large_updates(temp_uri, value_a, ds, 1, 60)

        self.session.checkpoint()

        # Open a checkpoint cursor and walk the first table
        self.session.create(stable_uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        cursor = self.session.open_cursor(stable_uri, None, 'checkpoint=WiredTigerCheckpoint')
        count = 0
        for k, v in cursor:
            count += 1
        self.assertEqual(count, nrows)
