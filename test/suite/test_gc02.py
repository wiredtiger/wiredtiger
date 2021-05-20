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

from test_gc01 import test_gc_base
from wiredtiger import stat
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_gc02.py
# Test that checkpoint cleans the obsolete history store internal pages.
class test_gc02(test_gc_base):
    conn_config = 'cache_size=1GB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_gc(self):
        nrows = 100000

        # Create a table without logging.
        uri = "table:gc02"
        ds = SimpleDataSet(
            self, uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        bigvalue = "aaaaa" * 100
        bigvalue2 = "ddddd" * 100
        self.large_updates(uri, bigvalue, ds, nrows, 10)

        # Check that all updates are seen.
        self.check(bigvalue, uri, nrows, 10)

        self.large_updates(uri, bigvalue2, ds, nrows, 100)

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue2, uri, nrows, 100)

        # Check that old updates are seen.
        self.check(bigvalue, uri, nrows, 10)

        # Checkpoint to ensure that the history store is checkpointed and not cleaned.
        self.session.checkpoint()
        c = self.session.open_cursor('statistics:')
        self.assertEqual(c[stat.conn.cc_pages_evict][2], 0)
        self.assertEqual(c[stat.conn.cc_pages_removed][2], 0)
        self.assertGreater(c[stat.conn.cc_pages_visited][2], 0)
        c.close()

        # Pin oldest and stable to timestamp 100.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(100) +
            ',stable_timestamp=' + timestamp_str(100))

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue2, uri, nrows, 100)

        # Load a slight modification with a later timestamp.
        self.large_modifies(uri, 'A', ds, 10, 1, nrows, 110)
        self.large_modifies(uri, 'B', ds, 20, 1, nrows, 120)
        self.large_modifies(uri, 'C', ds, 30, 1, nrows, 130)

        # Set of update operations with increased timestamp.
        self.large_updates(uri, bigvalue, ds, nrows, 150)

        # Set of update operations with increased timestamp.
        self.large_updates(uri, bigvalue2, ds, nrows, 180)

        # Set of update operations with increased timestamp.
        self.large_updates(uri, bigvalue, ds, nrows, 200)

        # Check that the modifies are seen.
        bigvalue_modA = bigvalue2[0:10] + 'A' + bigvalue2[11:]
        bigvalue_modB = bigvalue_modA[0:20] + 'B' + bigvalue_modA[21:]
        bigvalue_modC = bigvalue_modB[0:30] + 'C' + bigvalue_modB[31:]
        self.check(bigvalue_modA, uri, nrows, 110)
        self.check(bigvalue_modB, uri, nrows, 120)
        self.check(bigvalue_modC, uri, nrows, 130)

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue, uri, nrows, 150)

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue2, uri, nrows, 180)

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue, uri, nrows, 200)

        # Pin oldest and stable to timestamp 200.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(200) +
            ',stable_timestamp=' + timestamp_str(200))

        # Checkpoint to ensure that the history store is cleaned.
        self.session.checkpoint()
        self.check_gc_stats()

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue, uri, nrows, 200)

if __name__ == '__main__':
    wttest.run()
