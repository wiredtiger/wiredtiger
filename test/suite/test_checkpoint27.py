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
from wtthread import checkpoint_thread
import threading
import time
import wttest

# test_checkpoint27.py
# Test that checkpoint cursors can't evict large metadata pages.
# We don't allow eviction if we're in a checkpoint cursor transaction, but checkpoint cursors and
# metadata pages are both a little bit special, so test them together.
class test_checkpoint27(wttest.WiredTigerTestCase):
    def conn_config(self):
        return 'statistics=(all),timing_stress_for_test=[checkpoint_slow]'
    table_config = ',memory_page_max=8k,leaf_page_max=4k'

    def large_updates(self, uri, ds, nrows, value):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[ds.key(i)] = value
            if i % 101 == 0:
                self.session.commit_transaction()
                self.session.begin_transaction()
        self.session.commit_transaction()
        cursor.close()

    # def get_stat(self, session, stat):
    #     stat_cursor = session.open_cursor('statistics:')
    #     val = stat_cursor[stat][2]
    #     stat_cursor.close()
    #     return val

    # "expected" is a list of maps from values to counts of values.
    def check(self, ds, ckpt, expected):
        if ckpt is None:
            ckpt = 'WiredTigerCheckpoint'
        # cursor2 = self.session.open_cursor('metadata:', None, 'debug=(release_evict),checkpoint='+ckpt)
        cursor = self.session.open_cursor(ds.uri, None, 'checkpoint=' + ckpt)
        # cursor2.next()
        # cursor2.reset()
        seen = {}
        for k, v in cursor:
            if v in seen:
                seen[v] += 1
            else:
                seen[v] = 1
        self.assertTrue(seen in expected)
        cursor.close()

    def test_checkpoint_evict_page(self):
        self.key_format='r'
        self.value_format='S'

        uri = 'table:checkpoint27'
        nrows = 10000
        morerows = 10000

        # Create a table.
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format=self.value_format, config=self.table_config)
        ds.populate()

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100

        # Write some data.
        self.large_updates(uri, ds, nrows, value_a)
        # Write this data out now so we aren't waiting for it while trying to
        # race with the later data.
        self.session.checkpoint()

        # Write some more data, and hold the transaction open.
        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(uri)
        session2.begin_transaction()
        for i in range(nrows + 1, nrows + morerows + 1):
            cursor2[ds.key(i)] = value_b

        # Checkpoint in the background.
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        try:
            ckpt.start()

            # Wait for checkpoint to start before committing.
            ckpt_started = 0
            while not ckpt_started:
                stat_cursor = self.session.open_cursor('statistics:', None, None)
                ckpt_started = stat_cursor[stat.conn.txn_checkpoint_running][2]
                stat_cursor.close()
                time.sleep(1)

            session2.commit_transaction()
        finally:
            done.set()
            ckpt.join()

        # There are two states we should be able to produce: one with the original
        # data and one with the additional data.
        #
        # It is ok to see either in the checkpoint (since the checkpoint could
        # reasonably include or not include the second txn) but not ok to see
        # an intermediate state.
        expected_a = { value_a: nrows }
        expected_b = { value_a: nrows, value_b: morerows }
        expected = [expected_a, expected_b]

        app_metadata = value_a * 100

        # Create a lot of tables to generate a large metadata page.
        for i in range(0, 2000):
            temp_uri = 'table:test_checkpoint27_' + str(i)
            self.session.create(temp_uri, 'key_format={},value_format={},{},app_metadata={}'.format(self.key_format, self.value_format, self.table_config, app_metadata))
            self.large_updates(uri, ds, 1, value_a)
            if i % 100 == 0:
                self.session.create(uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
                self.check(ds, None, expected)

        # md_uri = 'metadata:'
        # cursor = self.session.open_cursor(md_uri, None, 'debug=(release_evict)')
        # for i in range(0, 2000):
        #     cursor.get(i)
        #     cursor.reset()
        # Dirty the metadata page on the last table.
        # TODO this doesn't work.
        # self.session.checkpoint()

        # Now read the checkpoint.
        self.session.create(uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        self.check(ds, None, expected)
