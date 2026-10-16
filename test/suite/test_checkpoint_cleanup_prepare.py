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

# Checkpoint cleanup must not reclaim a page whose records were deleted by a transaction
# that is only prepared: those deletions are not committed, so the page's committed
# records are still needed. Preparing a transaction removes its id from the global
# transaction table (so prepared data becomes visible), which makes the prepared stop
# point look globally visible to the cleanup's visibility test.

import time

import wiredtiger
import wttest
from wiredtiger import stat

class test_checkpoint_cleanup_prepare(wttest.WiredTigerTestCase):

    conn_config = 'cache_size=16MB,statistics=(all)'
    uri = 'table:checkpoint_cleanup_prepare'
    nrows = 3000
    value = 'x' * 8000
    commit_ts = 10
    prepare_ts = 100
    advance_ts = 200

    def checkpoint(self, config=None):
        """Checkpoint on a fresh session: a session with a prepared transaction cannot."""
        session = self.conn.open_session()
        session.checkpoint(config) if config else session.checkpoint()
        session.close()

    def removed(self):
        return self.get_stat(stat.conn.checkpoint_cleanup_pages_removed, conn=self.conn)

    def test_checkpoint_cleanup_prepare(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        # Load the records and checkpoint, so they are on disk.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            self.session.begin_transaction()
            cursor[i] = self.value
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.commit_ts))
        cursor.close()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.commit_ts) +
            ',stable_timestamp=' + self.timestamp_str(self.commit_ts))
        self.checkpoint()

        # Delete everything in a transaction and prepare it. None of this is committed.
        self.session.begin_transaction()
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(0)
        while cursor.next() == 0:
            cursor.remove()
        cursor.close()
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(self.prepare_ts))

        # Write the prepared tombstones to disk, then advance the timestamps past the prepare
        # point: both are needed for cleanup to reach the page's prepared stop.
        self.checkpoint()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.advance_ts) +
            ',stable_timestamp=' + self.timestamp_str(self.advance_ts))

        # Run checkpoint cleanup while the transaction is only prepared.
        before = self.removed()
        for _ in range(20):
            self.checkpoint('debug=(checkpoint_cleanup=true)')
            time.sleep(0.1)
            if self.removed() > before:
                break

        # Nothing was committed, so no page may be reclaimed. Without the fix the reclaimed
        # keys also make the rollback below abort.
        self.assertEqual(self.removed() - before, 0,
            'checkpoint cleanup reclaimed pages deleted by a prepared transaction')

        # Rolling back must restore the committed records.
        self.session.rollback_transaction()
        cursor = self.session.open_cursor(self.uri)
        count = sum(1 for _ in cursor)
        cursor.close()
        self.assertEqual(count, self.nrows, 'the committed records were lost')
