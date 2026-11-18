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

import time
from test_cc01 import test_cc_base
from wiredtiger import stat
import wttest


# Verify checkpoint cleanup processes timestamped live pages after their timestamps become old.
@wttest.skip_for_hook("disagg", "Checkpoint cleanup timestamp test requires standalone tables")
class test_cc11(test_cc_base):
    conn_config = 'statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0)'

    def test_timestamped_insert_cleanup(self):
        uri = 'table:cc11'
        nrows = 10000
        create_params = 'key_format=i,value_format=S,allocation_size=512,leaf_page_max=512,internal_page_max=512'

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, 'value', 10)
        self.session.checkpoint()

        # Cleanup only considers handles that are open after the restart.
        self.reopen_conn()
        self.conn.set_timestamp('oldest_timestamp=11')
        keep_open = self.session.open_cursor(uri)

        pages_before = self.get_stat(stat.conn.checkpoint_cleanup_pages_read_obsolete_tw)
        visited_before = self.get_stat(stat.conn.checkpoint_cleanup_pages_visited)
        self.wait_for_cc_to_run()
        pages_after = self.get_stat(stat.conn.checkpoint_cleanup_pages_read_obsolete_tw)
        visited_after = self.get_stat(stat.conn.checkpoint_cleanup_pages_visited)
        handles_processed = self.get_stat(stat.conn.checkpoint_cleanup_handle_processed)

        self.assertGreater(handles_processed, 0)
        self.assertGreater(visited_after, visited_before)
        self.assertGreater(pages_after, pages_before)
        keep_open.close()


if __name__ == '__main__':
    test_cc11.run()

class test_cc11_prepared(test_cc_base):
    conn_config = 'cache_size=16MB,statistics=(all)'
    uri = 'table:cc11'
    nrows = 1000
    value = 'x' * 4000
    commit_ts = 10
    prepare_ts = 100
    advance_ts = 200

    def test_cc11(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(self.uri, 0, self.nrows, self.value, ts=self.commit_ts)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.commit_ts) +
            ',stable_timestamp=' + self.timestamp_str(self.commit_ts))
        self.session.checkpoint()

        # Delete everything in a transaction and prepare it. None of this is committed. The
        # prepared transaction needs its own session: the checkpoints below cannot run on a
        # session that has one.
        prepare_session = self.conn.open_session()
        prepare_session.begin_transaction()
        cursor = prepare_session.open_cursor(self.uri)
        cursor.set_key(0)
        while cursor.next() == 0:
            cursor.remove()
        cursor.close()
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(self.prepare_ts))

        # Write the prepared tombstones to disk, then advance the timestamps past the prepare
        # point: both are needed for cleanup to reach the page's prepared stop.
        self.session.checkpoint()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.advance_ts) +
            ',stable_timestamp=' + self.timestamp_str(self.advance_ts))

        # Run checkpoint cleanup while the transaction is only prepared. Wait for a page to be
        # reclaimed rather than for a cleanup pass to finish: a pass can complete before the
        # pages it marked are evicted and reclaimed by a later pass.
        before = self.get_stat(stat.conn.checkpoint_cleanup_pages_removed)
        for _ in range(20):
            self.session.checkpoint('debug=(checkpoint_cleanup=true)')
            time.sleep(0.1)
            if self.get_stat(stat.conn.checkpoint_cleanup_pages_removed) > before:
                break

        # Nothing was committed, so no page may be reclaimed. Without the fix the reclaimed
        # keys also make the rollback below abort.
        self.assertEqual(
          self.get_stat(stat.conn.checkpoint_cleanup_pages_removed) - before, 0,
            'checkpoint cleanup reclaimed pages deleted by a prepared transaction')

        # Rolling back must restore the committed records.
        prepare_session.rollback_transaction()
        prepare_session.close()

        cursor = self.session.open_cursor(self.uri)
        count = sum(1 for _ in cursor)
        cursor.close()
        self.assertEqual(count, self.nrows, 'the committed records were lost')
