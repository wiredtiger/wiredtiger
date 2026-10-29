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
#

import wiredtiger, wttest
from wiredtiger import stat
from wtbound import set_prefix_bound

# test_cursor_skip_session.py
#    Verify that per-session cursor skip/call statistics attribute WiredTiger
#    cursor traversal work to the session doing the work, and that the session
#    totals agree with the connection-level counter deltas in an isolated run.
class test_cursor_skip_session(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all)'

    def get_session_stat(self, local_session, which):
        stat_cursor = local_session.open_cursor('statistics:session')
        val = stat_cursor[which][2]
        stat_cursor.close()
        return val

    def get_conn_stat(self, which):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[which][2]
        stat_cursor.close()
        return val

    def populate_invisible_range(self, uri):
        # Returns the number of keys that are invisible to an older reader. The
        # characters array yields key_count = 26*26*26 keys 'aaa' .. 'zzz'.
        self.session.create(uri, 'key_format=u,value_format=u')
        cursor = self.session.open_cursor(uri)
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict=true)")

        letters = "abcdefghijklmnopqrstuvwxyz"
        key_count = 26 * 26 * 26

        # Start the older reader before any of the keys are inserted so that the
        # entire range is invisible (and therefore skippable) for that reader.
        self.reader = self.conn.open_session()
        self.reader.begin_transaction()

        self.session.begin_transaction()
        for i in range(0, 26):
            for j in range(0, 26):
                for k in range(0, 26):
                    key = letters[i] + letters[j] + letters[k]
                    cursor[key] = key
        self.session.commit_transaction()

        # Evict the whole range so the reader has to walk it from disk.
        for i in range(0, 26):
            for j in range(0, 26):
                evict_cursor.set_key(letters[i] + letters[j] + 'a')
                evict_cursor.search()
                evict_cursor.reset()

        cursor.close()
        evict_cursor.close()
        return key_count

    def test_forward_skip_attribution(self):
        uri = 'table:test_cursor_skip_session_fwd'
        key_count = self.populate_invisible_range(uri)

        conn_before = self.get_conn_stat(stat.conn.cursor_next_skip_total)
        reader_before = self.get_session_stat(self.reader, stat.session.cursor_next_skip_total)

        # Traverse forward from 'aa'. Every inserted key is invisible to the
        # reader's snapshot so the forward walk skips key_count - 1 entries.
        reader_cursor = self.reader.open_cursor(uri)
        reader_cursor.set_key('aa')
        reader_cursor.search_near()

        conn_after = self.get_conn_stat(stat.conn.cursor_next_skip_total)
        reader_after = self.get_session_stat(self.reader, stat.session.cursor_next_skip_total)

        conn_delta = conn_after - conn_before
        reader_delta = reader_after - reader_before

        # The session total must match the connection delta exactly in this
        # isolated run, and must account for the whole invisible range.
        self.assertEqual(conn_delta, key_count - 1)
        self.assertEqual(reader_delta, conn_delta)

        reader_cursor.close()

    def test_backward_skip_attribution(self):
        uri = 'table:test_cursor_skip_session_bwd'
        key_count = self.populate_invisible_range(uri)

        conn_before = self.get_conn_stat(stat.conn.cursor_prev_skip_total)
        reader_before = self.get_session_stat(self.reader, stat.session.cursor_prev_skip_total)

        # Traverse backward from 'zzz'. Walking the whole invisible range
        # backward skips key_count entries.
        reader_cursor = self.reader.open_cursor(uri)
        reader_cursor.set_key('zzz')
        reader_cursor.search_near()
        while reader_cursor.prev() == 0:
            pass

        conn_after = self.get_conn_stat(stat.conn.cursor_prev_skip_total)
        reader_after = self.get_session_stat(self.reader, stat.session.cursor_prev_skip_total)

        conn_delta = conn_after - conn_before
        reader_delta = reader_after - reader_before

        self.assertGreater(reader_delta, 0)
        self.assertEqual(reader_delta, conn_delta)

        reader_cursor.close()

    def test_session_isolation_and_reset(self):
        uri = 'table:test_cursor_skip_session_iso'
        self.populate_invisible_range(uri)

        # Baseline for the committing session (self.session), which must not be
        # affected by work performed on the reader session.
        main_next_before = self.get_session_stat(self.session, stat.session.cursor_next_skip_total)

        reader_cursor = self.reader.open_cursor(uri)
        reader_cursor.set_key('aa')
        reader_cursor.search_near()

        # The reader accumulated skips.
        self.assertGreater(
            self.get_session_stat(self.reader, stat.session.cursor_next_skip_total), 0)

        # The main session's skip counter is unchanged by the reader's work.
        main_next_after = self.get_session_stat(self.session, stat.session.cursor_next_skip_total)
        self.assertEqual(main_next_after, main_next_before)

        reader_cursor.close()

        # Resetting a session clears its per-session statistics, mirroring the
        # reset WiredTiger performs before returning a session to the pool. The
        # reader's transaction must be resolved before the session can reset.
        self.reader.rollback_transaction()
        self.reader.reset()
        self.assertEqual(
            self.get_session_stat(self.reader, stat.session.cursor_next_skip_total), 0)
        self.assertEqual(
            self.get_session_stat(self.reader, stat.session.cursor_next), 0)
        self.assertEqual(
            self.get_session_stat(self.reader, stat.session.cursor_prev_skip_total), 0)
        self.assertEqual(
            self.get_session_stat(self.reader, stat.session.cursor_prev), 0)

if __name__ == '__main__':
    wttest.run()
