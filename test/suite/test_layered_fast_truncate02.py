#!/usr/bin/env python3
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

# test_layered_fast_truncate02.py
#
# Validates visibility and ingest-interleave behaviour when a follower picks
# up a checkpoint containing fast-truncated pages.
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

@disagg_test_class
class test_layered_fast_truncate02(wttest.WiredTigerTestCase):

    uri         = 'layered:test_layered_fast_truncate02'
    nrows       = 5000
    value       = 'a' * 500
    trunc_start = 1001
    trunc_stop  = 4000
    ts_insert   = 10
    ts_truncate = 20
    ts_read     = 25   # after truncation; deletion visible
    ts_write    = 30   # for follower ingest writes

    conn_config = 'cache_size=50MB,statistics=(all),disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate02', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def leader_checkpoint(self, ts):
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(ts) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

    def setup_leader(self):
        # Insert all rows, checkpoint, evict so pages are disk-resident, then
        # fast-truncate the middle range and checkpoint again.
        # Eviction before truncation is required: __wt_delete_page only produces
        # page-level fast delete markers (WT_CELL_ADDR_DEL) when the page is not
        # in cache.  Without it, truncation falls back to individual tombstones.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cur = self.session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            self.session.begin_transaction()
            cur[i] = self.value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_insert))
        cur.close()
        self.leader_checkpoint(self.ts_insert)

        # Evict stable btree pages so they are disk-resident before truncation.
        # Must search() to position the cursor before reset() triggers eviction;
        # set_key alone does not set WT_CURSTD_KEY_SET on the stable constituent.
        evict_cur = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        self.session.begin_transaction('ignore_prepare=true')
        for i in range(1, self.nrows + 1):
            evict_cur.set_key(i)
            evict_cur.search()
            evict_cur.reset()
        evict_cur.close()
        self.session.rollback_transaction()

        self.session.begin_transaction()
        c_start = self.session.open_cursor(self.uri)
        c_start.set_key(self.trunc_start)
        c_stop = self.session.open_cursor(self.uri)
        c_stop.set_key(self.trunc_stop)
        self.session.truncate(None, c_start, c_stop, None)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_truncate))
        c_start.close()
        c_stop.close()
        self.leader_checkpoint(self.ts_truncate)

    def open_follower(self):
        conn = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,cache_size=50MB,statistics=(all),disaggregated=(role="follower")')
        sess = conn.open_session('')
        sess.create(self.uri, 'key_format=i,value_format=S')
        self.disagg_advance_checkpoint(conn, self.conn)
        return conn, sess

    def search_at(self, sess, key, ts=None):
        """Read key at ts (or no timestamp if ts=None); return (wt_ret, value_or_None)."""
        cur = sess.open_cursor(self.uri)
        txn_cfg = ('read_timestamp=' + self.timestamp_str(ts)) if ts is not None else ''
        sess.begin_transaction(txn_cfg)
        cur.set_key(key)
        ret = cur.search()
        val = cur.get_value() if ret == 0 else None
        sess.rollback_transaction()
        cur.close()
        return ret, val

    # ------------------------------------------------------------------
    # Scenario 1 -- visibility
    # ------------------------------------------------------------------

    def test_visibility(self):
        """Truncated keys return WT_NOTFOUND; adjacent and boundary keys return the original value."""
        self.setup_leader()
        conn, sess = self.open_follower()

        mid = (self.trunc_start + self.trunc_stop) // 2
        for key in [self.trunc_start, mid, self.trunc_stop]:
            ret, _ = self.search_at(sess, key, self.ts_read)
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f'key {key} must be deleted')

        for key in [1, self.trunc_start - 1, self.trunc_stop + 1, self.nrows]:
            ret, val = self.search_at(sess, key, self.ts_read)
            self.assertEqual(ret, 0, f'key {key} must be found')
            self.assertEqual(val, self.value)

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 2 -- pre-truncation read sees all rows
    # ------------------------------------------------------------------

    def test_pre_truncation_read_sees_all_rows(self):
        """
        Reading at ts_insert (before truncation) returns all rows including those
        later deleted.  Spot-checks boundaries and interior, then verifies the
        total row count matches nrows.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        mid = (self.trunc_start + self.trunc_stop) // 2
        for key in [self.trunc_start, mid, self.trunc_stop]:
            ret, val = self.search_at(sess, key, self.ts_insert)
            self.assertEqual(ret, 0, f'key {key} must be visible before truncation')
            self.assertEqual(val, self.value)

        # Full scan at ts_insert must see every row that was inserted.
        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_insert))
        count = 0
        while cur.next() == 0:
            count += 1
        sess.rollback_transaction()
        cur.close()
        self.assertEqual(count, self.nrows, 'all rows must be visible before truncation timestamp')

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 3 -- cursor scanning
    # ------------------------------------------------------------------

    def test_cursor_scanning(self):
        """
        Forward scan visits exactly the non-truncated rows and jumps directly
        from trunc_start-1 to trunc_stop+1.  search_near on a deleted key
        lands outside the range with the original value.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        expected   = self.nrows - (self.trunc_stop - self.trunc_start + 1)
        trunc_range = range(self.trunc_start, self.trunc_stop + 1)
        mid        = (self.trunc_start + self.trunc_stop) // 2

        # Forward scan.
        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_read))
        count, prev_key, first_after_gap = 0, 0, None
        ret = cur.next()
        while ret == 0:
            key = cur.get_key()
            self.assertNotIn(key, trunc_range)
            if prev_key == self.trunc_start - 1 and first_after_gap is None:
                first_after_gap = key
            prev_key = key
            count += 1
            ret = cur.next()
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(count, expected)
        self.assertEqual(first_after_gap, self.trunc_stop + 1)
        sess.rollback_transaction()
        cur.close()

        # search_near on a deleted key must land outside the truncated range.
        # search_near returns -1 (positioned before) or 1 (positioned after);
        # 0 would mean an exact match on a deleted key, which is wrong.
        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_read))
        cur.set_key(mid)
        exact = cur.search_near()
        self.assertIn(exact, (-1, 1), 'search_near must not find an exact match in the truncated range')
        self.assertNotIn(cur.get_key(), trunc_range)
        self.assertEqual(cur.get_value(), self.value)
        sess.rollback_transaction()
        cur.close()

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 4 -- ingest write on a truncated key
    # ------------------------------------------------------------------

    def test_ingest_write_on_truncated_key(self):
        """
        Follower writes to fast-truncated keys; writes route to ingest, not stable.
        At ts_write the ingest value is visible.  At ts_read the stable deletion
        still wins.  Unwritten truncated keys stay WT_NOTFOUND at both timestamps.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        write_keys = [self.trunc_start, self.trunc_start + 500,
                      (self.trunc_start + self.trunc_stop) // 2]
        skip_key   = self.trunc_start + 250

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction()
        for key in write_keys:
            cur.set_key(key)
            cur.set_value(f'ingest_{key}')
            self.assertEqual(cur.insert(), 0)
        sess.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_write))
        cur.close()

        for key in write_keys:
            ret, val = self.search_at(sess, key, self.ts_write)
            self.assertEqual(ret, 0)
            self.assertEqual(val, f'ingest_{key}')

        ret, _ = self.search_at(sess, skip_key, self.ts_write)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)

        for key in write_keys + [skip_key]:
            ret, _ = self.search_at(sess, key, self.ts_read)
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND)

        sess.close()
        conn.close()


if __name__ == '__main__':
    wttest.run()
