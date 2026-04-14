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

# test_layered_fast_truncate03.py
#
# Tests that a follower correctly handles pages that were fast-truncated on the
# leader. The follower must not dirty stable pages it reads, and deleted state
# must survive eviction and reconnection.
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

@disagg_test_class
class test_layered_fast_truncate03(wttest.WiredTigerTestCase):

    uri         = 'layered:test_layered_fast_truncate03'
    nrows       = 5000
    value       = 'a' * 500
    trunc_start = 1001
    trunc_stop  = 4000
    ts_insert   = 10
    ts_truncate = 20
    ts_read     = 25   # after truncation; deleted keys return WT_NOTFOUND
    ts_write    = 30   # timestamp for follower ingest writes

    conn_config = 'cache_size=50MB,statistics=(all),disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate03', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def get_stat(self, conn, stat_key):
        s = conn.open_session('')
        val = s.open_cursor('statistics:')[stat_key][2]
        s.close()
        return val

    def leader_checkpoint(self, ts):
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(ts) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

    def _do_setup_leader(self, extra_table_cfg=''):
        # Insert all rows and checkpoint, then evict all pages to disk before truncating.
        # Pages must be on disk first so truncation uses page-level fast delete markers
        # rather than falling back to individual tombstones per key.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S' + extra_table_cfg)
        cur = self.session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            self.session.begin_transaction()
            cur[i] = self.value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_insert))
        cur.close()
        self.leader_checkpoint(self.ts_insert)

        # Evict all pages to disk before truncating.
        # search() must be called to position the cursor; set_key alone is not enough.
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

    def setup_leader(self):
        self._do_setup_leader()

    def setup_leader_with_small_pages(self):
        # Small pages (~8 rows each) so the truncated range spans many more pages.
        self._do_setup_leader(',leaf_page_max=4096')

    def open_follower(self):
        conn = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,cache_size=50MB,statistics=(all),disaggregated=(role="follower")')
        sess = conn.open_session('')
        sess.create(self.uri, 'key_format=i,value_format=S')
        self.disagg_advance_checkpoint(conn, self.conn)
        return conn, sess

    def advance_follower(self, conn):
        """Take a new leader checkpoint and have the follower pick it up,
        which evicts its cached stable pages the same way production does."""
        self.leader_checkpoint(self.ts_truncate)
        self.disagg_advance_checkpoint(conn, self.conn)

    def evict_truncated_pages(self, sess):
        """Evict every page in the truncated range from the follower cache.

        Reads at ts_insert (before the truncation is visible) to force each page
        to load, then reset() with release_evict drops it from cache."""
        evict_cur = sess.open_cursor(self.uri, None, 'debug=(release_evict)')
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_insert))
        for i in range(self.trunc_start, self.trunc_stop + 1):
            evict_cur.set_key(i)
            evict_cur.search()
            evict_cur.reset()
        evict_cur.close()
        sess.rollback_transaction()

    def evict_page(self, sess, key):
        """Evict the single page containing key from the follower cache.

        Reads at ts_insert to force the page to load, then evicts it.
        If the page were dirty it would need to be written on eviction."""
        evict_cur = sess.open_cursor(self.uri, None, 'debug=(release_evict)')
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_insert))
        evict_cur.set_key(key)
        evict_cur.search()
        evict_cur.reset()
        evict_cur.close()
        sess.rollback_transaction()

    def search_at(self, sess, key, ts=None):
        """Look up key at the given timestamp. Returns (ret, value) or (WT_NOTFOUND, None)."""
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
    # Scenario 1 -- reading a deleted page does not dirty it
    # ------------------------------------------------------------------

    def test_instantiation_does_not_dirty_page(self):
        """
        Reading a fast-truncated key should not mark the page as dirty.
        After eviction, re-reading from disk must still return WT_NOTFOUND.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        dirty_before = self.get_stat(conn, stat.conn.cache_pages_dirty)

        ret, _ = self.search_at(sess, self.trunc_start + 100, self.ts_read)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'loading a fast-truncated page must not dirty it')
        # Evict the loaded page — a dirty page would need to be written here.
        self.evict_page(sess, self.trunc_start + 100)

        # Evict all truncated pages, then re-read to confirm deleted state survived.
        self.evict_truncated_pages(sess)

        ret, _ = self.search_at(sess, self.trunc_start + 100, self.ts_read)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND, 'key must still be deleted after eviction')
        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'page must remain clean after re-load')
        # Evict the re-loaded page — it must also evict without a write.
        self.evict_page(sess, self.trunc_start + 100)

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 2 -- reading many deleted pages keeps them all clean
    # ------------------------------------------------------------------

    def test_bulk_instantiation_stays_clean(self):
        """
        Reading many keys across the truncated range must not dirty any page.
        After evicting all of them, re-reading from disk must still return
        WT_NOTFOUND with no dirty pages.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        sample = range(self.trunc_start, self.trunc_stop + 1, 10)
        dirty_before = self.get_stat(conn, stat.conn.cache_pages_dirty)

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_read))
        for key in sample:
            cur.set_key(key)
            self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
        sess.rollback_transaction()
        cur.close()

        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'no fast-truncated page must be dirtied on load')
        # Evict the loaded pages — dirty pages would need to be written here.
        self.evict_truncated_pages(sess)

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_read))
        for key in sample:
            cur.set_key(key)
            self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
        sess.rollback_transaction()
        cur.close()

        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'pages must stay clean across the full load-evict-reload cycle')
        # Evict the re-loaded pages — they must also evict without writes.
        self.evict_truncated_pages(sess)

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 3 -- ingest write on a truncated key; stable page stays clean
    # ------------------------------------------------------------------

    def test_ingest_write_then_evict_stable(self):
        """
        The follower writes to a truncated key; the write goes to ingest, not stable.
        The stable page must stay clean. After evicting stable pages, the ingest
        value is still visible, and the stable deletion is still visible at ts_read.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        target       = self.trunc_start + 100
        dirty_before = self.get_stat(conn, stat.conn.cache_pages_dirty)

        ret, _ = self.search_at(sess, target, self.ts_read)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'loading the fast-truncated stable page must not dirty it')
        # Evict the loaded page — it should evict without a write.
        self.evict_page(sess, target)

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction()
        cur.set_key(target)
        cur.set_value('ingest_value')
        self.assertEqual(cur.insert(), 0, 'write on follower must succeed (routed to ingest)')
        sess.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_write))
        cur.close()

        ret, val = self.search_at(sess, target, self.ts_write)
        self.assertEqual(ret, 0)
        self.assertEqual(val, 'ingest_value')

        # Advance the follower; the ingest write lives in the ingest layer and must survive.
        self.advance_follower(conn)

        ret, val = self.search_at(sess, target, self.ts_write)
        self.assertEqual(ret, 0, 'ingest value must survive eviction of the stable page')
        self.assertEqual(val, 'ingest_value')

        ret, _ = self.search_at(sess, target, self.ts_read)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
            'stable deletion must still be visible at ts_read')

        # A nearby truncated key that was never written must also stay deleted at ts_write.
        skip_key = target + 100
        ret, _ = self.search_at(sess, skip_key, self.ts_write)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
            'an unwritten truncated key must stay deleted even after ingest writes nearby')

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 4 -- many small pages stay clean; ingest writes merge correctly
    # ------------------------------------------------------------------

    def test_page_split_with_ingest_writes(self):
        """
        The truncated range spans many small pages. None must be dirtied on read.
        After eviction, ingest writes to some keys produce the correct merged view:
        ingest values visible for written keys, WT_NOTFOUND for the rest.
        """
        self.setup_leader_with_small_pages()
        conn, sess = self.open_follower()

        sample       = list(range(self.trunc_start, self.trunc_stop + 1, 10))
        dirty_before = self.get_stat(conn, stat.conn.cache_pages_dirty)

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_read))
        for key in sample:
            cur.set_key(key)
            self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
        sess.rollback_transaction()
        cur.close()

        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'no split page must be dirtied')
        # Evict the loaded pages — dirty pages would need to be written here.
        self.evict_truncated_pages(sess)

        self.advance_follower(conn)

        ingest_keys = set(sample[::3])
        cur = sess.open_cursor(self.uri)
        sess.begin_transaction()
        for key in ingest_keys:
            cur.set_key(key)
            cur.set_value(f'ingest_{key}')
            self.assertEqual(cur.insert(), 0)
        sess.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts_write))
        cur.close()

        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(self.ts_write))
        for key in ingest_keys:
            cur.set_key(key)
            self.assertEqual(cur.search(), 0)
            self.assertEqual(cur.get_value(), f'ingest_{key}')
        for key in set(sample) - ingest_keys:
            cur.set_key(key)
            self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
        sess.rollback_transaction()
        cur.close()

        sess.close()
        conn.close()

    # ------------------------------------------------------------------
    # Scenario 5 -- deleted state survives follower reconnect
    # ------------------------------------------------------------------

    def test_state_preserved_on_reopen(self):
        """
        After closing and reopening the follower connection, the same checkpoint
        must still show deleted keys as WT_NOTFOUND and live keys with their
        original values. Checks boundaries, an interior key, and keys outside
        the truncated range on both sides.
        """
        self.setup_leader()

        truncated_keys     = [self.trunc_start, self.trunc_start + 100, self.trunc_stop]
        non_truncated_keys = [1, self.trunc_start - 1, self.trunc_stop + 1, self.nrows]

        def verify(sess, label):
            for key in truncated_keys:
                ret, _ = self.search_at(sess, key, self.ts_read)
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
                    f'{label}: key {key} must be deleted')
            for key in non_truncated_keys:
                ret, val = self.search_at(sess, key, self.ts_read)
                self.assertEqual(ret, 0, f'{label}: key {key} must be found')
                self.assertEqual(val, self.value)

        conn1, sess1 = self.open_follower()
        verify(sess1, 'first open')
        sess1.close()
        conn1.close()

        conn2, sess2 = self.open_follower()
        verify(sess2, 'after reconnect')
        sess2.close()
        conn2.close()

    # ------------------------------------------------------------------
    # Scenario 6 -- reading before the truncation forces the page to load
    # ------------------------------------------------------------------

    def test_instantiation_not_globally_visible(self):
        """
        Since pages were evicted before truncation, the leader wrote page-level
        deletion markers rather than individual tombstones per key.

        Reading at ts_insert (before the truncation is visible) forces the page
        to be loaded to check the key. The original value must be found, the
        deleted-page counter must increment, and the page must not be dirtied.
        """
        self.setup_leader()
        conn, sess = self.open_follower()

        dirty_before = self.get_stat(conn, stat.conn.cache_pages_dirty)
        rd_before    = self.get_stat(conn, stat.conn.cache_read_deleted)

        # Read before the truncation is visible — the page must load to find the key.
        ret, val = self.search_at(sess, self.trunc_start + 100, self.ts_insert)
        self.assertEqual(ret, 0, 'key must be found before truncation timestamp')
        self.assertEqual(val, self.value)

        self.assertGreater(self.get_stat(conn, stat.conn.cache_read_deleted), rd_before,
            'loading a deleted page must increment cache_read_deleted')
        self.assertEqual(self.get_stat(conn, stat.conn.cache_pages_dirty), dirty_before,
            'loading the page must not dirty it')
        # Evict the loaded page — it should evict without a write.
        self.evict_page(sess, self.trunc_start + 100)

        sess.close()
        conn.close()


if __name__ == '__main__':
    wttest.run()
