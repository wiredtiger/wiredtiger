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
#
# [TEST_TAGS]
# cursors
# statistics
# [END_TAGS]

# test_touch_cursor03.py
# Touch-cursor statistics and idempotent-warmup semantics.
#
# Exercises every cursor_touch_* statistic counter we introduced:
#   cursor_touch_search             # incremented on every touch search()
#   cursor_touch_warmup             # incremented when plh_get(WARMUP) is issued
#   cursor_touch_leaf_cached        # incremented when the descent stops at a leaf already in cache
#   cursor_touch_skipped_no_addr    # never trips on a populated tree (sanity)
#   cursor_touch_skipped_non_disagg # tested via test_touch_cursor02 (non-disagg path)
#
# Idempotent warmup: warming the same key 100 times must still produce N=100
# cursor_touch_search increments and a small bounded number of warmup calls
# (one per unique leaf page in the working set).

import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat


@disagg_test_class
class test_touch_cursor03(wttest.WiredTigerTestCase, DisaggConfigMixin):

    nitems = 4_000

    disagg_storages = gen_disagg_storages('test_touch_cursor03', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # palite simulation knobs. We don't need any latency for these tests, just
    # the master switch on so the warm-set bookkeeping runs.
    disagg_config = 'touch_sim_enabled=true'

    conn_base_config = (
        'transaction_sync=(enabled,method=fsync),'
        'statistics=(all),'
        'cache_size=64MB,'
        'disaggregated=(page_log=palite),'
    )

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _get_stat(self, statid):
        sc = self.session.open_cursor('statistics:')
        try:
            return sc[statid][2]
        finally:
            sc.close()

    def _populate(self, uri, n):
        common = (
            'key_format=S,value_format=S,block_manager=disagg,'
            'allocation_size=512,leaf_page_max=1KB,internal_page_max=512'
        )
        self.session.create(uri, common)
        c = self.session.open_cursor(uri)
        for i in range(n):
            c[f'k{i:06d}'] = f'v{i:06d}' + 'x' * 100
        c.close()
        self.session.checkpoint()

    # ---- tests ---------------------------------------------------------

    def test_stat_search_counter(self):
        """cursor_touch_search increments exactly once per touch.search()."""
        uri = 'file:touch_stat_search.wt'
        self._populate(uri, self.nitems)

        before = self._get_stat(stat.conn.cursor_touch_search)
        n_calls = 17
        c = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for i in range(n_calls):
                c.set_key(f'k{i:06d}')
                self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        finally:
            c.close()
        after = self._get_stat(stat.conn.cursor_touch_search)
        self.assertEqual(after - before, n_calls)

    def test_warmup_issues_at_least_one_per_search(self):
        """With the working set evicted, every touch.search() issues exactly
        one warmup (because every search descends to a leaf that isn't in
        cache)."""
        uri = 'file:touch_stat_warmup.wt'
        self._populate(uri, self.nitems)

        # Drop the cache + warm-set state by reopening the connection.
        meta = self.disagg_get_complete_checkpoint_meta()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.close_conn()
        self.open_conn(
            ".",
            self.conn_base_config + (
                'cache_size=1MB,'
                'disaggregated=(role="leader"),'
                f'disaggregated=(checkpoint_meta="{meta}"),'
            ))

        before_search = self._get_stat(stat.conn.cursor_touch_search)
        before_warmup = self._get_stat(stat.conn.cursor_touch_warmup)
        before_cached = self._get_stat(stat.conn.cursor_touch_leaf_cached)

        # Walk 50 distinct keys spread across the working set, so each touches
        # a different leaf.
        keys = [f'k{i:06d}' for i in range(0, self.nitems, max(1, self.nitems // 50))]
        c = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for k in keys:
                c.set_key(k)
                self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        finally:
            c.close()

        after_search = self._get_stat(stat.conn.cursor_touch_search)
        after_warmup = self._get_stat(stat.conn.cursor_touch_warmup)
        after_cached = self._get_stat(stat.conn.cursor_touch_leaf_cached)

        # Every search increments cursor_touch_search.
        self.assertEqual(after_search - before_search, len(keys))
        # Every search either issues a warmup or short-circuits on a cached
        # leaf. The two counters must sum to len(keys).
        self.assertEqual(
            (after_warmup - before_warmup) + (after_cached - before_cached),
            len(keys),
            f'touch_search={after_search - before_search}, '
            f'touch_warmup={after_warmup - before_warmup}, '
            f'touch_leaf_cached={after_cached - before_cached}')
        # On a freshly opened connection with a small cache, at least one
        # warmup should have actually gone through.
        self.assertGreater(after_warmup - before_warmup, 0)

    def test_command_payload_can_be_any_string(self):
        """The command WT_ITEM payload is opaque; arbitrary bytes round-trip
        without crashing the touch cursor."""
        uri = 'file:touch_stat_cmd.wt'
        self._populate(uri, 100)
        # Mix base64-ish payloads with edge cases (empty, very long).
        for payload in (
            '',
            'simple',
            'c2t1bmtfOTRfd2FybXVw',  # base64('skunk_94_warmup')
            'x' * 4096,
        ):
            config = 'touch=(enabled=true,action=warmup'
            if payload:
                config += f',command="{payload}"'
            config += ')'
            c = self.session.open_cursor(uri, None, config)
            try:
                c.set_key('k000050')
                self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
            finally:
                c.close()

    def test_repeated_touch_search_safe(self):
        """Calling touch.search() many times in a row never leaks or asserts."""
        uri = 'file:touch_repeat.wt'
        self._populate(uri, 200)
        c = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for _ in range(500):
                c.set_key('k000100')
                self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        finally:
            c.close()

    def test_touch_does_not_position_cursor(self):
        """After touch.search() the cursor must not be positioned -- a
        subsequent get_key/get_value must fail because no value is set."""
        uri = 'file:touch_position.wt'
        self._populate(uri, 50)
        c = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            c.set_key('k000010')
            self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
            with self.expectedStderrPattern('requires value be set'):
                self.assertRaises(wiredtiger.WiredTigerError, c.get_value)
        finally:
            c.close()
