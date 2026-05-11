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
# search
# [END_TAGS]

# test_touch_cursor05.py
# Touch-cursor edge cases:
#   - empty table         -> search returns WT_NOTFOUND
#   - single-page tree    -> descent stops at the root leaf, cursor_touch_leaf_cached
#                            stat increments
#   - deeply nested tree  -> descent visits multiple internal pages before
#                            stopping at the leaf parent
#   - long keys / non-ASCII keys -> WT_ITEM handling is symmetric with __wt_compare

import wiredtiger, wttest
from wiredtiger import stat


class test_touch_cursor05(wttest.WiredTigerTestCase):

    conn_config = 'statistics=(all),cache_size=64MB'

    def _get_stat(self, statid):
        sc = self.session.open_cursor('statistics:')
        try:
            return sc[statid][2]
        finally:
            sc.close()

    def _create(self, uri, leaf_max='1KB', internal_max='512'):
        self.session.create(
            uri,
            f'key_format=S,value_format=S,'
            f'allocation_size=512,leaf_page_max={leaf_max},'
            f'internal_page_max={internal_max}')

    def test_empty_table(self):
        """A touch.search on an empty table returns WT_NOTFOUND cleanly."""
        uri = 'file:touch_empty.wt'
        self._create(uri)
        c = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            c.set_key('anything')
            self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        finally:
            c.close()

    def test_small_tree_non_disagg(self):
        """On a non-disaggregated tree the warmup hint has no PALI handle to
        forward to. Every touch.search must short-circuit via
        cursor_touch_skipped_non_disagg, not via the warmup or leaf-cached
        paths. cursor_touch_search itself still counts every call."""
        uri = 'file:touch_small.wt'
        self._create(uri)
        c = self.session.open_cursor(uri)
        for i in range(5):
            c[f'k{i:02d}'] = f'v{i:02d}'
        c.close()

        before_search = self._get_stat(stat.conn.cursor_touch_search)
        before_warmup = self._get_stat(stat.conn.cursor_touch_warmup)
        before_skipped = self._get_stat(stat.conn.cursor_touch_skipped_non_disagg)
        before_cached = self._get_stat(stat.conn.cursor_touch_leaf_cached)

        t = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for k in ('k00', 'k02', 'k99'):
                t.set_key(k)
                self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
        finally:
            t.close()

        self.assertEqual(
            self._get_stat(stat.conn.cursor_touch_search) - before_search, 3)
        # Non-disagg path: no warmup, no leaf-cached, all 3 are skipped.
        self.assertEqual(
            self._get_stat(stat.conn.cursor_touch_warmup) - before_warmup, 0)
        self.assertEqual(
            self._get_stat(stat.conn.cursor_touch_leaf_cached) - before_cached, 0)
        self.assertEqual(
            self._get_stat(stat.conn.cursor_touch_skipped_non_disagg) -
              before_skipped, 3)

    def test_deep_tree(self):
        """Force a multi-level tree with small pages + many keys. Touch
        search must traverse all the internal levels safely and return
        WT_NOTFOUND."""
        uri = 'file:touch_deep.wt'
        self._create(uri, leaf_max='512', internal_max='512')
        c = self.session.open_cursor(uri)
        # 10k records at small page sizes guarantees several internal levels.
        for i in range(10_000):
            c[f'key_{i:08d}_padding_to_grow_internal_keys'] = (
                f'v{i:08d}_' + 'x' * 16)
        c.close()
        self.session.checkpoint()

        t = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            # Probe a range of positions: start, middle, end, missing.
            for i in (0, 1, 100, 5000, 9999, 50000):
                t.set_key(f'key_{i:08d}_padding_to_grow_internal_keys')
                self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
        finally:
            t.close()

    def test_long_keys(self):
        """Touch search handles keys up to several KB without truncation."""
        uri = 'file:touch_long.wt'
        self._create(uri, leaf_max='4KB', internal_max='2KB')
        c = self.session.open_cursor(uri)
        for size in (1, 64, 256, 1024, 3 * 1024):
            c['k_' + 'x' * size] = f'v_size_{size}'
        c.close()

        t = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for size in (1, 256, 3 * 1024, 9999):
                t.set_key('k_' + 'x' * size)
                self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
        finally:
            t.close()

    def test_non_ascii_keys(self):
        """WT_ITEM handling is byte-oriented; non-ASCII keys work the same."""
        uri = 'file:touch_nonascii.wt'
        self._create(uri)
        c = self.session.open_cursor(uri)
        c['\xe4\xb8\x96\xe7\x95\x8c'] = 'world-in-utf8'    # utf-8 bytes for "world" in CJK
        c['key\x00with\x00nul'] = 'nul-in-key'
        c.close()

        t = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
        try:
            for k in ('\xe4\xb8\x96\xe7\x95\x8c', 'key\x00with\x00nul',
                      'never-existed'):
                t.set_key(k)
                self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
        finally:
            t.close()

    def test_search_does_not_corrupt_subsequent_normal_cursor(self):
        """A touch search must not leave the dhandle in a bad state for a
        subsequent regular cursor open."""
        uri = 'file:touch_no_corrupt.wt'
        self._create(uri)
        c = self.session.open_cursor(uri)
        for i in range(200):
            c[f'k{i:05d}'] = f'v{i:05d}'
        c.close()

        # Interleave touch searches and normal reads.
        for i in range(20):
            t = self.session.open_cursor(uri, None, 'touch=(enabled=true)')
            t.set_key(f'k{i:05d}')
            self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
            t.close()

            c = self.session.open_cursor(uri)
            c.set_key(f'k{i:05d}')
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), f'v{i:05d}')
            c.close()
