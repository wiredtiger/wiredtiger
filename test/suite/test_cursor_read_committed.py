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

# test_cursor_read_committed.py
#
# RC snapshot contract: search, search_near, next_random must refresh the
# snapshot on each call; next, prev must hold it across the scan.

import wiredtiger, wttest

class test_cursor_read_committed(wttest.WiredTigerTestCase):

    uri = 'table:test_cursor_read_committed'

    def conn_config(self):
        return 'statistics=(all)'

    def setUp(self):
        super().setUp()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.writer = self.conn.open_session('')
        self.reader = self.conn.open_session('')

    def tearDown(self):
        self.writer.close()
        self.reader.close()
        super().tearDown()

    def _write(self, key, value):
        wc = self.writer.open_cursor(self.uri)
        wc[key] = value
        wc.close()

    def _search_refreshes_rc_snapshot(self, op):
        self._write('ka', 'va')
        self.reader.begin_transaction('isolation=read-committed')
        rc = self.reader.open_cursor(self.uri)
        rc.set_key('ka')
        self.assertEqual(op(rc), 0)
        self._write('kb', 'vb')
        rc.set_key('kb')
        self.assertEqual(op(rc), 0, "S1 not released")
        rc.close()
        self.reader.commit_transaction()

    def test_search_refreshes_rc_snapshot(self):
        self._search_refreshes_rc_snapshot(lambda rc: rc.search())

    def test_search_near_refreshes_rc_snapshot(self):
        self._search_refreshes_rc_snapshot(lambda rc: rc.search_near())

    def test_largest_key_refreshes_rc_snapshot(self):
        # largest_key reads ignoring the snapshot, so its own key cannot witness
        # the refresh. Instead take a snapshot with search(), commit underneath,
        # then assert largest_key released it: a following next() is a held read
        # that reuses the session snapshot, so it must see the post-search value.
        self._write('ka', 'va')
        self._write('kc', 'vc')
        self.reader.begin_transaction('isolation=read-committed')
        rc = self.reader.open_cursor(self.uri)
        rc.set_key('ka')
        self.assertEqual(rc.search(), 0)
        self._write('ka', 'va2')
        self.assertEqual(rc.largest_key(), 0)
        self.assertEqual(rc.get_key(), 'kc')
        self.assertEqual(rc.next(), 0)
        self.assertEqual(rc.get_key(), 'ka')
        self.assertEqual(rc.get_value(), 'va2',
            "largest_key did not release the snapshot: next() still sees 'va'")
        rc.close()
        self.reader.commit_transaction()

    def _scan_holds_snapshot(self, forward):
        self._write('key_a', 'va')
        self._write('key_c', 'vc')
        self.reader.begin_transaction('isolation=read-committed')
        rc = self.reader.open_cursor(self.uri)
        step = rc.next if forward else rc.prev
        first, second = ('key_a', 'key_c') if forward else ('key_c', 'key_a')
        self.assertEqual(step(), 0)
        self.assertEqual(rc.get_key(), first)
        self._write('key_b', 'vb')
        self.assertEqual(step(), 0)
        self.assertEqual(rc.get_key(), second, "snapshot released mid-scan: 'key_b' became visible")
        self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        rc.close()
        self.reader.commit_transaction()

    def test_next_holds_rc_snapshot(self): self._scan_holds_snapshot(True)
    def test_prev_holds_rc_snapshot(self): self._scan_holds_snapshot(False)

    def test_next_random_refreshes_rc_snapshot(self):
        # Single key; snapshot refresh detected by value.
        self._write('k1', 'v1')
        self.reader.begin_transaction('isolation=read-committed')
        rc = self.reader.open_cursor(self.uri, None, 'next_random=true')
        self.assertEqual(rc.next(), 0)
        self.assertEqual(rc.get_value(), 'v1')
        self._write('k1', 'v2')
        self.assertEqual(rc.next(), 0)
        self.assertEqual(rc.get_value(), 'v2', "S1 not released; still seeing v1")
        rc.close()
        self.reader.commit_transaction()
