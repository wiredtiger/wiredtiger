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
# test_cursor_position02.py
#   WT_CURSOR.set_position and get_position on tables with one or more column groups.

import wiredtiger, wttest
from wtscenario import make_scenarios

@wttest.skip_for_hook("disagg", "Disagg rewrites table: creates; this test assumes ordinary tables")
class test_cursor_position02(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor_position02'
    nrows = 5000
    nsamples = 25

    scenarios = make_scenarios([
        ('single', dict(colgroups=False)),
        ('multi', dict(colgroups=True)),
    ])

    def key(self, i):
        return 'key%08d' % i

    def values(self, key):
        return ('first-' + key, 'second-' + key + '-' * 40)

    def create_table(self):
        config = 'key_format=S,value_format=SS,columns=(k,v1,v2),leaf_page_max=4KB'
        if self.colgroups:
            config += ',colgroups=(g1,g2)'
        self.session.create(self.uri, config)
        if self.colgroups:
            self.session.create('colgroup:test_cursor_position02:g1', 'columns=(v1)')
            self.session.create('colgroup:test_cursor_position02:g2', 'columns=(v2)')

    def populate(self):
        self.create_table()
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c.set_key(self.key(i))
            c.set_value(*self.values(self.key(i)))
            c.insert()
        c.close()
        # Reopen so the tree is read back from disk with its on-disk page shape.
        self.reopen_conn()
        return [self.key(i) for i in range(self.nrows)]

    def check_record(self, c):
        key = c.get_key()
        self.assertEqual(tuple(c.get_value()), self.values(key))
        return key

    def test_sampling_is_ordered_and_consistent(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()
        prev = None
        for i in range(self.nsamples + 1):
            p.pos = i / self.nsamples
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(p.pages_skipped, 0)
            key = self.check_record(c)
            self.assertIn(key, keys)
            if prev is not None:
                self.assertGreaterEqual(key, prev)
            prev = key

        # Out-of-range positions land on the first and last records.
        p.pos = -1.0
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(self.check_record(c), keys[0])
        p.pos = 2.0
        p.flags = wiredtiger.WT_POSITION_ANCHOR_LAST | wiredtiger.WT_POSITION_PREV
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(self.check_record(c), keys[-1])
        c.close()

    def test_next_prev_continue(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()
        for pos in (0.0, 0.3, 0.5, 0.8, 1.0):
            p.pos = pos
            self.assertEqual(c.set_position(p), 0)
            idx = keys.index(self.check_record(c))
            if idx + 1 < len(keys):
                self.assertEqual(c.next(), 0)
                self.assertEqual(self.check_record(c), keys[idx + 1])
                self.assertEqual(c.prev(), 0)
                self.assertEqual(self.check_record(c), keys[idx])
            if idx > 0:
                self.assertEqual(c.prev(), 0)
                self.assertEqual(self.check_record(c), keys[idx - 1])
        c.close()

    def test_get_position_ordered(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        msg = '/requires a positioned cursor/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: c.get_position(), msg)

        prev = -1.0
        count = 0
        while c.next() == 0:
            pos = c.get_position()
            self.assertGreaterEqual(pos, 0.0)
            self.assertLessEqual(pos, 1.0)
            self.assertGreaterEqual(pos, prev)
            prev = pos
            count += 1
        self.assertEqual(count, len(keys))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: c.get_position(), msg)
        c.close()

    def test_round_trip(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()
        for key in keys[::97]:
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            p.pos = c.get_position()
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(self.check_record(c), key)
        c.close()

    def test_key_only(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()
        p.flags = wiredtiger.WT_POSITION_ANCHOR_FIRST
        p.pos = 0.5
        self.assertEqual(c.set_position(p), 0)
        first_on_page = self.check_record(c)

        p.flags = wiredtiger.WT_POSITION_KEY_ONLY
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(p.pages_skipped, 0)
        boundary = c.get_key()
        self.assertLessEqual(boundary, first_on_page)

        # The cursor holds a key but no position.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_value(), '/requires value be set/')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_position(), '/requires a positioned cursor/')

        # The key is usable as a search key and lands on the first record of the page.
        exact = c.search_near()
        self.assertGreaterEqual(exact, 0)
        self.assertEqual(self.check_record(c), first_on_page)

        # The leftmost page has the empty key as its boundary, which sorts before every record.
        p.pos = 0.0
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(c.get_key(), '')
        self.assertGreater(c.search_near(), 0)
        self.assertEqual(self.check_record(c), keys[0])
        c.close()

    def test_empty_table(self):
        self.create_table()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()
        p.pos = 0.5
        self.assertEqual(c.set_position(p), wiredtiger.WT_NOTFOUND)
        p.flags = wiredtiger.WT_POSITION_KEY_ONLY
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(c.get_key(), '')
        c.close()
