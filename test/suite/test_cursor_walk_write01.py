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

import wiredtiger, wttest
from wtscenario import make_scenarios

# test_cursor_walk_write01.py
# Writes through a positioned cursor must not disturb a cursor walk.
class test_cursor_walk_write01(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor_walk_write01'
    nkeys = 200
    scenarios = make_scenarios([('next', dict(forward=True)), ('prev', dict(forward=False))])

    def populate(self, value_format='S'):
        self.session.create(self.uri, 'key_format=S,value_format=' + value_format)
        value = 'v' * 10 if value_format == 'S' else b'a' * 20
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            cursor.set_key('%05d' % i)
            cursor.set_value(value)
            cursor.insert()
        cursor.close()

    def walk(self, write_op):
        cursor = self.session.open_cursor(self.uri)
        step = cursor.next if self.forward else cursor.prev
        seen = []
        while step() == 0:
            seen.append(cursor.get_key())
            write_op(cursor)
        cursor.close()
        self.assertEqual(seen, sorted(seen, reverse=not self.forward))
        self.assertEqual(len(seen), self.nkeys)

    def test_update_in_walk(self):
        self.populate()
        def update(cursor):
            cursor.set_value('u' * 10)
            self.assertEqual(cursor.update(), 0)
        self.walk(update)

    def test_update_in_walk_txn(self):
        self.populate()
        self.session.begin_transaction()
        def update(cursor):
            cursor.set_value('u' * 10)
            self.assertEqual(cursor.update(), 0)
        self.walk(update)
        self.session.commit_transaction()

    def test_remove_in_walk(self):
        self.populate()
        self.walk(lambda cursor: self.assertEqual(cursor.remove(), 0))

    def test_modify_in_walk(self):
        self.populate('u')
        self.session.begin_transaction()
        self.walk(lambda cursor: self.assertEqual(
            cursor.modify([wiredtiger.Modify('b', 2, 1)]), 0))
        self.session.commit_transaction()

    def test_reserve_in_walk(self):
        self.populate()
        self.session.begin_transaction()
        self.walk(lambda cursor: self.assertEqual(cursor.reserve(), 0))
        self.session.rollback_transaction()

    def test_search_update_cycle(self):
        self.populate()
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            cursor.set_key('%05d' % i)
            self.assertEqual(cursor.search(), 0)
            cursor.set_value('s' * 10)
            self.assertEqual(cursor.update(), 0)
            if i + 1 < self.nkeys:
                self.assertEqual(cursor.next(), 0)
                self.assertEqual(cursor.get_key(), '%05d' % (i + 1))
        cursor.close()

    # A positioned remove that loses its position to a rollback must return the rollback to the
    # application instead of retrying without a key.
    def test_positioned_remove_rollback(self):
        self.populate()
        self.conn.reconfigure('debug_mode=(rollback_error=7)')
        cursor = self.session.open_cursor(self.uri)
        step = cursor.next if self.forward else cursor.prev
        rolled_back = False
        while step() == 0:
            try:
                cursor.remove()
            except wiredtiger.WiredTigerError as e:
                self.assertIn(wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK), str(e))
                rolled_back = True
                break
        self.conn.reconfigure('debug_mode=(rollback_error=0)')
        self.assertTrue(rolled_back)
