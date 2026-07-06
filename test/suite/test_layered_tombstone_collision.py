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

# test_layered_tombstone_collision.py
#   A user value byte-identical to the internal tombstone marker (0x14 0x14) must behave like any
#   other value across every cursor operation on a follower, never read back as a deletion. Each
#   value below is exercised by every test case.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_tombstone_collision(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    uri = f'layered:{test_name}'

    values = [
        ('collide', dict(value=b'\x14\x14')),      # exactly the tombstone
        ('control', dict(value=b'\x14\x14\xff')),  # tombstone prefix + a byte
        ('other', dict(value=b'\x14\x14\x14')),    # tombstone prefix + a tombstone byte
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, values)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        # Reading an escaped value off the stable table logs a warning; that is expected here.
        self.ignoreStdoutPattern('stable table value in the tombstone namespace')
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.follow_conn = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.follow = self.follow_conn.open_session('')
        self.follow.create(self.uri, 'key_format=S,value_format=u')

    def put(self, key, value):
        c = self.follow.open_cursor(self.uri)
        c[key] = value
        c.close()

    def cursor(self):
        return self.follow.open_cursor(self.uri)

    def check(self, key, expected):
        c = self.cursor()
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        self.assertEqual(c.get_value(), expected)
        c.close()

    def test_search(self):
        self.put('k', self.value)
        self.check('k', self.value)

    def test_next(self):
        self.put('a', self.value)
        self.put('b', self.value)
        c = self.cursor()
        self.assertEqual(c.next(), 0)
        self.assertEqual((c.get_key(), c.get_value()), ('a', self.value))
        self.assertEqual(c.next(), 0)
        self.assertEqual((c.get_key(), c.get_value()), ('b', self.value))

    def test_prev(self):
        self.put('a', self.value)
        self.put('b', self.value)
        c = self.cursor()
        self.assertEqual(c.prev(), 0)
        self.assertEqual((c.get_key(), c.get_value()), ('b', self.value))
        self.assertEqual(c.prev(), 0)
        self.assertEqual((c.get_key(), c.get_value()), ('a', self.value))

    def test_search_near(self):
        self.put('k', self.value)
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.search_near(), 0)
        self.assertEqual(c.get_value(), self.value)

    def test_search_near_nonexact(self):
        self.put('a', self.value)
        self.put('c', self.value)
        c = self.cursor()
        c.set_key('b')
        self.assertNotEqual(c.search_near(), 0)
        self.assertEqual(c.get_value(), self.value)

    def test_update(self):
        self.put('k', b'plain')
        self.put('k', self.value)
        self.check('k', self.value)

    def test_modify_from_value(self):
        # The value is the modify base; append a byte.
        self.put('k', self.value)
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.search(), 0)
        c.modify([wiredtiger.Modify(b'\xaa', len(self.value), 0)])
        c.close()
        self.check('k', self.value + b'\xaa')

    def test_modify_into_value(self):
        # Build the value with a modify by dropping a trailing byte.
        self.put('k', self.value + b'\xaa')
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.search(), 0)
        c.modify([wiredtiger.Modify(b'', len(self.value), 1)])
        c.close()
        self.check('k', self.value)

    def test_modify_out_of_namespace(self):
        # The value is the modify base; replace it with a value outside the namespace.
        self.put('k', self.value)
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.search(), 0)
        c.modify([wiredtiger.Modify(b'ab', 0, len(self.value))])
        c.close()
        self.check('k', b'ab')

    def test_remove(self):
        self.put('k', self.value)
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.remove(), 0)
        c.close()
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)

    def test_reinsert_after_remove(self):
        # A delete marker followed by an escaped value on the same update chain.
        self.put('k', self.value)
        c = self.cursor()
        c.set_key('k')
        self.assertEqual(c.remove(), 0)
        c.close()
        self.put('k', self.value)
        self.check('k', self.value)

    def test_leader_write_follower_read(self):
        # Written on the leader, read on the follower after it picks up the checkpoint.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['k'] = self.value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.follow_conn, self.conn)
        self.check('k', self.value)
