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
#   Follower writes land in the ingest table, where the two-byte tombstone (0x14 0x14) is also the
#   delete marker. A user value byte-identical to the tombstone must be escaped on the way in, or it
#   is read back as WT_NOTFOUND. This test writes such a value on a follower and confirms it round
#   trips, across the insert, update, and modify paths.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_tombstone_collision(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    uri = f'layered:{test_name}'

    # The tombstone itself, a longer value sharing its prefix, and an unrelated value.
    collide = b'\x14\x14'
    control = b'\x14\x14\xff'
    normal = b'hello'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def open_follower(self):
        # The framework tracks and closes connections opened through wiredtiger_open.
        conn = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        return conn.open_session('')

    def check(self, session, key, expected):
        c = session.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.search(), 0,
            f'{key}: value in the tombstone namespace read back as a delete')
        self.assertEqual(c.get_value(), expected)
        c.close()

    def test_tombstone_collision(self):
        self.session.create(self.uri, 'key_format=S,value_format=u')
        session = self.open_follower()
        session.create(self.uri, 'key_format=S,value_format=u')

        # Insert: the exact-tombstone value must survive alongside its longer sibling.
        c = session.open_cursor(self.uri)
        c['collide'] = self.collide
        c['control'] = self.control
        c['normal'] = self.normal
        c.close()

        self.check(session, 'collide', self.collide)
        self.check(session, 'control', self.control)
        self.check(session, 'normal', self.normal)

        # Update an existing key to the exact-tombstone value.
        c = session.open_cursor(self.uri)
        c['normal'] = self.collide
        c.close()
        self.check(session, 'normal', self.collide)

        # Modify a value into the exact-tombstone bytes.
        c = session.open_cursor(self.uri)
        c['control'] = b'\x14\x14\x14'
        c.close()
        c = session.open_cursor(self.uri)
        c.set_key('control')
        self.assertEqual(c.search(), 0)
        # Drop the trailing byte, leaving the two tombstone bytes.
        c.modify([wiredtiger.Modify(b'', 2, 1)])
        c.close()
        self.check(session, 'control', self.collide)
