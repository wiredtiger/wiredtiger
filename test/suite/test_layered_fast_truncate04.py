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

# test_layered_fast_truncate04.py
#   WT-17237: follower-initiated truncate resolves NULL start/stop cursors to
#   concrete keys before storing the entry in the truncate list.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_fast_truncate04(wttest.WiredTigerTestCase):

    conn_config = 'disaggregated=(role="leader"),'

    uris = [
        ('layered', dict(uri='layered:test_layered_fast_truncate04')),
    ]

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate04', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, uris)

    nitems = 100

    def setUp(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest("fast truncate support is not enabled")
        super().setUp()

    def key(self, n):
        return f'{n:04d}'

    def setup_follower(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, self.nitems + 1):
            self.session.begin_transaction()
            cursor[self.key(i)] = 'v'
            self.session.commit_transaction()
        cursor.close()
        self.session.checkpoint()
        follower_config = ('disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}")')
        self.reopen_conn(config=follower_config)

    def truncate(self, start=None, stop=None):
        c_start = c_stop = None
        uri = None
        if start is not None:
            c_start = self.session.open_cursor(self.uri)
            c_start.set_key(self.key(start))
        if stop is not None:
            c_stop = self.session.open_cursor(self.uri)
            c_stop.set_key(self.key(stop))
        if c_start is None and c_stop is None:
            # truncate API requires a URI when both cursors are NULL.
            uri = self.uri
        self.session.begin_transaction()
        self.session.truncate(uri, c_start, c_stop, None)
        self.session.commit_transaction()
        if c_start is not None:
            c_start.close()
        if c_stop is not None:
            c_stop.close()

    def put(self, key, value='v'):
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[self.key(key)] = value
        self.session.commit_transaction()
        c.close()

    def visible_keys(self):
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        keys = []
        while c.next() == 0:
            keys.append(c.get_key())
        self.session.rollback_transaction()
        c.close()
        return keys

    def test_null_start_resolves_to_first_key(self):
        # session.truncate(NULL, stop) on the follower resolves start to the
        # table's first key. A later insert outside the bounded range stays visible.
        self.setup_follower()
        self.truncate(start=None, stop=60)
        self.assertEqual(self.visible_keys(), [self.key(i) for i in range(61, self.nitems + 1)])

        self.put(200, 'appended')
        self.assertIn(self.key(200), self.visible_keys())

    def test_null_stop_resolves_to_last_key(self):
        # session.truncate(start, NULL) on the follower resolves stop to the
        # table's last key. Keys appended after the truncate remain visible.
        self.setup_follower()
        self.truncate(start=30, stop=None)
        self.assertEqual(self.visible_keys(), [self.key(i) for i in range(1, 30)])

        self.put(200, 'appended')
        self.assertIn(self.key(200), self.visible_keys())

    def test_both_null_is_full_table(self):
        # session.truncate(NULL, NULL) resolves to [first_key, last_key] — full
        # table becomes invisible; a subsequent insert outside the range stays visible.
        self.setup_follower()
        self.truncate(start=None, stop=None)
        self.assertEqual(self.visible_keys(), [])

        self.put(200, 'appended')
        self.assertEqual(self.visible_keys(), [self.key(200)])
