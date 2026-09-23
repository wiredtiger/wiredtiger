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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Demote without a step-down timestamp keeps commits from after the last checkpoint readable.
@disagg_test_class
class test_layered_frozen_overlay01(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def write(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def read(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        found = {}
        while cursor.next() == 0:
            found[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return found

    def test_post_checkpoint_visible_after_demote(self):
        uri = 'layered:' + self.test_name
        expect = {'a': '1', 'b': '2', 'c': '3', 'd': '4'}
        self.session.create(uri, 'key_format=S,value_format=S')

        self.write(uri, {'a': '1', 'b': '2'}, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()
        self.write(uri, {'c': '3', 'd': '4'}, 20)

        # No step-down timestamp: demote is only the role change.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read(uri, 20), expect)

        self.conn.reconfigure('disaggregated=(role="leader")')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()
        self.assertEqual(self.read(uri, 20), expect)

        stable = self.session.open_cursor(
            'file:' + self.test_name + '.wt_stable', None, 'checkpoint=WiredTigerCheckpoint')
        found = {}
        while stable.next() == 0:
            found[stable.get_key()] = stable.get_value()
        stable.close()
        self.assertEqual(found, expect)
