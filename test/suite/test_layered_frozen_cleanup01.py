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

# A leader that never checkpointed keeps its writes readable after it demotes.
@disagg_test_class
class test_layered_frozen_cleanup01(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = 'disaggregated=(role="leader")'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_never_checkpointed_leader_demotes(self):
        uri = 'layered:' + self.test_name
        expect = {'a': '1', 'b': '2', 'c': '3'}
        self.session.create(uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for k, v in expect.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        self.conn.reconfigure('disaggregated=(role="follower")')

        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        found = {}
        while cursor.next() == 0:
            found[cursor.get_key()] = cursor.get_value()
        self.assertEqual(found, expect)
        cursor.reset()
        cursor.set_key('b')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), '2')
        self.session.rollback_transaction()
        cursor.close()

if __name__ == '__main__':
    wttest.run()
