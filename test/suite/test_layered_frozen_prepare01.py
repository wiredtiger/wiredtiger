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

# A prepared transaction survives demote and is resolved on the frozen tree.
@disagg_test_class
class test_layered_frozen_prepare01(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    outcomes = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    scenarios = make_scenarios(disagg_storages, outcomes)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def read_at(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        found = {}
        while cursor.next() == 0:
            found[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return found

    def test_resolve_prepared_after_demote(self):
        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        cursor['k'] = 'base'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(10) + ',durable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        prep = self.conn.open_session()
        prep_cursor = prep.open_cursor(uri, None, None)
        prep.begin_transaction()
        prep_cursor['k'] = 'prepared'
        prep.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(20) + ',prepared_id=' + self.prepared_id_str(1))
        prep_cursor.close()

        self.conn.reconfigure('disaggregated=(role="follower")')

        if self.commit:
            prep.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30) + ',durable_timestamp=' +
                self.timestamp_str(30))
            expect = 'prepared'
        else:
            prep.rollback_transaction('rollback_timestamp=' + self.timestamp_str(30))
            expect = 'base'
        prep.close()

        self.assertEqual(self.read_at(uri, 30)['k'], expect)

        self.conn.reconfigure('disaggregated=(role="leader")')
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30) +
            ',durable_timestamp=' + self.timestamp_str(30))
        self.session.checkpoint()

        stable = self.session.open_cursor(
            'file:' + self.test_name + '.wt_stable', None, 'checkpoint=WiredTigerCheckpoint')
        found = {}
        while stable.next() == 0:
            found[stable.get_key()] = stable.get_value()
        stable.close()
        self.assertEqual(found, {'k': expect})
