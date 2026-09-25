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

# test_layered_arm02.py
#    A straddler (began before the arm, writes stable alone) and an armed transaction (mirrors to
#    ingest) writing the same key must conflict: two removes of one key, or two inserts of one key,
#    never both succeed, whichever writes first and whether or not the first has committed.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm02(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_config = 'precise_checkpoint=true,disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    ops = [
        ('remove', dict(op='remove')),
        ('insert', dict(op='insert')),
    ]
    orders = [
        # The straddler writes first and is still uncommitted when the armed transaction writes.
        ('straddler_uncommitted', dict(first='straddler', commit_first=False)),
        # The straddler writes first and commits after the armed transaction's snapshot.
        ('straddler_committed', dict(first='straddler', commit_first=True)),
        # The armed transaction writes first, uncommitted.
        ('armed_uncommitted', dict(first='armed', commit_first=False)),
    ]
    scenarios = make_scenarios(disagg_storages, ops, orders)

    uri = 'layered:test_layered_arm02'
    key = 'k'

    # Apply the scenario's operation; return True on success, False on a write conflict.
    def write(self, cursor, value):
        cursor.set_key(self.key)
        try:
            if self.op == 'remove':
                self.assertEqual(cursor.remove(), 0)
            else:
                cursor.set_value(value)
                self.assertEqual(cursor.insert(), 0)
            return True
        except wiredtiger.WiredTigerError as e:
            if not self.is_rollback(e):
                raise
            return False

    def test_conflict(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        if self.op == 'remove':
            self.write_at(self.uri, {self.key: 'base'}, 10)

        straddler = self.conn.open_session()
        s_cursor = straddler.open_cursor(self.uri, None, 'overwrite=false')
        straddler.begin_transaction()

        if self.first == 'straddler':
            self.assertTrue(self.write(s_cursor, 'straddler'))
        self.arm()

        armed = self.conn.open_session()
        a_cursor = armed.open_cursor(self.uri, None, 'overwrite=false')
        armed.begin_transaction()

        if self.first == 'straddler':
            if self.commit_first:
                straddler.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
            armed_ok = self.write(a_cursor, 'armed')
            self.assertFalse(armed_ok, 'the armed write did not conflict with the straddler')
            armed.rollback_transaction()
            if not self.commit_first:
                straddler.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
            winner = 'straddler'
        else:
            self.assertTrue(self.write(a_cursor, 'armed'))
            straddler_ok = self.write(s_cursor, 'straddler')
            self.assertFalse(straddler_ok, 'the straddler write did not conflict with the armed write')
            straddler.rollback_transaction()
            armed.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
            winner = 'armed'

        s_cursor.close()
        a_cursor.close()
        straddler.close()
        armed.close()

        expected = {} if self.op == 'remove' else {self.key: winner}
        self.assertEqual(self.read_kvs(self.uri), expected)
        # An armed write lands in both constituents, a straddler's in stable alone.
        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 30), expected)
        if winner == 'armed':
            ingest = self.read_kvs_at(self.ingest_uri(self.uri), 30)
            self.assertEqual(set(ingest), {self.key})

        self.complete_step_down(20)
        self.assertEqual(self.read_kvs(self.uri), expected)

if __name__ == '__main__':
    wttest.run()
