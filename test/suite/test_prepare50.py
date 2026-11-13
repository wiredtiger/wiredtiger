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
#
# With preserve_prepared configured, a prepared transaction may commit with
# the same prepare, commit and durable timestamp.

import wiredtiger, wttest

class test_prepare50(wttest.WiredTigerTestCase):

    test_name = __qualname__
    conn_config = 'precise_checkpoint=true,preserve_prepared=true'
    uri = f'table:{test_name}'

    def setUp(self):
        super().setUp()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S')

    def prepare(self, cursor, key, prepare_ts, prepared_id):
        self.session.begin_transaction()
        cursor[key] = 'value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(prepared_id))

    def commit(self, commit_ts, durable_ts):
        self.session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(commit_ts))
        self.session.timestamp_transaction(
            'durable_timestamp=' + self.timestamp_str(durable_ts))
        self.session.commit_transaction()

    def check(self, key, read_ts, visible):
        session = self.conn.open_session()
        cursor = session.open_cursor(self.uri)
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        cursor.set_key(key)
        if visible:
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'value')
        else:
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        session.rollback_transaction()
        session.close()

    def test_durable_ts_equal_to_prepare_ts_succeeds(self):
        cursor = self.session.open_cursor(self.uri)
        self.prepare(cursor, 1, 10, 1)
        self.commit(10, 10)

        self.check(1, 9, False)
        self.check(1, 10, True)

    def test_durable_ts_greater_than_prepare_ts_succeeds(self):
        cursor = self.session.open_cursor(self.uri)
        self.prepare(cursor, 3, 30, 3)
        self.commit(30, 31)

        self.check(3, 29, False)
        self.check(3, 31, True)

    def test_durable_ts_equal_to_prepare_ts_survives_restart(self):
        cursor = self.session.open_cursor(self.uri)
        self.prepare(cursor, 5, 50, 5)
        self.commit(50, 50)
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.session.checkpoint()
        self.reopen_conn()

        self.check(5, 49, False)
        self.check(5, 50, True)

    def test_durable_ts_equal_to_prepare_ts_rejected_once_stable(self):
        # Once stable reaches the prepare timestamp the transaction can be
        # checkpointed as prepared; durable must then be after stable.
        cursor = self.session.open_cursor(self.uri)
        self.prepare(cursor, 7, 70, 7)

        session = self.conn.open_session()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))
        session.checkpoint()
        session.close()

        self.session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(70))
        msg = '/durable timestamp.*must be after the stable timestamp/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.timestamp_transaction(
                'durable_timestamp=' + self.timestamp_str(70)), msg)
        self.session.rollback_transaction()

if __name__ == '__main__':
    wttest.run()
