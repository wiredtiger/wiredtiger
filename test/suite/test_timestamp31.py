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
# test_timestamp31.py
#   Timestamps: a read timestamp set after the transaction begins must see
#   updates committed in between at or before that timestamp.

import wiredtiger, wttest
from wtscenario import make_scenarios

class test_timestamp31(wttest.WiredTigerTestCase):
    uri = 'table:test_timestamp31'

    set_read_values = [
        ('config', dict(use_uint=False)),
        ('uint', dict(use_uint=True)),
    ]
    scenarios = make_scenarios(set_read_values)

    def set_read_timestamp(self, session, ts):
        if self.use_uint:
            session.timestamp_transaction_uint(wiredtiger.WT_TS_TXN_TYPE_READ, ts)
        else:
            session.timestamp_transaction('read_timestamp=' + self.timestamp_str(ts))

    def setup_reader(self, begin_config=None):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))

        # The reader begins before the writer commits.
        self.session.begin_transaction(begin_config)

        writer = self.conn.open_session()
        cursor = writer.open_cursor(self.uri)
        writer.begin_transaction()
        cursor[1] = 'committed at 10'
        writer.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()
        writer.close()

    def check_reader(self, expected):
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(1)
        if expected is None:
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), expected)
        cursor.close()
        self.session.rollback_transaction()

    def test_read_after_commit(self):
        self.setup_reader()
        self.set_read_timestamp(self.session, 15)
        self.check_reader('committed at 10')

    def test_read_before_commit(self):
        self.setup_reader()
        self.set_read_timestamp(self.session, 5)
        self.check_reader(None)

    def test_read_rounded_to_oldest(self):
        self.setup_reader('roundup_timestamps=(read=true)')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(12) +
            ',stable_timestamp=' + self.timestamp_str(12))
        self.set_read_timestamp(self.session, 5)
        self.check_reader('committed at 10')

    def test_read_after_rejected_timestamp(self):
        self.setup_reader()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(12) +
            ',stable_timestamp=' + self.timestamp_str(12))
        if wiredtiger.standalone_build():
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.set_read_timestamp(self.session, 5))
        else:
            # This is a MongoDB message, not written in standalone builds.
            with self.expectedStdoutPattern('less than the oldest timestamp'):
                self.assertRaisesException(wiredtiger.WiredTigerError,
                    lambda: self.set_read_timestamp(self.session, 5))
        self.set_read_timestamp(self.session, 15)
        self.check_reader('committed at 10')
